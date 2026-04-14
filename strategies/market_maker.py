"""
Simplified Market Maker for Polymarket BTC 5-minute markets.

Sell-only strategy:
  1. Split USDC -> YES + NO tokens at window start
  2. Place SELL ladder on both tokens every requote_interval seconds
  3. Cancel all at expiry_stop_seconds before window end
  4. On window change: merge leftover YES+NO -> USDC, redeem winner
"""

import time
import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger("mm")

try:
    from rich.console import Console
    _console = Console()
    def _log(msg, style="dim"):
        try:
            _console.print(f"[{style}]{msg}[/{style}]")
        except UnicodeEncodeError:
            try:
                ascii_msg = str(msg).encode("ascii", errors="replace").decode("ascii")
                _console.print(f"[{style}]{ascii_msg}[/{style}]")
            except Exception:
                pass
        except Exception:
            pass
except ImportError:
    def _log(msg, style=""):
        try:
            print(msg)
        except UnicodeEncodeError:
            print(str(msg).encode("ascii", errors="replace").decode("ascii"))


# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

class MMStrategy(str, Enum):
    BOOK = "book"
    FAIRVALUE = "fairvalue"


@dataclass
class MMConfig:
    enabled: bool = False
    strategy: str = "book"

    # Ladder
    ladder_levels: int = 3
    level_size_shares: float = 10.0

    # Split-based MM
    split_enabled: bool = False
    split_amount_usdc: float = 100.0
    trade_both_sides: bool = True
    sell_only_mode: bool = True

    # Time-based requote
    quote_ttl_seconds: int = 10
    requote_interval: float = 5.0

    # Expiry stop
    expiry_stop_seconds: int = 20

    # Legacy fields kept for backward-compat with price_lag_test.py
    # (runtime config sync from DB writes to these; logic ignores them)
    ladder_step_cents: float = 0.5
    offset_from_best_cents: float = 1.0
    half_spread_cents: float = 1.5
    sigma_per_minute: float = 15.0
    inventory_skew_enabled: bool = False
    gamma_risk: float = 0.0
    inventory_warn_threshold: int = 200
    inventory_critical_threshold: int = 400
    stale_quote_enabled: bool = False
    stale_threshold_cents: float = 0.5
    stale_max_age_seconds: float = 10.0
    expiry_protection_enabled: bool = True
    thin_book_enabled: bool = False
    thin_book_min_depth: float = 50.0
    thin_book_pause_seconds: int = 15
    thin_book_spread_threshold_cents: float = 3.0
    price_shift_enabled: bool = False
    price_shift_threshold_cents: float = 2.0
    volume_spike_enabled: bool = False
    volume_spike_multiplier: float = 3.0
    volume_spike_pause_seconds: int = 20
    strategy_id: str = ""


# ═══════════════════════════════════════════════════════════
# MM STATS
# ═══════════════════════════════════════════════════════════

@dataclass
class MMStats:
    buys_count: int = 0
    sells_count: int = 0
    buys_shares: float = 0.0
    sells_shares: float = 0.0
    buys_usdc: float = 0.0
    sells_usdc: float = 0.0
    rebates_est: float = 0.0
    window_start: float = 0.0
    window_id: str = ""

    yes_buys_count: int = 0
    yes_sells_count: int = 0
    no_buys_count: int = 0
    no_sells_count: int = 0
    yes_buys_usdc: float = 0.0
    yes_sells_usdc: float = 0.0
    no_buys_usdc: float = 0.0
    no_sells_usdc: float = 0.0

    @property
    def total_volume_usdc(self): return self.buys_usdc + self.sells_usdc
    @property
    def net_position_shares(self): return self.buys_shares - self.sells_shares
    @property
    def net_position_usdc(self): return self.sells_usdc - self.buys_usdc
    @property
    def total_fills(self): return self.buys_count + self.sells_count
    @property
    def spread_pnl(self):
        return ((self.yes_sells_usdc - self.yes_buys_usdc)
                + (self.no_sells_usdc - self.no_buys_usdc))

    def to_dict(self):
        return {
            "window_id": self.window_id,
            "buys": self.buys_count,
            "sells": self.sells_count,
            "buys_shares": round(self.buys_shares, 2),
            "sells_shares": round(self.sells_shares, 2),
            "buys_usdc": round(self.buys_usdc, 4),
            "sells_usdc": round(self.sells_usdc, 4),
            "volume_usdc": round(self.total_volume_usdc, 4),
            "net_position_shares": round(self.net_position_shares, 2),
            "net_position_usdc": round(self.net_position_usdc, 4),
            "rebates_est": round(self.rebates_est, 6),
        }


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def _round_tick(price: float, tick_size: str) -> float:
    tick = float(tick_size)
    decimals = len(tick_size.split(".")[-1]) if "." in tick_size else 2
    return round(round(price / tick) * tick, decimals)


def _best_bid(book: dict) -> Optional[float]:
    try:
        return float(book["bids"][0][0])
    except (IndexError, KeyError, TypeError, ValueError):
        return None


def _best_ask(book: dict) -> Optional[float]:
    try:
        return float(book["asks"][0][0])
    except (IndexError, KeyError, TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════
# MARKET MAKER
# ═══════════════════════════════════════════════════════════

class MarketMaker:
    def __init__(self, cfg, clob_client=None, on_event=None):
        self.cfg = cfg
        self.clob = clob_client
        self.on_event = on_event
        # Optional callbacks for outside observers (e.g. Telegram).
        # Signature: on_split_failed(condition_id: str, error: str) -> None
        self.on_split_failed = None
        self.active_orders: dict = {}  # oid -> {token, side, price, size}

        # Stats
        self.window_stats = MMStats(window_start=time.time())
        self.window_history: list = []
        self.total_stats = MMStats()

        # Window state
        self._yes_balance = 0.0
        self._no_balance = 0.0
        self._split_done = False
        self._next_split_prepared = False
        self._current_condition_id = ""
        self._window_question = ""
        self._window_start_iso = ""
        self._split_amount = 0.0

        # Previous window snapshot (for cleanup + save result)
        self._prev_condition_id = ""
        self._prev_yes_balance = 0.0
        self._prev_no_balance = 0.0
        self._prev_split_amount = 0.0
        self._prev_split_done = False
        self._prev_window_question = ""
        self._prev_window_start = ""
        self._prev_merge_amount = 0.0
        self._prev_window_stats = None

        # Requote timing
        self._last_requote_time = 0.0

        # Relayer (lazy init)
        self._relayer = None
        self._relayer_checked = False
        self._aio_session = None

        # Tick size cache
        self._tick_size: Optional[str] = None
        self._tick_token: str = ""

    # ── PUBLIC API ─────────────────────────────────────────

    async def run_cycle(
        self,
        market_info: dict,
        book: dict,
        coinbase_price: float = 0,
        inventory: dict = None,
        target_price: float = 0,
        seconds_remaining: float = 0,
        aio_session=None,
    ):
        """Simplified cycle: split at start, sell ladder every requote_interval,
        cancel at expiry."""
        if not self.cfg.enabled:
            return

        self._aio_session = aio_session

        up_token = market_info.get("up_token", "")
        down_token = market_info.get("down_token", "")
        if not up_token:
            return

        # Lazy init Relayer
        if not self._relayer_checked and self.cfg.split_enabled:
            try:
                from strategies.relayer_client import create_safe_relayer_client
                self._relayer = create_safe_relayer_client()
                if self._relayer:
                    _log(f"Relayer (SAFE) initialized: "
                         f"{self._relayer.proxy_wallet[:12]}...", "bold cyan")
                else:
                    _log("Relayer not available (check MM2_* env)", "yellow")
            except Exception as e:
                _log(f"Relayer init failed: {e}", "red")
            self._relayer_checked = True

        condition_id = market_info.get("conditionId", "")

        # === 1. EXPIRY STOP ===
        if seconds_remaining <= self.cfg.expiry_stop_seconds:
            if self.active_orders and self.clob and not self._next_split_prepared:
                try:
                    await asyncio.to_thread(
                        self.clob.cancel_orders,
                        list(self.active_orders.keys()))
                    self.active_orders.clear()
                    _log(f"Expiry: cancelled orders ({seconds_remaining:.0f}s)",
                         "yellow")
                except Exception as e:
                    _log(f"Cancel err at expiry: {e}", "yellow")
            self._next_split_prepared = True
            return

        # === 2. SPLIT ===
        if (self.cfg.split_enabled
                and self._relayer
                and condition_id
                and not self._split_done):
            self._current_condition_id = condition_id
            self._window_question = market_info.get("question", "")
            if not self._window_start_iso:
                from datetime import datetime, timezone
                self._window_start_iso = (datetime.now(timezone.utc)
                    .replace(tzinfo=None).isoformat())
            _log(f"MM: attempting split ${self.cfg.split_amount_usdc}...", "cyan")
            try:
                ok = await self._relayer.split(
                    condition_id=condition_id,
                    amount_usdc=self.cfg.split_amount_usdc,
                    session=aio_session,
                    wait=False,
                )
                if ok:
                    self._yes_balance += self.cfg.split_amount_usdc
                    self._no_balance += self.cfg.split_amount_usdc
                    self._split_done = True
                    self._split_amount = self.cfg.split_amount_usdc
                    _log(
                        f"Split OK: ${self.cfg.split_amount_usdc} "
                        f"YES={self._yes_balance:.0f} NO={self._no_balance:.0f}",
                        "bold cyan",
                    )
                    await asyncio.sleep(8)
                else:
                    _log("Split submit failed — will retry next cycle", "yellow")
                    # Do NOT set _split_done=True — allow retry
                    if callable(self.on_split_failed):
                        try:
                            self.on_split_failed(
                                condition_id, "submit returned False")
                        except Exception:
                            pass
            except Exception as e:
                _log(f"Split error: {e}", "red")
                if callable(self.on_split_failed):
                    try:
                        self.on_split_failed(condition_id, str(e))
                    except Exception:
                        pass
            return

        # === 3. NO TOKENS ===
        if (self._yes_balance < self.cfg.level_size_shares
                and self._no_balance < self.cfg.level_size_shares):
            return

        # === 4. REQUOTE GATE ===
        now = time.time()
        if now - self._last_requote_time < self.cfg.requote_interval:
            return
        self._last_requote_time = now

        # === 5. TICK SIZE ===
        tick = await self._get_tick_size(up_token)
        if not tick:
            return

        # === 6. NO BOOK ===
        down_book = None
        if self.cfg.trade_both_sides and down_token and self.clob:
            try:
                raw = await asyncio.to_thread(
                    self.clob.get_order_book, down_token)
                if raw:
                    bids_raw = getattr(raw, "bids", None) or (
                        raw.get("bids", []) if isinstance(raw, dict) else [])
                    asks_raw = getattr(raw, "asks", None) or (
                        raw.get("asks", []) if isinstance(raw, dict) else [])
                    down_book = {
                        "bids": sorted([
                            [float(b.price if hasattr(b, "price") else b[0]),
                             float(b.size if hasattr(b, "size") else b[1])]
                            for b in bids_raw[:10]
                        ], key=lambda x: -x[0]),
                        "asks": sorted([
                            [float(a.price if hasattr(a, "price") else a[0]),
                             float(a.size if hasattr(a, "size") else a[1])]
                            for a in asks_raw[:10]
                        ], key=lambda x: x[0]),
                    }
            except Exception as e:
                _log(f"NO book fetch err: {e}", "yellow")

        # === 7. CANCEL OLD ===
        if self.active_orders and self.clob:
            try:
                await asyncio.to_thread(
                    self.clob.cancel_orders,
                    list(self.active_orders.keys()))
            except Exception as e:
                _log(f"Cancel err: {e}", "yellow")
            self.active_orders.clear()

        # === 8. BUILD ORDERS ===
        from py_clob_client.clob_types import (
            OrderArgs, OrderType, PostOrdersArgs,
            PartialCreateOrderOptions,
        )

        exp = int(time.time()) + 60 + self.cfg.quote_ttl_seconds
        tick_f = float(tick)
        specs: list = []

        def plan_ladder(token_id, best_ask, balance, token_label):
            MIN_ORDER = 5.0  # Polymarket minimum order size
            if not best_ask or balance < MIN_ORDER:
                return
            if balance < self.cfg.level_size_shares:
                _log(
                    f"{token_label} partial balance: {balance:.1f}sh "
                    f"< level_size={self.cfg.level_size_shares}, "
                    f"placing remainder",
                    "dim",
                )
            remaining = balance
            for i in range(self.cfg.ladder_levels):
                if remaining <= 0:
                    break
                # Take full level_size or whatever's left
                size = min(remaining, self.cfg.level_size_shares)
                if size < MIN_ORDER:
                    break  # below Polymarket minimum
                size = round(size, 2)
                price = _round_tick(best_ask - tick_f + i * tick_f, tick)
                price = max(0.01, min(0.99, price))
                specs.append((token_id, price, size, token_label))
                remaining -= size

        yes_ask = _best_ask(book)
        plan_ladder(up_token, yes_ask, self._yes_balance, "YES")

        if self.cfg.trade_both_sides and down_token:
            no_ask = _best_ask(down_book) if down_book else None
            no_bid = _best_bid(down_book) if down_book else None
            if not no_ask or not no_bid or (no_ask - no_bid) > 0.3:
                yes_bid = _best_bid(book)
                if yes_bid:
                    no_ask = round(1.0 - yes_bid, 4)
            plan_ladder(down_token, no_ask, self._no_balance, "NO")

        if not specs:
            return

        # === 9. SIGN + POST IN THREAD ===
        def _sign_and_post():
            batch_local = []
            meta_local = []
            for token_id, price, size, token_label in specs:
                try:
                    args = OrderArgs(
                        token_id=token_id,
                        price=price,
                        size=size,
                        side="SELL",
                        expiration=exp,
                    )
                    opts = PartialCreateOrderOptions(tick_size=tick)
                    signed = self.clob.create_order(args, opts)
                    batch_local.append(PostOrdersArgs(
                        order=signed,
                        orderType=OrderType.GTD,
                        postOnly=True,
                    ))
                    meta_local.append({
                        "token": token_label,
                        "side": "SELL",
                        "price": price,
                        "size": size,
                    })
                except Exception as e:
                    _log(f"Sign err {token_label} @{price}: {e}", "red")
            if not batch_local:
                return None, meta_local
            try:
                r = self.clob.post_orders(batch_local)
                return r, meta_local
            except Exception as e:
                _log(f"post_orders err: {e}", "red")
                return None, meta_local

        if not self.clob:
            return
        resp, meta = await asyncio.to_thread(_sign_and_post)
        if resp is None:
            return

        # Parse response
        new_orders = {}
        if isinstance(resp, list):
            for i, r in enumerate(resp):
                oid = r.get("orderID", "") if isinstance(r, dict) else ""
                if oid and i < len(meta):
                    new_orders[oid] = meta[i]
        elif isinstance(resp, dict):
            oids = resp.get("orderIDs", [])
            for i, oid in enumerate(oids):
                if oid and i < len(meta):
                    new_orders[oid] = meta[i]

        self.active_orders = new_orders
        yes_s = sum(1 for m in new_orders.values() if m["token"] == "YES")
        no_s = sum(1 for m in new_orders.values() if m["token"] == "NO")
        _log(
            f"Orders: YES {yes_s}S NO {no_s}S "
            f"| bal: Y{self._yes_balance:.0f} N{self._no_balance:.0f} "
            f"| secs: {seconds_remaining:.0f}",
            "cyan",
        )

    def on_fill(self, side, price, size, order_id="", token="YES"):
        """Update balance + stats after a fill."""
        usdc = price * size
        taker_fee = size * price * 0.072 * (price * (1.0 - price))
        rebate = taker_fee * 0.20

        # Balance (sell_only: only SELL should happen)
        if token == "YES" and side == "SELL":
            self._yes_balance = max(0.0, self._yes_balance - size)
        elif token == "NO" and side == "SELL":
            self._no_balance = max(0.0, self._no_balance - size)
        elif token == "YES" and side == "BUY":
            self._yes_balance += size
        elif token == "NO" and side == "BUY":
            self._no_balance += size

        ws = self.window_stats
        if side == "BUY":
            ws.buys_count += 1
            ws.buys_shares += size
            ws.buys_usdc += usdc
            if token == "YES":
                ws.yes_buys_count += 1
                ws.yes_buys_usdc += usdc
            else:
                ws.no_buys_count += 1
                ws.no_buys_usdc += usdc
        elif side == "SELL":
            ws.sells_count += 1
            ws.sells_shares += size
            ws.sells_usdc += usdc
            if token == "YES":
                ws.yes_sells_count += 1
                ws.yes_sells_usdc += usdc
            else:
                ws.no_sells_count += 1
                ws.no_sells_usdc += usdc
        ws.rebates_est += rebate

        self._recalc_totals()

        if order_id and order_id in self.active_orders:
            del self.active_orders[order_id]

        _log(
            f"Fill: {side} {token} {size}sh @{price:.3f} "
            f"| YES={self._yes_balance:.1f} NO={self._no_balance:.1f}",
            "bold green",
        )

    def on_window_change(self, new_window_id=""):
        """Save stats + full reset on new 5min market."""
        if self.window_stats.total_fills > 0:
            self.window_history.append(self.window_stats)
            if len(self.window_history) > 288:
                self.window_history.pop(0)

        # Snapshot prev window
        self._prev_condition_id = self._current_condition_id
        self._prev_yes_balance = self._yes_balance
        self._prev_no_balance = self._no_balance
        self._prev_split_amount = self._split_amount
        self._prev_split_done = self._split_done
        self._prev_window_question = self._window_question
        self._prev_window_start = self._window_start_iso
        self._prev_merge_amount = 0.0
        self._prev_window_stats = self.window_stats

        # Cancel outstanding
        if self.active_orders and self.clob:
            try:
                self.clob.cancel_orders(list(self.active_orders.keys()))
            except Exception:
                pass
        self.active_orders = {}

        # Reset current window
        self._yes_balance = 0.0
        self._no_balance = 0.0
        self._split_done = False
        self._next_split_prepared = False
        self._current_condition_id = ""
        self._split_amount = 0.0
        self._window_question = ""
        self._last_requote_time = 0.0
        self._tick_size = None
        self._tick_token = ""
        from datetime import datetime, timezone
        self._window_start_iso = (datetime.now(timezone.utc)
            .replace(tzinfo=None).isoformat())

        self.window_stats = MMStats(
            window_start=time.time(),
            window_id=new_window_id,
        )
        self._recalc_totals()
        _log(
            f"MM: window reset | prev: YES={self._prev_yes_balance:.1f} "
            f"NO={self._prev_no_balance:.1f}",
            "cyan",
        )

    async def cleanup_prev_window(self, aio_session=None):
        """Merge leftovers + redeem + save result."""
        if not self._relayer or not self._prev_condition_id:
            return
        if aio_session is None:
            aio_session = self._aio_session
        if aio_session is None:
            return

        cid = self._prev_condition_id
        yes = self._prev_yes_balance
        no = self._prev_no_balance

        await asyncio.sleep(10)

        mergeable = min(yes, no)
        if mergeable > 0.01:
            try:
                ok = await self._relayer.merge(cid, mergeable, aio_session, wait=False)
                if ok:
                    yes -= mergeable
                    no -= mergeable
                    self._prev_merge_amount = mergeable
                    _log(f"Merge {mergeable:.2f} YES+NO -> USDC", "cyan")
            except Exception as e:
                _log(f"Merge err: {e}", "red")

        self._prev_yes_balance = yes
        self._prev_no_balance = no

        await self._save_window_result()

        if yes > 0.01 or no > 0.01:
            _log(
                f"Waiting 135s for resolution to redeem "
                f"YES={yes:.2f} NO={no:.2f}",
                "dim",
            )
            await asyncio.sleep(135)
            try:
                await self._relayer.redeem(cid, aio_session, wait=False)
                _log("Redeem submitted", "cyan")
            except Exception as e:
                _log(f"Redeem err: {e}", "red")

        self._prev_condition_id = ""
        self._prev_yes_balance = 0.0
        self._prev_no_balance = 0.0
        self._prev_merge_amount = 0.0
        self._prev_window_stats = None

    async def _save_window_result(self) -> None:
        if not self._prev_condition_id or not self._prev_window_stats:
            return
        ws = self._prev_window_stats
        try:
            from datetime import datetime, timezone
            spread_pnl = ws.spread_pnl
            net_pnl = (
                spread_pnl
                + self._prev_merge_amount
                - self._prev_split_amount
            )
            result = {
                "window_id": self._prev_condition_id,
                "window_question": self._prev_window_question,
                "started_at": self._prev_window_start,
                "completed_at": datetime.now(timezone.utc)
                    .replace(tzinfo=None).isoformat(),
                "split_amount_usdc": round(self._prev_split_amount, 4),
                "split_done": 1 if self._prev_split_done else 0,
                "yes_buys_count": ws.yes_buys_count,
                "yes_sells_count": ws.yes_sells_count,
                "no_buys_count": ws.no_buys_count,
                "no_sells_count": ws.no_sells_count,
                "yes_buys_usdc": round(ws.yes_buys_usdc, 4),
                "yes_sells_usdc": round(ws.yes_sells_usdc, 4),
                "no_buys_usdc": round(ws.no_buys_usdc, 4),
                "no_sells_usdc": round(ws.no_sells_usdc, 4),
                "total_volume_usdc": round(ws.total_volume_usdc, 4),
                "spread_pnl_usdc": round(spread_pnl, 4),
                "merge_amount_usdc": round(self._prev_merge_amount, 4),
                "yes_remaining": round(self._prev_yes_balance, 4),
                "no_remaining": round(self._prev_no_balance, 4),
                "rebates_est": round(ws.rebates_est, 6),
                "net_pnl_usdc": round(net_pnl, 4),
            }
            try:
                import btc_lab_db as labdb
                labdb.save_mm_window_result(result)
            except Exception as e:
                _log(f"save_window_result err: {e}", "red")

            _log(
                f"Window done: {self._prev_window_question[:40]} | "
                f"Split -${self._prev_split_amount:.2f} | "
                f"Spread {'+' if spread_pnl>=0 else ''}${spread_pnl:.4f} | "
                f"Merge +${self._prev_merge_amount:.2f} | "
                f"Vol ${ws.total_volume_usdc:.2f} | "
                f"Net {'+' if net_pnl>=0 else ''}${net_pnl:.4f}",
                "bold cyan",
            )
        except Exception as e:
            _log(f"_save_window_result err: {e}", "red")

    def get_status(self):
        ws = self.window_stats
        ts = self.total_stats
        return {
            "enabled": self.cfg.enabled,
            "strategy": self.cfg.strategy,
            "running": bool(self.active_orders),
            "pause_reason": "",
            "active_orders": len(self.active_orders),
            "total_fills": ts.total_fills,
            "spread_earned": 0,
            "rebates_est": round(ts.rebates_est, 4),
            "levels": self.cfg.ladder_levels,
            "level_size": self.cfg.level_size_shares,
            "window": ws.to_dict(),
            "total": {
                "windows_count": len(self.window_history),
                "buys": ts.buys_count,
                "sells": ts.sells_count,
                "buys_shares": round(ts.buys_shares, 2),
                "sells_shares": round(ts.sells_shares, 2),
                "volume_usdc": round(ts.total_volume_usdc, 4),
                "net_position_usdc": round(ts.net_position_usdc, 4),
                "rebates_est": round(ts.rebates_est, 6),
            },
            "split": {
                "done": self._split_done,
                "yes_balance": round(self._yes_balance, 2),
                "no_balance": round(self._no_balance, 2),
                "net": round(self._yes_balance - self._no_balance, 2),
                "trade_both_sides": self.cfg.trade_both_sides,
            },
        }

    def get_window_history(self, limit=10):
        return [w.to_dict() for w in reversed(self.window_history[-limit:])]

    def _recalc_totals(self):
        t = self.total_stats
        ws = self.window_stats
        hist = self.window_history
        t.buys_count = sum(w.buys_count for w in hist) + ws.buys_count
        t.sells_count = sum(w.sells_count for w in hist) + ws.sells_count
        t.buys_shares = sum(w.buys_shares for w in hist) + ws.buys_shares
        t.sells_shares = sum(w.sells_shares for w in hist) + ws.sells_shares
        t.buys_usdc = sum(w.buys_usdc for w in hist) + ws.buys_usdc
        t.sells_usdc = sum(w.sells_usdc for w in hist) + ws.sells_usdc
        t.rebates_est = sum(w.rebates_est for w in hist) + ws.rebates_est

    async def _get_tick_size(self, token_id):
        if self._tick_size and self._tick_token == token_id:
            return self._tick_size
        if not self.clob:
            return "0.01"
        try:
            ts = str(await asyncio.to_thread(self.clob.get_tick_size, token_id))
            self._tick_size = ts
            self._tick_token = token_id
            _log(f"tick_size={ts}")
            return ts
        except Exception as e:
            _log(f"tick_size err: {e}", "red")
            return "0.01"


# ═══════════════════════════════════════════════════════════
# CONFIG LOADER
# ═══════════════════════════════════════════════════════════

def load_mm_config_from_env():
    import os
    def b(k, d): return os.getenv(k, str(d)).lower() == "true"
    def f(k, d): return float(os.getenv(k, str(d)))
    def i(k, d): return int(os.getenv(k, str(d)))

    return MMConfig(
        enabled=b("MM_ENABLED", False),
        strategy=os.getenv("MM_STRATEGY", "book"),
        ladder_levels=i("MM_LADDER_LEVELS", 3),
        level_size_shares=f("MM_LEVEL_SIZE", 10),
        split_enabled=b("MM_SPLIT_ENABLED", False),
        split_amount_usdc=f("MM_SPLIT_AMOUNT", 100.0),
        trade_both_sides=b("MM_TRADE_BOTH_SIDES", True),
        sell_only_mode=b("MM_SELL_ONLY", True),
        quote_ttl_seconds=i("MM_QUOTE_TTL", 10),
        requote_interval=f("MM_REQUOTE_INTERVAL", 5.0),
        expiry_stop_seconds=i("MM_EXPIRY_STOP", 20),
    )
