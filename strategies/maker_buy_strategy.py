"""
Maker BUY strategy: BUY YES with limit orders when market almost decided.

Math at price 0.97:
  If YES wins: receive $1.00, profit $0.03 per share (3%)
  If YES loses: receive $0.00, loss $0.97 per share

  Break-even winrate at 0.97 = 97%
  Break-even winrate at 0.95 = 95%

Why as maker (post_only):
  - No taker fee
  - Maker rebate ~0.014% of volume
  - Order may not fill (post-only rejection) -> no loss, no profit
"""

import os
import time
import logging
from dataclasses import dataclass

logger = logging.getLogger("maker_buy")


@dataclass
class MakerBuyConfig:
    enabled: bool = False

    # Entry condition by YES token best ask
    entry_threshold: float = 0.95

    # Ladder: levels above threshold at step apart
    levels: int = 2
    step: float = 0.01

    # Size per level in USDC
    usdc_per_level: float = 20.0

    # Time window (seconds before window end)
    min_seconds: int = 15
    max_seconds: int = 90

    # Max bets per window
    max_bets_per_window: int = 2

    # Order TTL in seconds
    order_ttl: int = 20

    # Hard cap on total USDC filled per window (safety).
    # 0 = no cap.
    max_usdc_per_window: float = 0.0

    # Trigger on empty ask book when opposite side looks certain-loser.
    # Requires: our_ask == 0 AND 0 < opposite_ask <= empty_ask_opposite_max
    trigger_on_empty_ask: bool = True

    # Max opposite-side ask at which empty-ask trigger fires.
    # Default 0.05 (5¢) — wider than 1 - entry_threshold so we catch
    # 0.02-0.05 opposite-side scenarios too.
    empty_ask_opposite_max: float = 0.05

    # If true, detect when our resting orders are outbid (another user
    # placed a better BID above us) and re-ladder at the new top.
    chase_enabled: bool = True

    # Min tick units to trigger chase: chase fires when
    # best_bid > our_max_price + chase_min_ticks * tick_size.
    # Default 0 = any improvement triggers chase.
    chase_min_ticks: int = 0


class MakerBuyStrategy:
    """BUY YES with maker limit orders when ask price >= entry_threshold."""

    # Grace window for fills arriving after cancel. A real race condition:
    # cancel is sent, order might already be partially matched at the server,
    # the fill event arrives up to 1-2s later. We need to still save those
    # fills to DB and update _window_usdc_spent.
    CANCEL_GRACE_SEC = 90

    def __init__(self, cfg: MakerBuyConfig):
        self.cfg = cfg
        self._window_bets: int = 0
        self._window_usdc_spent: float = 0.0
        self._last_window_id: str = ""
        self._active_order_ids: set = set()
        self._active_order_expiry: dict = {}  # oid -> epoch seconds
        # oid -> cancel_timestamp. Fills for these OIDs within
        # CANCEL_GRACE_SEC are still accounted.
        self._recently_cancelled: dict = {}
        self._active_max_price: float = 0.0
        self._active_side: str = ""  # "YES" / "NO" / ""

    def on_window_change(self, window_id: str) -> None:
        if window_id != self._last_window_id:
            self._window_bets = 0
            self._window_usdc_spent = 0.0
            self._last_window_id = window_id
            self._active_order_ids.clear()
            self._active_order_expiry.clear()
            self._recently_cancelled.clear()
            self._active_max_price = 0.0
            self._active_side = ""
            logger.debug(f"MakerBuy: new window {window_id[:16]}")

    def _mark_cancelled(self, order_id: str) -> None:
        """Mark an OID as recently-cancelled so late fills are still
        counted. Safe to call multiple times."""
        self._recently_cancelled[order_id] = time.time()

    def _prune_recently_cancelled(self) -> None:
        if not self._recently_cancelled:
            return
        now = time.time()
        stale = [
            oid for oid, ts in self._recently_cancelled.items()
            if now - ts > self.CANCEL_GRACE_SEC
        ]
        for oid in stale:
            self._recently_cancelled.pop(oid, None)

    def is_tracked(self, order_id: str) -> bool:
        """True if we should account for a fill on this OID.
        Covers both currently-active orders and orders cancelled in the
        last CANCEL_GRACE_SEC seconds (race window)."""
        if not order_id:
            return False
        if order_id in self._active_order_ids:
            return True
        self._prune_recently_cancelled()
        return order_id in self._recently_cancelled

    def prune_expired_orders(self) -> int:
        """Drop any active_order_ids whose TTL has passed. Moves them to
        the recently-cancelled grace bucket so late fills are still
        accounted. Returns count pruned.
        """
        if not self._active_order_expiry:
            return 0
        now = int(time.time())
        stale = [oid for oid, exp in self._active_order_expiry.items()
                 if exp <= now]
        if not stale:
            return 0
        for oid in stale:
            self._active_order_ids.discard(oid)
            self._active_order_expiry.pop(oid, None)
            self._mark_cancelled(oid)
        self._reset_tracking_if_empty()
        logger.info(f"MAKER_BUY: pruned {len(stale)} expired phantom order(s)")
        return len(stale)

    def wants_top_refresh(
        self,
        yes_ask: float,
        no_ask: float,
        seconds_remaining: float,
    ) -> tuple:
        """Check if we should cancel resting orders and re-ladder from 0.99.
        Fires only in the empty-ask scenario and only when we have active
        orders sitting below 0.99 (otherwise there's nothing to refresh).
        Returns (should_refresh, reason, token_label).
        """
        if not self.cfg.enabled:
            return False, "disabled", None
        if not self.cfg.trigger_on_empty_ask:
            return False, "empty_ask_disabled", None
        if not self._active_order_ids:
            return False, "no_active_orders", None
        if (self.cfg.max_usdc_per_window > 0
                and self._window_usdc_spent >= self.cfg.max_usdc_per_window):
            return False, "window_cap_reached", None
        if seconds_remaining < self.cfg.min_seconds:
            return False, f"too_late={seconds_remaining:.0f}s", None
        if seconds_remaining > self.cfg.max_seconds:
            return False, f"too_early={seconds_remaining:.0f}s", None

        opp_max = float(self.cfg.empty_ask_opposite_max)
        # Target is the top of the book for the empty-ask scenario.
        target_price = 0.99

        # NO side empty -> refresh to BUY NO @ 0.99
        if no_ask <= 0 and 0 < yes_ask <= opp_max:
            # Skip if we're already sitting on NO at the target price —
            # re-posting would destroy our time-priority in the queue.
            if (self._active_side == "NO"
                    and self._active_max_price >= target_price):
                return False, (
                    f"already_top_NO our={self._active_max_price:.3f}"
                    f">={target_price}"
                ), None
            return True, (
                f"refresh NO: no_ask=empty yes_ask={yes_ask:.3f}<={opp_max}"
            ), "NO"
        # YES side empty -> refresh to BUY YES @ 0.99
        if yes_ask <= 0 and 0 < no_ask <= opp_max:
            if (self._active_side == "YES"
                    and self._active_max_price >= target_price):
                return False, (
                    f"already_top_YES our={self._active_max_price:.3f}"
                    f">={target_price}"
                ), None
            return True, (
                f"refresh YES: yes_ask=empty no_ask={no_ask:.3f}<={opp_max}"
            ), "YES"
        return False, "no_empty_ask_signal", None

    def clear_active_orders(self) -> None:
        """Forget active order IDs (caller already cancelled them).
        Moved OIDs keep living in _recently_cancelled so late fills are
        still saved to DB and counted in _window_usdc_spent.
        """
        for oid in list(self._active_order_ids):
            self._mark_cancelled(oid)
        self._active_order_ids.clear()
        self._active_order_expiry.clear()
        self._active_max_price = 0.0
        self._active_side = ""

    def wants_chase_refresh(
        self,
        yes_bid: float,
        yes_ask: float,
        no_bid: float,
        no_ask: float,
        seconds_remaining: float,
        tick_size: float,
    ) -> tuple:
        """Detect if someone outbid our resting orders: best_bid on our
        side is higher than our highest placed price. Returns
        (should, reason, token_label).
        """
        if not self.cfg.enabled:
            return False, "disabled", None
        if not self.cfg.chase_enabled:
            return False, "chase_disabled", None
        if not self._active_order_ids or not self._active_side:
            return False, "no_active_orders", None
        if self._active_max_price <= 0:
            return False, "no_tracked_price", None
        if (self.cfg.max_usdc_per_window > 0
                and self._window_usdc_spent >= self.cfg.max_usdc_per_window):
            return False, "window_cap_reached", None
        if seconds_remaining < self.cfg.min_seconds:
            return False, f"too_late={seconds_remaining:.0f}s", None
        if seconds_remaining > self.cfg.max_seconds:
            return False, f"too_early={seconds_remaining:.0f}s", None

        side = self._active_side
        side_bid = yes_bid if side == "YES" else no_bid
        side_ask = yes_ask if side == "YES" else no_ask
        if side_bid <= 0:
            return False, "side_bid_empty", None
        threshold = self._active_max_price + self.cfg.chase_min_ticks * tick_size
        if side_bid <= threshold:
            return False, (
                f"still_top bid={side_bid:.4f}<=our={self._active_max_price:.4f}"
            ), None

        # Compute the target for a re-ladder. build_and_place uses
        # `best_ask - tick` when ask book is populated, else 0.99.
        if side_ask > 0:
            new_target = round(side_ask - tick_size, 4)
        else:
            new_target = 0.99
        # If we can't actually improve our price, don't churn — re-posting
        # at the same price would destroy our time-priority in the queue.
        if new_target <= self._active_max_price:
            return False, (
                f"already_at_target our={self._active_max_price:.4f}"
                f">=target={new_target:.4f}"
            ), None
        # Safety: ensure target is a valid post-only maker price (non-crossing).
        if new_target < 0.02 or new_target >= 1.0:
            return False, f"invalid_target={new_target:.4f}", None
        return True, (
            f"chase {side}: bid={side_bid:.4f}>our={self._active_max_price:.4f}"
            f" target={new_target:.4f}"
        ), side

    def should_enter(
        self,
        yes_ask: float,
        no_ask: float,
        seconds_remaining: float,
    ) -> tuple:
        """Check if either YES or NO side triggers entry.
        Returns (should, reason, token_label).
        token_label: 'YES' / 'NO' / None
        """
        if not self.cfg.enabled:
            return False, "disabled", None
        if self._window_bets >= self.cfg.max_bets_per_window:
            return False, f"max_bets={self._window_bets}", None
        if (self.cfg.max_usdc_per_window > 0
                and self._window_usdc_spent >= self.cfg.max_usdc_per_window):
            return False, (
                f"window_cap_reached "
                f"${self._window_usdc_spent:.2f}>=${self.cfg.max_usdc_per_window:.2f}"
            ), None
        if seconds_remaining < self.cfg.min_seconds:
            return False, f"too_late={seconds_remaining:.0f}s", None
        if seconds_remaining > self.cfg.max_seconds:
            return False, f"too_early={seconds_remaining:.0f}s", None
        if self._active_order_ids:
            return False, "orders_pending", None

        # 1) Classic trigger: our side ask >= threshold
        if yes_ask >= self.cfg.entry_threshold:
            return True, f"YES_ask={yes_ask:.3f}>={self.cfg.entry_threshold}", "YES"
        if no_ask >= self.cfg.entry_threshold:
            return True, f"NO_ask={no_ask:.3f}>={self.cfg.entry_threshold}", "NO"

        # 2) Empty-ask trigger: our side's ask book is empty AND opposite side
        #    ask looks like a certain-loser (<= empty_ask_opposite_max).
        #    Example: NO ask = 0, YES ask = 0.02 -> BUY NO at 0.99
        if self.cfg.trigger_on_empty_ask:
            opp_max = float(self.cfg.empty_ask_opposite_max)
            # NO side empty -> BUY NO at ~0.99
            if no_ask <= 0 and 0 < yes_ask <= opp_max:
                return True, (
                    f"NO_ask=empty yes_ask={yes_ask:.3f}<={opp_max}"
                ), "NO"
            # YES side empty -> BUY YES at ~0.99
            if yes_ask <= 0 and 0 < no_ask <= opp_max:
                return True, (
                    f"YES_ask=empty no_ask={no_ask:.3f}<={opp_max}"
                ), "YES"

        return False, (
            f"yes={yes_ask:.3f} no={no_ask:.3f} "
            f"<{self.cfg.entry_threshold}"
        ), None

    def build_and_place(
        self,
        token_id: str,
        token_label: str,
        best_ask: float,
        tick_size: float,
        clob_client,
    ) -> int:
        """Build adaptive BUY ladder starting just below current best_ask.
        Returns count of placed orders.

        Ladder: [best_ask - tick, best_ask - tick - step, ...] for `levels`.
        Always non-crossing (price < best_ask) so post_only accepts it.

        Example:
          best_ask=0.99, tick=0.01 -> [0.98, 0.97, 0.96, 0.95, 0.94]
          best_ask=0.95, tick=0.01 -> [0.94, 0.93, 0.92, 0.91, 0.90]
          best_ask=0.70, tick=0.01 -> [0.69, 0.68, 0.67, 0.66, 0.65]
          best_ask=0 (empty) -> [0.99, 0.98, ...] (top of the book)
        """
        from py_clob_client.clob_types import (
            OrderArgs, OrderType, PostOrdersArgs,
            PartialCreateOrderOptions,
        )

        # Empty ask book OR ask at/above 1.0 -> start ladder at 0.99
        # Otherwise start just below best_ask (non-crossing maker).
        if best_ask is None or best_ask <= 0 or best_ask >= 1.0:
            start = 0.99
        else:
            start = max(0.01, min(0.99, best_ask - tick_size))

        # Remaining budget this window (None = no cap)
        budget_left = None
        if self.cfg.max_usdc_per_window > 0:
            budget_left = max(
                0.0,
                self.cfg.max_usdc_per_window - self._window_usdc_spent,
            )

        orders_to_place = []

        for i in range(self.cfg.levels):
            raw_price = start - self.cfg.step * i
            price = round(
                round(raw_price / tick_size) * tick_size, 4
            )
            price = max(0.01, min(0.99, price))

            # Safety: don't place below 0.02 (near-zero bids make no sense)
            if price < 0.02:
                break

            level_usdc = self.cfg.usdc_per_level
            # Shrink level size to remaining window budget if needed
            if budget_left is not None:
                if budget_left <= 0:
                    logger.info(
                        f"MAKER_BUY: level {i} skipped — window budget "
                        f"exhausted (${self._window_usdc_spent:.2f}/"
                        f"${self.cfg.max_usdc_per_window:.2f})"
                    )
                    break
                level_usdc = min(level_usdc, budget_left)

            size = round(level_usdc / price, 1)
            if size < 5.0:
                # Polymarket minimum order size
                break
            size = max(size, 5.0)

            if budget_left is not None:
                budget_left = max(0.0, budget_left - size * price)

            expiration = int(time.time()) + 60 + self.cfg.order_ttl

            try:
                signed = clob_client.create_order(
                    OrderArgs(
                        token_id=token_id,
                        price=price,
                        size=size,
                        side="BUY",
                        expiration=expiration,
                    ),
                    PartialCreateOrderOptions(tick_size=str(tick_size)),
                )
                orders_to_place.append({
                    "signed": signed,
                    "price": price,
                    "size": size,
                    "expiration": expiration,
                })
            except Exception as e:
                logger.warning(f"MakerBuy create_order: {e}")

        if not orders_to_place:
            return 0

        try:
            batch = [
                PostOrdersArgs(
                    order=o["signed"],
                    orderType=OrderType.GTD,
                    postOnly=True,
                )
                for o in orders_to_place
            ]
            resp = clob_client.post_orders(batch)

            placed = 0
            if isinstance(resp, list):
                for i, r in enumerate(resp):
                    if i >= len(orders_to_place):
                        break
                    oid = (r.get("orderID") or r.get("id", "")
                           if isinstance(r, dict) else "")
                    status = r.get("status", "") if isinstance(r, dict) else ""
                    error = r.get("errorMsg", "") if isinstance(r, dict) else ""

                    if oid and status not in ("unmatched", "error"):
                        self._active_order_ids.add(oid)
                        # Track expiration so prune_expired_orders() can
                        # reclaim this slot even if WS misses the event.
                        self._active_order_expiry[oid] = (
                            orders_to_place[i].get("expiration")
                            or (int(time.time()) + 60 + self.cfg.order_ttl)
                        )
                        placed += 1
                        placed_price = orders_to_place[i]['price']
                        if placed_price > self._active_max_price:
                            self._active_max_price = placed_price
                        self._active_side = token_label
                        logger.info(
                            f"MAKER_BUY: BUY {token_label} "
                            f"{orders_to_place[i]['size']}sh"
                            f" @ {placed_price}"
                            f" | bet {self._window_bets + 1}"
                            f"/{self.cfg.max_bets_per_window}"
                            f" | TTL {self.cfg.order_ttl}s"
                        )
                    else:
                        logger.warning(
                            f"MAKER_BUY: order rejected"
                            f" @ {orders_to_place[i]['price']}"
                            f" | {error}"
                        )

            if placed > 0:
                self._window_bets += 1

            return placed

        except Exception as e:
            logger.error(f"MakerBuy post_orders: {e}")
            return 0

    def _reset_tracking_if_empty(self) -> None:
        if not self._active_order_ids:
            self._active_order_expiry.clear()
            self._active_max_price = 0.0
            self._active_side = ""

    def on_order_expired(self, order_id: str) -> None:
        """Called on WS CANCELLATION or any external cancel. Keep OID
        alive in the grace bucket so post-cancel fills are still counted.
        """
        if order_id in self._active_order_ids:
            self._active_order_ids.discard(order_id)
            self._active_order_expiry.pop(order_id, None)
            self._mark_cancelled(order_id)
        self._reset_tracking_if_empty()

    def on_fill(self, order_id: str, size: float, price: float) -> None:
        self._active_order_ids.discard(order_id)
        self._active_order_expiry.pop(order_id, None)
        # Also clean from recently_cancelled if it was there
        self._recently_cancelled.pop(order_id, None)
        self._reset_tracking_if_empty()
        spent = round(price * size, 4)
        self._window_usdc_spent += spent
        profit_if_win = round((1.0 - price) * size, 4)
        cap_note = ""
        if self.cfg.max_usdc_per_window > 0:
            cap_note = (
                f" | window ${self._window_usdc_spent:.2f}"
                f"/${self.cfg.max_usdc_per_window:.2f}"
            )
        logger.info(
            f"MAKER_BUY FILL: {size}sh @ {price}"
            f" | win:+${profit_if_win} lose:-${spent}{cap_note}"
        )


def load_maker_buy_config() -> MakerBuyConfig:
    return MakerBuyConfig(
        enabled=os.getenv("MAKER_BUY_ENABLED", "false").lower() == "true",
        entry_threshold=float(os.getenv("MAKER_BUY_ENTRY", "0.95")),
        levels=int(os.getenv("MAKER_BUY_LEVELS", "2")),
        step=float(os.getenv("MAKER_BUY_STEP", "0.01")),
        usdc_per_level=float(os.getenv("MAKER_BUY_USDC_PER_LEVEL", "20.0")),
        min_seconds=int(os.getenv("MAKER_BUY_MIN_SEC", "15")),
        max_seconds=int(os.getenv("MAKER_BUY_MAX_SEC", "90")),
        max_bets_per_window=int(os.getenv("MAKER_BUY_MAX_BETS", "2")),
        order_ttl=int(os.getenv("MAKER_BUY_TTL", "20")),
        max_usdc_per_window=float(
            os.getenv("MAKER_BUY_MAX_USDC_PER_WINDOW", "0")
        ),
        trigger_on_empty_ask=(
            os.getenv("MAKER_BUY_TRIGGER_ON_EMPTY_ASK", "true").lower() == "true"
        ),
        empty_ask_opposite_max=float(
            os.getenv("MAKER_BUY_EMPTY_ASK_OPP_MAX", "0.05")
        ),
        chase_enabled=(
            os.getenv("MAKER_BUY_CHASE_ENABLED", "true").lower() == "true"
        ),
        chase_min_ticks=int(os.getenv("MAKER_BUY_CHASE_MIN_TICKS", "0")),
    )
