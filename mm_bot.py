# -*- coding: utf-8 -*-
"""
Market Making Bot — размещение ликвидности на Polymarket Sports.

Выставляет лесенку лимитных ордеров (bid/ask) вокруг текущего mid price
на выбранных маркетах. Зарабатывает на спреде принимая сделки с обеих сторон.

Бид (BUY YES) ниже mid, Аск (BUY NO по обратной цене) выше mid.
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

from config import Config
from db_bets import BetDatabase
from gamma_client import GammaClient
from polymarket_client import PolymarketClient

log = logging.getLogger(__name__)

# ── Global API timeout для requests (используется py-clob-client) ────────
API_TIMEOUT = 8  # секунд

def _set_requests_timeout():
    """Устанавливает дефолтный timeout для всех requests вызовов."""
    try:
        import requests
        old_request = requests.Session.request
        def _patched_request(self, method, url, **kwargs):
            kwargs.setdefault('timeout', API_TIMEOUT)
            return old_request(self, method, url, **kwargs)
        requests.Session.request = _patched_request
        log.info("[mm] Default requests timeout set to %ds", API_TIMEOUT)
    except Exception as e:
        log.warning("[mm] Failed to set timeout: %s", e)

_set_requests_timeout()


class MarketMaker:
    """Market-making бот для Polymarket."""

    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv(override=True)

        self.cfg = Config()
        self.gamma = GammaClient()
        self.pm = PolymarketClient(
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
            funder_address=os.getenv("POLYMARKET_FUNDER", ""),
        )
        self.db = BetDatabase(os.getenv("DB_PATH_VALUEBET", "valuebet.db"))

        # Активные маркеты: {condition_id: MarketState}
        self._markets: dict[str, dict] = {}
        self._load_markets_from_db()

        self._ticks = 0
        self._running = True
        self._ws_monitor = None
        self._danger_flags: dict[str, float] = {}  # cid → danger_until timestamp
        self._processed_fills: set = set()  # order_id-ы уже обработанных fills (дедупликация)
        self._fill_velocity: dict[str, list] = {}  # cid → [fill_timestamps] за последние 10 сек

    def _load_markets_from_db(self):
        """Загрузить активные маркеты из БД."""
        for m in self.db.mm_get_active_markets():
            cid = m["condition_id"]
            pos = self.db.mm_get_position(cid)
            self._markets[cid] = {
                "token_yes": m["token_yes"],
                "token_no": m["token_no"],
                "question": m["question"],
                "event": m["event_name"],
                "neg_risk": bool(m["neg_risk"]),
                "tick_size": m["tick_size"] or "0.01",
                "mid": 0.0,
                "last_quote_mid": 0.0,
                "bid_orders": [],
                "ask_orders": [],
                "yes_shares": pos.get("yes_shares", 0) or 0,
                "no_shares": pos.get("no_shares", 0) or 0,
                "total_cost": pos.get("total_cost", 0) or 0,
                "fills_count": pos.get("fills_count", 0) or 0,
                "pnl": 0.0,
                "paused": bool(m.get("paused", 0)),
                "prematch_only": bool(m.get("prematch_only", 0)),
                "game_start_time": 0,
            }
        if self._markets:
            log.info("[mm] Загружено %d активных маркетов из БД", len(self._markets))

    # ──────────────────────────────────────────────────────────────────────────
    # Управление маркетами
    # ──────────────────────────────────────────────────────────────────────────

    def add_market(self, condition_id: str, token_yes: str, token_no: str,
                   question: str = "", event_name: str = "", sport: str = "",
                   neg_risk: bool = False, tick_size: str = "0.01"):
        """Добавить маркет для MM. Проверяет order book перед добавлением."""
        if condition_id in self._markets:
            log.warning("[mm] Маркет %s уже активен", condition_id[:16])
            return "already active"
        if len(self._markets) >= self.cfg.MM_MAX_MARKETS:
            log.warning("[mm] Макс. маркетов (%d) достигнут", self.cfg.MM_MAX_MARKETS)
            return "max markets reached"

        # Валидация: проверяем цену через price API (order book ненадёжен для Sports)
        try:
            client = self.pm._get_client()
            mid_resp = client.get_midpoint(token_yes)
            spread_resp = client.get_spread(token_yes)
            mid_val = float(mid_resp.get("mid", 0)) if isinstance(mid_resp, dict) else float(mid_resp or 0)
            spread_val = float(spread_resp.get("spread", 1)) if isinstance(spread_resp, dict) else float(spread_resp or 1)
            tick = float(tick_size)
            spread_ticks = round(spread_val / tick) if tick > 0 else 999
            mid = round(mid_val, len(tick_size.split(".")[-1]))
            if mid <= 0 or mid >= 1:
                log.warning("[mm] ❌ Маркет %s: invalid mid=%.4f — не добавлен", question[:30], mid)
                return "invalid mid price"
            log.info("[mm] Маркет %s: mid=%.2f spread=%dc — OK", question[:30], mid, spread_ticks)
        except Exception as e:
            log.warning("[mm] ⚠ Маркет %s: price API недоступен (%s) — добавлен, ждём",
                        question[:30], e)
            mid = 0.5  # default, обновится при первом тике

        self.db.mm_add_market(condition_id, token_yes, token_no,
                              question, event_name, sport, neg_risk, tick_size)
        self._markets[condition_id] = {
            "token_yes": token_yes, "token_no": token_no,
            "question": question, "event": event_name,
            "neg_risk": neg_risk, "tick_size": tick_size,
            "mid": mid, "last_quote_mid": 0.0,
            "bid_orders": [], "ask_orders": [],
            "yes_shares": 0, "no_shares": 0,
            "total_cost": 0, "fills_count": 0, "pnl": 0.0,
        }
        log.info("[mm] ✅ Маркет добавлен: %s | %s | mid=%.2f", event_name, question, mid)
        # Подписать WS мониторинг
        if self._ws_monitor:
            self._ws_monitor.subscribe(token_yes, condition_id)
        return None  # success

    async def remove_market(self, condition_id: str):
        """Снять все ордера и удалить маркет."""
        mkt = self._markets.get(condition_id)
        if not mkt:
            return
        await self._cancel_all_orders(mkt)
        self.db.mm_remove_market(condition_id)
        del self._markets[condition_id]
        log.info("[mm] ❌ Маркет снят: %s", condition_id[:16])

    def _get_token_balance(self, token_id: str) -> float:
        """Получает реальный баланс shares для токена с Polymarket."""
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self.pm._get_client()
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            resp = client.get_balance_allowance(params)
            raw = int(resp.get("balance", 0))
            return raw / 1_000_000  # USDC decimals = 6
        except Exception as e:
            log.debug("[mm] balance check error: %s", e)
            return 0

    def get_active_markets(self) -> list[dict]:
        """Список активных маркетов с позициями и realized P&L."""
        result = []
        for cid, mkt in self._markets.items():
            # Realized P&L из БД (учитывает SELL-ы)
            db_pos = self.db.mm_get_position(cid)
            rpnl = db_pos.get("realized_pnl", 0)

            ys = mkt["yes_shares"]
            ns = mkt["no_shares"]
            avg_y = db_pos.get("avg_yes", 0)
            avg_n = db_pos.get("avg_no", 0)
            yes_cost = round(avg_y * ys, 2) if ys > 0 else 0
            no_cost = round(avg_n * ns, 2) if ns > 0 else 0
            cost = yes_cost + no_cost
            # Сценарии: payout $1 per winning share - total cost
            pnl_yes = round(ys - cost, 2)   # если YES выиграет
            pnl_no = round(ns - cost, 2)    # если NO выиграет

            result.append({
                "condition_id": cid,
                "question": mkt["question"],
                "event": mkt["event"],
                "mid": mkt["mid"],
                "bid_orders": [(entry[1], entry[2]) for entry in mkt["bid_orders"]],
                "ask_orders": [(entry[1], entry[2]) for entry in mkt["ask_orders"]],
                "yes_shares": ys,
                "no_shares": ns,
                "net": round(ys - ns, 2),
                "fills_count": mkt["fills_count"],
                "pnl": rpnl + round(min(ys, ns) * 1.0 - cost, 2),  # realized + paired value - cost
                "realized_pnl": rpnl,
                "total_cost": round(cost, 2),
                "yes_cost": yes_cost,
                "no_cost": no_cost,
                "pnl_if_yes": pnl_yes,
                "pnl_if_no": pnl_no,
                "avg_yes": avg_y,
                "avg_no": avg_n,
                "margin_pct": db_pos.get("margin_pct", 0),
                "paused": mkt.get("paused", False),
                "prematch_only": mkt.get("prematch_only", False),
                "odds_yes": round(1.0 / mkt["mid"], 2) if mkt["mid"] > 0.01 else 0,
                "odds_no": round(1.0 / (1.0 - mkt["mid"]), 2) if mkt["mid"] < 0.99 else 0,
            })
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Main loop
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self):
        log.info("=" * 60)
        log.info("[mm] Market Maker запущен")
        log.info("[mm]   Уровней: %d, Шаг: %d тик, Размер: $%.0f",
                 self.cfg.MM_LEVELS, self.cfg.MM_STEP, self.cfg.MM_ORDER_SIZE)
        log.info("[mm]   Интервал: %dс, Макс. маркетов: %d",
                 self.cfg.MM_POLL_INTERVAL, self.cfg.MM_MAX_MARKETS)
        log.info("[mm]   Активных маркетов: %d", len(self._markets))
        log.info("=" * 60)

        # Запускаем WS мониторинг ликвидности параллельно
        try:
            from mm_ws_monitor import LiquidityMonitor
            self._ws_monitor = LiquidityMonitor(on_danger=self._on_ws_danger)
            for cid, mkt in self._markets.items():
                self._ws_monitor.subscribe(mkt["token_yes"], cid)
            asyncio.ensure_future(self._ws_monitor.run())
            log.info("[mm] WS Liquidity Monitor запущен")
        except Exception as e:
            log.warning("[mm] WS Monitor не запущен: %s", e)

        while self._running:
            try:
                await self.tick()
            except Exception as e:
                log.error("[mm] tick error: %s", e, exc_info=True)
            await asyncio.sleep(self.cfg.MM_POLL_INTERVAL)

    def _on_ws_danger(self, condition_id: str, reason: str, details: dict):
        """Callback от WS монитора — обнаружена опасность."""
        log.warning("[mm] 🚨 WS DANGER %s: %s | %s", condition_id[:16], reason, details)
        # Ставим флаг опасности на 10 сек — в следующем tick ордера будут сняты
        self._danger_flags[condition_id] = time.time() + 10

    async def tick(self):
        self._ticks += 1

        # Cleanup протухших danger flags
        now = time.time()
        expired = [k for k, v in self._danger_flags.items() if now >= v]
        for k in expired:
            del self._danger_flags[k]

        # Cleanup processed_fills кэш (не больше 10000 записей)
        if len(self._processed_fills) > 10000:
            self._processed_fills = set(list(self._processed_fills)[-5000:])

        if not self._markets:
            if self._ticks % 10 == 1:
                log.info("[mm tick %d] Нет активных маркетов — добавьте через UI", self._ticks)
            return

        total_fills = 0
        for cid, mkt in list(self._markets.items()):
            try:
                fills = await self._process_market(cid, mkt)
                total_fills += fills
            except Exception as e:
                log.error("[mm] Ошибка %s: %s", cid[:12], e)

        if self._ticks <= 1 or self._ticks % 5 == 0 or total_fills > 0:
            n_orders = sum(len(m["bid_orders"]) + len(m["ask_orders"])
                           for m in self._markets.values())
            log.info("[mm tick %d] %d маркетов, %d ордеров live, %d fills",
                     self._ticks, len(self._markets), n_orders, total_fills)

    # ──────────────────────────────────────────────────────────────────────────
    # Per-market processing
    # ──────────────────────────────────────────────────────────────────────────

    async def _process_market(self, cid: str, mkt: dict) -> int:
        """Обрабатывает один маркет. Возвращает кол-во fills."""
        # 0a. Paused — ордера снимаются, ничего не делаем
        if mkt.get("paused"):
            if mkt["bid_orders"] or mkt["ask_orders"]:
                await self._cancel_all_orders(mkt)
                mkt["last_quote_mid"] = 0
                log.info("[mm] ⏸ %s: PAUSED — orders cancelled", mkt["question"][:25])
            return 0

        # 0b. Prematch Only — проверяем не начался ли матч
        if mkt.get("prematch_only"):
            game_start = mkt.get("game_start_time", 0)
            if game_start and time.time() >= game_start:
                if mkt["bid_orders"] or mkt["ask_orders"]:
                    await self._cancel_all_orders(mkt)
                    mkt["last_quote_mid"] = 0
                    log.info("[mm] ⏸ %s: PREMATCH ONLY — match started, orders cancelled",
                             mkt["question"][:25])
                return 0
            # Также проверяем через CLOB API — market может стать accepting_orders=false
            if not mkt.get("_live_checked") or self._ticks % 30 == 0:
                try:
                    client = self.pm._get_client()
                    m_info = client.get_market(condition_id=cid)
                    accepting = m_info.get("accepting_orders", True)
                    gst = m_info.get("game_start_time", "")
                    if gst:
                        from datetime import datetime, timezone
                        try:
                            dt = datetime.fromisoformat(gst.replace("Z", "+00:00"))
                            mkt["game_start_time"] = dt.timestamp()
                        except Exception:
                            pass
                    if not accepting:
                        if mkt["bid_orders"] or mkt["ask_orders"]:
                            await self._cancel_all_orders(mkt)
                            mkt["last_quote_mid"] = 0
                        log.info("[mm] ⏸ %s: not accepting orders — cancelled", mkt["question"][:25])
                        return 0
                    mkt["_live_checked"] = True
                except Exception:
                    pass

        # 0c. WS danger flag — немедленно снять ордера
        danger_until = self._danger_flags.get(cid, 0)
        if time.time() < danger_until:
            if mkt["bid_orders"] or mkt["ask_orders"]:
                log.warning("[mm] 🚨 WS DANGER active for %s — cancel ALL!", mkt["question"][:30])
                await self._cancel_all_orders(mkt)
                mkt["last_quote_mid"] = 0
            return 0

        # 1. Получить mid/spread через price API (order book ненадёжен для Sports)
        try:
            client = self.pm._get_client()
            mid_resp = client.get_midpoint(mkt["token_yes"])
            spread_resp = client.get_spread(mkt["token_yes"])
            price_buy = client.get_price(mkt["token_yes"], "BUY")
            price_sell = client.get_price(mkt["token_yes"], "SELL")

            mid_val = float(mid_resp.get("mid", 0)) if isinstance(mid_resp, dict) else float(mid_resp or 0)
            spread_val = float(spread_resp.get("spread", 1)) if isinstance(spread_resp, dict) else float(spread_resp or 1)
            best_bid = float(price_buy.get("price", 0)) if isinstance(price_buy, dict) else float(price_buy or 0)
            best_ask = float(price_sell.get("price", 0)) if isinstance(price_sell, dict) else float(price_sell or 0)
        except Exception as e:
            log.warning("[mm] price API error %s: %s", cid[:12], e)
            return 0

        if mid_val <= 0 or mid_val >= 1:
            log.warning("[mm] %s: invalid mid=%.4f", mkt["question"][:25], mid_val)
            return 0

        tick = float(mkt["tick_size"])
        decimals = len(mkt["tick_size"].split(".")[-1])
        mid = round(mid_val, decimals)
        mkt["mid"] = mid
        mkt["_best_bid"] = best_bid
        mkt["_best_ask"] = best_ask
        spread_ticks = round(spread_val / tick) if tick > 0 else 0

        # ── Circuit Breaker 1: спред слишком широкий ──────────
        panic_threshold = self.cfg.MM_SPREAD_PANIC

        # ── Circuit Breaker 2: резкий скачок mid цены (событие в игре) ──
        prev_mid = mkt.get("_prev_mid", mid)
        mkt["_prev_mid"] = mid
        if prev_mid > 0 and abs(mid - prev_mid) / prev_mid > 0.03:  # >3% за один тик
            if mkt["bid_orders"] or mkt["ask_orders"]:
                log.warning("[mm] 🚨 MID JUMP %s: %.2f→%.2f (%.1f%%) — cancel ALL! (event detected)",
                            mkt["question"][:30], prev_mid, mid,
                            (mid - prev_mid) / prev_mid * 100)
                await self._cancel_all_orders(mkt)
                mkt["last_quote_mid"] = 0
                mkt["_cooldown_until"] = time.time() + 15  # 15 сек пауза
                mkt["_mid_history"] = []  # reset trend tracking
            return 0

        # ── Circuit Breaker 3: тренд mid (монотонное движение) ──────
        # Если mid двигается в одну сторону 5+ тиков подряд → тренд = убираем ордера
        mid_history = mkt.get("_mid_history", [])
        mid_history.append(mid)
        if len(mid_history) > 8:
            mid_history = mid_history[-8:]
        mkt["_mid_history"] = mid_history

        if len(mid_history) >= 5:
            diffs = [mid_history[i+1] - mid_history[i] for i in range(len(mid_history)-1)]
            recent = diffs[-4:]  # последние 4 изменения
            all_up = all(d > 0.001 for d in recent)
            all_down = all(d < -0.001 for d in recent)
            if all_up or all_down:
                total_move = abs(mid_history[-1] - mid_history[-5])
                if total_move > 0.02 and (mkt["bid_orders"] or mkt["ask_orders"]):
                    direction = "UP" if all_up else "DOWN"
                    log.warning("[mm] 🚨 TREND %s: mid trending %s (%.2f→%.2f, Δ%.2f) — cancel ALL!",
                                mkt["question"][:30], direction, mid_history[-5], mid,
                                total_move)
                    await self._cancel_all_orders(mkt)
                    mkt["last_quote_mid"] = 0
                    mkt["_cooldown_until"] = time.time() + 10
                    mkt["_mid_history"] = []
                    return 0

        # ── Circuit Breaker 4: спред резко расширился ──────────────
        prev_spread = mkt.get("_prev_spread_ticks", spread_ticks)
        mkt["_prev_spread_ticks"] = spread_ticks
        if prev_spread > 0 and prev_spread <= 3 and spread_ticks >= prev_spread * 3:
            if mkt["bid_orders"] or mkt["ask_orders"]:
                log.warning("[mm] 🚨 SPREAD JUMP %s: spread %d→%d ticks — cancel ALL!",
                            mkt["question"][:30], prev_spread, spread_ticks)
                await self._cancel_all_orders(mkt)
                mkt["last_quote_mid"] = 0
                mkt["_cooldown_until"] = time.time() + 10
            return 0

        # Cooldown после circuit breaker
        if time.time() < mkt.get("_cooldown_until", 0):
            return 0

        if panic_threshold > 0 and spread_ticks > panic_threshold:
            if mkt["bid_orders"] or mkt["ask_orders"]:
                log.warning("[mm] 🚨 PANIC %s: spread=%d ticks (>%d) — cancel ALL orders! bid=%.2f ask=%.2f",
                            mkt["question"][:30], spread_ticks, panic_threshold, best_bid, best_ask)
                await self._cancel_all_orders(mkt)
                mkt["last_quote_mid"] = 0
            else:
                log.info("[mm] ⏸ %s: spread=%d ticks (>%d) — bid=%.2f ask=%.2f mid=%.2f",
                         mkt["question"][:30], spread_ticks, panic_threshold,
                         best_bid, best_ask, mid)
            return 0

        # 2. Проверить fills
        fills = await self._check_fills(cid, mkt)

        # ── Синхронизация позиции с реальными балансами PM ──
        real_yes = self._get_token_balance(mkt["token_yes"])
        real_no = self._get_token_balance(mkt["token_no"])
        if abs(mkt["yes_shares"] - real_yes) > 1 or abs(mkt["no_shares"] - real_no) > 1:
            log.info("[mm] 🔄 Sync %s: mem YES=%.0f→%.0f, NO=%.0f→%.0f",
                     mkt["question"][:25], mkt["yes_shares"], real_yes, mkt["no_shares"], real_no)
            mkt["yes_shares"] = real_yes
            mkt["no_shares"] = real_no

        # ── Max Position: не принимать на перегруженную сторону ──
        net = mkt["yes_shares"] - mkt["no_shares"]
        max_pos = self.cfg.MM_MAX_POSITION
        position_blocked = False
        total_shares = mkt["yes_shares"] + mkt["no_shares"]
        if max_pos > 0:
            # Блок по net позиции (перекос)
            if abs(net) >= max_pos:
                position_blocked = True
            # Блок по общему размеру позиции (обе стороны)
            if total_shares >= max_pos * 3:
                position_blocked = True
            if position_blocked and not mkt.get("_pos_warned"):
                log.warning("[mm] ⛔ %s: net=%+.0f total=%.0f (max=%+.0f) — blocking",
                            mkt["question"][:25], net, total_shares, max_pos)
                mkt["_pos_warned"] = True
            elif not position_blocked:
                mkt["_pos_warned"] = False

        # 3. Нужен ли requote?
        threshold = self.cfg.MM_REQUOTE_THRESHOLD * tick

        if mkt["last_quote_mid"] == 0:
            await self._place_ladder(cid, mkt, mid, position_blocked, net)
        elif abs(mid - mkt["last_quote_mid"]) >= threshold:
            log.info("[mm] %s requote: mid %.2f → %.2f",
                     mkt["question"][:30], mkt["last_quote_mid"], mid)
            await self._cancel_all_orders(mkt)
            await self._place_ladder(cid, mkt, mid, position_blocked, net)
        elif fills > 0:
            await self._cancel_all_orders(mkt)
            await self._place_ladder(cid, mkt, mid, position_blocked, net)

        return fills

    async def _get_mid(self, token_id: str) -> Optional[float]:
        """Получить mid price через CLOB."""
        try:
            mid = self.pm.get_midpoint(token_id)
            return mid
        except Exception as e:
            log.debug("[mm] get_mid error: %s", e)
            return None

    # ──────────────────────────────────────────────────────────────────────────
    # Fill detection
    # ──────────────────────────────────────────────────────────────────────────

    async def _check_fills(self, cid: str, mkt: dict) -> int:
        """Проверяет заполненные ордера. Возвращает кол-во fills.
        Ордера хранятся как (order_id, price, size, order_type).
        order_type: 'buy_yes' | 'sell_no' | 'buy_no' | 'sell_yes'
        """
        fills = 0
        now = time.time()
        client = self.pm._get_client()

        # Fee rate (кешируем в mkt)
        if "_fee_rate" not in mkt:
            mkt["_fee_rate"] = self.pm.get_fee_rate(mkt["token_yes"])
        fee_rate = mkt["_fee_rate"]

        # ── Fill velocity tracking ──────────────────────
        vel = self._fill_velocity.get(cid, [])
        vel = [t for t in vel if now - t < 10]  # последние 10 сек
        self._fill_velocity[cid] = vel
        if len(vel) >= 5:
            # Слишком много fills за короткий период — опасность, pause
            log.warning("[mm] 🚨 FILL VELOCITY %s: %d fills in 10s — cancel ALL + cooldown!",
                        mkt["question"][:25], len(vel))
            await self._cancel_all_orders(mkt)
            mkt["last_quote_mid"] = 0
            mkt["_cooldown_until"] = now + 15
            self._fill_velocity[cid] = []
            return 0

        # Проверяем bid ордера
        remaining_bids = []
        for entry in mkt["bid_orders"]:
            oid, price, size = entry[0], entry[1], entry[2]
            otype = entry[3] if len(entry) > 3 else "buy_yes"

            # Дедупликация — не обрабатывать один fill дважды
            if oid in self._processed_fills:
                continue

            try:
                info = client.get_order(oid)
                status = (info.get("status") or "").upper()
                matched = float(info.get("size_matched") or info.get("sizeMatched") or 0)

                if status in ("MATCHED", "FILLED") or (matched > 0 and status != "LIVE"):
                    self._processed_fills.add(oid)
                    vel.append(now)
                    fills += 1
                    mkt["fills_count"] += 1

                    if otype == "sell_no":
                        # Продали NO → уменьшаем NO позицию, получаем proceeds
                        shares = matched  # size_matched = shares для SELL
                        no_sell_price = round(1.0 - price, 2)  # цена NO токена
                        mkt["no_shares"] = max(0, mkt["no_shares"] - shares)
                        # Записываем с ценой NO (то что реально продали)
                        f_usdc = round(abs(no_sell_price * shares) * fee_rate, 4)
                        self.db.mm_record_fill(cid, mkt["token_no"], "no_sell",
                                               no_sell_price, -shares, oid, "bid_sell_no",
                                               mkt["question"], mkt["event"], fee_rate, f_usdc)
                        log.info("[mm] 📤 BID SELL NO: %s | -%.0f NO sh @ NO=%.2f | pos: %.0f NO",
                                 mkt["question"][:25], shares, no_sell_price, mkt["no_shares"])
                    else:
                        # BUY YES → увеличиваем YES позицию
                        shares = matched / price if price > 0 else matched
                        mkt["yes_shares"] += shares
                        mkt["total_cost"] += round(matched, 4)
                        f_usdc = round(abs(price * shares) * fee_rate, 4)
                        self.db.mm_record_fill(cid, mkt["token_yes"], "yes",
                                               price, shares, oid, "bid_fill",
                                               mkt["question"], mkt["event"], fee_rate, f_usdc)
                        log.info("[mm] 📥 BID fill: %s | +%.0f YES sh @ %.2f | pos: %.0f YES",
                                 mkt["question"][:25], shares, price, mkt["yes_shares"])
                else:
                    remaining_bids.append(entry)
            except Exception as e:
                remaining_bids.append(entry)
                log.debug("[mm] check bid %s: %s", oid[:12], e)
        mkt["bid_orders"] = remaining_bids

        # Проверяем ask ордера
        remaining_asks = []
        for entry in mkt["ask_orders"]:
            oid, price, size = entry[0], entry[1], entry[2]
            otype = entry[3] if len(entry) > 3 else "buy_no"

            if oid in self._processed_fills:
                continue

            try:
                info = client.get_order(oid)
                status = (info.get("status") or "").upper()
                matched = float(info.get("size_matched") or info.get("sizeMatched") or 0)

                if status in ("MATCHED", "FILLED") or (matched > 0 and status != "LIVE"):
                    self._processed_fills.add(oid)
                    vel.append(now)
                    fills += 1
                    mkt["fills_count"] += 1

                    if otype == "sell_yes":
                        # Продали YES → уменьшаем YES позицию
                        shares = matched
                        ask_price = round(1.0 - price, 2)
                        mkt["yes_shares"] = max(0, mkt["yes_shares"] - shares)
                        f_usdc = round(abs(ask_price * shares) * fee_rate, 4)
                        self.db.mm_record_fill(cid, mkt["token_yes"], "yes_sell",
                                               ask_price, -shares, oid, "ask_sell_yes",
                                               mkt["question"], mkt["event"], fee_rate, f_usdc)
                        log.info("[mm] 📤 ASK SELL YES: %s | -%.0f YES sh @ %.2f | pos: %.0f YES",
                                 mkt["question"][:25], shares, ask_price, mkt["yes_shares"])
                    else:
                        # BUY NO → увеличиваем NO позицию
                        no_price = price
                        shares = matched / no_price if no_price > 0 else matched
                        mkt["no_shares"] += shares
                        mkt["total_cost"] += round(matched, 4)
                        f_usdc = round(abs(no_price * shares) * fee_rate, 4)
                        self.db.mm_record_fill(cid, mkt["token_no"], "no",
                                               no_price, shares, oid, "ask_fill",
                                               mkt["question"], mkt["event"], fee_rate, f_usdc)
                        ask_price = round(1.0 - no_price, 2)
                        log.info("[mm] 📥 ASK fill: %s | +%.0f NO sh @ NO=%.2f (YES=%.2f) | pos: %.0f NO",
                                 mkt["question"][:25], shares, no_price, ask_price, mkt["no_shares"])
                else:
                    remaining_asks.append(entry)
            except Exception as e:
                remaining_asks.append(entry)
                log.debug("[mm] check ask %s: %s", oid[:12], e)
        mkt["ask_orders"] = remaining_asks

        return fills

    # ──────────────────────────────────────────────────────────────────────────
    # Ladder placement
    # ──────────────────────────────────────────────────────────────────────────

    async def _place_ladder(self, cid: str, mkt: dict, mid: float,
                            position_blocked: bool = False, net: float = 0):
        """Размещает лесенку bid/ask с учётом anchor, skew и SELL логики."""
        levels = self.cfg.MM_LEVELS
        tick = float(mkt["tick_size"])
        step = self.cfg.MM_STEP * tick
        order_size = self.cfg.MM_ORDER_SIZE
        decimals = len(mkt["tick_size"].split(".")[-1])
        anchor = getattr(self.cfg, "MM_ANCHOR", "mid")

        # Округляем mid до tick_size
        mid = round(round(mid / tick) * tick, decimals)

        # ── Anchor: определяем базовые bid/ask ────────────────
        # best_bid / best_ask хранятся из _process_market
        best_bid = mkt.get("_best_bid", mid - tick)
        best_ask = mkt.get("_best_ask", mid + tick)

        if anchor == "spread":
            bid_base = best_bid
            ask_base = best_ask
        elif anchor == "spread1":
            bid_base = round(best_bid + tick, decimals)
            ask_base = round(best_ask - tick, decimals)
        else:  # mid
            bid_base = mid
            ask_base = round(mid + tick, decimals)

        # Защита: bid_base < ask_base, минимум 1 тик спред
        if bid_base >= ask_base:
            # Откатываемся к mid с минимальным спредом
            bid_base = round(mid - tick / 2, decimals) if mid > tick else tick
            ask_base = round(bid_base + tick, decimals)
            bid_base = round(round(bid_base / tick) * tick, decimals)
            ask_base = round(round(ask_base / tick) * tick, decimals)
            if bid_base >= ask_base:
                ask_base = round(bid_base + tick, decimals)

        # ── Skew-балансировка (по $ value, не по shares) ────
        # $ value = shares × mid_price (сколько стоит позиция в $)
        yes_value = mkt["yes_shares"] * mid if mid > 0 else 0
        no_value = mkt["no_shares"] * (1.0 - mid) if mid < 1 else 0
        net_value = yes_value - no_value  # $ перекос
        net = mkt["yes_shares"] - mkt["no_shares"]  # shares перекос (для логов)
        skew_step = self.cfg.MM_SKEW_STEP if self.cfg.MM_SKEW_STEP > 0 else 50
        skew_max = self.cfg.MM_SKEW_MAX

        skew_ticks = min(int(abs(net_value) / skew_step), skew_max)
        skew_offset = skew_ticks * tick

        if net_value > 0:  # перевес в YES → сдвигаем bid дальше (меньше покупаем YES)
            bid_shift = skew_offset
            ask_shift = -skew_offset
        elif net_value < 0:  # перевес в NO → сдвигаем ask дальше
            bid_shift = -skew_offset
            ask_shift = skew_offset
        else:
            bid_shift = 0
            ask_shift = 0

        if skew_ticks > 0:
            log.info("[mm] ⚖ %s: net=$%+.0f (%+.0f sh), skew=%d ticks (bid %+.2f, ask %+.2f)",
                     mkt["question"][:25], net_value, net, skew_ticks, -bid_shift, ask_shift)

        # ── SELL логика: проверяем РЕАЛЬНЫЙ баланс на PM ──────────
        # MM_SELL_ENABLED=false → только BUY, без продажи имеющихся shares
        from dotenv import load_dotenv
        load_dotenv(override=True)
        sell_enabled = os.getenv("MM_SELL_ENABLED", "true").lower() in ("true", "1", "yes")

        min_sell_shares = order_size
        if sell_enabled:
            real_yes = self._get_token_balance(mkt["token_yes"])
            real_no = self._get_token_balance(mkt["token_no"])
            sellable_yes = real_yes if real_yes >= min_sell_shares else 0
            sellable_no = real_no if real_no >= min_sell_shares else 0
        else:
            sellable_yes = 0
            sellable_no = 0

        total_shares = mkt["yes_shares"] + mkt["no_shares"]
        total_blocked = position_blocked and total_shares >= (self.cfg.MM_MAX_POSITION * 3)
        block_buy_yes = position_blocked and (net > 0 or total_blocked)
        block_buy_no = position_blocked and (net < 0 or total_blocked)

        # ── Защита от экстремальных цен (конец матча / resolved) ──
        if mid > 0.95 or mid < 0.05:
            log.debug("[mm] Skip %s: mid=%.2f too extreme", mkt["question"][:25], mid)
            if mkt["bid_orders"] or mkt["ask_orders"]:
                await self._cancel_all_orders(mkt)
                log.info("[mm] ⏸ %s: mid=%.2f (>0.95 or <0.05) — cancelled all, market decided",
                         mkt["question"][:25], mid)
            return

        # ── BID: лучшая цена первая (i=0 ближе к mid) ────────
        for i in range(0, levels):
            bid_price = round(bid_base - i * step - bid_shift, decimals)
            if bid_price < tick or bid_price > 0.95:
                continue
            try:
                shares_for_level = round(order_size / bid_price, 2) if bid_price > 0 else 0

                sell_placed = False
                if sellable_no >= shares_for_level and shares_for_level >= 5:
                    sell_no_price = round(1.0 - bid_price, decimals)
                    res = await self.pm.place_sell_order(
                        token_id=mkt["token_no"], price=sell_no_price,
                        size=shares_for_level, neg_risk=mkt["neg_risk"],
                        tick_size=mkt["tick_size"],
                    )
                    if res.success and res.bet_id:
                        mkt["bid_orders"].append((res.bet_id, bid_price, order_size, "sell_no"))
                        sellable_no -= shares_for_level
                        sell_placed = True
                        log.info("[mm] 📤 bid SELL NO %.0f sh @ %.2f (YES=%.2f)",
                                 shares_for_level, sell_no_price, bid_price)
                    else:
                        log.warning("[mm] bid SELL NO failed @ %.2f: %s — fallback to BUY YES",
                                    sell_no_price, res.error if hasattr(res,'error') else 'unknown')
                if not sell_placed and not block_buy_yes:
                    res = await self.pm.place_order(
                        token_id=mkt["token_yes"], price=bid_price,
                        size=order_size, neg_risk=mkt["neg_risk"],
                        tick_size=mkt["tick_size"],
                    )
                    if res.success and res.bet_id:
                        mkt["bid_orders"].append((res.bet_id, bid_price, order_size, "buy_yes"))
            except Exception as e:
                log.warning("[mm] bid error @ %.2f: %s", bid_price, e)

        # ── ASK: лучшая цена первая (i=0 ближе к mid) ────────
        for i in range(0, levels):
            ask_yes_price = round(ask_base + i * step + ask_shift, decimals)
            no_price = round(1.0 - ask_yes_price, decimals)
            if no_price < tick or no_price > 0.95 or ask_yes_price > 0.95:
                continue
            try:
                shares_for_level = round(order_size / ask_yes_price, 2) if ask_yes_price > 0 else 0

                sell_placed = False
                if sellable_yes >= shares_for_level and shares_for_level >= 5:
                    res = await self.pm.place_sell_order(
                        token_id=mkt["token_yes"], price=ask_yes_price,
                        size=shares_for_level, neg_risk=mkt["neg_risk"],
                        tick_size=mkt["tick_size"],
                    )
                    if res.success and res.bet_id:
                        mkt["ask_orders"].append((res.bet_id, no_price, order_size, "sell_yes"))
                        sellable_yes -= shares_for_level
                        sell_placed = True
                        log.info("[mm] 📤 ask SELL YES %.0f sh @ %.2f",
                                 shares_for_level, ask_yes_price)
                    else:
                        log.warning("[mm] ask SELL YES failed @ %.2f: %s — fallback to BUY NO",
                                    ask_yes_price, res.error if hasattr(res,'error') else 'unknown')
                if not sell_placed and not block_buy_no:
                    res = await self.pm.place_order(
                        token_id=mkt["token_no"], price=no_price,
                        size=order_size, neg_risk=mkt["neg_risk"],
                        tick_size=mkt["tick_size"],
                    )
                    if res.success and res.bet_id:
                        mkt["ask_orders"].append((res.bet_id, no_price, order_size, "buy_no"))
            except Exception as e:
                log.warning("[mm] ask error @ %.2f: %s", ask_yes_price, e)

        mkt["last_quote_mid"] = mid
        n_bids = len(mkt["bid_orders"])
        n_asks = len(mkt["ask_orders"])
        if n_bids + n_asks > 0:
            bid_prices = [entry[1] for entry in mkt["bid_orders"]]
            ask_prices = [round(1.0 - entry[1], 2) for entry in mkt["ask_orders"]]
            log.info("[mm] 📊 %s: mid=%.2f bids=%s asks=%s | pos: YES=%.0f NO=%.0f net=%+.0f",
                     mkt["question"][:30], mid,
                     [f"{p:.2f}" for p in bid_prices],
                     [f"{p:.2f}" for p in ask_prices],
                     mkt["yes_shares"], mkt["no_shares"], net)

    async def _cancel_all_orders(self, mkt: dict):
        """Cancel все ордера маркета — используем cancel_market_orders для надёжности."""
        client = self.pm._get_client()
        cancelled = 0
        # Cancel по token_id (YES и NO) — снимает ВСЕ наши ордера на маркете
        for token in [mkt["token_yes"], mkt["token_no"]]:
            try:
                client.cancel_market_orders(asset_id=token)
                cancelled += 1
            except Exception as e:
                log.debug("[mm] cancel_market_orders error for %s: %s", token[:16], e)
                # Fallback: cancel по ID
                for entry in mkt["bid_orders"] + mkt["ask_orders"]:
                    try:
                        self.pm.cancel_order(entry[0])
                    except Exception:
                        pass
                break
        if cancelled:
            log.debug("[mm] Cancelled all orders for %s", mkt["question"][:25])
        mkt["bid_orders"] = []
        mkt["ask_orders"] = []

    # ──────────────────────────────────────────────────────────────────────────

    def stop(self):
        """Остановить бот. Ордера cancel при следующем tick или вручную."""
        self._running = False
        log.info("[mm] Market Maker остановлен")

    async def stop_and_cancel(self):
        """Остановить и cancel все ордера."""
        self._running = False
        for mkt in self._markets.values():
            await self._cancel_all_orders(mkt)
        log.info("[mm] Market Maker остановлен, все ордера cancelled")
