# -*- coding: utf-8 -*-
"""
Dutching Bot — Internal Arbitrage на Polymarket Sports.

Сканирует спортивные маркеты на Polymarket, находит пары где
ask_YES + ask_NO < 1.00 (гарантированная прибыль) и ставит обе стороны.

На большинстве спортивных маркетов Polymarket комиссия = 0%
(кроме NCAAB и Serie A), что делает даже маленький спред прибыльным.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass

from config import Config
from db_bets import BetDatabase
from gamma_client import GammaClient
from polymarket_client import PolymarketClient

log = logging.getLogger(__name__)

# Спорты с комиссией тейкера — учитываем при расчёте
FEE_SPORTS = {"ncaa", "march-madness", "college-basketball", "serie-a"}
FEE_RATE = 0.0175  # макс effective ~0.44% при 50%


@dataclass
class DutchOpportunity:
    """Найденная возможность для dutching."""
    condition_id: str
    token_yes: str
    token_no: str
    question: str
    event_name: str
    sport: str
    neg_risk: bool
    ask_yes: float
    ask_no: float
    spread: float       # 1.0 - (ask_yes + ask_no)
    spread_pct: float   # spread / total_cost * 100
    liq_yes: float      # $ ликвидность на стороне YES
    liq_no: float       # $ ликвидность на стороне NO
    shares: float       # кол-во shares на каждую сторону
    cost_total: float   # shares * (ask_yes + ask_no)
    profit_expected: float  # shares * spread


class DutchingBot:
    """Бот для internal arbitrage (dutching) на Polymarket."""

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

        # Dedup: condition_id → timestamp
        self._dutched_markets: dict[str, float] = {}
        self._load_active_from_db()

        # Pending orders: {pair_id: {yes_oid, no_oid, cancel_at, ...}}
        self._active_pairs: dict[str, dict] = {}

        # Counters
        self._ticks = 0
        self._pairs_found = 0
        self._pairs_placed = 0
        self._running = True

    def _load_active_from_db(self):
        """Загрузить активные dutching пары из БД в кэш."""
        try:
            rows = self.db.conn.execute(
                "SELECT DISTINCT market_id FROM bets "
                "WHERE bet_mode='dutching' AND status IN ('placed','pending')"
            ).fetchall()
            for r in rows:
                self._dutched_markets[r[0]] = time.time()
            if rows:
                log.info("Загружено %d активных dutching маркетов из БД", len(rows))
        except Exception as e:
            log.debug("_load_active_from_db: %s", e)

    async def run(self):
        """Основной цикл бота."""
        sports = [s.strip() for s in self.cfg.DUTCH_SPORTS.split(",") if s.strip()]
        log.info("=" * 60)
        log.info("⚡ Dutching бот запущен")
        log.info("   Спорты: %s", ", ".join(sports))
        log.info("   Min spread: %.2f%%", self.cfg.DUTCH_MIN_SPREAD * 100)
        log.info("   Min liquidity: $%.0f", self.cfg.DUTCH_MIN_LIQUIDITY)
        log.info("   Stake: $%.2f (max $%.2f)", self.cfg.DUTCH_STAKE, self.cfg.DUTCH_MAX_STAKE)
        log.info("   Poll: %ds, TTL: %ds", self.cfg.DUTCH_POLL_INTERVAL, self.cfg.DUTCH_ORDER_TTL_SECS)
        log.info("=" * 60)

        while self._running:
            try:
                await self.tick()
            except Exception as e:
                log.error("[dutch] Критическая ошибка в tick: %s", e, exc_info=True)
            await asyncio.sleep(self.cfg.DUTCH_POLL_INTERVAL)

    async def tick(self):
        self._ticks += 1
        await self._monitor_orders()

        # Сканируем маркеты
        opportunities = await self._scan_all_sports()
        if opportunities:
            log.info("[dutch tick %d] Найдено %d возможностей", self._ticks, len(opportunities))
            for opp in opportunities:
                await self._execute_dutch(opp)

    # ──────────────────────────────────────────────────────────────────────────
    # Сканирование
    # ──────────────────────────────────────────────────────────────────────────

    async def _scan_all_sports(self) -> list[DutchOpportunity]:
        """Сканирует все спорты, возвращает список возможностей."""
        sports = [s.strip() for s in self.cfg.DUTCH_SPORTS.split(",") if s.strip()]
        all_opps = []
        total_events = 0
        total_markets = 0
        checked_books = 0

        for sport in sports:
            try:
                opps, n_events, n_markets, n_books = await self._scan_sport(sport)
                total_events += n_events
                total_markets += n_markets
                checked_books += n_books
                all_opps.extend(opps)
            except Exception as e:
                log.warning("[dutch] Ошибка сканирования %s: %s", sport, e)

        log.info("[dutch tick %d] Просканировано: %d спортов, %d событий, %d маркетов, "
                 "%d order books → %d возможностей",
                 self._ticks, len(sports), total_events, total_markets,
                 checked_books, len(all_opps))

        # Сортируем по спреду (лучшие первые)
        all_opps.sort(key=lambda o: o.spread_pct, reverse=True)
        return all_opps

    async def _scan_sport(self, sport: str) -> tuple[list[DutchOpportunity], int, int, int]:
        """Сканирует один спорт через Gamma API. Returns (opps, n_events, n_markets, n_books)."""
        events = await asyncio.to_thread(self.gamma.get_events, tag=sport, limit=100)
        if not events:
            return [], 0, 0, 0

        opportunities = []
        n_markets = 0
        n_books = 0

        for event in events:
            markets_raw = event.get("markets", [])
            if not markets_raw:
                continue

            event_title = event.get("title", "")

            for mkt_raw in markets_raw:
                n_markets += 1
                try:
                    opp, checked_book = await self._check_market(mkt_raw, event_title, sport)
                    if checked_book:
                        n_books += 1
                    if opp:
                        opportunities.append(opp)
                except Exception as e:
                    log.debug("[dutch] check_market error: %s", e)

        if opportunities:
            log.info("[dutch] %s: %d событий, %d маркетов → %d возможностей",
                     sport, len(events), n_markets, len(opportunities))

        return opportunities, len(events), n_markets, n_books

    async def _check_market(self, mkt_raw: dict, event_title: str,
                            sport: str) -> tuple[DutchOpportunity | None, bool]:
        """Проверяет один маркет на возможность dutching. Returns (opp, checked_orderbook)."""
        condition_id = mkt_raw.get("conditionId", mkt_raw.get("condition_id", ""))
        if not condition_id:
            return None, False

        # Dedup
        if condition_id in self._dutched_markets:
            return None, False

        # Парсим token IDs
        tokens_raw = mkt_raw.get("clobTokenIds", "")
        if isinstance(tokens_raw, str):
            try:
                tokens = json.loads(tokens_raw) if tokens_raw else []
            except (json.JSONDecodeError, TypeError):
                tokens = []
        elif isinstance(tokens_raw, list):
            tokens = tokens_raw
        else:
            tokens = []

        if len(tokens) < 2:
            return None, False

        token_yes = tokens[0]
        token_no = tokens[1]

        # Pre-filter: проверяем outcomePrices (быстро, без CLOB)
        prices_raw = mkt_raw.get("outcomePrices", "")
        if prices_raw:
            try:
                if isinstance(prices_raw, str):
                    prices = json.loads(prices_raw)
                else:
                    prices = prices_raw
                if isinstance(prices, list) and len(prices) >= 2:
                    p_sum = float(prices[0]) + float(prices[1])
                    if p_sum >= (1.0 - self.cfg.DUTCH_MIN_SPREAD):
                        return None, False  # Спред слишком маленький
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

        # DB dedup
        if self.db.already_dutched(condition_id):
            self._dutched_markets[condition_id] = time.time()
            return None, False

        neg_risk = mkt_raw.get("negRisk", False)
        question = mkt_raw.get("question", "")

        # Проверяем реальный order book через CLOB
        opp = await self._evaluate_orderbook(
            condition_id, token_yes, token_no,
            question, event_title, sport, neg_risk
        )
        return opp, True  # True = we checked the order book

    async def _evaluate_orderbook(
        self, condition_id: str, token_yes: str, token_no: str,
        question: str, event_title: str, sport: str, neg_risk: bool
    ) -> DutchOpportunity | None:
        """Проверяет order book обеих сторон через CLOB API."""
        try:
            client = self.pm._get_client()
            book_yes = await asyncio.to_thread(client.get_order_book, token_yes)
            book_no = await asyncio.to_thread(client.get_order_book, token_no)
        except Exception as e:
            log.debug("[dutch] order book error %s: %s", condition_id[:12], e)
            return None

        # Извлекаем asks (предложения на продажу — то что мы покупаем)
        asks_yes = book_yes.asks if hasattr(book_yes, "asks") else []
        asks_no = book_no.asks if hasattr(book_no, "asks") else []

        if not asks_yes or not asks_no:
            return None

        # Best ask (самая низкая цена продажи)
        best_ask_yes = float(asks_yes[0].price if hasattr(asks_yes[0], "price")
                             else asks_yes[0].get("price", 0))
        best_ask_no = float(asks_no[0].price if hasattr(asks_no[0], "price")
                            else asks_no[0].get("price", 0))

        if best_ask_yes <= 0 or best_ask_no <= 0:
            return None

        total_cost = best_ask_yes + best_ask_no
        spread = 1.0 - total_cost

        if spread <= 0:
            return None

        spread_pct = spread / total_cost * 100

        # Учитываем комиссию для NCAAB/Serie A
        if sport in FEE_SPORTS:
            # Макс fee = FEE_RATE * price * (1 - price) для каждой стороны
            fee_yes = FEE_RATE * best_ask_yes * (1 - best_ask_yes)
            fee_no = FEE_RATE * best_ask_no * (1 - best_ask_no)
            effective_spread = spread - fee_yes - fee_no
            if effective_spread <= 0:
                return None
            spread = effective_spread
            spread_pct = spread / total_cost * 100

        # Проверяем минимальный спред
        if spread_pct < self.cfg.DUTCH_MIN_SPREAD * 100:
            return None

        # Ликвидность ($ доступно на best ask)
        liq_yes = float(asks_yes[0].size if hasattr(asks_yes[0], "size")
                        else asks_yes[0].get("size", 0)) * best_ask_yes
        liq_no = float(asks_no[0].size if hasattr(asks_no[0], "size")
                       else asks_no[0].get("size", 0)) * best_ask_no

        if liq_yes < self.cfg.DUTCH_MIN_LIQUIDITY or liq_no < self.cfg.DUTCH_MIN_LIQUIDITY:
            return None

        # Расчёт ставки: равное кол-во shares на обе стороны
        budget = min(self.cfg.DUTCH_STAKE, self.cfg.DUTCH_MAX_STAKE)
        shares = budget / total_cost
        shares = round(shares, 2)

        if shares < 5:
            return None  # PM минимум ~5 shares

        cost_total = round(shares * total_cost, 2)
        profit_expected = round(shares * spread, 4)

        return DutchOpportunity(
            condition_id=condition_id,
            token_yes=token_yes,
            token_no=token_no,
            question=question,
            event_name=event_title,
            sport=sport,
            neg_risk=neg_risk,
            ask_yes=best_ask_yes,
            ask_no=best_ask_no,
            spread=spread,
            spread_pct=spread_pct,
            liq_yes=liq_yes,
            liq_no=liq_no,
            shares=shares,
            cost_total=cost_total,
            profit_expected=profit_expected,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Размещение ордеров
    # ──────────────────────────────────────────────────────────────────────────

    async def _execute_dutch(self, opp: DutchOpportunity):
        """Размещает пару ордеров YES + NO."""
        pair_id = f"dutch_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        log.info("[dutch] ⚡ %s | %s", opp.event_name, opp.question)
        log.info("[dutch]   YES=%.2f NO=%.2f spread=%.2f%% shares=%.2f cost=$%.2f profit=$%.4f",
                 opp.ask_yes, opp.ask_no, opp.spread_pct, opp.shares,
                 opp.cost_total, opp.profit_expected)

        # Размер ордера в USDC (price * shares)
        size_yes = round(opp.shares * opp.ask_yes, 2)
        size_no = round(opp.shares * opp.ask_no, 2)

        # Проверяем баланс
        try:
            free = self.db.get_free_usdc()
            if free < opp.cost_total:
                log.warning("[dutch] Недостаточно баланса: $%.2f < $%.2f",
                            free, opp.cost_total)
                return
        except Exception:
            pass

        # Размещаем ОБА ордера
        try:
            res_yes = await self.pm.place_order(
                token_id=opp.token_yes,
                price=opp.ask_yes,
                size=size_yes,
                neg_risk=opp.neg_risk,
            )
        except Exception as e:
            log.error("[dutch] YES order error: %s", e)
            return

        try:
            res_no = await self.pm.place_order(
                token_id=opp.token_no,
                price=opp.ask_no,
                size=size_no,
                neg_risk=opp.neg_risk,
            )
        except Exception as e:
            log.error("[dutch] NO order error: %s", e)
            # Cancel YES если NO не прошёл
            if res_yes.success and res_yes.bet_id:
                try:
                    self.pm.cancel_order(res_yes.bet_id)
                except Exception:
                    pass
            return

        # Обе ноги не прошли
        if not res_yes.success and not res_no.success:
            log.warning("[dutch] Обе ноги отклонены: YES=%s NO=%s",
                        res_yes.error, res_no.error)
            return

        # Одна нога не прошла — cancel другую
        if not res_yes.success or not res_no.success:
            failed = "YES" if not res_yes.success else "NO"
            success_res = res_no if not res_yes.success else res_yes
            log.warning("[dutch] %s нога отклонена — cancel другую", failed)
            if success_res.bet_id:
                try:
                    self.pm.cancel_order(success_res.bet_id)
                except Exception:
                    pass
            return

        # Обе ноги размещены — записываем в БД
        cancel_at = time.time() + self.cfg.DUTCH_ORDER_TTL_SECS

        # YES leg
        yes_id = self._insert_dutch_leg(
            opp, pair_id, "Yes", opp.token_yes, opp.ask_yes,
            opp.shares, res_yes.bet_id
        )
        # NO leg
        no_id = self._insert_dutch_leg(
            opp, pair_id, "No", opp.token_no, opp.ask_no,
            opp.shares, res_no.bet_id
        )

        # Вычитаем из баланса
        self.db.adjust_free_usdc(-opp.cost_total)

        # В трекер
        self._active_pairs[pair_id] = {
            "yes_oid": res_yes.bet_id,
            "no_oid": res_no.bet_id,
            "yes_rec": yes_id,
            "no_rec": no_id,
            "cancel_at": cancel_at,
            "cost": opp.cost_total,
            "shares": opp.shares,
            "ask_yes": opp.ask_yes,
            "ask_no": opp.ask_no,
            "condition_id": opp.condition_id,
        }

        self._dutched_markets[opp.condition_id] = time.time()
        self._pairs_placed += 1

        log.info("[dutch] ✅ Пара %s размещена: YES=%s NO=%s | cost=$%.2f exp_profit=$%.4f",
                 pair_id[:16], res_yes.bet_id[:16], res_no.bet_id[:16],
                 opp.cost_total, opp.profit_expected)

    def _insert_dutch_leg(self, opp: DutchOpportunity, pair_id: str,
                          side: str, token_id: str, price: float,
                          shares: float, order_id: str) -> int:
        """Записывает одну ногу dutching в БД."""
        self.db.conn.execute("""
            INSERT INTO bets (
                created_at, outcome_id, market_id, home, away, league,
                outcome_name, stake, stake_price, status, order_id,
                placed_at, bet_mode, dutch_pair_id, outcome_result
            ) VALUES (
                datetime('now'), ?, ?, ?, ?, ?,
                ?, ?, ?, 'placed', ?,
                datetime('now'), 'dutching', ?, 'pending'
            )
        """, (
            token_id, opp.condition_id,
            opp.event_name, "", opp.sport,
            f"{side} — {opp.question}",
            shares, price, order_id, pair_id,
        ))
        self.db.conn.commit()
        rec_id = self.db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        return rec_id

    # ──────────────────────────────────────────────────────────────────────────
    # Мониторинг ордеров
    # ──────────────────────────────────────────────────────────────────────────

    async def _monitor_orders(self):
        """Проверяет статус активных dutching пар."""
        if not self._active_pairs:
            return

        now = time.time()
        to_remove = []

        for pair_id, info in list(self._active_pairs.items()):
            try:
                yes_info = await asyncio.to_thread(
                    self.pm._get_client().get_order, info["yes_oid"])
                no_info = await asyncio.to_thread(
                    self.pm._get_client().get_order, info["no_oid"])

                yes_status = (yes_info.get("status") or "").upper()
                no_status = (no_info.get("status") or "").upper()
                yes_filled = yes_status in ("MATCHED", "FILLED")
                no_filled = no_status in ("MATCHED", "FILLED")

                yes_matched = float(yes_info.get("size_matched") or
                                    yes_info.get("sizeMatched") or 0)
                no_matched = float(no_info.get("size_matched") or
                                   no_info.get("sizeMatched") or 0)

                # Обе ноги заполнены
                if yes_filled and no_filled:
                    proceeds = min(yes_matched, no_matched)  # гарантированный payout
                    cost = (yes_matched * info["ask_yes"]) + (no_matched * info["ask_no"])
                    profit = round(proceeds - cost, 4)

                    self._settle_pair(info, "settled", profit)
                    self.db.adjust_free_usdc(proceeds)  # payout (одна сторона = $1 * shares)
                    log.info("[dutch] 💰 Пара %s заполнена! profit=$%.4f", pair_id[:16], profit)
                    to_remove.append(pair_id)
                    continue

                # TTL истёк
                if now >= info["cancel_at"]:
                    if yes_filled and not no_filled:
                        # YES filled, NO не — cancel NO, YES остаётся
                        try:
                            self.pm.cancel_order(info["no_oid"])
                        except Exception:
                            pass
                        cost_yes = round(yes_matched * info["ask_yes"], 2)
                        self._settle_leg(info["yes_rec"], "placed", 0)  # YES ждёт результат
                        self._settle_leg(info["no_rec"], "cancelled", 0)
                        # Возвращаем cost NO
                        cost_no_planned = round(info["shares"] * info["ask_no"], 2)
                        self.db.adjust_free_usdc(cost_no_planned)
                        log.warning("[dutch] ⏱ Пара %s: YES filled, NO cancelled (TTL)",
                                    pair_id[:16])
                    elif no_filled and not yes_filled:
                        try:
                            self.pm.cancel_order(info["yes_oid"])
                        except Exception:
                            pass
                        self._settle_leg(info["no_rec"], "placed", 0)
                        self._settle_leg(info["yes_rec"], "cancelled", 0)
                        cost_yes_planned = round(info["shares"] * info["ask_yes"], 2)
                        self.db.adjust_free_usdc(cost_yes_planned)
                        log.warning("[dutch] ⏱ Пара %s: NO filled, YES cancelled (TTL)",
                                    pair_id[:16])
                    else:
                        # Обе не заполнены — cancel обе
                        for oid in [info["yes_oid"], info["no_oid"]]:
                            try:
                                self.pm.cancel_order(oid)
                            except Exception:
                                pass
                        self._settle_leg(info["yes_rec"], "cancelled", 0)
                        self._settle_leg(info["no_rec"], "cancelled", 0)
                        self.db.adjust_free_usdc(info["cost"])
                        log.info("[dutch] ⏱ Пара %s: обе ноги cancelled (TTL)", pair_id[:16])

                    # Разблокируем маркет для повторной попытки
                    cid = info.get("condition_id", "")
                    if cid in self._dutched_markets:
                        del self._dutched_markets[cid]

                    to_remove.append(pair_id)

            except Exception as e:
                log.error("[dutch] monitor error %s: %s", pair_id[:16], e)

        for pid in to_remove:
            self._active_pairs.pop(pid, None)

    def _settle_pair(self, info: dict, status: str, profit: float):
        """Закрывает обе ноги пары."""
        half_profit = round(profit / 2, 4)
        self._settle_leg(info["yes_rec"], status, half_profit)
        self._settle_leg(info["no_rec"], status, half_profit)

    def _settle_leg(self, rec_id: int, status: str, profit: float):
        """Обновляет статус одной ноги."""
        try:
            result = "void" if status == "cancelled" else "pending"
            if status == "settled":
                result = "won"  # одна из сторон выиграет
            self.db.conn.execute(
                "UPDATE bets SET status=?, outcome_result=?, profit_actual=? WHERE id=?",
                (status, result, profit, rec_id)
            )
            self.db.conn.commit()
        except Exception as e:
            log.error("[dutch] settle_leg error #%d: %s", rec_id, e)

    def stop(self):
        """Остановить бот."""
        self._running = False
        log.info("[dutch] Бот остановлен")
