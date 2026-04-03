# -*- coding: utf-8 -*-
"""
Hedge Bot — автоматический дельта-нейтральный бот.

Мониторит пары матч+турнир на Polymarket, считает позиции
и исполняет хеджи при достаточном ROI.

Использование:
  python run.py hedge
"""

import asyncio
import logging
from config import Config
from gamma_client import GammaClient
from hedge_calculator import calc_delta_neutral, suggest_exit_prices, validate_hedge_opportunity
from db_hedge import HedgeDatabase
from polymarket_client import PolymarketClient

log = logging.getLogger(__name__)


class HedgeBotRunner:
    def __init__(self, cfg: Config = None):
        self.cfg = cfg or Config()
        self.gamma = GammaClient()
        self.db = HedgeDatabase(self.cfg.HEDGE_DB_PATH)
        self.pm = PolymarketClient(self.cfg.POLYMARKET_PRIVATE_KEY, self.cfg.POLYMARKET_FUNDER)
        self._running = True

    async def run(self):
        """Главный цикл: сканирование + мониторинг."""
        log.info("═" * 50)
        log.info("HedgeBotRunner запущен")
        log.info("  MIN_ROI:      %.1f%%", self.cfg.HEDGE_MIN_ROI * 100)
        log.info("  MAX_BUDGET:   $%.0f", self.cfg.HEDGE_MAX_BUDGET)
        log.info("  BUDGET_PCT:   %.1f%%", self.cfg.HEDGE_BUDGET_PCT * 100)
        log.info("  SCAN_INTERVAL: %ds", self.cfg.HEDGE_SCAN_INTERVAL)
        log.info("  PRICE_INTERVAL: %ds", self.cfg.HEDGE_PRICE_INTERVAL)
        log.info("  AUTO_EXECUTE: %s", self.cfg.HEDGE_AUTO_EXECUTE)
        log.info("  SPORTS:       %s", self.cfg.HEDGE_SPORTS)
        log.info("═" * 50)

        await asyncio.gather(
            self._scan_loop(),
            self._monitor_loop(),
        )

    async def _scan_loop(self):
        """Периодически сканирует Gamma API на новые пары."""
        while self._running:
            try:
                sports = [s.strip() for s in self.cfg.HEDGE_SPORTS.split(",") if s.strip()]
                pairs = self.gamma.find_hedge_pairs(sport_tags=sports)

                new_count = 0
                for p in pairs:
                    row_id = self.db.insert_pair(p)
                    if row_id:
                        new_count += 1
                        log.info("Новая пара: %s — %s vs %s | %s (%s)",
                                 p.sport, p.player_a, p.player_b,
                                 p.tournament_player,
                                 p.tournament_market.question if p.tournament_market else "?")

                if new_count:
                    log.info("Найдено %d новых пар (всего %d)", new_count, len(pairs))

            except Exception as e:
                log.error("Scan error: %s", e)

            await asyncio.sleep(self.cfg.HEDGE_SCAN_INTERVAL)

    async def _monitor_loop(self):
        """Мониторит watching-пары и оценивает ROI."""
        while self._running:
            try:
                # Получаем пары со статусом watching
                watching_pairs = self.db.get_pairs(status="watching")

                for pair in watching_pairs:
                    await self._evaluate_pair(pair)

                # Проверяем активные позиции
                active = self.db.get_active_positions()
                for pos in active:
                    await self._check_position(pos)

            except Exception as e:
                log.error("Monitor error: %s", e)

            await asyncio.sleep(self.cfg.HEDGE_PRICE_INTERVAL)

    async def _evaluate_pair(self, pair: dict):
        """Оценить пару: получить цены, посчитать ROI."""
        try:
            match_token = pair.get("match_token_id", "")
            tourney_token = pair.get("tourney_token_id", "")

            if not match_token or not tourney_token:
                return

            # Получаем текущие цены
            price_a = await asyncio.to_thread(self.pm.get_midpoint, match_token)
            price_b = await asyncio.to_thread(self.pm.get_midpoint, tourney_token)

            if not price_a or not price_b or price_a <= 0 or price_b <= 0:
                return

            # Предлагаем сценарии (дефолтные)
            scenarios = suggest_exit_prices(pair.get("player_a", "Player A"))

            # Считаем бюджет
            balance = await asyncio.to_thread(self.pm.get_balance)
            budget = min(
                float(balance) * self.cfg.HEDGE_BUDGET_PCT,
                self.cfg.HEDGE_MAX_BUDGET,
            )

            if budget < 10:
                return

            # Считаем хедж
            result = calc_delta_neutral(
                price_a, price_b,
                scenarios[0]["exit_b"],  # default exits
                scenarios[0]["exit_b"],
                scenarios[1]["exit_a"],
                scenarios[1]["exit_b"],
                budget,
                scenario_names=(scenarios[0]["name"], scenarios[1]["name"]),
            )

            if not result.is_profitable:
                return

            issues = validate_hedge_opportunity(
                result,
                min_roi=self.cfg.HEDGE_MIN_ROI,
                max_budget=self.cfg.HEDGE_MAX_BUDGET,
            )

            if issues:
                log.debug("Пара %s: %s", pair.get("pair_id"), "; ".join(issues))
                return

            log.info("✅ Хедж-возможность: %s vs %s | ROI=%.1f%% | Profit=$%.2f",
                     pair.get("player_a"), pair.get("player_b"),
                     result.roi_pct, result.profit)

            # Авто-исполнение
            if self.cfg.HEDGE_AUTO_EXECUTE:
                await self._execute_hedge(pair, result, match_token, tourney_token)

        except Exception as e:
            log.error("Evaluate pair error: %s — %s", pair.get("pair_id"), e)

    async def _execute_hedge(self, pair: dict, result, match_token: str, tourney_token: str):
        """Разместить оба ордера."""
        try:
            neg_risk_a = bool(pair.get("match_neg_risk", 0))
            neg_risk_b = bool(pair.get("tourney_neg_risk", 0))

            # Записываем позицию в БД
            pos_id = self.db.insert_position({
                "pair_id": pair.get("pair_id", ""),
                "token_id_a": match_token,
                "entry_price_a": result.cost_a / result.size_a if result.size_a else 0,
                "size_a": result.size_a,
                "cost_a": result.cost_a,
                "neg_risk_a": 1 if neg_risk_a else 0,
                "token_id_b": tourney_token,
                "entry_price_b": result.cost_b / result.size_b if result.size_b else 0,
                "size_b": result.size_b,
                "cost_b": result.cost_b,
                "neg_risk_b": 1 if neg_risk_b else 0,
                "budget": result.total_cost,
                "expected_profit": result.profit,
                "expected_roi": result.roi,
                "scenarios": [s.__dict__ for s in result.scenarios],
            })

            # Ордер A
            price_a = result.cost_a / result.size_a if result.size_a else 0
            res_a = await self.pm.place_order(
                token_id=match_token,
                price=price_a,
                size=result.cost_a,
                neg_risk=neg_risk_a,
            )
            if res_a.success:
                self.db.update_order(pos_id, "a", res_a.bet_id, "placed")
                log.info("Ордер A размещён: %s @ %.2f¢ | $%.2f", match_token[:16], price_a*100, result.cost_a)
            else:
                self.db.update_order(pos_id, "a", "", "failed")
                log.error("Ордер A FAILED: %s", res_a.error)

            # Ордер B
            price_b = result.cost_b / result.size_b if result.size_b else 0
            res_b = await self.pm.place_order(
                token_id=tourney_token,
                price=price_b,
                size=result.cost_b,
                neg_risk=neg_risk_b,
            )
            if res_b.success:
                self.db.update_order(pos_id, "b", res_b.bet_id, "placed")
                log.info("Ордер B размещён: %s @ %.2f¢ | $%.2f", tourney_token[:16], price_b*100, result.cost_b)
            else:
                self.db.update_order(pos_id, "b", "", "failed")
                log.error("Ордер B FAILED: %s", res_b.error)

            # Обновляем статус пары
            self.db.update_pair_status(pair.get("pair_id"), "hedged")

        except Exception as e:
            log.error("Execute hedge error: %s", e)

    async def _check_position(self, pos: dict):
        """Проверить статус активной позиции."""
        try:
            # Проверяем статус ордеров если pending
            if pos.get("order_status_a") == "placed" and pos.get("order_id_a"):
                status = await asyncio.to_thread(
                    self.pm.get_order_status, pos["order_id_a"]
                )
                if status and status.get("status") == "FILLED":
                    self.db.update_order(pos["id"], "a", pos["order_id_a"], "filled")

            if pos.get("order_status_b") == "placed" and pos.get("order_id_b"):
                status = await asyncio.to_thread(
                    self.pm.get_order_status, pos["order_id_b"]
                )
                if status and status.get("status") == "FILLED":
                    self.db.update_order(pos["id"], "b", pos["order_id_b"], "filled")

        except Exception as e:
            log.debug("Check position error: %s — %s", pos.get("id"), e)

    def stop(self):
        self._running = False
