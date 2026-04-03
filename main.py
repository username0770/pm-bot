"""
Arbitrage Bot: BetBurger + PS3838 (Pinnacle) + Polymarket
Находит вилки через BetBurger API, ставит на PS3838 и Polymarket
"""

import asyncio
import logging
from config import Config
from betburger_client import BetBurgerClient
from ps3838_client import PS3838Client
from polymarket_client import PolymarketClient
from db import Database
from models import Arb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


class ArbBot:
    def __init__(self):
        self.cfg = Config()
        self.bb = BetBurgerClient(self.cfg.BETBURGER_TOKEN, self.cfg.BETBURGER_FILTER_ID)
        self.ps = PS3838Client(self.cfg.PS3838_USERNAME, self.cfg.PS3838_PASSWORD)
        self.pm = PolymarketClient(self.cfg.POLYMARKET_PRIVATE_KEY, self.cfg.POLYMARKET_FUNDER)
        self.db = Database(self.cfg.DB_PATH)
        self.placed_ids: set = set()  # дедупликация

    async def run(self):
        log.info("🤖 Бот запущен. Polling BetBurger каждые %ds", self.cfg.POLL_INTERVAL)
        while True:
            try:
                await self.tick()
            except Exception as e:
                log.error("Ошибка в основном цикле: %s", e)
            await asyncio.sleep(self.cfg.POLL_INTERVAL)

    async def tick(self):
        arbs = await self.bb.get_arbs()
        log.info("BetBurger вернул %d вилок", len(arbs))

        for arb in arbs:
            # Пропускаем уже обработанные
            if arb.uid in self.placed_ids:
                continue

            # Фильтрация: нужны только вилки с Polymarket + PS3838
            if not arb.has_polymarket_leg or not arb.has_ps3838_leg:
                continue

            # Минимальный ROI
            if arb.roi < self.cfg.MIN_ROI:
                continue

            # Минимальная ликвидность на Polymarket
            if arb.polymarket_liquidity < self.cfg.MIN_LIQUIDITY:
                log.debug("Пропуск %s: ликвидность $%.2f < $%d",
                          arb.event_name, arb.polymarket_liquidity, self.cfg.MIN_LIQUIDITY)
                continue

            log.info(
                "✅ Вилка: %s | ROI: %.2f%% | Ликв: $%.0f",
                arb.event_name, arb.roi * 100, arb.polymarket_liquidity
            )

            await self.place_arb(arb)

    async def place_arb(self, arb: "Arb"):
        stake = self.calc_stake(arb)
        if stake < self.cfg.MIN_STAKE:
            log.info("Ставка слишком мала: $%.2f, пропуск", stake)
            return

        # Размещаем обе ставки параллельно
        try:
            ps_task = asyncio.create_task(
                self.ps.place_bet(
                    event_id=arb.ps3838_event_id,
                    market_id=arb.ps3838_market_id,
                    selection=arb.ps3838_selection,
                    odds=arb.ps3838_odds,
                    stake=stake * arb.ps3838_stake_ratio
                )
            )
            pm_task = asyncio.create_task(
                self.pm.place_order(
                    token_id=arb.polymarket_token_id,
                    price=arb.polymarket_price,
                    size=stake * arb.polymarket_stake_ratio
                )
            )
            ps_result, pm_result = await asyncio.gather(ps_task, pm_task)

        except Exception as e:
            log.error("Ошибка при размещении вилки %s: %s", arb.event_name, e)
            return

        # Логируем в БД
        self.db.save_arb(arb, stake, ps_result, pm_result)
        self.placed_ids.add(arb.uid)

        log.info(
            "💰 Поставлено: %s | PS3838: $%.2f @ %.3f | PM: $%.2f @ %.3f",
            arb.event_name,
            stake * arb.ps3838_stake_ratio, arb.ps3838_odds,
            stake * arb.polymarket_stake_ratio, 1 / arb.polymarket_price
        )

    def calc_stake(self, arb: "Arb") -> float:
        """
        Размер ставки = min(
          доступная ликвидность на PM (весь лимит),
          1% банкролла
        )
        """
        bankroll = self.db.get_bankroll()
        by_bankroll = bankroll * self.cfg.STAKE_PCT
        by_liquidity = arb.polymarket_liquidity  # берём весь лимит
        return min(by_bankroll, by_liquidity)


if __name__ == "__main__":
    bot = ArbBot()
    asyncio.run(bot.run())
