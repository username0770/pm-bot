"""
Единый запуск обоих ботов:
  - ArbBot    — вилки PS3838 + Polymarket
  - ValueBetBot — вэлью-беты только на Polymarket

Можно запускать оба вместе или по отдельности.

Использование:
  python run.py           # оба бота
  python run.py arb       # только вилки
  python run.py value     # только вэлью
  python run.py stats     # статистика и выход
"""

import asyncio
import logging
import sys
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("runner")


async def run_both():
    from main import ArbBot
    from valuebet_bot import ValueBetBot

    cfg = Config()
    arb = ArbBot()
    vb = ValueBetBot(cfg)

    log.info("🚀 Запуск: ArbBot + ValueBetBot")
    await asyncio.gather(
        arb.run(),
        vb.run(),
    )


async def run_arb_only():
    from main import ArbBot
    log.info("🚀 Запуск: только ArbBot (вилки)")
    await ArbBot().run()


async def run_value_only():
    from valuebet_bot import ValueBetBot
    log.info("🚀 Запуск: только ValueBetBot (вэлью)")
    await ValueBetBot().run()


def show_stats():
    from db import Database
    from db_valuebet import ValueBetDatabase
    from config import Config

    cfg = Config()

    print("\n" + "="*50)
    print("СТАТИСТИКА ВИЛОК (PS3838 + Polymarket)")
    print("="*50)
    db_arb = Database(cfg.DB_PATH)
    db_arb.print_stats()

    print("\n" + "="*50)
    print("СТАТИСТИКА ВЭЛЬЮ-БЕТОВ (Polymarket only)")
    print("="*50)
    db_vb = ValueBetDatabase(cfg.DB_PATH_VALUEBET)
    for period in ["today", "7d", "30d", "all"]:
        db_vb.print_stats(period)

    print("\nНЕЗАКРЫТЫЕ ВЭЛЬЮ-БЕТЫ:")
    db_vb.print_pending()


def settle_valuebet(uid: str, won: bool):
    """Закрыть вэлью-бет вручную: python run.py settle <uid> won/lost"""
    from db_valuebet import ValueBetDatabase
    db = ValueBetDatabase(Config().DB_PATH_VALUEBET)
    db.settle_bet(uid, won)
    result = "✅ ВЫИГРАНО" if won else "❌ ПРОИГРАНО"
    print(f"Ставка {uid}: {result}")
    db.print_stats("all")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args or args[0] == "both":
        asyncio.run(run_both())

    elif args[0] == "arb":
        asyncio.run(run_arb_only())

    elif args[0] == "value":
        asyncio.run(run_value_only())

    elif args[0] == "stats":
        show_stats()

    elif args[0] == "hedge":
        from hedge_bot import HedgeBotRunner
        log.info("🚀 Запуск: HedgeBotRunner (дельта-нейтральный хедж)")
        asyncio.run(HedgeBotRunner(Config()).run())

    elif args[0] == "settle" and len(args) == 3:
        # python run.py settle abc123 won
        uid = args[1]
        won = args[2].lower() in ("won", "win", "yes", "1", "true")
        settle_valuebet(uid, won)

    else:
        print(__doc__)
