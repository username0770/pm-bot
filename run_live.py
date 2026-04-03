# -*- coding: utf-8 -*-
"""
Точка запуска LIVE Value-Bet бота.

Запуск:
    python run_live.py

Требует в .env:
    BETBURGER_FILTER_ID_LIVE=1308405   ← фильтр для лайв велью-бетов
    LV_POLL_INTERVAL=5                 ← интервал опроса (сек)
    LV_ORDER_TTL_SECS=30               ← TTL ордера до автоотмены
    # Остальные LV_* настройки — см. .env.example

Для запуска прематч-бота используй run.py
"""

import asyncio
import logging
import sys
import os

# ── Логирование ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── Загружаем .env ────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
    log.info(".env загружен")
except ImportError:
    log.warning("python-dotenv не установлен — читаем переменные окружения напрямую")


def main():
    log.info("=" * 60)
    log.info("⚡ POLYBOT — LIVE mode")
    log.info("=" * 60)

    # Проверяем обязательный параметр
    filter_id = os.getenv("BETBURGER_FILTER_ID_LIVE", "")
    if not filter_id or filter_id == "0":
        log.error("Не задан BETBURGER_FILTER_ID_LIVE в .env!")
        log.error("Добавь строку: BETBURGER_FILTER_ID_LIVE=1308405")
        sys.exit(1)

    try:
        from live_bot import LiveValueBetBot
        bot = LiveValueBetBot()
    except ValueError as e:
        log.error("Ошибка конфигурации: %s", e)
        sys.exit(1)
    except Exception as e:
        log.error("Ошибка инициализации бота: %s", e, exc_info=True)
        sys.exit(1)

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("\n⚡ LIVE бот остановлен пользователем")
    except Exception as e:
        log.error("Критическая ошибка: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()