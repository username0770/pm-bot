#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_cancelled_live.py

Пересчитывает исторические лайв ставки со статусом cancelled + outcome_result=pending.
Для каждой запрашивает реальный статус ордера через CLOB API:
  - size_matched > 0 → ставка частично/полностью исполнилась → статус placed, пересчёт cost/stake
  - size_matched = 0 → ставка не исполнилась → outcome_result=void

После этого авто-расчёт (кнопка в дашборде) закроет placed ставки через Gamma/CLOB midpoint.

Запуск: python fix_cancelled_live.py
"""

import os, sys, json, time, sqlite3, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("fix_cancelled")

# ── Читаем .env ───────────────────────────────────────────────────────────────
def load_env(path=".env"):
    env = {}
    if not os.path.exists(path):
        return env
    for line in open(path, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
    return env

env = load_env()
PRIVATE_KEY = env.get("POLYMARKET_PRIVATE_KEY", "")
FUNDER      = env.get("POLYMARKET_FUNDER", "")
DB_PATH     = env.get("DB_PATH_VALUEBET", "valuebet.db")
CLOB_HOST   = "https://clob.polymarket.com"
CHAIN_ID    = 137

if not PRIVATE_KEY or PRIVATE_KEY.startswith("0xYOUR"):
    log.error("POLYMARKET_PRIVATE_KEY не задан в .env")
    sys.exit(1)
if not FUNDER or FUNDER.startswith("0xYOUR"):
    log.error("POLYMARKET_FUNDER не задан в .env")
    sys.exit(1)
if not os.path.exists(DB_PATH):
    log.error("БД не найдена: %s", DB_PATH)
    sys.exit(1)

log.info("БД: %s", DB_PATH)
log.info("Funder: %s...", FUNDER[:12])

# ── CLOB клиент ───────────────────────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    client = ClobClient(
        host           = CLOB_HOST,
        key            = PRIVATE_KEY,
        chain_id       = CHAIN_ID,
        signature_type = 1,
        funder         = FUNDER,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    log.info("✓ CLOB клиент готов")
except Exception as e:
    log.error("Не удалось создать CLOB клиент: %s", e)
    sys.exit(1)

# ── БД ────────────────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Выбираем все cancelled лайв ставки у которых outcome_result ещё pending
rows = conn.execute("""
    SELECT id, order_id, stake, stake_price, cost_usdc, outcome_id,
           home, away, outcome_name, created_at
    FROM bets
    WHERE status = 'cancelled'
      AND outcome_result = 'pending'
      AND bet_mode = 'live'
      AND order_id IS NOT NULL AND order_id != ''
    ORDER BY id
""").fetchall()

log.info("Найдено %d cancelled лайв ставок для проверки", len(rows))
if not rows:
    log.info("Нечего исправлять — выход")
    sys.exit(0)

# ── Статистика ────────────────────────────────────────────────────────────────
stats = {"voided": 0, "restored": 0, "partial": 0, "errors": 0}

now_ts = time.strftime("%Y-%m-%dT%H:%M:%S")

for row in rows:
    bet_id    = row["id"]
    order_id  = row["order_id"]
    orig_stake = float(row["stake"] or 0)
    orig_price = float(row["stake_price"] or 0)
    orig_cost  = float(row["cost_usdc"] or 0)

    try:
        resp = client.get_order(order_id)
    except Exception as e:
        log.warning("  #%d order=%s... ошибка API: %s", bet_id, order_id[:16], e)
        stats["errors"] += 1
        continue

    if not resp:
        log.warning("  #%d order=%s... пустой ответ", bet_id, order_id[:16])
        stats["errors"] += 1
        continue

    status_raw     = (resp.get("status") or "").lower()
    size_matched   = float(resp.get("size_matched")   or resp.get("sizeMatched")   or 0)
    size_remaining = float(resp.get("size_remaining") or resp.get("sizeRemaining") or 0)
    price_filled   = float(resp.get("price") or orig_price or 0)

    log.info("  #%d [%s vs %s] %s | order=%s... | status=%s matched=%.4f remaining=%.4f",
             bet_id, row["home"] or "?", row["away"] or "?", row["outcome_name"] or "",
             order_id[:16], status_raw, size_matched, size_remaining)

    if size_matched <= 0.001:
        # Ничего не исполнилось — помечаем void (не влияет на P&L)
        conn.execute("""
            UPDATE bets SET outcome_result='void', profit_actual=0, settled_at=?
            WHERE id=?
        """, (now_ts, bet_id))
        log.info("    → void (0 исполнено)")
        stats["voided"] += 1

    else:
        # Что-то исполнилось — восстанавливаем как placed ставку
        real_cost = round(size_matched * price_filled, 2)
        is_full   = size_remaining <= 0.001 or status_raw in ("matched", "filled")
        label     = "полностью" if is_full else f"частично ({size_matched:.4f} из {orig_stake:.4f})"

        conn.execute("""
            UPDATE bets
            SET status      = 'placed',
                stake       = ?,
                cost_usdc   = ?,
                stake_price = ?,
                error_msg   = ''
            WHERE id = ?
        """, (size_matched, real_cost, price_filled, bet_id))

        log.info("    → placed (%s) | stake=%.4f cost=$%.2f", label, size_matched, real_cost)

        if is_full:
            stats["restored"] += 1
        else:
            stats["partial"] += 1

    time.sleep(0.3)  # не спамим API

conn.commit()
conn.close()

log.info("")
log.info("═" * 50)
log.info("Готово!")
log.info("  void     : %d (не исполнились)", stats["voided"])
log.info("  restored : %d (полностью исполнились → placed)", stats["restored"])
log.info("  partial  : %d (частично исполнились → placed)", stats["partial"])
log.info("  errors   : %d", stats["errors"])
log.info("")
log.info("Теперь нажми ⚡ АВТО-РАСЧЁТ в дашборде чтобы закрыть placed ставки через Gamma/CLOB")