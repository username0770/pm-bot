#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_cancelled_results.py

Находит все ставки где status=cancelled/failed но outcome_result=won/lost
(авторасчёт ошибочно записал результат в отменённый ордер).

Сбрасывает их в outcome_result=void, profit_actual=0.

Запуск: python fix_cancelled_results.py
"""
import os, sqlite3, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("fix_cancelled")

def load_env():
    env = {}
    if not os.path.exists('.env'): return env
    for line in open('.env', encoding='utf-8', errors='ignore'):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip("'\"")
    return env

DB = load_env().get('DB_PATH_VALUEBET', 'valuebet.db')
if not os.path.exists(DB):
    log.error("БД не найдена: %s", DB); exit(1)

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Находим все проблемные ставки
rows = conn.execute("""
    SELECT id, status, outcome_result, profit_actual,
           home, away, outcome_name, bet_mode, error_msg
    FROM bets
    WHERE status IN ('cancelled', 'failed')
      AND outcome_result IN ('won', 'lost')
    ORDER BY id
""").fetchall()

if not rows:
    log.info("✓ Проблемных ставок нет")
    conn.close()
    exit(0)

log.info("Найдено %d ставок с неправильным результатом:", len(rows))
log.info("")

total_false_profit = 0.0
for r in rows:
    total_false_profit += float(r['profit_actual'] or 0)
    log.info("  #%d [%s] %s vs %s / %s  status=%s → outcome=%s  profit=%+.2f  err=%s",
             r['id'], r['bet_mode'] or 'pre',
             r['home'] or '?', r['away'] or '?', r['outcome_name'] or '?',
             r['status'], r['outcome_result'],
             float(r['profit_actual'] or 0),
             r['error_msg'] or '')

log.info("")
log.info("Суммарный ложный P&L: %+.2f$", total_false_profit)
log.info("")

# Применяем исправление
conn.execute("""
    UPDATE bets
    SET outcome_result = 'void',
        profit_actual  = 0,
        settled_at     = ''
    WHERE status IN ('cancelled', 'failed')
      AND outcome_result IN ('won', 'lost')
""")
conn.commit()

log.info("✅ Исправлено %d ставок → outcome_result=void, profit_actual=0", len(rows))
log.info("   P&L скорректирован на %+.2f$", -total_false_profit)
conn.close()