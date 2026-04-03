#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_live_outcome_ids.py

Восстанавливает outcome_id для лайв ставок через data-api.polymarket.com/activity.
Сопоставляет по: price (stake_price) + timestamp (created_at ± 120 сек).

После восстановления — запускает авторасчёт.

Запуск: python fix_live_outcome_ids.py [--dry-run]
"""
import os, sys, sqlite3, json, urllib.request, time, logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("fix_oids")

DRY_RUN = "--dry-run" in sys.argv

def load_env():
    env = {}
    for f in ['.env', '../.env']:
        if os.path.exists(f):
            for line in open(f, encoding='utf-8', errors='ignore'):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip().strip("'\"")
            break
    return env

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

env = load_env()
DB      = env.get('DB_PATH_VALUEBET', 'valuebet.db')
FUNDER  = (env.get('POLYMARKET_FUNDER', '') or '').lower()

if not os.path.exists(DB):
    log.error("БД не найдена: %s", DB); sys.exit(1)
if not FUNDER:
    log.error("POLYMARKET_FUNDER не задан в .env"); sys.exit(1)

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# ── 1. Ставки без outcome_id ──────────────────────────────────────────────────
rows = conn.execute("""
    SELECT id, order_id, stake_price, bb_price, created_at, home, away, bet_mode
    FROM bets
    WHERE status IN ('placed', 'pending')
      AND outcome_result = 'pending'
      AND (outcome_id IS NULL OR outcome_id = '')
    ORDER BY id DESC
""").fetchall()

log.info("Ставок без outcome_id: %d", len(rows))
if not rows:
    log.info("Нечего исправлять"); sys.exit(0)

# ── 2. Загружаем ВСЕ BUY трейды из data-api ──────────────────────────────────
log.info("Загружаем трейды из Polymarket data-api...")
all_trades = []
limit = 500
offset = 0
for attempt in range(10):
    url = (f"https://data-api.polymarket.com/activity"
           f"?user={FUNDER}&type=TRADE&limit={limit}&offset={offset}")
    try:
        data = fetch(url)
        if not isinstance(data, list) or not data:
            break
        buys = [t for t in data if t.get('side') == 'BUY' and t.get('asset')]
        all_trades.extend(buys)
        log.info("  offset=%d: %d записей (%d BUY)", offset, len(data), len(buys))
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.3)
    except Exception as e:
        log.error("  fetch error: %s", e)
        break

log.info("Загружено BUY трейдов: %d", len(all_trades))
if not all_trades:
    log.error("Нет трейдов — проверьте FUNDER"); sys.exit(1)

# Индекс трейдов по price (округл. до 3 знаков) для быстрого поиска
from collections import defaultdict
trades_by_price = defaultdict(list)
for t in all_trades:
    key = round(float(t.get('price', 0)), 3)
    trades_by_price[key].append(t)

# ── 3. Сопоставление ─────────────────────────────────────────────────────────
matched = 0
not_matched = 0
used_assets = set()  # не дублировать один asset на разные ставки

for row in rows:
    bet_id    = row['id']
    price     = float(row['stake_price'] or row['bb_price'] or 0)
    price_key = round(price, 3)

    # Время создания ставки в UTC unix
    try:
        dt = datetime.fromisoformat(row['created_at'].replace('Z',''))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        bet_ts = dt.timestamp()
    except:
        bet_ts = 0

    candidates = trades_by_price.get(price_key, [])

    # Ищем трейд с минимальной разницей по времени (в пределах 5 минут)
    best = None
    best_diff = 999999
    for t in candidates:
        asset = str(t.get('asset', ''))
        if asset in used_assets:
            continue
        t_ts = float(t.get('timestamp', 0))
        diff = abs(t_ts - bet_ts)
        if diff < best_diff and diff < 300:  # 5 минут окно
            best_diff = diff
            best = (t, asset)

    if best:
        t, asset = best
        used_assets.add(asset)
        log.info("  ✓ #%d %s vs %s | price=%.3f | diff=%ds | asset=...%s",
                 bet_id, row['home'] or '?', row['away'] or '?',
                 price, int(best_diff), asset[-15:])
        if not DRY_RUN:
            conn.execute("UPDATE bets SET outcome_id=? WHERE id=?", (asset, bet_id))
        matched += 1
    else:
        log.warning("  ✗ #%d %s vs %s | price=%.3f | нет совпадения (кандидатов: %d)",
                    bet_id, row['home'] or '?', row['away'] or '?',
                    price, len(candidates))
        not_matched += 1

if not DRY_RUN:
    conn.commit()

log.info("")
log.info("Результат: совпало %d / не найдено %d", matched, not_matched)

if DRY_RUN:
    log.info("DRY RUN — БД не изменена")
    sys.exit(0)

# ── 4. Запускаем авторасчёт ───────────────────────────────────────────────────
log.info("")
log.info("Запускаем авторасчёт...")
sys.path.insert(0, '.')
try:
    from auto_settle import check_and_settle
    from db_bets import BetDatabase
    db2 = BetDatabase(DB)
    n = check_and_settle(db2, FUNDER)
    log.info("Расчитано ставок: %d", n)
except Exception as e:
    log.error("auto_settle error: %s", e)

conn.close()