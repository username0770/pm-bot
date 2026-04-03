#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
diagnose_pending.py — диагностика нерасчитанных ставок
Запуск: python diagnose_pending.py
"""
import os, sqlite3, urllib.request, json, time
from datetime import datetime, timezone

def load_env():
    for f in ['.env', '../.env']:
        if os.path.exists(f):
            env = {}
            for line in open(f, encoding='utf-8', errors='ignore'):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip().strip("'\"")
            return env
    return {}

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

env  = load_env()
DB   = env.get('DB_PATH_VALUEBET', 'valuebet.db')
FUNDER = (env.get('POLYMARKET_FUNDER','') or '').lower()

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT id, bet_mode, status, outcome_result, outcome_id, order_id,
           stake, stake_price, home, away, created_at, started_at
    FROM bets
    WHERE status IN ('placed','pending') AND outcome_result='pending'
    ORDER BY id DESC LIMIT 20
""").fetchall()

print(f"\n{'='*70}")
print(f"Нерасчитанных ставок (топ 20 из БД):")
print(f"{'='*70}")
now_ts = time.time()
for r in rows:
    oid = (r['outcome_id'] or '')
    age_h = (now_ts - (r['started_at'] or now_ts)) / 3600
    print(f"  #{r['id']} [{r['bet_mode']}] {r['home']} vs {r['away']}")
    print(f"      status={r['status']} | created={r['created_at']}")
    print(f"      started_at_h={age_h:.1f}h назад | stake_price={r['stake_price']}")
    print(f"      outcome_id={'...'+oid[-20:] if len(oid)>5 else 'ПУСТО'}")
    print(f"      order_id={r['order_id'] and r['order_id'][:20] or 'ПУСТО'}")
    print()

# Итого
totals = conn.execute("""
    SELECT bet_mode,
           COUNT(*) as cnt,
           SUM(CASE WHEN outcome_id IS NULL OR outcome_id='' THEN 1 ELSE 0 END) as no_oid
    FROM bets
    WHERE status IN ('placed','pending') AND outcome_result='pending'
    GROUP BY bet_mode
""").fetchall()
print(f"{'='*70}")
print("Итого нерасчитанных:")
for t in totals:
    print(f"  {t['bet_mode'] or 'pre'}: {t['cnt']} ставок, без outcome_id: {t['no_oid']}")

# Проверяем одну ставку с outcome_id через Gamma
print(f"\n{'='*70}")
print("Проверяем 3 ставки с outcome_id через Gamma API:")
sample = conn.execute("""
    SELECT id, outcome_id, home, away, stake, stake_price
    FROM bets
    WHERE status IN ('placed','pending') AND outcome_result='pending'
      AND outcome_id IS NOT NULL AND outcome_id != ''
    ORDER BY id DESC LIMIT 3
""").fetchall()
for r in sample:
    oid = r['outcome_id']
    try:
        mkt = fetch(f"https://gamma-api.polymarket.com/markets?clobTokenIds={oid}&limit=1")
        if mkt and isinstance(mkt, list):
            m = mkt[0]
            print(f"  #{r['id']} {r['home']} vs {r['away']}")
            print(f"    closed={m.get('closed')} active={m.get('active')} resolved={m.get('resolved')}")
            print(f"    outcomePrices={m.get('outcomePrices')} question={str(m.get('question',''))[:60]}")
        else:
            print(f"  #{r['id']} — Gamma не нашла рынок (пустой ответ)")
        time.sleep(0.3)
    except Exception as e:
        print(f"  #{r['id']} — ошибка Gamma: {e}")

conn.close()