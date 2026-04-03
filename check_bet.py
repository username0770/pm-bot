#!/usr/bin/env python3
import os, sqlite3, json, urllib.request

def load_env():
    for f in ['.env','../arb_bot/.env']:
        if os.path.exists(f):
            env = {}
            for line in open(f, encoding='utf-8', errors='ignore'):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k,v = line.split('=',1)
                    env[k.strip()] = v.strip().strip("'\"")
            return env
    return {}

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

env = load_env()
DB = env.get('DB_PATH_VALUEBET','valuebet.db')
FUNDER = (env.get('POLYMARKET_FUNDER','') or '').lower()

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

r = conn.execute("SELECT * FROM bets WHERE id=1710").fetchone()
if not r:
    print("Ставка #1710 не найдена")
    exit()

oid = r['outcome_id'] or ''
print(f"=== Ставка #1710 ===")
print(f"  Match:      {r['home']} vs {r['away']}")
print(f"  Created:    {r['created_at']}")
print(f"  outcome_id: {oid}")
print(f"  order_id:   {r['order_id']}")
print(f"  stake_price:{r['stake_price']}")
print(f"  bet_mode:   {r['bet_mode']}")

if not oid:
    print("\n  outcome_id ПУСТОЙ!")
    exit()

print(f"\n=== Проверяем в Positions API ===")
items = fetch(f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0.00001&limit=500")
pos_map = {str(p.get('asset','')): p for p in items}
pos = pos_map.get(oid)
if pos:
    print(f"  НАЙДЕНА: redeemable={pos.get('redeemable')} curPrice={pos.get('curPrice')} title={pos.get('title','')[:60]}")
else:
    print(f"  НЕ НАЙДЕНА в positions (всего позиций: {len(pos_map)})")
    # Проверяем через CLOB order
    print(f"\n=== Проверяем CLOB order ===")
    try:
        order = fetch(f"https://clob.polymarket.com/order/{r['order_id']}")
        print(f"  CLOB: status={order.get('status')} asset_id={str(order.get('asset_id',''))[:30]} size_matched={order.get('size_matched')}")
    except Exception as e:
        print(f"  CLOB error: {e}")

    # Ищем через data-api trades
    print(f"\n=== Ищем в activity/trades ===")
    try:
        trades = fetch(f"https://data-api.polymarket.com/activity?user={FUNDER}&type=TRADE&limit=500")
        matching = [t for t in trades if str(t.get('asset','')) == oid]
        if matching:
            t = matching[0]
            print(f"  Трейд найден: side={t.get('side')} asset={t.get('asset','')[:30]} size={t.get('size')} price={t.get('price')} title={t.get('title','')[:50]}")
        else:
            print(f"  Трейдов с этим asset нет (проверено {len(trades)} трейдов)")
            # Ищем по времени и цене
            ts_bet = 1741826640  # ~2026-03-12 23:44 UTC
            price = float(r['stake_price'] or 0)
            close = [t for t in trades if abs(float(t.get('price',0))-price)<0.01 and abs(t.get('timestamp',0)-ts_bet)<600]
            print(f"  По цене {price} и времени: {len(close)} совпадений")
            for c in close[:3]:
                print(f"    asset={c.get('asset','')[:40]} title={c.get('title','')[:50]}")
    except Exception as e:
        print(f"  trades error: {e}")

conn.close()