#!/usr/bin/env python3
import os, sqlite3, json, urllib.request, time

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

# Берём 5 разных ставок с outcome_id
rows = conn.execute("""
    SELECT id, outcome_id, order_id, home, away, stake_price, created_at, bet_mode
    FROM bets
    WHERE status IN ('placed','pending') AND outcome_result='pending'
      AND outcome_id IS NOT NULL AND outcome_id != ''
    ORDER BY id DESC LIMIT 5
""").fetchall()

print("=== Проверка outcome_id через Gamma ===\n")
for r in rows:
    oid = r['outcome_id']
    print(f"#{r['id']} [{r['bet_mode']}] {r['home']} vs {r['away']}")
    print(f"  outcome_id (полный): {oid}")
    print(f"  order_id: {r['order_id']}")
    try:
        mkt = fetch(f"https://gamma-api.polymarket.com/markets?clobTokenIds={oid}&limit=1")
        if mkt and isinstance(mkt, list) and mkt:
            m = mkt[0]
            print(f"  Gamma question: {m.get('question','')}")
            print(f"  Gamma closed={m.get('closed')} active={m.get('active')} resolved={m.get('resolved')}")
            print(f"  Gamma outcomePrices={m.get('outcomePrices')}")
            print(f"  Gamma clobTokenIds={str(m.get('clobTokenIds',''))[:80]}")
        else:
            print(f"  Gamma: пустой ответ")
    except Exception as e:
        print(f"  Gamma ошибка: {e}")
    print()
    time.sleep(0.5)

# Теперь проверяем positions API — что там реально есть
print("\n=== Positions API (реальные активные позиции) ===\n")
try:
    pos = fetch(f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0.00001&limit=20")
    if isinstance(pos, list):
        print(f"Всего позиций: {len(pos)}")
        for p in pos[:5]:
            asset = p.get('asset','')
            print(f"  asset=...{asset[-20:]} | curPrice={p.get('curPrice')} | redeemable={p.get('redeemable')} | title={str(p.get('title',''))[:50]}")
except Exception as e:
    print(f"Positions ошибка: {e}")

# Сравниваем: есть ли совпадение между outcome_id в БД и asset в positions
print("\n=== Совпадение БД outcome_id vs Positions asset ===\n")
try:
    all_pos = fetch(f"https://data-api.polymarket.com/positions?user={FUNDER}&sizeThreshold=0.00001&limit=500")
    pos_assets = {str(p.get('asset','')): p for p in all_pos if p.get('asset')}
    
    db_oids = conn.execute("""
        SELECT id, outcome_id, home, away FROM bets
        WHERE status IN ('placed','pending') AND outcome_result='pending'
          AND outcome_id IS NOT NULL AND outcome_id != ''
        LIMIT 50
    """).fetchall()
    
    matched = [(r['id'], r['outcome_id']) for r in db_oids if r['outcome_id'] in pos_assets]
    not_matched = [r['id'] for r in db_oids if r['outcome_id'] not in pos_assets]
    
    print(f"Positions assets: {len(pos_assets)}")
    print(f"БД ставок с outcome_id: {len(db_oids)}")
    print(f"Совпадений: {len(matched)}")
    print(f"Не совпадают: {len(not_matched)}")
    if matched:
        r_id, oid = matched[0]
        p = pos_assets[oid]
        print(f"\nПример совпадения: #{r_id} → {p.get('title','')[:60]}")
        print(f"  curPrice={p.get('curPrice')} redeemable={p.get('redeemable')}")
except Exception as e:
    print(f"Ошибка: {e}")

conn.close()