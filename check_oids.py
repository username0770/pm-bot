#!/usr/bin/env python3
import os, sqlite3

def load_env():
    for f in ['.env','../arb_bot/.env','../arb_bot/../.env']:
        if os.path.exists(f):
            env = {}
            for line in open(f, encoding='utf-8', errors='ignore'):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k,v = line.split('=',1)
                    env[k.strip()] = v.strip().strip("'\"")
            return env
    return {}

env = load_env()
DB = env.get('DB_PATH_VALUEBET','valuebet.db')
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Точно те же ID что видны в дашборде
rows = conn.execute("""
    SELECT id, outcome_id, order_id, home, away, bet_mode, status, outcome_result
    FROM bets WHERE id IN (2062,2061,2060,2059,2058,2057,2055,2054,2053)
    ORDER BY id DESC
""").fetchall()

print(f"{'ID':<6} {'OID_LEN':<8} {'OID_TAIL':<25} {'STATUS':<12} {'MATCH'}")
for r in rows:
    oid = r['outcome_id'] or ''
    print(f"{r['id']:<6} {len(oid):<8} {oid[-25:] if oid else 'EMPTY':<25} {r['status']:<12} {r['home']} vs {r['away']}")

# Итого
totals = conn.execute("""
    SELECT 
        COUNT(*) total,
        SUM(CASE WHEN outcome_id IS NULL OR outcome_id='' THEN 1 ELSE 0 END) no_oid,
        SUM(CASE WHEN outcome_id IS NOT NULL AND outcome_id!='' THEN 1 ELSE 0 END) has_oid
    FROM bets
    WHERE status IN ('placed','pending') AND outcome_result='pending'
""").fetchone()
print(f"\nИтого pending: {totals['total']}, с OID: {totals['has_oid']}, без OID: {totals['no_oid']}")
conn.close()