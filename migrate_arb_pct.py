#!/usr/bin/env python3
import sqlite3, os

db_path = "valuebet.db"

# Читаем путь из .env
if os.path.exists(".env"):
    try:
        for line in open(".env", encoding="utf-8", errors="ignore"):
            line = line.strip()
            if line.startswith("DB_PATH_VALUEBET="):
                path = line.split("=", 1)[1].strip().strip("'\"")
                if os.path.exists(path):
                    db_path = path
                break
    except Exception:
        pass

if not os.path.exists(db_path):
    print(f"BD ne naydena: {db_path}")
    exit(1)

print(f"BD: {db_path}")
conn = sqlite3.connect(db_path)
existing = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
print(f"Tekushie kolonki ({len(existing)}): {sorted(existing)}")

migrations = [
    ("arb_pct",  "REAL DEFAULT 0"),
    ("bet_mode", "TEXT DEFAULT 'prematch'"),
]

added = []
for col, defn in migrations:
    if col not in existing:
        conn.execute(f"ALTER TABLE bets ADD COLUMN {col} {defn}")
        added.append(col)
        print(f"+ Dobavlena: {col}")
    else:
        print(f"= Uzhe est: {col}")

conn.commit()

# Проверяем результат
final = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
print(f"\nRezultat: arb_pct={'DA' if 'arb_pct' in final else 'NET'}, bet_mode={'DA' if 'bet_mode' in final else 'NET'}")
conn.close()

if added:
    print(f"Gotovo! Dobavleny: {added}")
    print("Perezapusti dashboard_server.py")
else:
    print("Vse kolonki uzhe byli — nichego ne izmenilos")