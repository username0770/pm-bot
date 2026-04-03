#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reconcile_positions.py v3

Полная сверка всех placed+pending ставок с Polymarket.

Шаг 1: Загружаем все позиции с data-api.polymarket.com (по funder адресу)
Шаг 2: Для ставок без outcome_id — восстанавливаем через CLOB order API
Шаг 3: Сверяем outcome_id с позициями → won/lost/open
Шаг 4: Если позиции нет → Gamma API финальная проверка

Запуск:
  python reconcile_positions.py --dry-run   # показать без изменений
  python reconcile_positions.py             # применить
"""

import os, sys, json, time, logging, urllib.request, urllib.error, argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("reconcile")

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true")
args = parser.parse_args()
DRY_RUN = args.dry_run
if DRY_RUN:
    log.info("⚠️  DRY-RUN — изменения в БД не вносятся")

# ── .env ──────────────────────────────────────────────────────────────────────
def load_env(path=".env"):
    env = {}
    if not os.path.exists(path): return env
    for line in open(path, encoding="utf-8", errors="ignore"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")
    return env

env         = load_env()
PRIVATE_KEY = env.get("POLYMARKET_PRIVATE_KEY", "")
FUNDER      = env.get("POLYMARKET_FUNDER", "")
DB_PATH     = env.get("DB_PATH_VALUEBET", "valuebet.db")

if not FUNDER or FUNDER.startswith("0xYOUR"):
    log.error("POLYMARKET_FUNDER не задан в .env"); sys.exit(1)
if not os.path.exists(DB_PATH):
    log.error("БД не найдена: %s", DB_PATH); sys.exit(1)

log.info("Funder : %s", FUNDER)
log.info("БД     : %s", DB_PATH)

# ── HTTP ──────────────────────────────────────────────────────────────────────
def fetch(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

# ── CLOB клиент (для восстановления outcome_id) ───────────────────────────────
clob_client = None
if PRIVATE_KEY and not PRIVATE_KEY.startswith("0xYOUR"):
    try:
        from py_clob_client.client import ClobClient
        clob_client = ClobClient(
            host="https://clob.polymarket.com",
            key=PRIVATE_KEY, chain_id=137,
            signature_type=1, funder=FUNDER,
        )
        clob_client.set_api_creds(clob_client.create_or_derive_api_creds())
        log.info("✓ CLOB клиент готов")
    except Exception as e:
        log.warning("CLOB клиент недоступен: %s", e)

# ── БД ────────────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_bets import BetDatabase
db = BetDatabase(DB_PATH)
log.info("✓ БД открыта")

# ── ШАГ 1: Загружаем все позиции с Polymarket ─────────────────────────────────
log.info("")
log.info("── ШАГ 1: Позиции с Polymarket ──────────────────────────────")
positions = {}  # asset_str → pos_dict
funder_lc = FUNDER.lower()
try:
    items = fetch(
        f"https://data-api.polymarket.com/positions"
        f"?user={funder_lc}&sizeThreshold=0.00001&limit=500"
    )
    for p in (items if isinstance(items, list) else []):
        asset = str(p.get("asset") or "").strip()
        if asset:
            positions[asset] = p
    log.info("Загружено %d позиций", len(positions))
except Exception as e:
    log.error("Ошибка загрузки позиций: %s", e); sys.exit(1)

# Статистика позиций
rdm_cnt   = sum(1 for p in positions.values() if p.get("redeemable"))
won_cnt   = sum(1 for p in positions.values() if p.get("curPrice", 0) >= 0.98)
lost_cnt  = sum(1 for p in positions.values() if p.get("curPrice", 0) <= 0.02)
log.info("  redeemable=%d  price≥0.98=%d  price≤0.02=%d  прочих=%d",
         rdm_cnt, won_cnt, lost_cnt, len(positions)-won_cnt-lost_cnt)

# ── ШАГ 2: Получаем ставки из БД и восстанавливаем outcome_id ─────────────────
log.info("")
log.info("── ШАГ 2: Ставки из БД ──────────────────────────────────────")
rows = db.conn.execute("""
    SELECT id, order_id, outcome_id, stake, stake_price,
           ROUND(stake * stake_price, 2) AS cost,
           home, away, outcome_name, bet_mode
    FROM bets
    WHERE status = 'placed'
      AND outcome_result = 'pending'
    ORDER BY id
""").fetchall()
log.info("Всего placed+pending: %d", len(rows))

no_oid = [r for r in rows if not (r["outcome_id"] or "").strip()]
has_oid = [r for r in rows if (r["outcome_id"] or "").strip()]
log.info("  С outcome_id: %d   Без outcome_id: %d", len(has_oid), len(no_oid))

# Восстанавливаем outcome_id через CLOB для тех у кого его нет
restored = {}  # bet_id → outcome_id
if no_oid and clob_client:
    log.info("Восстанавливаем outcome_id через CLOB для %d ставок...", len(no_oid))
    for row in no_oid:
        order_id = (row["order_id"] or "").strip()
        if not order_id:
            log.warning("  #%d нет ни outcome_id ни order_id — пропуск", row["id"])
            continue
        try:
            resp = clob_client.get_order(order_id)
            if resp:
                oid = str(
                    resp.get("asset_id") or resp.get("assetId") or
                    resp.get("token_id") or resp.get("tokenId") or ""
                ).strip()
                size_matched = float(resp.get("size_matched") or resp.get("sizeMatched") or 0)
                status_clob  = (resp.get("status") or "").upper()
                log.info("  #%d order=%s... → oid=...%s matched=%.2f status=%s",
                         row["id"], order_id[:10], oid[-10:] if oid else "?",
                         size_matched, status_clob)
                if oid:
                    restored[row["id"]] = oid
                    if not DRY_RUN:
                        db.conn.execute("UPDATE bets SET outcome_id=? WHERE id=?", (oid, row["id"]))
                        # Если ордер не исполнился → сразу void
                        if size_matched < 0.001 and status_clob not in ("MATCHED","FILLED","LIVE","OPEN"):
                            db.settle_by_id(row["id"], "void", 0.0)
                            log.info("    → void (не исполнился)")
                            restored.pop(row["id"], None)
                elif size_matched < 0.001:
                    if not DRY_RUN:
                        db.settle_by_id(row["id"], "void", 0.0)
                    log.info("  #%d → void (нет oid и matched=0)", row["id"])
            time.sleep(0.3)
        except Exception as e:
            log.warning("  #%d CLOB ошибка: %s", row["id"], e)
    if not DRY_RUN:
        db.conn.commit()
    log.info("Восстановлено outcome_id: %d", len(restored))
elif no_oid:
    log.warning("CLOB клиент недоступен — %d ставок без outcome_id не обработаны", len(no_oid))

# ── ШАГ 3: Сверяем с позициями ────────────────────────────────────────────────
log.info("")
log.info("── ШАГ 3: Сверка с позициями Polymarket ─────────────────────")

# Перечитываем из БД после обновления outcome_id
rows = db.conn.execute("""
    SELECT id, order_id, outcome_id, stake, stake_price,
           ROUND(stake * stake_price, 2) AS cost,
           home, away, outcome_name, bet_mode
    FROM bets
    WHERE status = 'placed'
      AND outcome_result = 'pending'
    ORDER BY id
""").fetchall()

stats = {"won": 0, "lost": 0, "void": 0, "open": 0, "no_oid": 0}

for row in rows:
    bet_id     = row["id"]
    outcome_id = str(row["outcome_id"] or "").strip()
    stake      = float(row["stake"] or 0)
    cost       = float(row["cost"] or 0)
    mode       = row["bet_mode"] or "pre"
    label      = (f"#{bet_id}[{mode}] "
                  f"{row['home'] or '?'} vs {row['away'] or '?'} / "
                  f"{row['outcome_name'] or '?'}")

    if not outcome_id:
        log.warning("  ⚠️  %s | нет outcome_id", label)
        stats["no_oid"] += 1
        continue

    result = profit = reason = None

    # ── A. Ищем в positions ────────────────────────────────────────────────────
    pos = positions.get(outcome_id)
    if pos:
        cur  = float(pos.get("curPrice") or 0)
        rdm  = bool(pos.get("redeemable") or False)
        size = float(pos.get("size") or stake)

        if cur >= 0.98:
            result = "won";  profit = round(size - cost, 2)
            reason = f"pos:price={cur:.4f}"
        elif cur <= 0.02:
            result = "lost"; profit = round(-cost, 2)
            reason = f"pos:price={cur:.4f}"
        else:
            reason = f"pos:open,price={cur:.4f},rdm={rdm}"

    # ── B. Позиции нет → Gamma ─────────────────────────────────────────────────
    else:
        try:
            gdata = fetch(
                f"https://gamma-api.polymarket.com/markets"
                f"?clobTokenIds={outcome_id}&limit=1"
            )
            mkts = gdata if isinstance(gdata, list) else []
            if mkts:
                mkt    = mkts[0]
                closed = mkt.get("closed") is True or mkt.get("resolved") is True
                raw_ids = mkt.get("clobTokenIds") or "[]"
                raw_prc = mkt.get("outcomePrices") or "[]"
                if isinstance(raw_ids, str): raw_ids = json.loads(raw_ids)
                if isinstance(raw_prc, str): raw_prc = json.loads(raw_prc)
                prices  = [float(x) for x in raw_prc]
                for i, tid in enumerate(raw_ids):
                    if str(tid) == outcome_id and i < len(prices):
                        p = prices[i]
                        if closed:
                            if p >= 0.99:
                                result = "won";  profit = round(stake - cost, 2)
                                reason = f"gamma:closed,p={p:.3f}"
                            elif p <= 0.01:
                                result = "lost"; profit = round(-cost, 2)
                                reason = f"gamma:closed,p={p:.3f}"
                            else:
                                reason = f"gamma:closed,p={p:.3f}(ambiguous)"
                        else:
                            reason = f"gamma:open,p={p:.3f}"
                        break
                else:
                    if closed:
                        # Рынок закрыт, токена нет в позициях, не нашли в Gamma → уже redeemed
                        result = "won"; profit = round(stake - cost, 2)
                        reason = "gamma:closed,redeemed_already"
                    else:
                        reason = "gamma:open,token_not_matched"
            else:
                # Рынок не найден нигде
                result = "void"; profit = 0.0
                reason = "not_in_positions,gamma_empty"
        except urllib.error.HTTPError as e:
            if e.code == 404:
                result = "void"; profit = 0.0; reason = "gamma:404(removed)"
            else:
                reason = f"gamma:http_err={e.code}"
        except Exception as e:
            reason = f"gamma:err={e}"
        time.sleep(0.2)

    # ── Вывод ─────────────────────────────────────────────────────────────────
    if result == "won":
        log.info("  ✅ %s → WON  +$%.2f  [%s]", label, profit, reason)
        stats["won"] += 1
    elif result == "lost":
        log.info("  ❌ %s → LOST -$%.2f  [%s]", label, abs(profit), reason)
        stats["lost"] += 1
    elif result == "void":
        log.info("  ⚪ %s → VOID         [%s]", label, reason)
        stats["void"] += 1
    else:
        log.info("  ⏳ %s → OPEN         [%s]", label, reason)
        stats["open"] += 1

    if result and not DRY_RUN:
        db.settle_by_id(bet_id, outcome_result=result, profit_actual=profit)

# ── Итог ──────────────────────────────────────────────────────────────────────
log.info("")
log.info("=" * 65)
log.info("ИТОГО (placed+pending → %d ставок):", sum(stats.values()))
log.info("  ✅ WON        : %d", stats["won"])
log.info("  ❌ LOST       : %d", stats["lost"])
log.info("  ⚪ VOID       : %d", stats["void"])
log.info("  ⏳ OPEN       : %d  (матчи ещё идут)", stats["open"])
log.info("  ⚠️  нет oid   : %d", stats["no_oid"])
if DRY_RUN:
    log.info("")
    log.info("DRY-RUN: запусти без --dry-run чтобы применить")