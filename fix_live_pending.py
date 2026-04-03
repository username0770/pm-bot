#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fix_live_pending.py  v2

Исправляет лайв ставки placed+pending у которых нет outcome_id.
Шаг 1: По order_id → CLOB API → получаем asset_id (= outcome_id) + size_matched
Шаг 2: Пересчитываем stake/stake_price по реальному заполнению
Шаг 3: По outcome_id → Gamma API → won/lost
Шаг 4: Fallback на CLOB midpoint
Шаг 5: Записываем результат через db.settle_by_id()

Запуск: python fix_live_pending.py
"""

import os, sys, json, time, logging, urllib.request, urllib.error
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("fix_live")

# ── .env ──────────────────────────────────────────────────────────────────────
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

if not PRIVATE_KEY or PRIVATE_KEY.startswith("0xYOUR"):
    log.error("POLYMARKET_PRIVATE_KEY не задан в .env"); sys.exit(1)
if not FUNDER or FUNDER.startswith("0xYOUR"):
    log.error("POLYMARKET_FUNDER не задан в .env"); sys.exit(1)
if not os.path.exists(DB_PATH):
    log.error("БД не найдена: %s", DB_PATH); sys.exit(1)

log.info("БД: %s  Funder: %s...", DB_PATH, FUNDER[:12])

# ── CLOB клиент ───────────────────────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    client = ClobClient(
        host="https://clob.polymarket.com",
        key=PRIVATE_KEY, chain_id=137,
        signature_type=1, funder=FUNDER,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    log.info("✓ CLOB клиент готов")
except Exception as e:
    log.error("CLOB клиент: %s", e); sys.exit(1)

# ── BetsDB ────────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from db_bets import BetDatabase as BetsDB
    db = BetsDB(DB_PATH)
    log.info("✓ BetsDB готов")
except Exception as e:
    log.error("BetsDB: %s", e); sys.exit(1)

# ── HTTP хелпер ───────────────────────────────────────────────────────────────
def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

# ── Gamma settle ──────────────────────────────────────────────────────────────
def gamma_settle(outcome_id, cost, shares):
    try:
        data = fetch(f"https://gamma-api.polymarket.com/markets?clobTokenIds={outcome_id}&limit=1")
        mkts = data if isinstance(data, list) else [data] if isinstance(data, dict) and data else []
        if not mkts:
            return None, 0, "gamma:no_market"
        mkt = mkts[0]
        is_closed = mkt.get("closed") is True or mkt.get("resolved") is True or mkt.get("active") is False
        if not is_closed:
            return None, 0, "gamma:still_open"
        raw_ids = mkt.get("clobTokenIds") or "[]"
        raw_prc = mkt.get("outcomePrices") or "[]"
        if isinstance(raw_ids, str): raw_ids = json.loads(raw_ids)
        if isinstance(raw_prc, str): raw_prc = json.loads(raw_prc)
        prices = [float(p) for p in raw_prc]
        for i, tid in enumerate(raw_ids):
            if tid != str(outcome_id): continue
            if i < len(prices):
                p = prices[i]
                if p >= 0.99: return "won",  round(shares - cost, 2), f"gamma:p={p:.3f}"
                if p <= 0.01: return "lost", round(-cost, 2),         f"gamma:p={p:.3f}"
                return None, 0, f"gamma:p={p:.3f}(ambiguous)"
        return None, 0, "gamma:token_not_found"
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, 0, "gamma:404"
        return None, 0, f"gamma:err={e}"
    except Exception as e:
        return None, 0, f"gamma:err={e}"

# ── CLOB midpoint fallback ────────────────────────────────────────────────────
def midpoint_settle(outcome_id, cost, shares):
    try:
        data = fetch(f"https://clob.polymarket.com/midpoint?token_id={outcome_id}", timeout=10)
        mid = float(data.get("mid") or 0)
        if mid <= 0.02:  return "lost", round(-cost, 2),         f"mid={mid:.4f}"
        if mid >= 0.98:  return "won",  round(shares - cost, 2), f"mid={mid:.4f}"
        return None, 0, f"mid={mid:.4f}(open)"
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Рынок удалён с Polymarket — ставка void (деньги должны вернуться)
            return "void", 0, f"mid:404(market_removed)"
        return None, 0, f"mid:err={e}"
    except Exception as e:
        return None, 0, f"mid:err={e}"

# ── Основной цикл ─────────────────────────────────────────────────────────────
rows = db.conn.execute("""
    SELECT id, order_id, stake, stake_price,
           ROUND(stake * stake_price, 2) AS cost_usdc,
           outcome_id, home, away, outcome_name
    FROM bets
    WHERE status = 'placed'
      AND outcome_result = 'pending'
      AND bet_mode = 'live'
    ORDER BY id
""").fetchall()

log.info("Найдено %d лайв placed+pending ставок", len(rows))
if not rows:
    log.info("Нечего исправлять — выход"); sys.exit(0)

stats = {"settled": 0, "oid_restored": 0, "still_open": 0, "errors": 0}

for row in rows:
    bet_id     = row["id"]
    order_id   = row["order_id"] or ""
    outcome_id = row["outcome_id"] or ""
    stake      = float(row["stake"] or 0)
    price      = float(row["stake_price"] or 0)
    cost       = float(row["cost_usdc"] or 0)
    label      = f"#{bet_id} {row['home'] or '?'} vs {row['away'] or '?'} [{row['outcome_name'] or '?'}]"

    # ── Шаг 1: восстанавливаем outcome_id через CLOB если нет ─────────────────
    if not outcome_id and order_id:
        try:
            resp = client.get_order(order_id)
            if resp:
                outcome_id = str(
                    resp.get("asset_id") or resp.get("assetId") or
                    resp.get("token_id") or resp.get("tokenId") or ""
                )
                size_matched   = float(resp.get("size_matched")   or resp.get("sizeMatched")   or 0)
                size_remaining = float(resp.get("size_remaining") or resp.get("sizeRemaining") or 0)
                price_clob     = float(resp.get("price") or price or 0)
                status_clob    = (resp.get("status") or "").lower()

                log.info("  %s | CLOB status=%s matched=%.4f remaining=%.4f oid=...%s",
                         label, status_clob, size_matched, size_remaining, outcome_id[-14:] if outcome_id else "?")

                # Ордер не исполнился совсем
                if size_matched <= 0.001 and status_clob not in ("matched", "filled", "live"):
                    db.settle_by_id(bet_id, outcome_result="void", profit_actual=0.0)
                    log.info("    → void (0 matched)")
                    stats["settled"] += 1
                    continue

                # Пересчитываем stake по реальному заполнению
                if size_matched > 0.001 and abs(size_matched - stake) > 0.001:
                    ep = price_clob or price
                    db.conn.execute(
                        "UPDATE bets SET stake=?, stake_price=? WHERE id=?",
                        (size_matched, ep, bet_id)
                    )
                    db.conn.commit()
                    stake = size_matched
                    price = ep
                    cost  = round(stake * price, 2)
                    log.info("    → stake пересчитан: %.4f shares @ %.4f = $%.2f", stake, price, cost)

                # Сохраняем outcome_id
                if outcome_id:
                    db.conn.execute("UPDATE bets SET outcome_id=? WHERE id=?", (outcome_id, bet_id))
                    db.conn.commit()
                    stats["oid_restored"] += 1
                    log.info("    → outcome_id: ...%s", outcome_id[-14:])

        except Exception as e:
            log.warning("  %s | CLOB ошибка: %s", label, e)
            stats["errors"] += 1
            continue
        time.sleep(0.4)

    if not outcome_id:
        log.warning("  %s | нет outcome_id — пропуск", label)
        stats["errors"] += 1
        continue

    # ── Шаг 2: Gamma ──────────────────────────────────────────────────────────
    result, profit, reason = gamma_settle(outcome_id, cost, stake)

    # ── Шаг 3: CLOB midpoint fallback ─────────────────────────────────────────
    if result is None:
        result, profit, reason = midpoint_settle(outcome_id, cost, stake)

    # ── Шаг 4: записываем ─────────────────────────────────────────────────────
    if result:
        db.settle_by_id(bet_id, outcome_result=result, profit_actual=profit)
        icon = "✅" if result == "won" else ("❌" if result == "lost" else "⚪")
        log.info("  %s %s → %s  P&L:%+.2f$  (%s)", icon, label, result.upper(), profit, reason)
        stats["settled"] += 1
    else:
        log.info("  ⏳ %s → ещё открыт (%s)", label, reason)
        stats["still_open"] += 1

    time.sleep(0.3)

log.info("")
log.info("=" * 55)
log.info("Итого:")
log.info("  outcome_id восстановлен : %d", stats["oid_restored"])
log.info("  расчитано               : %d", stats["settled"])
log.info("  ещё открыты             : %d", stats["still_open"])
log.info("  ошибки                  : %d", stats["errors"])