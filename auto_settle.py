# -*- coding: utf-8 -*-
"""
auto_settle.py — авторасчёт ставок через Polymarket API.

Источники (в порядке приоритета):
  A) REDEEM activity → slug → Gamma by slug → clobTokenIds → совпадение с outcome_id
  B) Positions API   → redeemable=True + curPrice=1/0

Gamma API по clobTokenIds НЕ работает (возвращает случайный рынок).
Gamma API по slug — работает корректно.
"""
import threading, logging, urllib.request, json, time

log = logging.getLogger("auto_settle")
CHECK_INTERVAL = 3600


def _fetch(url: str, timeout: int = 15):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _parse_json_field(val):
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            pass
    return []


def _get_activity(funder: str, act_type: str, limit: int = 500) -> list:
    result = []
    for base in [
        f"https://data-api.polymarket.com/activity?user={funder}&type={act_type}",
        f"https://data-api.polymarket.com/activity?proxyWallet={funder}&type={act_type}",
    ]:
        try:
            data = _fetch(f"{base}&limit={limit}")
            if isinstance(data, list) and data:
                log.info("[авто-сеттл] Activity[%s]: %d записей", act_type, len(data))
                return data
        except Exception as e:
            log.debug("[авто-сеттл] activity[%s] error: %s", act_type, e)
    return []


def _gamma_by_slug(slug: str) -> dict:
    if not slug:
        return {}
    try:
        data = _fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1")
        if isinstance(data, list) and data:
            return data[0]
    except Exception as e:
        log.debug("[авто-сеттл] gamma slug=%s: %s", slug, e)
    return {}


def _mkt_token_ids(mkt: dict) -> list:
    return [str(t) for t in _parse_json_field(mkt.get("clobTokenIds") or "[]") if t]


def _mkt_outcome_prices(mkt: dict) -> list:
    return [float(p) for p in _parse_json_field(mkt.get("outcomePrices") or "[]")]


def _load_positions(funder: str) -> dict:
    for url in [
        f"https://data-api.polymarket.com/positions?user={funder}&sizeThreshold=0.00001&limit=500",
        f"https://data-api.polymarket.com/positions?proxyWallet={funder}&sizeThreshold=0.00001&limit=500",
    ]:
        try:
            items = _fetch(url)
            if isinstance(items, list) and items:
                positions = {str(p.get("asset") or ""): p for p in items if p.get("asset")}
                redeemable = sum(1 for p in positions.values() if p.get("redeemable"))
                log.info("[авто-сеттл] Позиций: %d, redeemable: %d", len(positions), redeemable)
                return positions
        except Exception as e:
            log.debug("[авто-сеттл] positions error: %s", e)
    return {}


def check_and_settle(db, funder: str) -> int:
    settled = 0
    try:
        active = db.get_active_bets()
        if not active:
            log.info("[авто-сеттл] Нет активных ставок")
            return 0
        log.info("[авто-сеттл] Проверяем %d ставок...", len(active))
        funder = funder.lower()

        # Индекс: outcome_id → ставка
        bets_by_oid = {}
        for bet in active:
            if getattr(bet, "status", "") in ("cancelled", "failed"):
                continue
            if bet.outcome_result != "pending":
                continue
            oid = str(bet.outcome_id or "").strip()
            if oid:
                bets_by_oid[oid] = bet

        log.info("[авто-сеттл] Ставок с outcome_id: %d", len(bets_by_oid))

        results = {}  # bet.id → (result, profit, reason)

        # ── A) REDEEM activity → slug → Gamma → clobTokenIds ─────────────
        redeem_acts = _get_activity(funder, "REDEEM")
        seen_slugs = set()
        for act in redeem_acts:
            slug = act.get("slug") or act.get("eventSlug") or ""
            if not slug or slug in seen_slugs:
                continue
            seen_slugs.add(slug)

            mkt = _gamma_by_slug(slug)
            if not mkt:
                continue

            token_ids = _mkt_token_ids(mkt)
            prices    = _mkt_outcome_prices(mkt)

            for i, tid in enumerate(token_ids):
                bet = bets_by_oid.get(tid)
                if not bet or bet.id in results:
                    continue

                shares = float(bet.stake or 0)
                ep     = float(getattr(bet, "stake_price", 0) or bet.bb_price or 0)
                cost   = round(shares * ep, 2)

                # Смотрим цену этого токена
                price = prices[i] if i < len(prices) else -1

                if price >= 0.99:
                    # Победитель
                    usdc = float(act.get("usdcSize") or act.get("size") or 0)
                    payout = usdc if usdc > 0 else shares
                    profit = round(payout - cost, 2)
                    results[bet.id] = ("won", profit, f"REDEEM slug={slug} usdc={usdc:.2f}")
                elif price <= 0.01:
                    profit = round(-cost, 2)
                    results[bet.id] = ("lost", profit, f"REDEEM slug={slug} price≈0")
                # else — цена промежуточная, ждём

            time.sleep(0.1)  # не флудим Gamma

        log.info("[авто-сеттл] REDEEM: обработано %d slug, найдено %d результатов", len(seen_slugs), len(results))

        # ── B) Positions API → redeemable=True ───────────────────────────
        positions = _load_positions(funder)
        for oid, pos in positions.items():
            bet = bets_by_oid.get(oid)
            if not bet or bet.id in results:
                continue

            if not pos.get("redeemable"):
                continue

            cur = float(pos.get("curPrice") or -1)
            shares = float(bet.stake or 0)
            ep     = float(getattr(bet, "stake_price", 0) or bet.bb_price or 0)
            cost   = round(shares * ep, 2)
            pos_size = float(pos.get("size") or shares)

            if cur == 1:
                results[bet.id] = ("won", round(pos_size - cost, 2), f"positions:redeemable,p=1")
            elif cur == 0:
                results[bet.id] = ("lost", round(-cost, 2), f"positions:redeemable,p=0")

        log.info("[авто-сеттл] Итого результатов (A+B): %d", len(results))

        # ── C) Проданные позиции: нет в positions, есть SELL-трейды ────
        # Для ставок без результата — проверяем не была ли позиция продана
        remaining = {oid: bet for oid, bet in bets_by_oid.items()
                     if bet.id not in results}
        if remaining:
            try:
                import os
                pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
                funder_addr = os.getenv("POLYMARKET_FUNDER", "")
                if pk and funder_addr:
                    from polymarket_client import PolymarketClient
                    pm = PolymarketClient(pk, funder_addr)
                    for oid, bet in remaining.items():
                        # Если позиция отсутствует или size ≈ 0 — возможно продана
                        pos = positions.get(oid)
                        pos_size = float(pos.get("size", 0)) if pos else 0

                        if pos_size >= 0.01:
                            continue  # Позиция ещё есть — не продана

                        # Проверяем SELL-трейды через CLOB API
                        try:
                            trades = pm.get_trades(asset_id=oid)
                            sell_trades = [t for t in trades
                                           if (t.get("side") or "").upper() == "SELL"]
                            if not sell_trades:
                                continue

                            shares = float(bet.stake or 0)
                            ep = float(getattr(bet, "stake_price", 0) or bet.bb_price or 0)
                            cost = round(shares * ep, 2)

                            total_proceeds = sum(
                                float(t.get("price", 0)) * float(t.get("size", 0))
                                for t in sell_trades
                            )
                            total_sold = sum(float(t.get("size", 0)) for t in sell_trades)
                            avg_sell = total_proceeds / total_sold if total_sold > 0 else 0
                            profit = round(total_proceeds - cost, 2)

                            results[bet.id] = (
                                "sold", profit,
                                f"SELL trades: {len(sell_trades)}, avg={avg_sell:.4f}, proceeds=${total_proceeds:.2f}"
                            )
                            # Сохраняем sell_price отдельно
                            try:
                                db.conn.execute(
                                    "UPDATE bets SET sell_price=? WHERE id=?",
                                    (round(avg_sell, 4), bet.id))
                            except Exception:
                                pass  # sell_price колонка может отсутствовать в старых БД

                            time.sleep(0.15)  # не флудим API
                        except Exception as e:
                            log.debug("[авто-сеттл] sell check %s: %s", oid[:16], e)
                else:
                    log.debug("[авто-сеттл] POLYMARKET_PRIVATE_KEY не задан — пропуск проверки SELL")
            except Exception as e:
                log.debug("[авто-сеттл] секция C ошибка: %s", e)

        log.info("[авто-сеттл] Итого результатов (A+B+C): %d", len(results))

        # ── Сохранение ────────────────────────────────────────────────────
        for bet in active:
            if bet.id not in results:
                continue
            result, profit, reason = results[bet.id]
            try:
                db.settle_by_id(bet.id, outcome_result=result, profit_actual=profit)
                if result == "won":
                    shares = float(bet.stake or 0)
                    db.adjust_free_usdc(shares)
                elif result == "sold":
                    # При продаже возвращаем proceeds
                    shares = float(bet.stake or 0)
                    ep = float(getattr(bet, "stake_price", 0) or bet.bb_price or 0)
                    cost = round(shares * ep, 2)
                    proceeds = round(cost + profit, 2)
                    db.adjust_free_usdc(proceeds)
                icon = "✅" if result == "won" else ("❌" if result == "lost" else ("💰" if result == "sold" else "⚪"))
                log.info("[авто-сеттл] %s #%d %s vs %s → %s P&L:%+.2f$ (%s)",
                         icon, bet.id, bet.home or "?", bet.away or "?",
                         result.upper(), profit, reason)
                settled += 1
            except Exception as e:
                log.error("[авто-сеттл] settle error #%d: %s", bet.id, e)

        log.info("[авто-сеттл] Расчитано: %d из %d", settled, len(active))

    except Exception as e:
        log.error("[авто-сеттл] Критическая ошибка: %s", e, exc_info=True)
    return settled


class AutoSettleWorker:
    def __init__(self, db, funder: str, interval: int = CHECK_INTERVAL):
        self.db = db
        self.funder = funder
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="auto-settle")
        self._thread.start()
        log.info("[авто-сеттл] Запущен (интервал %d сек)", self.interval)

    def stop(self):
        self._stop.set()

    def run_now(self) -> int:
        return check_and_settle(self.db, self.funder)

    def _loop(self):
        self._stop.wait(30)
        while not self._stop.is_set():
            try:
                check_and_settle(self.db, self.funder)
            except Exception as e:
                log.error("[авто-сеттл] loop error: %s", e)
            self._stop.wait(self.interval)