"""
pm_sync.py — Синхронизация статистики бота с Polymarket API.

Пересчитывает реальный P&L для каждой ставки на основе:
  - Трейдов из CLOB API (реальный объём, статус исполнения)
  - Resolved price из Gamma API (won=1.0, lost=0.0)
  - Комиссии из feeSchedule (sports_fees для новых рынков)

Запуск:
  python pm_sync.py              # dry run — только показывает, не пишет в БД
  python pm_sync.py --live       # применяет изменения в БД
  python pm_sync.py --summary    # только сводная таблица из БД, без API
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys

# Windows cp1251 консоль — форсируем UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CLOB_HOST  = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID   = 137

# Новые колонки для миграции БД
NEW_COLS = [
    ("shares_bought",   "REAL DEFAULT 0"),
    ("fee_shares",      "REAL DEFAULT 0"),
    ("fee_usdc_actual", "REAL DEFAULT 0"),
    ("payout_actual",   "REAL DEFAULT 0"),
    ("resolved_price",  "REAL DEFAULT NULL"),
    ("pm_trade_status", "TEXT DEFAULT NULL"),
    ("pm_synced_at",    "TEXT DEFAULT NULL"),
    ("profit_correct",  "REAL DEFAULT NULL"),
]


# -----------------------------------------------------------------------------

class PMSync:

    def __init__(self):
        # Ключи: сначала config.py, потом env
        try:
            import importlib
            cfg_mod = importlib.import_module("config")
            importlib.reload(cfg_mod)
            cfg = cfg_mod.Config()
            self.private_key = cfg.POLYMARKET_PRIVATE_KEY
            self.funder      = cfg.POLYMARKET_FUNDER
            db_path          = cfg.DB_PATH_VALUEBET
        except Exception:
            self.private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
            self.funder      = os.getenv("POLYMARKET_FUNDER", "")
            db_path          = os.getenv("DB_PATH_VALUEBET", "valuebet.db")

        if not self.private_key or not self.funder:
            raise ValueError(
                "Нужны POLYMARKET_PRIVATE_KEY и POLYMARKET_FUNDER в .env"
            )

        # valuebet.db или что задано в конфиге
        if not os.path.exists(db_path):
            alt = db_path.replace("valuebets.db", "valuebet.db")
            if os.path.exists(alt):
                db_path = alt
        self.db_path = db_path

        self._clob_client = None
        self._gamma_cache: dict[str, Optional[dict]] = {}

    # -- CLOB клиент ----------------------------------------------------------

    def _get_clob(self):
        if self._clob_client is None:
            from py_clob_client.client import ClobClient
            c = ClobClient(
                host=CLOB_HOST,
                key=self.private_key,
                chain_id=CHAIN_ID,
                signature_type=1,
                funder=self.funder,
            )
            c.set_api_creds(c.create_or_derive_api_creds())
            self._clob_client = c
            log.info("CLOB клиент готов. Funder: %s...", self.funder[:12])
        return self._clob_client

    # -- БД -------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def migrate_db(self):
        """Добавляет новые колонки в bets если их нет."""
        conn = self._conn()
        try:
            existing = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
            added = []
            for col, defn in NEW_COLS:
                if col not in existing:
                    conn.execute(f"ALTER TABLE bets ADD COLUMN {col} {defn}")
                    added.append(col)
            conn.commit()
            if added:
                log.info("Добавлены колонки: %s", ", ".join(added))
            else:
                log.debug("Схема bets актуальна")
        finally:
            conn.close()

    # -- Gamma API -------------------------------------------------------------

    def _gamma_get(self, url: str) -> list:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "PMSync/1.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())

    def _get_market(self, market_id: str) -> Optional[dict]:
        """Данные рынка из Gamma API. Кеш по market_id."""
        if market_id in self._gamma_cache:
            return self._gamma_cache[market_id]
        try:
            data = self._gamma_get(f"{GAMMA_HOST}/markets?id={market_id}")
            m = data[0] if isinstance(data, list) and data else None
            self._gamma_cache[market_id] = m
            time.sleep(0.1)  # rate limit
            return m
        except Exception as e:
            log.debug("Gamma /markets?id=%s: %s", market_id, e)
            self._gamma_cache[market_id] = None
            return None

    def get_resolved_price(self, market_id: str, outcome_id: str) -> Optional[float]:
        """
        1.0 = won, 0.0 = lost, None = не resolved или рынок не закрыт.
        Сопоставляет outcome_id с clobTokenIds и берёт соответствующий outcomePrices.
        """
        m = self._get_market(market_id)
        if not m or not m.get("closed"):
            return None

        raw_tids   = m.get("clobTokenIds", [])
        raw_prices = m.get("outcomePrices", [])

        # Иногда приходит JSON-строка вместо списка
        if isinstance(raw_tids, str):
            try:
                raw_tids = json.loads(raw_tids)
            except Exception:
                raw_tids = []
        if isinstance(raw_prices, str):
            try:
                raw_prices = json.loads(raw_prices)
            except Exception:
                raw_prices = []

        for i, tid in enumerate(raw_tids):
            if str(tid) == str(outcome_id):
                try:
                    return float(raw_prices[i])
                except (IndexError, ValueError):
                    return None
        return None

    def get_fee_rate(self, market_id: str) -> float:
        """
        Реальная ставка комиссии из Gamma API feeSchedule.rate.
        0.0 если комиссий нет или рынок не найден.
        """
        m = self._get_market(market_id)
        if not m or not m.get("feesEnabled"):
            return 0.0
        schedule = m.get("feeSchedule") or {}
        return float(schedule.get("rate", 0.0))

    # -- CLOB Trades -----------------------------------------------------------

    def fetch_all_trades(self) -> dict[str, dict]:
        """
        Загружает все трейды аккаунта через CLOB API (автопагинация).
        Возвращает dict: {order_id_lower: trade_dict}
        Индексирует по taker_order_id и maker_order_id.
        """
        from py_clob_client.clob_types import TradeParams

        log.info("Загрузка трейдов CLOB (maker_address=%s...)...", self.funder[:12])
        try:
            client = self._get_clob()
            trades = client.get_trades(TradeParams(maker_address=self.funder))
        except Exception as e:
            log.error("get_trades ошибка: %s", e)
            return {}

        log.info("Получено трейдов из CLOB: %d", len(trades))

        by_order: dict[str, dict] = {}
        for t in trades:
            # Наш ордер может быть taker_order_id (обычно) или maker_order_id (лимитный)
            for field in ("taker_order_id", "id", "order_id"):
                oid = (t.get(field) or "").lower()
                if oid and oid not in by_order:
                    by_order[oid] = t
            # maker_order_id как запасной вариант
            moid = (t.get("maker_order_id") or "").lower()
            if moid:
                by_order.setdefault(moid, t)

        log.info("Уникальных order_id: %d", len(by_order))
        return by_order

    # -- P&L -------------------------------------------------------------------

    def calculate_pnl(
        self,
        bet: dict,
        trade: Optional[dict],
        resolved_price: Optional[float],
        sell_trade: Optional[dict] = None,
    ) -> dict:
        """
        Рассчитывает правильный P&L и возвращает dict для UPDATE.

        Формулы комиссии (только для feesEnabled рынков):
          fee_shares = shares_gross × fee_rate × price × (1 - price)
          shares_net = shares_gross - fee_shares
          fee_usdc   = fee_shares × price  (приближение)

        P&L:
          WON:  payout = shares_net × 1.0;  profit = payout - stake
          LOST: payout = 0;                 profit = -stake
          SOLD: payout = shares_net × sell_price - sell_fee_usdc; profit = payout - stake
          VOID: payout = stake;             profit = 0
        """
        stake       = float(bet.get("stake") or 0)
        stake_price = float(bet.get("stake_price") or 0)
        outcome     = (bet.get("outcome_result") or "").lower()
        market_id   = str(bet.get("market_id") or "")
        result: dict = {}

        if stake <= 0 or stake_price <= 0:
            return result  # нет данных для расчёта

        # -- 1. Количество шаров -----------------------------------------------
        # stake в БД = количество шаров (shares), НЕ доллары USDC!
        # cost_usdc = stake * stake_price — реально потраченные доллары
        shares_gross = float(stake)  # stake IS already the shares count
        cost_usdc    = round(shares_gross * stake_price, 6)

        # Если есть трейд — берём реальный объём исполнения
        if trade:
            raw_size = trade.get("size") or trade.get("shares") or 0
            if raw_size:
                try:
                    shares_gross = float(raw_size)
                    cost_usdc    = round(shares_gross * stake_price, 6)
                except ValueError:
                    pass
            result["pm_trade_status"] = str(trade.get("status") or "CONFIRMED")

        # -- 2. Комиссия -------------------------------------------------------
        # Приоритет: fee_rate из трейда > Gamma API feeSchedule > БД
        fee_rate = 0.0
        if trade:
            raw_bps = trade.get("fee_rate_bps") or trade.get("feeRateBps") or 0
            if raw_bps:
                fee_rate = float(raw_bps) / 10_000
        if fee_rate == 0.0 and market_id:
            fee_rate = self.get_fee_rate(market_id)
        if fee_rate == 0.0:
            fee_rate = float(bet.get("fee_rate") or 0)

        p = stake_price
        fee_shares = shares_gross * fee_rate * p * (1.0 - p) if fee_rate > 0 else 0.0
        shares_net = shares_gross - fee_shares
        fee_usdc   = round(fee_shares * p, 6)

        result["shares_bought"]   = round(shares_gross, 6)
        result["fee_shares"]      = round(fee_shares, 6)
        result["fee_usdc_actual"] = fee_usdc

        # -- 3. Resolved price -------------------------------------------------
        if resolved_price is not None:
            result["resolved_price"] = resolved_price

        # -- 4. P&L ------------------------------------------------------------
        # resolved_price: 1.0=won, 0.0=lost, None=не resolved
        # profit = payout - cost_usdc  (cost_usdc = stake_shares * stake_price)
        profit: Optional[float] = None

        if outcome == "won":
            if resolved_price is None:
                # Нет данных из Gamma API — не считаем
                result["_no_resolved"] = True
            elif resolved_price < 0.5:
                # БД говорит WON, но PM говорит LOST → реальный убыток
                result["_db_api_mismatch"] = "won→lost"
                result["resolved_price"] = resolved_price
                result["payout_actual"] = 0.0
                profit = round(-cost_usdc, 6)
            else:
                result["resolved_price"] = resolved_price
                payout = round(shares_net * resolved_price, 6)
                result["payout_actual"] = payout
                profit = round(payout - cost_usdc, 6)

        elif outcome == "lost":
            # LOST: теряем потраченные USDC (cost_usdc = shares * stake_price)
            # resolved_price для нашего токена = 0.0 (проигравший)
            # Если API вернул 1.0 — расхождение (БД wrong)
            if resolved_price is not None and resolved_price > 0.5:
                # БД говорит LOST, но PM говорит WON → реальный выигрыш
                result["_db_api_mismatch"] = "lost→won"
                result["resolved_price"] = resolved_price
                payout = round(shares_net * resolved_price, 6)
                result["payout_actual"] = payout
                profit = round(payout - cost_usdc, 6)
            else:
                # Нормальный LOST (resolved_price=0.0 или None — оба означают проигрыш)
                result["resolved_price"] = 0.0  # всегда ставим 0.0 для LOST
                result["payout_actual"] = 0.0
                profit = round(-cost_usdc, 6)

        elif outcome == "sold":
            sell_price = float(bet.get("sell_price") or 0)
            # Уточняем цену продажи из sell_trade если есть
            if sell_trade:
                raw_sp = sell_trade.get("price") or 0
                if raw_sp:
                    sell_price = float(raw_sp)
            if sell_price > 0:
                # sell_fee аналогично: fee на шары при продаже
                sell_fee_rate = 0.0
                if sell_trade:
                    raw_bps = sell_trade.get("fee_rate_bps") or sell_trade.get("feeRateBps") or 0
                    sell_fee_rate = float(raw_bps) / 10_000 if raw_bps else fee_rate
                else:
                    sell_fee_rate = fee_rate
                sell_fee_shares = shares_net * sell_fee_rate * sell_price * (1.0 - sell_price)
                sell_shares_net = shares_net - sell_fee_shares
                payout = round(sell_shares_net * sell_price, 6)
                result["payout_actual"] = payout
                profit = round(payout - cost_usdc, 6)
            # Если нет sell_price → profit остаётся None (нет данных)

        elif outcome in ("void", "push"):
            result["payout_actual"] = round(cost_usdc, 6)
            profit = 0.0

        # pending/placed без результата → profit = None

        if profit is not None:
            result["profit_correct"] = profit

        result["pm_synced_at"] = datetime.now(timezone.utc).isoformat()
        return result

    # -- Основной метод --------------------------------------------------------

    async def sync(self, dry_run: bool = True):
        """Загружает трейды, пересчитывает P&L, при --live пишет в БД."""
        self.migrate_db()

        # Считаем ставки без order_id (failed/not placed)
        conn = self._conn()
        try:
            no_order_count = conn.execute(
                "SELECT COUNT(*) FROM bets WHERE order_id IS NULL OR order_id = ''"
            ).fetchone()[0]
            rows = conn.execute("""
                SELECT id, order_id, stake, stake_price, fee_rate,
                       outcome_result, status, market_id, outcome_id,
                       sell_price, sell_order_id, sell_price_target,
                       profit_actual, bet_mode,
                       home, away, league, outcome_name, neg_risk
                FROM bets
                WHERE order_id IS NOT NULL AND order_id != ''
                ORDER BY id
            """).fetchall()
            bets = [dict(r) for r in rows]
        finally:
            conn.close()

        log.info("Ставок для синхронизации: %d (без order_id: %d)", len(bets), no_order_count)

        # Загружаем все трейды одним запросом
        trades = self.fetch_all_trades()

        # Обрабатываем каждую ставку
        updates: list[tuple[int, dict]] = []
        stats = {
            "total":          len(bets),
            "no_order":       no_order_count,
            "trade_found":    0,
            "trade_missing":  0,   # order_id есть, но трейд не найден в CLOB
            "resolved":       0,
            "no_resolved":    0,   # won/lost, но resolved_price не получен
            "pnl_ok":         0,
            "skipped":        0,
            "mismatch_won_lost": 0,  # БД=won, PM=lost
            "mismatch_lost_won": 0,  # БД=lost, PM=won
            "old_pnl":        0.0,
            "new_pnl":        0.0,
            "total_fees":     0.0,
        }

        for bet in bets:
            bet_id   = bet["id"]
            order_id = (bet["order_id"] or "").lower()
            outcome  = (bet["outcome_result"] or "").lower()
            status   = (bet["status"] or "").lower()

            # Пропускаем явно мусорные записи
            if status == "failed" and not outcome or outcome == "":
                stats["skipped"] += 1
                continue

            # Трейд по buy-ордеру
            trade = trades.get(order_id)
            if trade:
                stats["trade_found"] += 1
            elif outcome in ("won", "lost", "sold"):
                # order_id есть, ставка размещена, но трейд не найден в CLOB
                stats["trade_missing"] += 1
                log.debug("ID=%d [%s] трейд не найден в CLOB (order=%s...)",
                          bet_id, outcome.upper(), order_id[:16])

            # Трейд по sell-ордеру (если была продажа)
            sell_trade = None
            sell_oid = (bet.get("sell_order_id") or "").lower()
            if sell_oid:
                sell_trade = trades.get(sell_oid)

            # Resolved price из Gamma API
            resolved_price: Optional[float] = None
            market_id  = str(bet.get("market_id") or "")
            outcome_id = str(bet.get("outcome_id") or "")

            if outcome in ("won", "lost") and market_id and outcome_id:
                resolved_price = self.get_resolved_price(market_id, outcome_id)
                if resolved_price is not None:
                    stats["resolved"] += 1
                else:
                    stats["no_resolved"] += 1

            # Считаем P&L
            upd = self.calculate_pnl(bet, trade, resolved_price, sell_trade)

            old_profit = float(bet.get("profit_actual") or 0)
            new_profit = upd.get("profit_correct")
            label = f"{bet.get('home','')} vs {bet.get('away','')} | {bet.get('outcome_name','')}"

            # Учёт расхождений БД vs PM
            mismatch = upd.pop("_db_api_mismatch", None)
            if mismatch == "won→lost":
                stats["mismatch_won_lost"] += 1
                log.warning("ID=%-4d РАСХОЖДЕНИЕ: БД=WON но PM=LOST  %-42s  profit_correct=%+.2f",
                            bet_id, label[:42], new_profit or 0)
            elif mismatch == "lost→won":
                stats["mismatch_lost_won"] += 1
                log.warning("ID=%-4d РАСХОЖДЕНИЕ: БД=LOST но PM=WON  %-42s  profit_correct=%+.2f",
                            bet_id, label[:42], new_profit or 0)

            # Учёт "нет resolved_price"
            no_resolved = upd.pop("_no_resolved", False)
            if no_resolved:
                log.debug("ID=%d [WON] нет resolved_price — profit_correct=None", bet_id)

            # Сохраняем outcome для сводки (поле не пишется в БД — pop при апдейте не нужен)
            upd["_outcome"] = outcome

            if new_profit is not None:
                stats["pnl_ok"]    += 1
                stats["old_pnl"]   += old_profit
                stats["new_pnl"]   += new_profit
                stats["total_fees"] += upd.get("fee_usdc_actual", 0)
                delta = new_profit - old_profit
                log.info(
                    "ID=%-4d [%-5s] %-42s  old=%+.2f → new=%+.2f (Δ%+.4f)  fee=%.4f",
                    bet_id, outcome.upper(), label[:42],
                    old_profit, new_profit, delta,
                    upd.get("fee_usdc_actual", 0),
                )
            else:
                log.debug("ID=%d [%s] P&L не рассчитан (нет данных)", bet_id, outcome.upper())

            if upd:
                updates.append((bet_id, upd))

        # Пишем в БД
        if not dry_run and updates:
            conn = self._conn()
            try:
                applied = 0
                for bet_id, upd in updates:
                    # Убираем служебные поля перед записью в БД
                    db_upd = {k: v for k, v in upd.items() if not k.startswith("_")}
                    if not db_upd:
                        continue
                    cols = ", ".join(f"{k} = ?" for k in db_upd)
                    vals = list(db_upd.values()) + [bet_id]
                    conn.execute(f"UPDATE bets SET {cols} WHERE id = ?", vals)
                    applied += 1
                conn.commit()
                log.info("Записей обновлено в БД: %d", applied)
            finally:
                conn.close()
        elif dry_run:
            log.info("DRY RUN — изменения НЕ записаны (добавь --live)")

        self._print_summary(stats, updates)

    # -- Сводка ---------------------------------------------------------------

    def _print_summary(self, stats: dict, updates: list):
        # Фильтруем по _outcome (надёжнее чем resolved_price)
        w  = [(bid, u) for bid, u in updates
              if u.get("profit_correct") is not None and u.get("_outcome") == "won"]
        lo = [(bid, u) for bid, u in updates
              if u.get("profit_correct") is not None and u.get("_outcome") == "lost"]
        so = [(bid, u) for bid, u in updates
              if u.get("profit_correct") is not None and u.get("_outcome") == "sold"]

        won_sum = sum(u.get("profit_correct", 0) for _, u in w)
        los_sum = sum(u.get("profit_correct", 0) for _, u in lo)
        sol_sum = sum(u.get("profit_correct", 0) for _, u in so)
        delta   = stats["new_pnl"] - stats["old_pnl"]
        mm_tot  = stats["mismatch_won_lost"] + stats["mismatch_lost_won"]
        W = 56  # ширина строки

        def row(label, val, pad=W):
            s = f"|  {label}"
            s += " " * max(1, pad - len(s) - len(str(val)) - 1) + str(val) + "|"
            return s

        print()
        print("+" + "-" * W + "+")
        print(f"|  СВОДКА СИНХРОНИЗАЦИИ PM_SYNC{' ' * (W - 30)}|")
        print("+" + "-" * W + "+")
        print(row("Всего ставок с order_id:", stats["total"]))
        print(row("Без order_id (failed/not placed):", stats["no_order"]))
        print(row("Найдено в CLOB трейдах:", stats["trade_found"]))
        print(row("Не исполнились (есть order, нет в CLOB):", stats["trade_missing"]))
        print(row("Resolved price получен из Gamma:", stats["resolved"]))
        print(row("Нет resolved_price (WON, пропущено):", stats["no_resolved"]))
        print(row("P&L пересчитан:", stats["pnl_ok"]))
        print(row("Пропущено (pending/void/failed):", stats["skipped"]))
        print("+" + "-" * W + "+")
        print(row("Старый суммарный P&L:", f"${stats['old_pnl']:>+.2f}"))
        print(row("Новый суммарный P&L:", f"${stats['new_pnl']:>+.2f}"))
        print(row("Разница:", f"${delta:>+.2f}"))
        print(row("Суммарные комиссии:", f"${stats['total_fees']:>.4f}"))
        print("+" + "-" * W + "+")
        print(row(f"WON  ({len(w):4d}):", f"${won_sum:>+.2f}"))
        print(row(f"LOST ({len(lo):4d}):", f"${los_sum:>+.2f}"))
        if so:
            print(row(f"SOLD ({len(so):4d}):", f"${sol_sum:>+.2f}"))
        print("+" + "-" * W + "+")
        mml = stats["mismatch_won_lost"]
        mmw = stats["mismatch_lost_won"]
        print(row("РАСХОЖДЕНИЙ БД vs API:", f"{mm_tot}  (won->lost:{mml}  lost->won:{mmw})"))
        print("+" + "-" * W + "+")
        print()


# -- Команда --summary (только БД, без API) ------------------------------------

def cmd_summary(db_path: str, after_date: Optional[str] = None):
    """Выводит статистику напрямую из БД без обращения к API.

    after_date: "YYYY-MM-DD" — фильтровать ставки по placed_at >= этой даты.

    P&L считается на лету по правильным формулам:
      cost_usdc = shares * stake_price
      WON:  proceeds = shares * 1.0;  pnl = proceeds - cost_usdc
      LOST: proceeds = 0.0;           pnl = -cost_usdc
      SOLD: proceeds = shares * sell_price; pnl = proceeds - cost_usdc
      VOID: proceeds = cost_usdc;     pnl = 0.0  (не включается в итог)
    """
    if not os.path.exists(db_path):
        alt = db_path.replace("valuebets.db", "valuebet.db")
        if os.path.exists(alt):
            db_path = alt

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    W = 66  # ширина строки

    def sep():
        print("+" + "-" * W + "+")

    def row(label, val):
        s = f"|  {label}"
        v = str(val)
        s += " " * max(1, W - len(s) - len(v) - 1) + v + "|"
        print(s)

    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(bets)")}
        has_fee_col = "fee_usdc_actual" in cols

        # ---- Загружаем все ставки ---------------------------------------------
        fee_sel    = "COALESCE(fee_usdc_actual, 0)" if has_fee_col else "0"
        date_where = f"AND placed_at >= '{after_date}'" if after_date else ""
        all_rows = conn.execute(f"""
            SELECT
                id, outcome_result, stake, stake_price, sell_price,
                profit_actual, placed_at,
                {fee_sel} as fee_usdc_actual
            FROM bets
            WHERE stake > 0 AND stake_price > 0
            {date_where}
        """).fetchall()

        # ---- Структуры для агрегации -----------------------------------------
        # Каждая запись: dict с cost, proceeds, pnl
        agg = {
            "won":  {"n": 0, "cost": 0.0, "proceeds": 0.0, "pnl": 0.0},
            "lost": {"n": 0, "cost": 0.0, "proceeds": 0.0, "pnl": 0.0},
            "sold": {"n": 0, "cost": 0.0, "proceeds": 0.0, "pnl": 0.0},
            "void": {"n": 0, "cost": 0.0, "proceeds": 0.0, "pnl": 0.0},
        }
        cnt_pending = 0
        fees_tot    = 0.0

        # breakdown по размеру: ключ = группа → {n, cost, proceeds, pnl}
        grp_keys   = ("<=10", "11-20", "21-40", ">40")
        size_grp: dict[str, dict] = {g: {"n": 0, "cost": 0.0, "proceeds": 0.0, "pnl": 0.0}
                                      for g in grp_keys}

        # breakdown по месяцу: ключ = "2026-03" → {n_won, n_lost, cost, proceeds, pnl}
        months: dict[str, dict] = {}

        for x in all_rows:
            oc     = (x["outcome_result"] or "").lower()
            shares = float(x["stake"])
            price  = float(x["stake_price"])
            cost   = shares * price
            fee    = float(x["fee_usdc_actual"] or 0)
            fees_tot += fee

            # Считаем proceeds и pnl по правильной формуле
            if oc == "won":
                proceeds = shares * 1.0
                pnl      = proceeds - cost
            elif oc == "lost":
                proceeds = 0.0
                pnl      = -cost
            elif oc == "sold":
                sp       = float(x["sell_price"] or 0)
                proceeds = shares * sp if sp > 0 else 0.0
                pnl      = proceeds - cost
            elif oc in ("void", "push"):
                proceeds = cost   # возврат
                pnl      = 0.0
                oc       = "void"
            else:
                # pending / placed / failed — не включаем в итог
                cnt_pending += 1
                continue

            a = agg[oc]
            a["n"]        += 1
            a["cost"]     += cost
            a["proceeds"] += proceeds
            a["pnl"]      += pnl

            # Для void — не включаем в size / month breakdown
            if oc == "void":
                continue

            # ---- размерная группа (только won/lost/sold) ---------------------
            if shares <= 10:
                g = "<=10"
            elif shares <= 20:
                g = "11-20"
            elif shares <= 40:
                g = "21-40"
            else:
                g = ">40"
            sg = size_grp[g]
            sg["n"]        += 1
            sg["cost"]     += cost
            sg["proceeds"] += proceeds
            sg["pnl"]      += pnl

            # ---- месяц (только won/lost/sold) --------------------------------
            placed = x["placed_at"] or ""
            mon    = placed[:7] if len(placed) >= 7 else "unknown"
            if mon not in months:
                months[mon] = {"n_won": 0, "n_lost": 0, "n_sold": 0,
                               "cost": 0.0, "proceeds": 0.0, "pnl": 0.0}
            m = months[mon]
            m["cost"]     += cost
            m["proceeds"] += proceeds
            m["pnl"]      += pnl
            if oc == "won":
                m["n_won"]  += 1
            elif oc == "lost":
                m["n_lost"] += 1
            elif oc == "sold":
                m["n_sold"] += 1

        # ---- Итоговые агрегаты (won + lost + sold, без void) -----------------
        tot_cost     = agg["won"]["cost"]     + agg["lost"]["cost"]     + agg["sold"]["cost"]
        tot_proceeds = agg["won"]["proceeds"] + agg["lost"]["proceeds"] + agg["sold"]["proceeds"]
        tot_pnl      = agg["won"]["pnl"]      + agg["lost"]["pnl"]      + agg["sold"]["pnl"]
        tot_n        = agg["won"]["n"]        + agg["lost"]["n"]        + agg["sold"]["n"]

        # ROI по won+lost (исключая sold у которых нет sell_price корректно)
        roi_base = agg["won"]["cost"] + agg["lost"]["cost"]
        roi_pnl  = agg["won"]["pnl"]  + agg["lost"]["pnl"]
        roi      = roi_pnl / roi_base * 100 if roi_base else 0.0

        # ===== ПЕЧАТЬ ==========================================================
        print()
        sep()
        lbl = "СТАТИСТИКА ИЗ БД (pm_sync --summary)"
        if after_date:
            lbl += f"  after={after_date}"
        print(f"|  {lbl}{' ' * (W - len(lbl) - 3)}|")
        sep()
        row("Ставок итого (won+lost+sold):", tot_n)
        row("  WON:", agg["won"]["n"])
        row("  LOST:", agg["lost"]["n"])
        row("  SOLD:", agg["sold"]["n"])
        row("  VOID/PUSH (не в итоге):", agg["void"]["n"])
        row("  Pending/placed (нет исхода):", cnt_pending)
        sep()

        # ---- Итого -----------------------------------------------------------
        lbl2 = "ИТОГО (won + lost + sold)"
        print(f"|  {lbl2}{' ' * (W - len(lbl2) - 3)}|")
        sep()
        row("Потрачено USDC  (cost = shares × price):", f"${tot_cost:>10.2f}")
        row("Получено USDC   (proceeds):",              f"${tot_proceeds:>10.2f}")
        row("Чистый P&L      (proceeds - cost):",       f"${tot_pnl:>+10.2f}")
        row("Комиссии        (fee_usdc_actual):",       f"${fees_tot:>10.4f}")
        row("P&L с комиссиями:",                        f"${tot_pnl - fees_tot:>+10.2f}")
        row(f"ROI (won+lost, {agg['won']['n']+agg['lost']['n']} ставок):", f"{roi:>+.2f}%")
        sep()

        # ---- По результатам --------------------------------------------------
        lbl3 = "ПО РЕЗУЛЬТАТАМ"
        print(f"|  {lbl3}{' ' * (W - len(lbl3) - 3)}|")
        hdr = f"  {'Исход':<6} {'N':>5}  {'Cost':>10}  {'Proceeds':>10}  {'P&L':>10}  {'ROI':>7}"
        print(f"|{hdr}{' ' * (W - len(hdr))}|")
        sep()
        for oc_key, label in (("won","WON"), ("lost","LOST"), ("sold","SOLD"), ("void","VOID")):
            a    = agg[oc_key]
            if a["n"] == 0:
                continue
            roi_ = a["pnl"] / a["cost"] * 100 if a["cost"] else 0.0
            roi_s = f"{roi_:>+6.1f}%" if oc_key != "void" else "   N/A"
            line = (f"  {label:<6} {a['n']:>5}  ${a['cost']:>9.2f}  "
                    f"${a['proceeds']:>9.2f}  ${a['pnl']:>+9.2f}  {roi_s}")
            print(f"|{line}{' ' * (W - len(line))}|")
        sep()

        # ---- По месяцу -------------------------------------------------------
        lbl4 = "ПО МЕСЯЦАМ (placed_at)"
        print(f"|  {lbl4}{' ' * (W - len(lbl4) - 3)}|")
        mhdr = f"  {'Месяц':<9} {'N':>4}  {'Cost':>9}  {'Proceeds':>9}  {'P&L':>9}  {'ROI':>6}  W/L/S"
        print(f"|{mhdr}{' ' * (W - len(mhdr))}|")
        sep()
        for mon in sorted(months.keys()):
            m     = months[mon]
            m_roi = m["pnl"] / m["cost"] * 100 if m["cost"] else 0.0
            mn    = m["n_won"] + m["n_lost"] + m["n_sold"]
            wls   = f"{m['n_won']}/{m['n_lost']}/{m['n_sold']}"
            line  = (f"  {mon:<9} {mn:>4}  ${m['cost']:>8.2f}  "
                     f"${m['proceeds']:>8.2f}  ${m['pnl']:>+8.2f}  {m_roi:>+5.1f}%  {wls}")
            print(f"|{line}{' ' * (W - len(line))}|")
        sep()

        # ---- По размеру ставки -----------------------------------------------
        lbl5 = "ПО РАЗМЕРУ СТАВКИ (shares)"
        print(f"|  {lbl5}{' ' * (W - len(lbl5) - 3)}|")
        ghdr = f"  {'Группа':<10} {'N':>5}  {'Cost':>9}  {'Proceeds':>9}  {'P&L':>9}  {'ROI':>6}"
        print(f"|{ghdr}{' ' * (W - len(ghdr))}|")
        sep()
        for g in grp_keys:
            sg = size_grp[g]
            if sg["n"] == 0:
                continue
            g_roi = sg["pnl"] / sg["cost"] * 100 if sg["cost"] else 0.0
            line  = (f"  {g:<10} {sg['n']:>5}  ${sg['cost']:>8.2f}  "
                     f"${sg['proceeds']:>8.2f}  ${sg['pnl']:>+8.2f}  {g_roi:>+5.1f}%")
            print(f"|{line}{' ' * (W - len(line))}|")
        sep()
        print()

    finally:
        conn.close()


# -- Entrypoint ----------------------------------------------------------------

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--summary" in args:
        try:
            import importlib
            cfg = importlib.import_module("config").Config()
            db  = cfg.DB_PATH_VALUEBET
        except Exception:
            db = os.getenv("DB_PATH_VALUEBET", "valuebet.db")
        # --after=YYYY-MM-DD
        after_date = None
        for a in args:
            if a.startswith("--after="):
                after_date = a.split("=", 1)[1].strip()
                break
        cmd_summary(db, after_date=after_date)
        sys.exit(0)

    dry_run = "--live" not in args

    if dry_run:
        print()
        print("+" + "-" * 51 + "+")
        print("|  DRY RUN MODE — изменения НЕ записываются в БД  |")
        print("|  Запусти с  --live  чтобы применить изменения    |")
        print("+" + "-" * 51 + "+")
        print()

    sync = PMSync()
    asyncio.run(sync.sync(dry_run=dry_run))
