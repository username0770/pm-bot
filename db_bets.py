# -*- coding: utf-8 -*-
"""
База данных вэлью-бетов — полная схема с защитой от дублей.

Ключевая логика дедупликации:
  - outcome_id   (Polymarket token ID)  — главный уникальный ключ для исхода
  - market_id    (Polymarket market ID) — маркет
  - Ставка считается дублем если:
      1. outcome_id совпадает И status IN ('placed', 'pending')
      2. Матч ещё не завершён (started_at > now())

Использование:
  db = BetDatabase()
  
  # Проверка перед ставкой
  if db.already_bet(outcome_id):
      skip()
  
  # Запись ставки
  record_id = db.insert_bet(pm_bet, stake, ...)
  
  # Обновление после размещения
  db.update_placed(record_id, order_id="0xabc...", status="placed")
  
  # Сеттл
  db.settle(outcome_id, outcome="won", profit=10.5)
"""

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS bets (
    -- Первичный ключ
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),

    -- Идентификаторы Polymarket
    outcome_id      TEXT NOT NULL,          -- токен ID (главный ключ дедупликации)
    market_id       TEXT NOT NULL DEFAULT '',
    pm_event_id     TEXT NOT NULL DEFAULT '',

    -- Идентификаторы BetBurger
    bb_bet_id       TEXT NOT NULL DEFAULT '',
    bb_ref_event_id INTEGER DEFAULT 0,

    -- Матч
    home            TEXT NOT NULL DEFAULT '',
    away            TEXT NOT NULL DEFAULT '',
    league          TEXT NOT NULL DEFAULT '',
    sport_id        INTEGER DEFAULT 0,
    started_at      INTEGER DEFAULT 0,      -- Unix timestamp начала матча

    -- Исход
    outcome_name    TEXT NOT NULL DEFAULT '',   -- "Under", "Yes", "Team1 Win"
    market_type     INTEGER DEFAULT 0,          -- market_and_bet_type
    market_type_name TEXT DEFAULT '',
    market_param    REAL DEFAULT 0,             -- линия (229.5 для тотала)

    -- Коэффициенты
    bb_odds         REAL NOT NULL DEFAULT 0,    -- коэф от BetBurger
    bb_price        REAL NOT NULL DEFAULT 0,    -- implied prob (1/odds)
    value_pct       REAL NOT NULL DEFAULT 0,    -- велью% = middle_value (напр. 1.99)
    arb_pct         REAL DEFAULT 0,             -- доходность вилки arbs[].percent (напр. 2.8)
    edge            REAL GENERATED ALWAYS AS (value_pct / 100.0) STORED,

    -- Ликвидность
    total_liquidity REAL DEFAULT 0,
    depth_at_price  REAL DEFAULT 0,
    best_ask        REAL DEFAULT 0,
    best_ask_size   REAL DEFAULT 0,
    competitive     REAL DEFAULT 0,
    neg_risk        INTEGER DEFAULT 0,

    -- Ставка
    -- ВАЖНО: stake = количество токенов (shares), НЕ доллары
    -- Реально потрачено = stake * stake_price (cost_usdc)
    -- Выигрыш при победе = stake * $1 = stake (payout_target)
    stake           REAL DEFAULT 0,             -- токены (shares) куплено на Polymarket
    stake_price     REAL DEFAULT 0,             -- цена входа (implied prob, 0..1)
    cost_usdc       REAL GENERATED ALWAYS AS (
        ROUND(stake * stake_price, 2)
    ) STORED,                                   -- реально потрачено USDC
    payout_target   REAL GENERATED ALWAYS AS (
        stake                                   -- каждый токен = $1 при победе
    ) STORED,
    profit_target   REAL GENERATED ALWAYS AS (
        ROUND(stake * (1.0 - stake_price), 2)  -- чистая прибыль при победе
    ) STORED,

    -- Результат размещения
    status          TEXT DEFAULT 'pending',     -- pending/placed/failed/cancelled
    order_id        TEXT DEFAULT '',            -- Polymarket order ID (0x...)
    placed_at       TEXT DEFAULT '',
    error_msg       TEXT DEFAULT '',

    -- Результат события
    outcome_result  TEXT DEFAULT 'pending',     -- pending/won/lost/void/push/sold
    profit_actual   REAL DEFAULT 0,
    sell_price      REAL DEFAULT 0,             -- цена продажи (0..1) при досрочной продаже
    settled_at      TEXT DEFAULT '',

    -- Расчёты
    roi_expected    REAL GENERATED ALWAYS AS (edge * 100) STORED,  -- %
    roi_actual      REAL GENERATED ALWAYS AS (
        CASE WHEN stake > 0 AND stake_price > 0 AND outcome_result != 'pending'
        THEN (profit_actual / (stake * stake_price)) * 100
        ELSE NULL END
    ) STORED,                                   -- ROI считается от реально вложенных $

    -- Доп. данные
    direct_link_raw TEXT DEFAULT '',
    order_book_json TEXT DEFAULT '[]',          -- JSON стакана на момент ставки
    notes           TEXT DEFAULT '',
    bet_mode        TEXT DEFAULT 'prematch'        -- 'prematch' или 'live'
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_bets_outcome_id   ON bets(outcome_id);
CREATE INDEX IF NOT EXISTS idx_bets_market_id    ON bets(market_id);
CREATE INDEX IF NOT EXISTS idx_bets_status       ON bets(status);
CREATE INDEX IF NOT EXISTS idx_bets_started_at   ON bets(started_at);
CREATE INDEX IF NOT EXISTS idx_bets_created_at   ON bets(created_at);

-- Статистика по дням
CREATE TABLE IF NOT EXISTS daily_stats (
    date            TEXT PRIMARY KEY,
    bets_count      INTEGER DEFAULT 0,
    bets_placed     INTEGER DEFAULT 0,
    volume_usdc     REAL DEFAULT 0,
    expected_profit REAL DEFAULT 0,
    actual_profit   REAL DEFAULT 0,
    won             INTEGER DEFAULT 0,
    lost            INTEGER DEFAULT 0,
    void            INTEGER DEFAULT 0,
    roi_actual_pct  REAL DEFAULT 0
);

-- Bankroll tracker
CREATE TABLE IF NOT EXISTS bankroll (
    id          INTEGER PRIMARY KEY,
    amount      REAL NOT NULL DEFAULT 500.0,
    updated_at  TEXT
);
INSERT OR IGNORE INTO bankroll(id, amount) VALUES(1, 500.0);

-- Settings (key-value)
CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    updated_at  TEXT
);
INSERT OR IGNORE INTO settings(key, value) VALUES('free_usdc', NULL);
"""


@dataclass
class BetRecord:
    """Запись ставки из БД"""
    id: int
    outcome_id: str
    market_id: str
    home: str
    away: str
    league: str
    outcome_name: str
    market_type_name: str
    market_param: float
    bb_odds: float
    bb_price: float
    value_pct: float
    arb_pct: float            # доходность вилки arbs[].percent (для анализа)
    total_liquidity: float
    depth_at_price: float
    stake: float
    stake_price: float        # цена входа (implied prob)
    cost_usdc: float          # реально потрачено USDC (stake × stake_price)
    payout_target: float      # выигрыш при победе (= stake, т.к. 1 токен = $1)
    profit_target: float      # чистая прибыль при победе
    status: str
    order_id: str
    outcome_result: str
    profit_actual: float
    created_at: str
    started_at: int
    error_msg: str


class BetDatabase:
    def __init__(self, path: str = "valuebets.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        log.info("БД инициализирована: %s", path)

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate()
        self._init_mm_tables()
        self._init_settlement_tables()
        self._init_backlog_table()

    def _init_mm_tables(self):
        """Создаёт таблицы для Market Making модуля."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS mm_markets (
                condition_id TEXT PRIMARY KEY,
                token_yes TEXT NOT NULL,
                token_no TEXT NOT NULL,
                question TEXT DEFAULT '',
                event_name TEXT DEFAULT '',
                sport TEXT DEFAULT '',
                neg_risk INTEGER DEFAULT 0,
                tick_size TEXT DEFAULT '0.01',
                status TEXT DEFAULT 'active',
                added_at TEXT DEFAULT (datetime('now')),
                config_json TEXT DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS mm_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT (datetime('now')),
                condition_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                shares REAL NOT NULL,
                cost_usdc REAL NOT NULL,
                order_id TEXT DEFAULT '',
                fill_type TEXT DEFAULT '',
                market_question TEXT DEFAULT '',
                event_name TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_mm_fills_cid ON mm_fills(condition_id);
        """)

        # ── Line Movement Snapshots ───────────────────────────
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS bet_line_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bet_id INTEGER NOT NULL,
                ts REAL NOT NULL,
                mid_price REAL NOT NULL,
                minutes_after INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_bls_bet ON bet_line_snapshots(bet_id);
        """)
        self.conn.commit()

    def _init_backlog_table(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS backlog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                status TEXT DEFAULT 'idea',
                priority TEXT DEFAULT 'medium',
                category TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0
            );
        """)
        self.conn.commit()

    def _init_settlement_tables(self):
        """Таблица для Settlement Sniper модуля."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS settlement_snipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT (datetime('now')),
                condition_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                market_question TEXT DEFAULT '',
                sport TEXT DEFAULT '',
                side TEXT DEFAULT 'YES',
                snipe_price REAL DEFAULT 0,
                snipe_amount REAL DEFAULT 0,
                snipe_cost_usdc REAL DEFAULT 0,
                order_id TEXT DEFAULT '',
                status TEXT DEFAULT 'watching',
                profit_target REAL DEFAULT 0,
                profit_actual REAL DEFAULT 0,
                executed_at TEXT DEFAULT '',
                settled_at TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_snipe_cid ON settlement_snipes(condition_id);
            CREATE INDEX IF NOT EXISTS idx_snipe_status ON settlement_snipes(status);
        """)
        self.conn.commit()

    def _migrate(self):
        """Добавляем новые колонки в существующие БД без потери данных"""
        cur = self.conn.cursor()
        # Проверяем и добавляем колонки которых может не быть в старых БД
        existing = {row[1] for row in cur.execute("PRAGMA table_info(bets)")}
        migrations = [
            ("bet_mode",          "TEXT DEFAULT 'prematch'"),
            ("arb_pct",           "REAL DEFAULT 0"),
            ("sell_price",        "REAL DEFAULT 0"),
            ("resell_enabled",    "INTEGER DEFAULT 0"),
            ("sell_order_id",     "TEXT DEFAULT ''"),
            ("sell_price_target", "REAL DEFAULT 0"),
            ("resell_status",     "TEXT DEFAULT ''"),
            ("dutch_pair_id",     "TEXT DEFAULT ''"),
        ]
        # GENERATED ALWAYS нельзя добавить через ALTER TABLE в SQLite
        # Добавляем только обычные колонки
        for col, defn in migrations:
            if col not in existing:
                try:
                    cur.execute(f"ALTER TABLE bets ADD COLUMN {col} {defn}")
                    log.info("БД миграция: добавлена колонка %s", col)
                except Exception as e:
                    log.debug("migrate %s: %s", col, e)
        # Индексы
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_bets_resell_status ON bets(resell_status)",
            "CREATE INDEX IF NOT EXISTS idx_bets_dutch_pair ON bets(dutch_pair_id) WHERE dutch_pair_id != ''",
        ]:
            try:
                cur.execute(idx_sql)
            except Exception:
                pass
        self.conn.commit()

    # ──────────────────────────────────────────────────────
    # ДЕДУПЛИКАЦИЯ
    # ──────────────────────────────────────────────────────

    def already_bet(self, outcome_id: str, only_active: bool = True) -> Optional[BetRecord]:
        """
        Проверяет, ставилась ли уже ставка на этот исход.
        
        only_active=True → считается дублем только если статус placed/pending
                           и матч ещё не начался (или только что начался)
        only_active=False → любая историческая запись
        
        Возвращает BetRecord если дубль, иначе None.
        """
        if only_active:
            cur = self.conn.execute("""
                SELECT * FROM bets 
                WHERE outcome_id = ?
                  AND status IN ('pending', 'placed')
                  AND outcome_result = 'pending'
                ORDER BY id DESC LIMIT 1
            """, (outcome_id,))
        else:
            cur = self.conn.execute("""
                SELECT * FROM bets 
                WHERE outcome_id = ?
                ORDER BY id DESC LIMIT 1
            """, (outcome_id,))

        row = cur.fetchone()
        if not row:
            return None
        return self._row_to_record(row)

    def already_bet_market(self, market_id: str, outcome_name: str) -> Optional[BetRecord]:
        """
        Проверяет ставку по market_id + outcome_name (запасной вариант если outcome_id нет).
        """
        cur = self.conn.execute("""
            SELECT * FROM bets
            WHERE market_id = ? AND outcome_name = ?
              AND status IN ('pending', 'placed')
              AND outcome_result = 'pending'
            ORDER BY id DESC LIMIT 1
        """, (market_id, outcome_name))
        row = cur.fetchone()
        return self._row_to_record(row) if row else None

    # ──────────────────────────────────────────────────────
    # ЗАПИСЬ СТАВКИ
    # ──────────────────────────────────────────────────────

    def insert_bet(
        self,
        pm_bet,          # PolymarketBet объект
        stake: float,
        stake_price: float = 0.0,
        status: str = "pending",
        notes: str = "",
        bet_mode: str = "prematch",   # 'prematch' или 'live'
    ) -> int:
        """
        Записывает намерение сделать ставку в БД (до реального размещения).
        Возвращает ID записи.
        """
        import json

        ob_json = json.dumps([
            {"odds": lvl.odds, "price": lvl.price, "size": lvl.size}
            for lvl in (pm_bet.order_book or [])
        ])

        cur = self.conn.execute("""
            INSERT INTO bets (
                outcome_id, market_id, pm_event_id,
                bb_bet_id, bb_ref_event_id,
                home, away, league, sport_id, started_at,
                outcome_name, market_type, market_type_name, market_param,
                bb_odds, bb_price, value_pct, arb_pct,
                total_liquidity, depth_at_price, best_ask, best_ask_size,
                competitive, neg_risk,
                stake, stake_price,
                status,
                direct_link_raw, order_book_json, notes, bet_mode
            ) VALUES (
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?,
                ?, ?, ?, ?
            )
        """, (
            pm_bet.outcome_id or "",
            pm_bet.market_id or "",
            pm_bet.event_id or "",
            pm_bet.bet_id or "",
            pm_bet.ref_event_id or 0,
            pm_bet.home, pm_bet.away, pm_bet.league,
            pm_bet.sport_id, pm_bet.started_at,
            pm_bet.outcome_name,
            pm_bet.market_type, pm_bet.market_type_name, pm_bet.market_param,
            pm_bet.bb_odds, pm_bet.bb_price, pm_bet.value_pct, pm_bet.arb_pct,
            pm_bet.total_liquidity, pm_bet.depth_at_price,
            pm_bet.best_ask, pm_bet.best_ask_size,
            pm_bet.competitive, int(pm_bet.neg_risk),
            stake, stake_price,
            status,
            pm_bet.direct_link_raw or "", ob_json, notes, bet_mode,
        ))
        self.conn.commit()
        rec_id = cur.lastrowid
        log.info("БД: записан бет #%d [%s]  %s vs %s  outcome=%s  stake=$%.2f",
                 rec_id, bet_mode, pm_bet.home, pm_bet.away, pm_bet.outcome_name, stake)
        return rec_id

    def update_placed(
        self,
        record_id: int,
        order_id: str,
        status: str = "placed",
        stake_price: float = 0.0,
        error_msg: str = "",
    ):
        """Обновляет запись после реального размещения на Polymarket"""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            UPDATE bets SET
                order_id   = ?,
                status     = ?,
                stake_price= CASE WHEN ? > 0 THEN ? ELSE stake_price END,
                placed_at  = ?,
                error_msg  = ?
            WHERE id = ?
        """, (order_id, status, stake_price, stake_price, now, error_msg, record_id))
        self.conn.commit()
        log.info("БД: обновлён бет #%d  status=%s  order=%s", record_id, status, order_id[:16] if order_id else "")

    def update_failed(self, record_id: int, error_msg: str):
        """Помечает ставку как неудачную"""
        self.conn.execute("""
            UPDATE bets SET status = 'failed', error_msg = ?
            WHERE id = ?
        """, (error_msg[:500], record_id))
        self.conn.commit()

    # ──────────────────────────────────────────────────────
    # СЕТТЛ / РАСЧЁТ
    # ──────────────────────────────────────────────────────

    def settle(
        self,
        outcome_id: str,
        outcome_result: str,  # won/lost/void/push
        profit_actual: float = 0.0,
    ) -> int:
        """
        Закрывает ставку по исходу.
        outcome_result: 'won' | 'lost' | 'void' | 'push'
        profit_actual:  фактический P&L в USDC (положительный = прибыль, отрицательный = убыток)
        Возвращает кол-во обновлённых записей.
        """
        now = datetime.now(timezone.utc).isoformat()
        cur = self.conn.execute("""
            UPDATE bets SET
                outcome_result = ?,
                profit_actual  = ?,
                settled_at     = ?,
                status         = CASE WHEN status = 'placed' THEN 'settled' ELSE status END
            WHERE outcome_id = ?
              AND outcome_result = 'pending'
        """, (outcome_result, profit_actual, now, outcome_id))
        self.conn.commit()
        return cur.rowcount

    def settle_by_id(self, record_id: int, outcome_result: str,
                     profit_actual: float = 0.0, sell_price: float = 0.0) -> int:
        now = datetime.now(timezone.utc).isoformat()
        # ЗАЩИТА: не перезаписываем cancelled/failed ставки как won/lost
        # (ордер мог быть отменён по TTL, но авторасчёт нашёл выигравший токен)
        cur = self.conn.execute("""
            UPDATE bets SET
                outcome_result = ?,
                profit_actual  = ?,
                sell_price     = ?,
                settled_at     = ?,
                status         = CASE WHEN status = 'placed' THEN 'settled' ELSE status END
            WHERE id = ?
              AND status NOT IN ('cancelled', 'failed')
        """, (outcome_result, profit_actual, sell_price, now, record_id))
        self.conn.commit()
        return cur.rowcount

    # ──────────────────────────────────────────────────────
    # RESELL
    # ──────────────────────────────────────────────────────

    def get_active_resells(self) -> list:
        """Все ставки с активным resell (ждут размещения SELL или SELL активен)."""
        cur = self.conn.execute("""
            SELECT * FROM bets
            WHERE resell_status IN ('pending_sell', 'selling')
            ORDER BY id DESC
        """)
        return [self._row_to_record(r) for r in cur.fetchall()]

    def update_resell_placed(self, record_id: int, sell_order_id: str = "",
                             sell_price_target: float = 0, resell_status: str = "selling"):
        """Обновляет данные resell после размещения SELL ордера."""
        self.conn.execute("""
            UPDATE bets SET
                sell_order_id = ?,
                sell_price_target = ?,
                resell_status = ?,
                resell_enabled = 1
            WHERE id = ?
        """, (sell_order_id, sell_price_target, resell_status, record_id))
        self.conn.commit()

    def update_resell_result(self, record_id: int, resell_status: str,
                             profit_actual: float = 0, sell_price: float = 0):
        """Обновляет результат resell (sold/expired/cancelled)."""
        now = datetime.now(timezone.utc).isoformat()
        update_fields = "resell_status = ?"
        params = [resell_status]

        if resell_status == "sold":
            update_fields += ", outcome_result = 'sold', profit_actual = ?, sell_price = ?, settled_at = ?, status = 'settled'"
            params.extend([profit_actual, sell_price, now])
        elif resell_status in ("expired", "cancelled"):
            update_fields += ", resell_status = ?"
            # Не меняем outcome_result — оставляем pending для обычного расчёта
            params.append(resell_status)
            # Убираем дубликат — уже есть в первом ?
            update_fields = "resell_status = ?"
            params = [resell_status]

        self.conn.execute(f"UPDATE bets SET {update_fields} WHERE id = ?",
                          params + [record_id])
        self.conn.commit()

    def get_resell_stats(self) -> dict:
        """Агрегированная статистика по resell."""
        row = self.conn.execute("""
            SELECT
                SUM(CASE WHEN resell_status='sold' THEN 1 ELSE 0 END) as total_resold,
                SUM(CASE WHEN resell_status='sold' THEN profit_actual ELSE 0 END) as total_markup_profit,
                SUM(CASE WHEN resell_status IN('pending_sell','selling') THEN 1 ELSE 0 END) as pending_resells,
                SUM(CASE WHEN resell_status='expired' THEN 1 ELSE 0 END) as expired,
                SUM(CASE WHEN resell_enabled=1 THEN 1 ELSE 0 END) as total_resell_bets,
                ROUND(SUM(CASE WHEN resell_status='sold'
                    THEN (CASE WHEN stake_price>0 THEN stake*stake_price ELSE stake*bb_price END)
                    ELSE 0 END), 2) as resell_volume,
                ROUND(AVG(CASE WHEN resell_status='sold' AND sell_price_target > 0 AND stake_price > 0
                    THEN (sell_price_target - stake_price) / stake_price * 100
                    ELSE NULL END), 2) as avg_markup_pct,
                -- По режимам
                SUM(CASE WHEN resell_status='sold' AND (bet_mode IS NULL OR bet_mode='' OR bet_mode='prematch') THEN 1 ELSE 0 END) as pm_resold,
                SUM(CASE WHEN resell_status='sold' AND (bet_mode IS NULL OR bet_mode='' OR bet_mode='prematch') THEN profit_actual ELSE 0 END) as pm_profit,
                SUM(CASE WHEN resell_status='sold' AND bet_mode='live' THEN 1 ELSE 0 END) as lv_resold,
                SUM(CASE WHEN resell_status='sold' AND bet_mode='live' THEN profit_actual ELSE 0 END) as lv_profit
            FROM bets
            WHERE resell_enabled = 1
        """).fetchone()
        return dict(row) if row else {}

    # ──────────────────────────────────────────────────────
    # DUTCHING
    # ──────────────────────────────────────────────────────

    def already_dutched(self, condition_id: str) -> bool:
        """Есть ли активная dutching пара на этом маркете."""
        row = self.conn.execute(
            "SELECT 1 FROM bets WHERE market_id=? AND bet_mode='dutching' "
            "AND status IN ('pending','placed') LIMIT 1",
            (condition_id,)
        ).fetchone()
        return row is not None

    def get_dutch_stats(self) -> dict:
        """Агрегированная статистика по dutching."""
        row = self.conn.execute("""
            SELECT
                COUNT(DISTINCT dutch_pair_id) as total_pairs,
                SUM(CASE WHEN status='settled' THEN 1 ELSE 0 END) / 2 as settled_pairs,
                SUM(CASE WHEN status IN ('placed','pending') THEN 1 ELSE 0 END) / 2 as active_pairs,
                ROUND(SUM(CASE WHEN status='settled' THEN profit_actual ELSE 0 END), 2) as total_profit,
                ROUND(SUM(stake * stake_price), 2) as total_volume
            FROM bets
            WHERE bet_mode='dutching' AND dutch_pair_id != ''
        """).fetchone()
        return dict(row) if row else {}

    def get_dutch_pairs(self, limit: int = 50) -> list[dict]:
        """Последние dutching пары (обе ноги)."""
        rows = self.conn.execute("""
            SELECT dutch_pair_id,
                   GROUP_CONCAT(outcome_name, ' / ') as sides,
                   MIN(home || ' vs ' || away) as event,
                   SUM(stake * stake_price) as total_cost,
                   GROUP_CONCAT(stake_price, ',') as prices,
                   GROUP_CONCAT(status, ',') as statuses,
                   GROUP_CONCAT(order_id, ',') as order_ids,
                   SUM(profit_actual) as profit,
                   MIN(created_at) as created_at
            FROM bets
            WHERE bet_mode='dutching' AND dutch_pair_id != ''
            GROUP BY dutch_pair_id
            ORDER BY MIN(id) DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ──────────────────────────────────────────────────────
    # MARKET MAKING
    # ──────────────────────────────────────────────────────

    def mm_add_market(self, condition_id: str, token_yes: str, token_no: str,
                      question: str = "", event_name: str = "", sport: str = "",
                      neg_risk: bool = False, tick_size: str = "0.01"):
        self.conn.execute(
            "INSERT OR REPLACE INTO mm_markets "
            "(condition_id, token_yes, token_no, question, event_name, sport, neg_risk, tick_size) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (condition_id, token_yes, token_no, question, event_name, sport,
             1 if neg_risk else 0, tick_size)
        )
        self.conn.commit()

    # ──────────────────────────────────────────────────────
    # LINE MOVEMENT (мониторинг движения линии после ставки)
    # ──────────────────────────────────────────────────────

    def line_record_snapshot(self, bet_id: int, ts: float, mid_price: float, minutes_after: int):
        self.conn.execute(
            "INSERT INTO bet_line_snapshots (bet_id, ts, mid_price, minutes_after) VALUES (?,?,?,?)",
            (bet_id, ts, mid_price, minutes_after))
        self.conn.commit()

    def line_get_bets_to_track(self) -> list:
        """Ставки для отслеживания: placed, pending, матч ещё не начался."""
        import time
        now = time.time()
        rows = self.conn.execute("""
            SELECT id, outcome_id, stake_price, placed_at, started_at, sport_id, league
            FROM bets
            WHERE status = 'placed' AND outcome_result = 'pending'
              AND started_at > ?
        """, (now,)).fetchall()
        return [dict(r) for r in rows]

    def line_get_live_bets_to_track(self) -> list:
        """Live ставки для отслеживания: placed, pending (матч уже идёт — started_at <= now)."""
        import time
        now = time.time()
        rows = self.conn.execute("""
            SELECT id, outcome_id, stake_price, placed_at, started_at, sport_id, league
            FROM bets
            WHERE status = 'placed' AND outcome_result = 'pending'
              AND bet_mode = 'live'
        """).fetchall()
        return [dict(r) for r in rows]

    def line_get_movement(self, bet_id: int) -> dict:
        """Снимки движения линии для одной ставки."""
        bet = self.conn.execute(
            "SELECT stake_price, sport_id, league, home, away, outcome_name FROM bets WHERE id=?",
            (bet_id,)).fetchone()
        if not bet:
            return {}
        entry = float(bet["stake_price"] or 0)

        rows = self.conn.execute(
            "SELECT ts, mid_price, minutes_after FROM bet_line_snapshots WHERE bet_id=? ORDER BY ts",
            (bet_id,)).fetchall()
        snapshots = [{"t": r["minutes_after"], "p": round(r["mid_price"], 4)} for r in rows]

        last_price = snapshots[-1]["p"] if snapshots else entry
        move_abs = round(last_price - entry, 4)
        move_pct = round(move_abs / entry * 100, 2) if entry > 0 else 0

        return {
            "bet_id": bet_id,
            "entry_price": entry,
            "snapshots": snapshots,
            "last_price": last_price,
            "move_abs": move_abs,
            "move_pct": move_pct,
            "favorable": move_abs > 0,  # цена выросла = рынок подтвердил нашу ставку
            "sport_id": bet["sport_id"],
            "league": bet["league"],
            "match": f"{bet['home']} vs {bet['away']}",
            "outcome": bet["outcome_name"],
        }

    def line_get_stats(self) -> dict:
        """Агрегированная статистика движения линии по спортам."""
        # Для каждого бета: entry_price vs последний snapshot
        rows = self.conn.execute("""
            SELECT b.id, b.stake_price, b.sport_id, b.league,
                   (SELECT mid_price FROM bet_line_snapshots s
                    WHERE s.bet_id = b.id ORDER BY s.ts DESC LIMIT 1) as last_mid
            FROM bets b
            WHERE b.status IN ('placed','settled')
              AND EXISTS (SELECT 1 FROM bet_line_snapshots s WHERE s.bet_id = b.id)
        """).fetchall()

        by_sport = {}
        total_moves = []
        for r in rows:
            entry = float(r["stake_price"] or 0)
            last = float(r["last_mid"] or entry)
            if entry <= 0:
                continue
            move = round((last - entry) / entry * 100, 2)
            favorable = move > 0  # цена выросла = рынок подтвердил ставку
            total_moves.append({"move": move, "favorable": favorable})

            sid = str(r["sport_id"] or "?")
            if sid not in by_sport:
                by_sport[sid] = {"count": 0, "moves": [], "favorable": 0}
            by_sport[sid]["count"] += 1
            by_sport[sid]["moves"].append(move)
            if favorable:
                by_sport[sid]["favorable"] += 1

        # Формируем ответ
        sport_stats = {}
        for sid, d in by_sport.items():
            avg_move = round(sum(d["moves"]) / len(d["moves"]), 2) if d["moves"] else 0
            fav_pct = round(d["favorable"] / d["count"] * 100) if d["count"] > 0 else 0
            sport_stats[sid] = {
                "count": d["count"],
                "avg_move": avg_move,
                "favorable_pct": fav_pct,
            }

        total_fav = sum(1 for m in total_moves if m["favorable"])
        total_avg = round(sum(m["move"] for m in total_moves) / len(total_moves), 2) if total_moves else 0

        return {
            "total_tracked": len(total_moves),
            "favorable_pct": round(total_fav / len(total_moves) * 100) if total_moves else 0,
            "avg_move_pct": total_avg,
            "by_sport": sport_stats,
        }

    def mm_remove_market(self, condition_id: str):
        self.conn.execute("UPDATE mm_markets SET status='stopped' WHERE condition_id=?",
                          (condition_id,))
        self.conn.commit()

    def mm_get_active_markets(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM mm_markets WHERE status='active'"
        ).fetchall()
        return [dict(r) for r in rows]

    def mm_record_fill(self, condition_id: str, token_id: str, side: str,
                       price: float, shares: float, order_id: str = "",
                       fill_type: str = "", question: str = "", event: str = "",
                       fee_rate: float = 0, fee_usdc: float = 0):
        cost = round(price * shares, 4)
        self.conn.execute(
            "INSERT INTO mm_fills (condition_id, token_id, side, price, shares, "
            "cost_usdc, order_id, fill_type, market_question, event_name, fee_rate, fee_usdc) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (condition_id, token_id, side, price, shares, cost,
             order_id, fill_type, question, event, fee_rate, fee_usdc)
        )
        self.conn.commit()

    def mm_get_stats(self) -> dict:
        row = self.conn.execute("""
            SELECT
                COUNT(*) as total_fills,
                COUNT(DISTINCT condition_id) as markets_traded
            FROM mm_fills
        """).fetchone()

        # Суммы по направлению: потрачено (BUY) и получено (SELL)
        cash = self.conn.execute("""
            SELECT
                SUM(CASE WHEN side IN ('yes','no') THEN ABS(cost_usdc) ELSE 0 END) as total_spent,
                SUM(CASE WHEN side IN ('yes_sell','no_sell') THEN ABS(cost_usdc) ELSE 0 END) as total_received
            FROM mm_fills
        """).fetchone()
        total_spent = cash["total_spent"] or 0
        total_received = cash["total_received"] or 0

        # Позиция по маркетам
        pos = self.conn.execute("""
            SELECT condition_id,
                SUM(CASE WHEN side='yes' THEN shares ELSE 0 END) as yes_bought,
                SUM(CASE WHEN side='no' THEN shares ELSE 0 END) as no_bought,
                SUM(CASE WHEN side='yes' THEN cost_usdc ELSE 0 END) as yes_buy_cost,
                SUM(CASE WHEN side='no' THEN cost_usdc ELSE 0 END) as no_buy_cost,
                SUM(CASE WHEN side='yes_sell' THEN ABS(shares) ELSE 0 END) as yes_sold,
                SUM(CASE WHEN side='no_sell' THEN ABS(shares) ELSE 0 END) as no_sold,
                SUM(CASE WHEN side='yes_sell' THEN ABS(cost_usdc) ELSE 0 END) as yes_sell_proceeds,
                SUM(CASE WHEN side='no_sell' THEN ABS(cost_usdc) ELSE 0 END) as no_sell_proceeds
            FROM mm_fills GROUP BY condition_id
        """).fetchall()

        total_paired_value = 0  # гарантированный payout от paired shares
        total_unpaired_cost = 0  # стоимость непарных shares (risk)
        total_sell_pnl = 0       # P&L от закрытых через sell позиций

        for p in pos:
            yes_bought = p["yes_bought"] or 0
            no_bought  = p["no_bought"] or 0
            yes_buy_cost = p["yes_buy_cost"] or 0
            no_buy_cost  = p["no_buy_cost"] or 0
            yes_sold = p["yes_sold"] or 0
            no_sold  = p["no_sold"] or 0
            yes_sell_proceeds = p["yes_sell_proceeds"] or 0
            no_sell_proceeds  = p["no_sell_proceeds"] or 0

            avg_yes = yes_buy_cost / yes_bought if yes_bought > 0 else 0
            avg_no  = no_buy_cost / no_bought if no_bought > 0 else 0

            # Realized P&L от SELLs
            if yes_sold > 0:
                total_sell_pnl += yes_sell_proceeds - yes_sold * avg_yes
            if no_sold > 0:
                total_sell_pnl += no_sell_proceeds - no_sold * avg_no

            # Текущая позиция
            yes_now = max(yes_bought - yes_sold, 0)
            no_now  = max(no_bought - no_sold, 0)

            # Paired = гарантированный $1 payout за пару
            paired = min(yes_now, no_now)
            if paired > 0:
                paired_cost = paired * (avg_yes + avg_no)
                total_paired_value += paired  # $1 per pair
                # P&L пар = payout - cost
                # Но это UNREALIZED пока маркет не settled

            # Непарные = risk
            unpaired = abs(yes_now - no_now)
            if yes_now > no_now:
                total_unpaired_cost += unpaired * avg_yes
            elif no_now > yes_now:
                total_unpaired_cost += unpaired * avg_no

        # ── Итоговые метрики ──────────────────────────────
        # Spent/Received — точные цифры из fills
        # Paired P&L = paired_shares × $1 payout - paired_cost (гарантировано при settlement)
        total_paired_cost = 0
        for p in pos:
            yb = p["yes_bought"] or 0; nb = p["no_bought"] or 0
            yc = p["yes_buy_cost"] or 0; nc = p["no_buy_cost"] or 0
            ys = p["yes_sold"] or 0; ns = p["no_sold"] or 0
            avg_y = yc / yb if yb > 0 else 0
            avg_n = nc / nb if nb > 0 else 0
            y_now = max(yb - ys, 0); n_now = max(nb - ns, 0)
            paired = min(y_now, n_now)
            if paired > 0:
                total_paired_cost += paired * (avg_y + avg_n)

        paired_pnl = round(total_paired_value - total_paired_cost, 2)  # $1 × paired - cost

        # Captured spread — средний спред между bid и ask fills
        spread_row = self.conn.execute("""
            SELECT
                AVG(CASE WHEN fill_type IN ('bid_fill','bid_sell_no') THEN price END) as avg_bid,
                AVG(CASE WHEN fill_type IN ('ask_fill','ask_sell_yes') THEN price END) as avg_ask,
                SUM(CASE WHEN side IN ('yes','no') THEN ABS(fee_usdc) ELSE 0 END) as total_fees
            FROM mm_fills
        """).fetchone()
        avg_bid = spread_row["avg_bid"] or 0
        avg_ask = spread_row["avg_ask"] or 0
        # Captured spread как % от mid: (ask - bid) / mid * 100
        avg_mid = (avg_bid + avg_ask) / 2 if avg_bid > 0 and avg_ask > 0 else 0
        captured_spread = round((avg_ask - avg_bid) / avg_mid * 100, 1) if avg_mid > 0 else 0
        total_fees = round(spread_row["total_fees"] or 0, 2)

        active = self.conn.execute(
            "SELECT COUNT(*) FROM mm_markets WHERE status='active'"
        ).fetchone()[0]
        return {
            "active_markets": active,
            "total_fills": dict(row)["total_fills"] or 0,
            "total_spent": round(total_spent, 2),
            "total_received": round(total_received, 2),
            "realized_pnl": round(total_sell_pnl, 2),
            "paired_pnl": paired_pnl,
            "total_pnl": round(total_sell_pnl + paired_pnl - total_fees, 2),
            "net_exposure": round(total_unpaired_cost, 2),
            "captured_spread": captured_spread,  # средний captured spread в центах
            "total_fees": total_fees,
        }

    def mm_get_fills(self, condition_id: str = None, limit: int = 50) -> list[dict]:
        if condition_id:
            rows = self.conn.execute(
                "SELECT * FROM mm_fills WHERE condition_id=? ORDER BY id DESC LIMIT ?",
                (condition_id, limit)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM mm_fills ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def mm_get_position(self, condition_id: str) -> dict:
        row = self.conn.execute("""
            SELECT
                SUM(CASE WHEN side='yes' THEN shares ELSE 0 END) as yes_bought,
                SUM(CASE WHEN side='no' THEN shares ELSE 0 END) as no_bought,
                SUM(CASE WHEN side='yes_sell' THEN ABS(shares) ELSE 0 END) as yes_sold,
                SUM(CASE WHEN side='no_sell' THEN ABS(shares) ELSE 0 END) as no_sold,
                SUM(CASE WHEN side='yes' THEN cost_usdc ELSE 0 END) as yes_cost,
                SUM(CASE WHEN side='no' THEN cost_usdc ELSE 0 END) as no_cost,
                SUM(CASE WHEN side='yes_sell' THEN ABS(cost_usdc) ELSE 0 END) as yes_sell_proceeds,
                SUM(CASE WHEN side='no_sell' THEN ABS(cost_usdc) ELSE 0 END) as no_sell_proceeds,
                SUM(ABS(cost_usdc)) as total_volume,
                COUNT(*) as fills_count,
                AVG(CASE WHEN fill_type IN ('bid_fill','bid_sell_no') THEN price END) as avg_bid_price,
                AVG(CASE WHEN fill_type IN ('ask_fill','ask_sell_yes') THEN price END) as avg_ask_price
            FROM mm_fills WHERE condition_id=?
        """, (condition_id,)).fetchone()
        if not row:
            return {"yes_shares": 0, "no_shares": 0, "net": 0, "total_cost": 0,
                    "fills_count": 0, "realized_pnl": 0, "avg_yes": 0, "avg_no": 0}
        d = dict(row)
        yb = d["yes_bought"] or 0
        nb = d["no_bought"] or 0
        ys = d["yes_sold"] or 0
        ns = d["no_sold"] or 0
        yc = d["yes_cost"] or 0
        nc = d["no_cost"] or 0
        ysp = d["yes_sell_proceeds"] or 0
        nsp = d["no_sell_proceeds"] or 0

        avg_yes = yc / yb if yb > 0 else 0
        avg_no  = nc / nb if nb > 0 else 0

        # Realized P&L от продаж
        realized = 0
        if ys > 0:
            realized += ysp - ys * avg_yes
        if ns > 0:
            realized += nsp - ns * avg_no

        # Текущая позиция
        yes_now = max(yb - ys, 0)
        no_now  = max(nb - ns, 0)

        # Captured spread per market
        abp = d.get("avg_bid_price") or 0
        aap = d.get("avg_ask_price") or 0
        avg_mid_m = (abp + aap) / 2 if abp > 0 and aap > 0 else 0
        margin_pct = round((aap - abp) / avg_mid_m * 100, 1) if avg_mid_m > 0 else 0

        return {
            "yes_shares": round(yes_now, 2),
            "no_shares": round(no_now, 2),
            "net": round(yes_now - no_now, 2),
            "total_cost": round((d["total_volume"] or 0), 2),
            "fills_count": d["fills_count"] or 0,
            "realized_pnl": round(realized, 4),
            "margin_pct": margin_pct,
            "avg_yes": round(avg_yes, 4),
            "avg_no": round(avg_no, 4),
            "yes_bought": round(yb, 2),
            "no_bought": round(nb, 2),
            "yes_sold": round(ys, 2),
            "no_sold": round(ns, 2),
        }

    # ──────────────────────────────────────────────────────
    # ЧТЕНИЕ
    # ──────────────────────────────────────────────────────

    def get_active_bets(self) -> list:
        """Все активные ставки (placed/pending, ещё не рассчитаны)"""
        cur = self.conn.execute("""
            SELECT * FROM bets
            WHERE status IN ('pending', 'placed')
              AND outcome_result = 'pending'
            ORDER BY id DESC
        """)
        return [self._row_to_record(r) for r in cur.fetchall()]

    def get_recent(self, limit: int = 50) -> list:
        """Последние N ставок"""
        cur = self.conn.execute("""
            SELECT * FROM bets ORDER BY id DESC LIMIT ?
        """, (limit,))
        return [self._row_to_record(r) for r in cur.fetchall()]

    def get_stats(self) -> dict:
        """Общая статистика"""
        cur = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status IN ('placed','settled') THEN 1 ELSE 0 END) as placed,
                SUM(CASE WHEN outcome_result = 'won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN outcome_result = 'lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN outcome_result = 'sold' THEN 1 ELSE 0 END) as sold,
                SUM(CASE WHEN outcome_result = 'void' THEN 1 ELSE 0 END) as void,
                ROUND(SUM(stake * stake_price), 2) as total_volume,  -- реально потрачено USDC
                ROUND(SUM(profit_actual), 2) as total_profit,
                ROUND(AVG(value_pct), 2) as avg_edge,
                ROUND(
                    CASE WHEN SUM(stake * stake_price) > 0
                    THEN (SUM(profit_actual) / SUM(stake * stake_price)) * 100
                    ELSE 0 END, 2
                ) as roi_actual_pct
            FROM bets
        """)
        row = cur.fetchone()
        return dict(row) if row else {}

    def get_bankroll(self) -> float:
        cur = self.conn.execute("SELECT amount FROM bankroll WHERE id = 1")
        row = cur.fetchone()
        return float(row["amount"]) if row else 500.0

    def set_bankroll(self, amount: float):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute("""
            UPDATE bankroll SET amount = ?, updated_at = ? WHERE id = 1
        """, (amount, now))
        self.conn.commit()

    def print_stats(self):
        """Красивая печать статистики в консоль"""
        stats = self.get_stats()
        active = self.get_active_bets()
        bankroll = self.get_bankroll()

        settled = (stats.get("won", 0) or 0) + (stats.get("lost", 0) or 0)
        winrate = (stats.get("won", 0) / settled * 100) if settled > 0 else 0

        print(f"{'═'*60}")
        print(f"  📊 СТАТИСТИКА БД  ({self.path})")
        print(f"{'═'*60}")
        print(f"  Всего записей:    {stats.get('total', 0)}")
        print(f"  Размещено:        {stats.get('placed', 0)}")
        print(f"  Активных:         {len(active)}")
        print(f"  Выиграно:         {stats.get('won', 0)}")
        print(f"  Проиграно:        {stats.get('lost', 0)}")
        print(f"  Void:             {stats.get('void', 0)}")
        print(f"  Win Rate:         {winrate:.1f}%")
        print(f"  Объём ставок:     ${stats.get('total_volume', 0) or 0:,.2f}")
        print(f"  Прибыль:          ${stats.get('total_profit', 0) or 0:+,.2f}")
        print(f"  ROI фактический:  {stats.get('roi_actual_pct', 0) or 0:+.2f}%")
        print(f"  Avg Edge:         +{stats.get('avg_edge', 0) or 0:.2f}%")
        print(f"  Банкролл:         ${bankroll:,.2f}")

        if active:
            print(f"  ⏳ АКТИВНЫЕ СТАВКИ ({len(active)}):")
            for b in active[:10]:
                import datetime as dt
                started = dt.datetime.fromtimestamp(b.started_at).strftime("%d.%m %H:%M") if b.started_at else "?"
                print(f"    #{b.id:04d}  {b.home} vs {b.away}")
                print(f"          {b.outcome_name}  |  ${b.stake:.2f}  |  {b.status}  |  матч: {started}")
        print(f"{'═'*60}")

    def _row_to_record(self, row) -> BetRecord:
        r = dict(row)  # sqlite3.Row → dict
        stake       = r.get("stake", 0) or 0
        stake_price = r.get("stake_price", 0) or 0
        # GENERATED columns may not exist in old DBs — compute fallback
        cost_usdc     = r.get("cost_usdc")
        payout_target = r.get("payout_target")
        profit_target = r.get("profit_target")
        if cost_usdc is None:
            cost_usdc = round(stake * stake_price, 2)
        if payout_target is None:
            payout_target = stake
        if profit_target is None:
            profit_target = round(stake * (1.0 - stake_price), 2)
        return BetRecord(
            id=r["id"],
            outcome_id=r["outcome_id"],
            market_id=r["market_id"],
            home=r["home"],
            away=r["away"],
            league=r["league"],
            outcome_name=r["outcome_name"],
            market_type_name=r.get("market_type_name", ""),
            market_param=r.get("market_param", 0) or 0,
            bb_odds=r["bb_odds"],
            bb_price=r["bb_price"],
            value_pct=r["value_pct"],
            arb_pct=float(r["arb_pct"]) if r["arb_pct"] is not None else 0.0,
            total_liquidity=r.get("total_liquidity", 0) or 0,
            depth_at_price=r.get("depth_at_price", 0) or 0,
            stake=stake,
            stake_price=stake_price,
            cost_usdc=float(cost_usdc or 0),
            payout_target=float(payout_target or stake),
            profit_target=float(profit_target or 0),
            status=r["status"],
            order_id=r.get("order_id", "") or "",
            outcome_result=r.get("outcome_result", "pending") or "pending",
            profit_actual=r.get("profit_actual", 0) or 0,
            created_at=r.get("created_at", ""),
            started_at=r.get("started_at", 0) or 0,
            error_msg=r.get("error_msg", "") or "",
        )


    # ── Free USDC tracking ────────────────────────────────────────────────────

    def get_free_usdc(self):
        """Возвращает свободный USDC или None если не задан."""
        try:
            cur = self.conn.execute("SELECT value FROM settings WHERE key='free_usdc'")
            row = cur.fetchone()
            return float(row["value"]) if row and row["value"] is not None else None
        except Exception:
            return None

    def set_free_usdc(self, amount: float):
        """Устанавливает свободный USDC вручную."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "INSERT OR REPLACE INTO settings(key, value, updated_at) VALUES('free_usdc', ?, ?)",
            (str(round(amount, 2)), now)
        )
        self.conn.commit()

    def adjust_free_usdc(self, delta: float):
        """Изменяет свободный USDC на delta. Если не задан — ничего не делает."""
        from datetime import datetime, timezone
        try:
            cur = self.conn.execute("SELECT value FROM settings WHERE key='free_usdc'")
            row = cur.fetchone()
            if row and row["value"] is not None:
                new_val = round(float(row["value"]) + delta, 2)
                now = datetime.now(timezone.utc).isoformat()
                self.conn.execute(
                    "UPDATE settings SET value=?, updated_at=? WHERE key='free_usdc'",
                    (str(new_val), now)
                )
                self.conn.commit()
        except Exception:
            pass