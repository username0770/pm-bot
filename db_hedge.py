# -*- coding: utf-8 -*-
"""
База данных хедж-позиций — пары матч + турнир, расчёты, позиции.

Паттерн: db_bets.py (SQLite, WAL, row_factory, migrate).
"""

import json
import sqlite3
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

SCHEMA = """
-- Обнаруженные / вручную созданные пары (матч + турнир)
CREATE TABLE IF NOT EXISTS hedge_pairs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),
    pair_id         TEXT NOT NULL UNIQUE,

    sport           TEXT NOT NULL DEFAULT '',
    event_name      TEXT NOT NULL DEFAULT '',
    player_a        TEXT NOT NULL DEFAULT '',
    player_b        TEXT NOT NULL DEFAULT '',

    -- Match market
    match_condition_id  TEXT NOT NULL DEFAULT '',
    match_token_id      TEXT NOT NULL DEFAULT '',
    match_question      TEXT NOT NULL DEFAULT '',
    match_end_date      TEXT NOT NULL DEFAULT '',
    match_neg_risk      INTEGER DEFAULT 0,

    -- Tournament market
    tourney_condition_id TEXT NOT NULL DEFAULT '',
    tourney_token_id     TEXT NOT NULL DEFAULT '',
    tourney_question     TEXT NOT NULL DEFAULT '',
    tourney_end_date     TEXT NOT NULL DEFAULT '',
    tourney_neg_risk     INTEGER DEFAULT 0,
    tourney_player       TEXT NOT NULL DEFAULT '',

    status          TEXT DEFAULT 'discovered',  -- discovered/watching/hedged/closed/expired
    notes           TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_hp_pair_id ON hedge_pairs(pair_id);
CREATE INDEX IF NOT EXISTS idx_hp_status ON hedge_pairs(status);

-- Сохранённые расчёты калькулятора
CREATE TABLE IF NOT EXISTS hedge_saved_calcs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),

    -- Описание
    label           TEXT NOT NULL DEFAULT '',

    -- Position A (match)
    pos_a_name      TEXT NOT NULL DEFAULT '',
    pos_a_side      TEXT NOT NULL DEFAULT '',
    pos_a_price     REAL NOT NULL DEFAULT 0,
    pos_a_token_id  TEXT NOT NULL DEFAULT '',

    -- Position B (tournament)
    pos_b_name      TEXT NOT NULL DEFAULT '',
    pos_b_player    TEXT NOT NULL DEFAULT '',
    pos_b_side      TEXT NOT NULL DEFAULT '',
    pos_b_price     REAL NOT NULL DEFAULT 0,
    pos_b_token_id  TEXT NOT NULL DEFAULT '',

    -- Budget & Result
    budget          REAL NOT NULL DEFAULT 0,
    scenarios_json  TEXT DEFAULT '[]',
    result_json     TEXT DEFAULT '{}',

    -- Computed (from result)
    profit          REAL DEFAULT 0,
    roi_pct         REAL DEFAULT 0
);

-- Исполненные хедж-позиции
CREATE TABLE IF NOT EXISTS hedge_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),

    pair_id         TEXT NOT NULL DEFAULT '',
    calc_id         INTEGER DEFAULT 0,           -- ссылка на saved_calc

    -- Позиция A (match)
    token_id_a      TEXT NOT NULL DEFAULT '',
    entry_price_a   REAL NOT NULL DEFAULT 0,
    size_a          REAL NOT NULL DEFAULT 0,
    cost_a          REAL NOT NULL DEFAULT 0,
    neg_risk_a      INTEGER DEFAULT 0,

    -- Позиция B (tournament)
    token_id_b      TEXT NOT NULL DEFAULT '',
    entry_price_b   REAL NOT NULL DEFAULT 0,
    size_b          REAL NOT NULL DEFAULT 0,
    cost_b          REAL NOT NULL DEFAULT 0,
    neg_risk_b      INTEGER DEFAULT 0,

    budget          REAL NOT NULL DEFAULT 0,
    expected_profit REAL DEFAULT 0,
    expected_roi    REAL DEFAULT 0,
    scenarios_json  TEXT DEFAULT '[]',

    -- Ордера
    order_id_a      TEXT DEFAULT '',
    order_status_a  TEXT DEFAULT 'pending',       -- pending/placed/filled/failed/cancelled
    order_id_b      TEXT DEFAULT '',
    order_status_b  TEXT DEFAULT 'pending',

    -- Выход
    exit_price_a    REAL DEFAULT 0,
    exit_price_b    REAL DEFAULT 0,
    actual_profit   REAL DEFAULT 0,

    status          TEXT DEFAULT 'pending',       -- pending/open/partial/closed/failed
    closed_at       TEXT DEFAULT '',
    notes           TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_hpos_pair_id ON hedge_positions(pair_id);
CREATE INDEX IF NOT EXISTS idx_hpos_status ON hedge_positions(status);
"""


@dataclass
class HedgePairRecord:
    id: int
    pair_id: str
    sport: str
    event_name: str
    player_a: str
    player_b: str
    match_condition_id: str
    match_token_id: str
    match_question: str
    tourney_condition_id: str
    tourney_token_id: str
    tourney_question: str
    tourney_player: str
    status: str
    created_at: str


@dataclass
class HedgePositionRecord:
    id: int
    pair_id: str
    token_id_a: str
    token_id_b: str
    entry_price_a: float
    entry_price_b: float
    size_a: float
    size_b: float
    cost_a: float
    cost_b: float
    budget: float
    expected_profit: float
    expected_roi: float
    order_id_a: str
    order_status_a: str
    order_id_b: str
    order_status_b: str
    exit_price_a: float
    exit_price_b: float
    actual_profit: float
    status: str
    created_at: str
    notes: str


class HedgeDatabase:
    def __init__(self, path: str = "hedge.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        log.info("HedgeDatabase ready: %s", path)

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ── Pairs ───────────────────────────────────────────────────────────────

    def insert_pair(self, pair) -> int:
        """Вставить обнаруженную пару. Возвращает id."""
        try:
            cur = self.conn.execute("""
                INSERT OR IGNORE INTO hedge_pairs
                (pair_id, sport, event_name, player_a, player_b,
                 match_condition_id, match_token_id, match_question, match_end_date, match_neg_risk,
                 tourney_condition_id, tourney_token_id, tourney_question, tourney_end_date, tourney_neg_risk,
                 tourney_player, status)
                VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?)
            """, (
                pair.pair_id, pair.sport, pair.event_name,
                pair.player_a, pair.player_b,
                pair.match_market.condition_id,
                pair.match_market.token_id_yes,
                pair.match_market.question,
                pair.match_market.end_date,
                1 if pair.match_market.neg_risk else 0,
                pair.tournament_market.condition_id,
                pair.tournament_market.token_id_yes,
                pair.tournament_market.question,
                pair.tournament_market.end_date,
                1 if pair.tournament_market.neg_risk else 0,
                pair.tournament_player,
                "discovered",
            ))
            self.conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return 0  # already exists

    def get_pairs(self, status: str = None) -> list[dict]:
        """Получить пары, опционально фильтруя по статусу."""
        if status:
            rows = self.conn.execute(
                "SELECT * FROM hedge_pairs WHERE status = ? ORDER BY created_at DESC", (status,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM hedge_pairs ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def update_pair_status(self, pair_id: str, status: str):
        self.conn.execute(
            "UPDATE hedge_pairs SET status = ? WHERE pair_id = ?", (status, pair_id)
        )
        self.conn.commit()

    def delete_pair(self, pair_id: str):
        self.conn.execute("DELETE FROM hedge_pairs WHERE pair_id = ?", (pair_id,))
        self.conn.commit()

    # ── Saved Calculations ──────────────────────────────────────────────────

    def save_calc(self, data: dict) -> int:
        """Сохранить расчёт калькулятора."""
        cur = self.conn.execute("""
            INSERT INTO hedge_saved_calcs
            (label, pos_a_name, pos_a_side, pos_a_price, pos_a_token_id,
             pos_b_name, pos_b_player, pos_b_side, pos_b_price, pos_b_token_id,
             budget, scenarios_json, result_json, profit, roi_pct)
            VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?)
        """, (
            data.get("label", ""),
            data.get("pos_a_name", ""),
            data.get("pos_a_side", ""),
            data.get("pos_a_price", 0),
            data.get("pos_a_token_id", ""),
            data.get("pos_b_name", ""),
            data.get("pos_b_player", ""),
            data.get("pos_b_side", ""),
            data.get("pos_b_price", 0),
            data.get("pos_b_token_id", ""),
            data.get("budget", 0),
            json.dumps(data.get("scenarios", [])),
            json.dumps(data.get("result", {})),
            data.get("profit", 0),
            data.get("roi_pct", 0),
        ))
        self.conn.commit()
        return cur.lastrowid

    def get_saved_calcs(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM hedge_saved_calcs ORDER BY created_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["scenarios"] = json.loads(d.get("scenarios_json", "[]"))
            d["result"] = json.loads(d.get("result_json", "{}"))
            result.append(d)
        return result

    def delete_saved_calc(self, calc_id: int):
        self.conn.execute("DELETE FROM hedge_saved_calcs WHERE id = ?", (calc_id,))
        self.conn.commit()

    # ── Positions ───────────────────────────────────────────────────────────

    def insert_position(self, data: dict) -> int:
        """Записать исполненную хедж-позицию."""
        cur = self.conn.execute("""
            INSERT INTO hedge_positions
            (pair_id, calc_id,
             token_id_a, entry_price_a, size_a, cost_a, neg_risk_a,
             token_id_b, entry_price_b, size_b, cost_b, neg_risk_b,
             budget, expected_profit, expected_roi, scenarios_json,
             status, notes)
            VALUES (?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?)
        """, (
            data.get("pair_id", ""),
            data.get("calc_id", 0),
            data.get("token_id_a", ""),
            data.get("entry_price_a", 0),
            data.get("size_a", 0),
            data.get("cost_a", 0),
            data.get("neg_risk_a", 0),
            data.get("token_id_b", ""),
            data.get("entry_price_b", 0),
            data.get("size_b", 0),
            data.get("cost_b", 0),
            data.get("neg_risk_b", 0),
            data.get("budget", 0),
            data.get("expected_profit", 0),
            data.get("expected_roi", 0),
            json.dumps(data.get("scenarios", [])),
            "pending",
            data.get("notes", ""),
        ))
        self.conn.commit()
        return cur.lastrowid

    def update_order(self, position_id: int, leg: str, order_id: str, status: str):
        """Обновить статус ордера для ноги A или B."""
        if leg == "a":
            self.conn.execute(
                "UPDATE hedge_positions SET order_id_a = ?, order_status_a = ? WHERE id = ?",
                (order_id, status, position_id)
            )
        else:
            self.conn.execute(
                "UPDATE hedge_positions SET order_id_b = ?, order_status_b = ? WHERE id = ?",
                (order_id, status, position_id)
            )
        # Если обе ноги заполнены — status = open
        row = self.conn.execute(
            "SELECT order_status_a, order_status_b FROM hedge_positions WHERE id = ?",
            (position_id,)
        ).fetchone()
        if row and row["order_status_a"] == "filled" and row["order_status_b"] == "filled":
            self.conn.execute(
                "UPDATE hedge_positions SET status = 'open' WHERE id = ?", (position_id,)
            )
        elif row and (row["order_status_a"] == "filled" or row["order_status_b"] == "filled"):
            self.conn.execute(
                "UPDATE hedge_positions SET status = 'partial' WHERE id = ?", (position_id,)
            )
        self.conn.commit()

    def close_position(self, position_id: int, exit_price_a: float,
                       exit_price_b: float, actual_profit: float):
        """Закрыть позицию."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        self.conn.execute("""
            UPDATE hedge_positions
            SET exit_price_a = ?, exit_price_b = ?, actual_profit = ?,
                status = 'closed', closed_at = ?
            WHERE id = ?
        """, (exit_price_a, exit_price_b, actual_profit, now, position_id))
        self.conn.commit()

    def get_positions(self, status: str = None) -> list[dict]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM hedge_positions WHERE status = ? ORDER BY created_at DESC",
                (status,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM hedge_positions ORDER BY created_at DESC"
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["scenarios"] = json.loads(d.get("scenarios_json", "[]"))
            result.append(d)
        return result

    def get_active_positions(self) -> list[dict]:
        """Позиции со статусом pending/open/partial."""
        rows = self.conn.execute(
            "SELECT * FROM hedge_positions WHERE status IN ('pending','open','partial') "
            "ORDER BY created_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["scenarios"] = json.loads(d.get("scenarios_json", "[]"))
            result.append(d)
        return result

    def get_stats(self) -> dict:
        """Агрегированная статистика."""
        total = self.conn.execute(
            "SELECT COUNT(*) as cnt, SUM(budget) as vol FROM hedge_positions"
        ).fetchone()
        closed = self.conn.execute(
            "SELECT COUNT(*) as cnt, SUM(actual_profit) as pnl FROM hedge_positions WHERE status='closed'"
        ).fetchone()
        active = self.conn.execute(
            "SELECT COUNT(*) as cnt, SUM(budget) as vol FROM hedge_positions WHERE status IN ('pending','open','partial')"
        ).fetchone()
        return {
            "total_positions": total["cnt"] if total else 0,
            "total_volume": total["vol"] or 0,
            "closed_count": closed["cnt"] if closed else 0,
            "total_pnl": closed["pnl"] or 0,
            "active_count": active["cnt"] if active else 0,
            "active_volume": active["vol"] or 0,
        }
