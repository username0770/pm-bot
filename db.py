"""
SQLite база данных для учёта ставок и P&L
"""

import sqlite3
import json
from datetime import datetime
from models import Arb, BetResult

SCHEMA = """
CREATE TABLE IF NOT EXISTS bets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    uid             TEXT UNIQUE,
    placed_at       TEXT,
    event_name      TEXT,
    sport           TEXT,
    roi_expected    REAL,

    -- PS3838 плечо
    ps_event_id     TEXT,
    ps_market_id    TEXT,
    ps_selection    TEXT,
    ps_odds         REAL,
    ps_stake        REAL,
    ps_bet_id       TEXT,
    ps_success      INTEGER,
    ps_error        TEXT,

    -- Polymarket плечо
    pm_token_id     TEXT,
    pm_price        REAL,
    pm_odds         REAL,
    pm_stake        REAL,
    pm_bet_id       TEXT,
    pm_success      INTEGER,
    pm_error        TEXT,

    -- Итог
    total_stake     REAL,
    status          TEXT DEFAULT 'open'   -- open / won / lost / void
);

CREATE TABLE IF NOT EXISTS bankroll (
    id          INTEGER PRIMARY KEY,
    amount      REAL NOT NULL DEFAULT 1000.0,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS daily_pnl (
    date        TEXT PRIMARY KEY,
    bets_count  INTEGER DEFAULT 0,
    volume      REAL DEFAULT 0,
    profit      REAL DEFAULT 0
);
"""


class Database:
    def __init__(self, path: str = "arb_bot.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        # Инициализируем банкролл если нет
        cur = self.conn.execute("SELECT COUNT(*) FROM bankroll")
        if cur.fetchone()[0] == 0:
            self.conn.execute(
                "INSERT INTO bankroll (id, amount, updated_at) VALUES (1, 1000.0, ?)",
                (datetime.now().isoformat(),)
            )
        self.conn.commit()

    def save_arb(self, arb: Arb, total_stake: float, ps_result: BetResult, pm_result: BetResult):
        pm_leg = arb.polymarket_leg
        ps_leg = arb.ps3838_leg

        self.conn.execute("""
            INSERT OR IGNORE INTO bets (
                uid, placed_at, event_name, sport, roi_expected,
                ps_event_id, ps_market_id, ps_selection, ps_odds, ps_stake,
                ps_bet_id, ps_success, ps_error,
                pm_token_id, pm_price, pm_odds, pm_stake,
                pm_bet_id, pm_success, pm_error,
                total_stake, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            arb.uid,
            datetime.now().isoformat(),
            arb.event_name,
            arb.sport,
            arb.roi,
            ps_leg.event_id if ps_leg else None,
            ps_leg.market_id if ps_leg else None,
            ps_leg.selection if ps_leg else None,
            ps_leg.odds if ps_leg else None,
            total_stake * arb.ps3838_stake_ratio,
            ps_result.bet_id,
            int(ps_result.success),
            ps_result.error,
            pm_leg.token_id if pm_leg else None,
            pm_leg.price if pm_leg else None,
            1 / pm_leg.price if pm_leg and pm_leg.price else None,
            total_stake * arb.polymarket_stake_ratio,
            pm_result.bet_id,
            int(pm_result.success),
            pm_result.error,
            total_stake,
            "open" if (ps_result.success and pm_result.success) else "error"
        ))

        # Обновляем дневной P&L
        today = datetime.now().strftime("%Y-%m-%d")
        expected_profit = total_stake * arb.roi if (ps_result.success and pm_result.success) else 0
        self.conn.execute("""
            INSERT INTO daily_pnl (date, bets_count, volume, profit)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                bets_count = bets_count + 1,
                volume = volume + excluded.volume,
                profit = profit + excluded.profit
        """, (today, total_stake, expected_profit))

        self.conn.commit()

    def get_bankroll(self) -> float:
        row = self.conn.execute("SELECT amount FROM bankroll WHERE id=1").fetchone()
        return row["amount"] if row else 1000.0

    def update_bankroll(self, new_amount: float):
        self.conn.execute(
            "UPDATE bankroll SET amount=?, updated_at=? WHERE id=1",
            (new_amount, datetime.now().isoformat())
        )
        self.conn.commit()

    def get_stats(self) -> dict:
        """Сводная статистика"""
        row = self.conn.execute("""
            SELECT
                COUNT(*) as total_bets,
                SUM(total_stake) as total_volume,
                SUM(CASE WHEN ps_success=1 AND pm_success=1 THEN total_stake * roi_expected ELSE 0 END) as expected_profit,
                SUM(CASE WHEN ps_success=0 OR pm_success=0 THEN 1 ELSE 0 END) as failed_bets
            FROM bets
        """).fetchone()

        today = datetime.now().strftime("%Y-%m-%d")
        today_row = self.conn.execute(
            "SELECT * FROM daily_pnl WHERE date=?", (today,)
        ).fetchone()

        return {
            "total_bets": row["total_bets"],
            "total_volume": row["total_volume"] or 0,
            "expected_profit": row["expected_profit"] or 0,
            "failed_bets": row["failed_bets"],
            "bankroll": self.get_bankroll(),
            "today_bets": today_row["bets_count"] if today_row else 0,
            "today_volume": today_row["volume"] if today_row else 0,
            "today_profit": today_row["profit"] if today_row else 0,
        }

    def print_stats(self):
        s = self.get_stats()
        print(f"""
╔══════════════════════════════════════╗
║           ARB BOT СТАТИСТИКА         ║
╠══════════════════════════════════════╣
║  Банкролл:        ${s['bankroll']:>10.2f}         ║
║  Всего ставок:    {s['total_bets']:>10}         ║
║  Оборот всего:    ${s['total_volume']:>10.2f}         ║
║  Ожид. прибыль:   ${s['expected_profit']:>10.2f}         ║
║  Ошибок:          {s['failed_bets']:>10}         ║
╠══════════════════════════════════════╣
║  Сегодня ставок:  {s['today_bets']:>10}         ║
║  Сегодня оборот:  ${s['today_volume']:>10.2f}         ║
║  Сегодня профит:  ${s['today_profit']:>10.2f}         ║
╚══════════════════════════════════════╝
        """)
