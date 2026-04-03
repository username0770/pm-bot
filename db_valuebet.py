"""
Отдельная SQLite БД для вэлью-бетов с собственной статистикой
"""

import sqlite3
from datetime import datetime
from models import BetResult

SCHEMA = """
CREATE TABLE IF NOT EXISTS valuebets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    uid             TEXT UNIQUE,
    placed_at       TEXT,
    event_name      TEXT,
    sport           TEXT,

    -- Ставка
    token_id        TEXT,
    pm_price        REAL,   -- вероятность 0..1
    pm_odds         REAL,   -- коэффициент = 1/price
    stake           REAL,
    roi_expected    REAL,   -- edge от BetBurger
    liquidity       REAL,

    -- Результат размещения
    bet_id          TEXT,
    placed_ok       INTEGER,
    error           TEXT,

    -- Результат события (заполняется вручную или через settle)
    outcome         TEXT DEFAULT 'pending',  -- pending / won / lost / void
    profit_actual   REAL DEFAULT 0,
    settled_at      TEXT
);

CREATE TABLE IF NOT EXISTS vb_bankroll (
    id          INTEGER PRIMARY KEY,
    amount      REAL NOT NULL DEFAULT 1000.0,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS vb_daily (
    date            TEXT PRIMARY KEY,
    bets_count      INTEGER DEFAULT 0,
    volume          REAL DEFAULT 0,
    expected_profit REAL DEFAULT 0,
    actual_profit   REAL DEFAULT 0,
    won             INTEGER DEFAULT 0,
    lost            INTEGER DEFAULT 0
);
"""


class ValueBetDatabase:
    def __init__(self, path: str = "valuebet.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self.conn.executescript(SCHEMA)
        cur = self.conn.execute("SELECT COUNT(*) FROM vb_bankroll")
        if cur.fetchone()[0] == 0:
            self.conn.execute(
                "INSERT INTO vb_bankroll (id, amount, updated_at) VALUES (1, 1000.0, ?)",
                (datetime.now().isoformat(),)
            )
        self.conn.commit()

    def save_valuebet(self, vb, stake: float, result: BetResult):
        pm = vb.polymarket_leg
        today = datetime.now().strftime("%Y-%m-%d")

        self.conn.execute("""
            INSERT OR IGNORE INTO valuebets (
                uid, placed_at, event_name, sport,
                token_id, pm_price, pm_odds, stake, roi_expected, liquidity,
                bet_id, placed_ok, error
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            vb.uid,
            datetime.now().isoformat(),
            vb.event_name,
            vb.sport,
            pm.token_id if pm else None,
            pm.price if pm else None,
            1 / pm.price if pm and pm.price else None,
            stake,
            vb.roi,
            vb.polymarket_liquidity,
            result.bet_id,
            int(result.success),
            result.error,
        ))

        if result.success:
            expected_profit = stake * vb.roi
            self.conn.execute("""
                INSERT INTO vb_daily (date, bets_count, volume, expected_profit)
                VALUES (?, 1, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    bets_count = bets_count + 1,
                    volume = volume + excluded.volume,
                    expected_profit = expected_profit + excluded.expected_profit
            """, (today, stake, expected_profit))

        self.conn.commit()

    def settle_bet(self, uid: str, won: bool):
        """
        Зафиксировать результат ставки после завершения события.
        Вызывать вручную или через settle_all().
        """
        row = self.conn.execute(
            "SELECT stake, pm_odds FROM valuebets WHERE uid=?", (uid,)
        ).fetchone()
        if not row:
            return

        stake = row["stake"]
        odds = row["pm_odds"]
        profit = (stake * odds - stake) if won else -stake
        outcome = "won" if won else "lost"
        today = datetime.now().strftime("%Y-%m-%d")

        self.conn.execute("""
            UPDATE valuebets
            SET outcome=?, profit_actual=?, settled_at=?
            WHERE uid=?
        """, (outcome, profit, datetime.now().isoformat(), uid))

        # Обновляем дневную статистику
        self.conn.execute("""
            INSERT INTO vb_daily (date, actual_profit, won, lost)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                actual_profit = actual_profit + excluded.actual_profit,
                won = won + excluded.won,
                lost = lost + excluded.lost
        """, (today, profit, 1 if won else 0, 0 if won else 1))

        # Обновляем банкролл
        bankroll = self.get_bankroll()
        self.update_bankroll(bankroll + profit)

        self.conn.commit()

    def get_bankroll(self) -> float:
        row = self.conn.execute("SELECT amount FROM vb_bankroll WHERE id=1").fetchone()
        return row["amount"] if row else 1000.0

    def update_bankroll(self, amount: float):
        self.conn.execute(
            "UPDATE vb_bankroll SET amount=?, updated_at=? WHERE id=1",
            (round(amount, 2), datetime.now().isoformat())
        )
        self.conn.commit()

    def get_stats(self, period: str = "all") -> dict:
        """
        Статистика за период: 'today', '7d', '30d', 'all'
        """
        where = self._period_where(period)

        row = self.conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN placed_ok=1 THEN 1 ELSE 0 END) as placed,
                SUM(CASE WHEN placed_ok=1 THEN stake ELSE 0 END) as volume,
                SUM(CASE WHEN placed_ok=1 THEN stake * roi_expected ELSE 0 END) as expected_profit,
                SUM(CASE WHEN outcome='won' THEN profit_actual ELSE 0 END) as profit_won,
                SUM(CASE WHEN outcome='lost' THEN profit_actual ELSE 0 END) as profit_lost,
                SUM(CASE WHEN outcome='won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN outcome='lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN outcome='pending' THEN 1 ELSE 0 END) as pending,
                AVG(CASE WHEN placed_ok=1 THEN roi_expected ELSE NULL END) as avg_roi,
                AVG(CASE WHEN placed_ok=1 THEN pm_odds ELSE NULL END) as avg_odds
            FROM valuebets
            {where}
        """).fetchone()

        actual_profit = (row["profit_won"] or 0) + (row["profit_lost"] or 0)
        settled = (row["won"] or 0) + (row["lost"] or 0)
        win_rate = row["won"] / settled * 100 if settled > 0 else 0
        volume = row["volume"] or 0
        actual_roi = actual_profit / volume * 100 if volume > 0 else 0

        return {
            "period": period,
            "total_bets": row["total"] or 0,
            "placed_ok": row["placed"] or 0,
            "volume": volume,
            "expected_profit": row["expected_profit"] or 0,
            "actual_profit": round(actual_profit, 2),
            "won": row["won"] or 0,
            "lost": row["lost"] or 0,
            "pending": row["pending"] or 0,
            "win_rate": round(win_rate, 1),
            "avg_roi_pct": round((row["avg_roi"] or 0) * 100, 2),
            "avg_odds": round(row["avg_odds"] or 0, 3),
            "actual_roi_pct": round(actual_roi, 2),
            "bankroll": self.get_bankroll(),
        }

    def _period_where(self, period: str) -> str:
        if period == "today":
            return f"WHERE DATE(placed_at) = '{datetime.now().strftime('%Y-%m-%d')}'"
        if period == "7d":
            return "WHERE placed_at >= datetime('now', '-7 days')"
        if period == "30d":
            return "WHERE placed_at >= datetime('now', '-30 days')"
        return ""  # all

    def print_stats(self, period: str = "all"):
        s = self.get_stats(period)
        period_label = {"today": "Сегодня", "7d": "7 дней", "30d": "30 дней", "all": "Всё время"}.get(period, period)
        print(f"""
╔══════════════════════════════════════════╗
║      ВЭЛЬЮ-БЕТЫ СТАТИСТИКА: {period_label:<12} ║
╠══════════════════════════════════════════╣
║  Банкролл:          ${s['bankroll']:>10.2f}           ║
║  Ставок всего:      {s['total_bets']:>10}           ║
║  Успешно размещено: {s['placed_ok']:>10}           ║
║  Оборот:            ${s['volume']:>10.2f}           ║
╠══════════════════════════════════════════╣
║  Завершённые:       {s['won'] + s['lost']:>10}           ║
║  Выиграно:          {s['won']:>10}           ║
║  Проиграно:         {s['lost']:>10}           ║
║  В ожидании:        {s['pending']:>10}           ║
║  Win rate:          {s['win_rate']:>9.1f}%           ║
╠══════════════════════════════════════════╣
║  Ожид. ROI:         {s['avg_roi_pct']:>9.2f}%           ║
║  Факт. ROI:         {s['actual_roi_pct']:>9.2f}%           ║
║  Ожид. прибыль:     ${s['expected_profit']:>10.2f}           ║
║  Факт. прибыль:     ${s['actual_profit']:>10.2f}           ║
║  Средний коэф:      {s['avg_odds']:>10.3f}           ║
╚══════════════════════════════════════════╝
        """)

    def get_pending_bets(self) -> list:
        """Получить все незавершённые ставки"""
        rows = self.conn.execute("""
            SELECT uid, event_name, pm_odds, stake, placed_at
            FROM valuebets
            WHERE outcome = 'pending' AND placed_ok = 1
            ORDER BY placed_at DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def print_pending(self):
        """Вывести список незакрытых ставок"""
        pending = self.get_pending_bets()
        if not pending:
            print("Нет незакрытых ставок.")
            return
        print(f"\n{'='*60}")
        print(f"НЕЗАКРЫТЫЕ СТАВКИ ({len(pending)} шт):")
        print(f"{'='*60}")
        for b in pending:
            print(f"  [{b['uid'][:8]}]  {b['event_name']:<30}  @{b['pm_odds']:.2f}  ${b['stake']:.2f}  {b['placed_at'][:10]}")
        print(f"{'='*60}\n")
