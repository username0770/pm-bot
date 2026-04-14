"""
BTC Lab — local SQLite database for all data.
No limits, full control, fast.
"""
import sqlite3
import json
import uuid
import os
from datetime import datetime
from pathlib import Path

DB_PATH = Path(os.getenv("BTC_LAB_DB_PATH", str(Path(__file__).parent / "btc_lab.db")))


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS strategies (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        color TEXT DEFAULT '#3b82f6',
        is_active INTEGER DEFAULT 1,
        min_edge REAL DEFAULT 7.0,
        bet_amount_usdc REAL DEFAULT 10.0,
        autobet INTEGER DEFAULT 0,
        autobet_phase TEXT DEFAULT 'all',
        timer_min INTEGER DEFAULT 0,
        timer_max INTEGER DEFAULT 300,
        price_min REAL DEFAULT 0.01,
        price_max REAL DEFAULT 0.99,
        mirror INTEGER DEFAULT 0,
        fair_min REAL DEFAULT 0,
        cooldown INTEGER DEFAULT 30,
        max_bets_per_window INTEGER DEFAULT 5,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bets (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        strategy_id TEXT,
        strategy_name TEXT,
        bet_type TEXT NOT NULL CHECK(bet_type IN ('real', 'paper')),
        market_question TEXT,
        market_slug TEXT,
        up_token_id TEXT,
        down_token_id TEXT,
        event_start_time TEXT,
        event_end_time TEXT,
        side TEXT NOT NULL CHECK(side IN ('UP', 'DOWN')),
        amount_usdc REAL NOT NULL,
        intended_price REAL,
        executed_price REAL,
        slippage REAL,
        shares_received REAL,
        fee_rate REAL DEFAULT 0.072,
        fee_calculated REAL,
        fee_actual REAL,
        target_price REAL,
        target_price_source TEXT DEFAULT 'auto',
        cex_median_at_bet REAL,
        binance_at_bet REAL,
        coinbase_at_bet REAL,
        okx_at_bet REAL,
        bybit_at_bet REAL,
        move_from_target REAL,
        move_percent REAL,
        pm_up_price_at_bet REAL,
        pm_down_price_at_bet REAL,
        pm_bid_at_bet REAL,
        pm_ask_at_bet REAL,
        pm_spread_at_bet REAL,
        order_book_bids TEXT,
        order_book_asks TEXT,
        fair_probability REAL,
        edge REAL,
        model_volatility REAL DEFAULT 15.0,
        seconds_left_at_bet INTEGER,
        timer_phase TEXT CHECK(timer_phase IN ('early', 'mid', 'late')),
        placed_at TEXT NOT NULL,
        tx_hash TEXT,
        order_id TEXT,
        order_status TEXT,
        outcome TEXT DEFAULT 'PENDING' CHECK(outcome IN ('WIN', 'LOSS', 'VOID', 'PENDING')),
        resolved_price REAL,
        resolved_at TEXT,
        gross_pnl REAL,
        net_pnl REAL,
        roi REAL,
        note TEXT,
        auto_placed INTEGER DEFAULT 0,
        signal_type TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        question TEXT,
        event_start_time TEXT,
        end_date TEXT,
        target_price REAL,
        target_price_source TEXT DEFAULT 'auto',
        resolved_outcome TEXT,
        up_token_id TEXT,
        down_token_id TEXT,
        created_at TEXT NOT NULL,
        completed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS ticks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        ts INTEGER NOT NULL,
        binance REAL,
        coinbase REAL,
        okx REAL,
        bybit REAL,
        cex_median REAL,
        chainlink REAL,
        pm_up_price REAL,
        pm_bid REAL,
        pm_ask REAL,
        seconds_left INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_bets_strategy ON bets(strategy_id);
    CREATE INDEX IF NOT EXISTS idx_bets_type ON bets(bet_type);
    CREATE INDEX IF NOT EXISTS idx_bets_outcome ON bets(outcome);
    CREATE INDEX IF NOT EXISTS idx_bets_placed ON bets(placed_at);
    CREATE INDEX IF NOT EXISTS idx_bets_phase ON bets(timer_phase);
    CREATE INDEX IF NOT EXISTS idx_bets_session ON bets(session_id);
    CREATE INDEX IF NOT EXISTS idx_ticks_session ON ticks(session_id);

    CREATE TABLE IF NOT EXISTS kv_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS mm_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT,
        event_type TEXT,
        side TEXT,
        price REAL,
        size REAL,
        level INTEGER,
        pause_reason TEXT,
        pause_duration_sec REAL,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS mm_window_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        window_id TEXT NOT NULL,
        buys_count INTEGER DEFAULT 0,
        sells_count INTEGER DEFAULT 0,
        buys_shares REAL DEFAULT 0,
        sells_shares REAL DEFAULT 0,
        buys_usdc REAL DEFAULT 0,
        sells_usdc REAL DEFAULT 0,
        volume_usdc REAL DEFAULT 0,
        net_position_shares REAL DEFAULT 0,
        rebates_est REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS maker_buy_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        window_id TEXT,
        condition_id TEXT,
        order_id TEXT UNIQUE,
        token_label TEXT,
        price REAL,
        size REAL,
        usdc_spent REAL,
        outcome TEXT,
        won INTEGER,
        pnl REAL,
        redeemed INTEGER DEFAULT 0,
        filled_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS mm_window_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        window_id TEXT NOT NULL UNIQUE,
        window_question TEXT,
        started_at TEXT,
        completed_at TEXT,
        split_amount_usdc REAL DEFAULT 0,
        split_done INTEGER DEFAULT 0,
        yes_buys_count INTEGER DEFAULT 0,
        yes_sells_count INTEGER DEFAULT 0,
        no_buys_count INTEGER DEFAULT 0,
        no_sells_count INTEGER DEFAULT 0,
        yes_buys_usdc REAL DEFAULT 0,
        yes_sells_usdc REAL DEFAULT 0,
        no_buys_usdc REAL DEFAULT 0,
        no_sells_usdc REAL DEFAULT 0,
        total_volume_usdc REAL DEFAULT 0,
        spread_pnl_usdc REAL DEFAULT 0,
        merge_amount_usdc REAL DEFAULT 0,
        yes_remaining REAL DEFAULT 0,
        no_remaining REAL DEFAULT 0,
        rebates_est REAL DEFAULT 0,
        resolved_outcome TEXT,
        redeem_amount_usdc REAL DEFAULT 0,
        net_pnl_usdc REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    # Safe migration: add new columns if table already existed without them
    for col_def in (
        "ALTER TABLE maker_buy_trades ADD COLUMN condition_id TEXT",
        "ALTER TABLE maker_buy_trades ADD COLUMN token_label TEXT",
        "ALTER TABLE maker_buy_trades ADD COLUMN redeemed INTEGER DEFAULT 0",
        "ALTER TABLE maker_buy_trades ADD COLUMN seconds_to_expiry REAL",
        "ALTER TABLE maker_buy_trades ADD COLUMN maker_rebate_usdc REAL DEFAULT 0",
        "ALTER TABLE maker_buy_trades ADD COLUMN fee_usdc REAL DEFAULT 0",
    ):
        try:
            conn.execute(col_def)
        except sqlite3.OperationalError:
            pass  # column exists
    conn.commit()
    conn.close()
    print(f"DB ready: {DB_PATH}")


# === Strategies ===

def create_strategy(name, description="", color="#3b82f6", **kwargs):
    sid = f"strat_{int(datetime.utcnow().timestamp()*1000)}_{uuid.uuid4().hex[:4]}"
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute("""
        INSERT INTO strategies (id,name,description,color,
            min_edge,bet_amount_usdc,autobet,autobet_phase,
            timer_min,timer_max,price_min,price_max,
            mirror,fair_min,cooldown,max_bets_per_window,
            created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (sid, name, description, color,
          kwargs.get("min_edge", 7), kwargs.get("bet_amount_usdc", 10),
          int(kwargs.get("autobet", False)), kwargs.get("autobet_phase", "all"),
          kwargs.get("timer_min", 0), kwargs.get("timer_max", 300),
          kwargs.get("price_min", 0.01), kwargs.get("price_max", 0.99),
          int(kwargs.get("mirror", False)), kwargs.get("fair_min", 0),
          kwargs.get("cooldown", 30), kwargs.get("max_bets_per_window", 5),
          now, now))
    conn.commit()
    conn.close()
    return get_strategy(sid)


def get_strategy(sid):
    conn = get_conn()
    row = conn.execute("SELECT * FROM strategies WHERE id=?", (sid,)).fetchone()
    conn.close()
    return _strat_dict(row) if row else None


def list_strategies():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM strategies ORDER BY created_at").fetchall()
    conn.close()
    return [_strat_dict(r) for r in rows]


def list_active_autobet_strategies():
    """Get active strategies with autobet=ON, formatted for the bot loop."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM strategies WHERE is_active=1 AND autobet=1"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        s = dict(r)
        result.append({
            "id": s["id"], "name": s["name"], "enabled": True,
            "mirror": bool(s.get("mirror")),
            "minEdge": s.get("min_edge", 7),
            "timerMin": s.get("timer_min", 0),
            "timerMax": s.get("timer_max", 300),
            "betAmount": s.get("bet_amount_usdc", 10),
            "maxBetsPerWindow": s.get("max_bets_per_window", 5),
            "cooldown": s.get("cooldown", 30),
            "priceMin": s.get("price_min", 0.01),
            "priceMax": s.get("price_max", 0.99),
            "fairMin": s.get("fair_min", 0),
            "_v2": True,
        })
    return result


def update_strategy(sid, **kwargs):
    if not kwargs:
        return get_strategy(sid)
    kwargs["updated_at"] = datetime.utcnow().isoformat()
    # Convert bools to int for SQLite
    for k in ("is_active", "autobet", "mirror"):
        if k in kwargs:
            kwargs[k] = int(kwargs[k])
    cols = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [sid]
    conn = get_conn()
    conn.execute(f"UPDATE strategies SET {cols} WHERE id=?", vals)
    conn.commit()
    conn.close()
    return get_strategy(sid)


def delete_strategy(sid):
    conn = get_conn()
    conn.execute("DELETE FROM bets WHERE strategy_id=?", (sid,))
    conn.execute("DELETE FROM strategies WHERE id=?", (sid,))
    conn.commit()
    conn.close()
    return True


def _strat_dict(row):
    """Convert SQLite row to dict with stats."""
    d = dict(row)
    d["is_active"] = bool(d.get("is_active"))
    d["autobet"] = bool(d.get("autobet"))
    d["mirror"] = bool(d.get("mirror"))
    # Add bet stats
    conn = get_conn()
    sid = d["id"]
    stats = conn.execute("""
        SELECT COUNT(*) as total,
            SUM(CASE WHEN bet_type='real' THEN 1 ELSE 0 END) as real_count,
            SUM(CASE WHEN bet_type='paper' THEN 1 ELSE 0 END) as paper_count,
            SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN outcome IN ('WIN','LOSS') THEN 1 ELSE 0 END) as settled,
            COALESCE(SUM(net_pnl), 0) as pnl,
            COALESCE(SUM(fee_calculated), 0) as fees,
            COALESCE(AVG(ABS(edge)), 0) as avg_edge
        FROM bets WHERE strategy_id=?
    """, (sid,)).fetchone()
    conn.close()
    d["total_bets"] = stats["total"]
    d["total_real_bets"] = stats["real_count"] or 0
    d["total_paper_bets"] = stats["paper_count"] or 0
    d["wins"] = stats["wins"] or 0
    d["winrate"] = round((stats["wins"] or 0) / max(stats["settled"] or 1, 1) * 100, 1)
    d["total_pnl"] = round(stats["pnl"], 2)
    d["total_fees"] = round(stats["fees"], 4)
    d["avg_edge"] = round(stats["avg_edge"], 4)
    return d


# === Sessions ===

def upsert_session(session_id, **kwargs):
    conn = get_conn()
    exists = conn.execute("SELECT id FROM sessions WHERE id=?",
                          (session_id,)).fetchone()
    now = datetime.utcnow().isoformat()
    if exists:
        if kwargs:
            cols = ", ".join(f"{k}=?" for k in kwargs)
            conn.execute(f"UPDATE sessions SET {cols} WHERE id=?",
                         list(kwargs.values()) + [session_id])
    else:
        kwargs["id"] = session_id
        kwargs.setdefault("created_at", now)
        cols = ", ".join(kwargs.keys())
        ph = ", ".join("?" * len(kwargs))
        conn.execute(f"INSERT INTO sessions ({cols}) VALUES ({ph})",
                     list(kwargs.values()))
    conn.commit()
    conn.close()


def list_sessions(limit=50):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# === Bets ===

FIELD_MAP = {
    "sessionId": "session_id", "strategyId": "strategy_id",
    "strategyName": "strategy_name", "betType": "bet_type",
    "marketQuestion": "market_question", "marketSlug": "market_slug",
    "upTokenId": "up_token_id", "downTokenId": "down_token_id",
    "eventStartTime": "event_start_time", "eventEndTime": "event_end_time",
    "amountUSDC": "amount_usdc", "intendedPrice": "intended_price",
    "executedPrice": "executed_price", "sharesReceived": "shares_received",
    "feeRate": "fee_rate", "feeCalculated": "fee_calculated",
    "feeActual": "fee_actual", "targetPrice": "target_price",
    "targetPriceSource": "target_price_source",
    "cexMedianAtBet": "cex_median_at_bet",
    "binancePriceAtBet": "binance_at_bet",
    "coinbasePriceAtBet": "coinbase_at_bet",
    "okxPriceAtBet": "okx_at_bet", "bybitPriceAtBet": "bybit_at_bet",
    "moveFromTarget": "move_from_target", "movePercent": "move_percent",
    "pmUpPriceAtBet": "pm_up_price_at_bet",
    "pmDownPriceAtBet": "pm_down_price_at_bet",
    "pmBidAtBet": "pm_bid_at_bet", "pmAskAtBet": "pm_ask_at_bet",
    "pmSpreadAtBet": "pm_spread_at_bet",
    "orderBookBids": "order_book_bids", "orderBookAsks": "order_book_asks",
    "fairProbability": "fair_probability",
    "modelVolatility": "model_volatility",
    "secondsLeftAtBet": "seconds_left_at_bet",
    "timerPhase": "timer_phase", "placedAt": "placed_at",
    "txHash": "tx_hash", "orderId": "order_id",
    "orderStatus": "order_status", "resolvedPrice": "resolved_price",
    "resolvedAt": "resolved_at", "grossPnl": "gross_pnl",
    "netPnl": "net_pnl", "autoPlaced": "auto_placed",
    "signalType": "signal_type",
}

VALID_COLS = {
    "id", "session_id", "strategy_id", "strategy_name", "bet_type",
    "market_question", "market_slug", "up_token_id", "down_token_id",
    "event_start_time", "event_end_time", "side", "amount_usdc",
    "intended_price", "executed_price", "slippage", "shares_received",
    "fee_rate", "fee_calculated", "fee_actual", "target_price",
    "target_price_source", "cex_median_at_bet", "binance_at_bet",
    "coinbase_at_bet", "okx_at_bet", "bybit_at_bet",
    "move_from_target", "move_percent", "pm_up_price_at_bet",
    "pm_down_price_at_bet", "pm_bid_at_bet", "pm_ask_at_bet",
    "pm_spread_at_bet", "order_book_bids", "order_book_asks",
    "fair_probability", "edge", "model_volatility",
    "seconds_left_at_bet", "timer_phase", "placed_at",
    "tx_hash", "order_id", "order_status", "outcome",
    "resolved_price", "resolved_at", "gross_pnl", "net_pnl",
    "roi", "note", "auto_placed", "signal_type", "created_at",
}


def save_bet(bet_data):
    now = datetime.utcnow().isoformat()
    bid = bet_data.get("id") or f"bet_{int(datetime.utcnow().timestamp()*1000)}_{uuid.uuid4().hex[:6]}"

    # Normalize keys
    norm = {}
    for k, v in bet_data.items():
        col = FIELD_MAP.get(k, k)
        if isinstance(v, list):
            v = json.dumps(v)
        if isinstance(v, bool):
            v = int(v)
        norm[col] = v

    norm["id"] = bid
    norm.setdefault("created_at", now)
    norm.setdefault("placed_at", now)
    norm.setdefault("outcome", "PENDING")

    filtered = {k: v for k, v in norm.items() if k in VALID_COLS}
    cols = ", ".join(filtered.keys())
    ph = ", ".join("?" * len(filtered))
    conn = get_conn()
    conn.execute(f"INSERT OR REPLACE INTO bets ({cols}) VALUES ({ph})",
                 list(filtered.values()))
    conn.commit()
    conn.close()
    return bid


def settle_bet(bet_id, outcome, resolved_price, gross_pnl, net_pnl, roi):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute("""UPDATE bets SET outcome=?,resolved_price=?,
        gross_pnl=?,net_pnl=?,roi=?,resolved_at=? WHERE id=?""",
                 (outcome, resolved_price, gross_pnl, net_pnl, roi, now, bet_id))
    conn.commit()
    conn.close()


def get_pending_bets():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM bets WHERE outcome='PENDING' ORDER BY placed_at"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_bets(bet_type=None, strategy_id=None, outcome=None,
             timer_phase=None, side=None, limit=100, offset=0):
    where, params = [], []
    if bet_type:
        where.append("bet_type=?"); params.append(bet_type)
    if strategy_id:
        where.append("strategy_id=?"); params.append(strategy_id)
    if outcome:
        where.append("outcome=?"); params.append(outcome)
    if timer_phase:
        where.append("timer_phase=?"); params.append(timer_phase)
    if side:
        where.append("side=?"); params.append(side)
    sql = "SELECT * FROM bets"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY placed_at DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_bet(bet_id):
    conn = get_conn()
    conn.execute("DELETE FROM bets WHERE id=?", (bet_id,))
    conn.commit()
    conn.close()


# === Ticks ===

def save_tick(session_id, tick):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute("""INSERT INTO ticks
        (session_id,ts,binance,coinbase,okx,bybit,cex_median,chainlink,
         pm_up_price,pm_bid,pm_ask,seconds_left,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                 (session_id, tick.get("ts", 0),
                  tick.get("binance"), tick.get("coinbase"),
                  tick.get("okx"), tick.get("bybit"),
                  tick.get("cexMedian"), tick.get("chainlink"),
                  tick.get("pmUpPrice"), tick.get("pmBid"),
                  tick.get("pmAsk"), tick.get("secondsLeft"), now))
    conn.commit()
    conn.close()


def get_session_ticks(session_id, limit=500):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM ticks WHERE session_id=? ORDER BY ts LIMIT ?",
        (session_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# === Stats ===

def get_stats(bet_type=None, strategy_id=None):
    where, params = [], []
    if bet_type:
        where.append("bet_type=?"); params.append(bet_type)
    if strategy_id:
        where.append("strategy_id=?"); params.append(strategy_id)
    w = (" WHERE " + " AND ".join(where)) if where else ""

    conn = get_conn()
    row = conn.execute(f"""
        SELECT COUNT(*) as total,
            SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN outcome='PENDING' THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(net_pnl),0) as pnl,
            COALESCE(SUM(fee_calculated),0) as fees,
            COALESCE(AVG(ABS(edge)),0) as avg_edge
        FROM bets {w}""", params).fetchone()

    # By phase
    phases = {}
    for phase in ("early", "mid", "late"):
        p = conn.execute(f"""
            SELECT COUNT(*) as n,
                SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as w,
                COALESCE(SUM(net_pnl),0) as p
            FROM bets {w} {"AND" if w else "WHERE"} timer_phase=?
        """, params + [phase]).fetchone()
        phases[phase] = {
            "bets": p["n"] or 0,
            "wins": p["w"] or 0,
            "winrate": round((p["w"] or 0) / max(p["n"] or 0, 1) * 100, 1),
            "pnl": round(p["p"] or 0, 2),
        }
    conn.close()

    settled = (row["wins"] or 0) + (row["losses"] or 0)
    return {
        "total_bets": row["total"],
        "wins": row["wins"] or 0,
        "losses": row["losses"] or 0,
        "pending": row["pending"] or 0,
        "winrate": round((row["wins"] or 0) / max(settled, 1) * 100, 1),
        "total_pnl": round(row["pnl"], 2),
        "total_fees": round(row["fees"], 4),
        "avg_edge": round(row["avg_edge"], 4),
        "by_phase": phases,
    }


# === Settlement ===

async def auto_settle_pending(aio_session):
    """Settle PENDING bets using Gamma API."""
    import aiohttp
    pending = get_pending_bets()
    if not pending:
        return 0

    settled_count = 0
    by_session = {}
    for b in pending:
        sid = b["session_id"]
        by_session.setdefault(sid, []).append(b)

    for session_id, bets in by_session.items():
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={session_id}"
            async with aio_session.get(url,
                                       timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status != 200:
                    continue
                events = await r.json()
                if not events or not events[0].get("closed"):
                    continue
                m = events[0].get("markets", [{}])[0]
                op = json.loads(m.get("outcomePrices", "[0,0]"))
                resolved_outcome = "UP" if float(op[0]) > 0.5 else "DOWN"

                # Update maker_buy trades for this window
                try:
                    update_maker_buy_outcome(session_id, resolved_outcome)
                except Exception:
                    pass

                for b in bets:
                    usdc_spent = float(b["amount_usdc"] or 0)
                    shares = float(b["shares_received"] or 0)
                    if shares == 0:
                        # Fallback: calculate from price
                        price = float(b["intended_price"] or 0.5)
                        shares = usdc_spent / price if price > 0 else 0
                    won = b["side"] == resolved_outcome
                    if won:
                        # Each share pays $1 on win
                        gross = shares * 1.0 - usdc_spent
                        net = gross  # fee already included in usdc_spent
                    else:
                        gross = -usdc_spent
                        net = -usdc_spent
                    roi_val = round(net / max(usdc_spent, 0.01) * 100, 2)
                    settle_bet(b["id"], "WIN" if won else "LOSS",
                               0, round(gross, 4), round(net, 4), roi_val)
                    settled_count += 1
        except Exception:
            pass
    return settled_count


# === KV Store (config, status) ===

def save_kv(key, value):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO kv_store (key, value, updated_at) VALUES (?,?,?)",
        (key, json.dumps(value) if not isinstance(value, str) else value, now))
    conn.commit()
    conn.close()


def get_kv(key):
    conn = get_conn()
    row = conn.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row[0])
        except Exception:
            return row[0]
    return None


def save_mm_event(event):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    conn.execute("""INSERT INTO mm_events
        (session_id, event_type, side, price, size, level,
         pause_reason, pause_duration_sec, created_at)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (event.get("session_id"), event.get("event_type"),
         event.get("side"), event.get("price"), event.get("size"),
         event.get("level"), event.get("pause_reason"),
         event.get("pause_duration_sec"), now))
    conn.commit()
    conn.close()


def get_mm_stats(session_id=None):
    conn = get_conn()
    where = ""
    params: list = []
    if session_id:
        where = " WHERE session_id=?"
        params = [session_id]

    fills = conn.execute(
        f"SELECT COUNT(*) FROM mm_events WHERE event_type='fill'{' AND session_id=?' if session_id else ''}",
        [session_id] if session_id else []
    ).fetchone()[0]

    pauses = conn.execute(f"""
        SELECT pause_reason, COUNT(*) as c,
            AVG(pause_duration_sec) as avg_dur
        FROM mm_events WHERE event_type='pause'{' AND session_id=?' if session_id else ''}
        GROUP BY pause_reason
    """, [session_id] if session_id else []).fetchall()

    # Per-session summary (last 50 sessions)
    # side can be 'UP'/'DOWN' (new) or 'BUY'/'SELL' (old data)
    per_session = conn.execute("""
        SELECT session_id,
            SUM(CASE WHEN event_type='fill' THEN 1 ELSE 0 END) as fills,
            SUM(CASE WHEN event_type='fill' AND side IN ('UP','BUY') THEN 1 ELSE 0 END) as up_fills,
            SUM(CASE WHEN event_type='fill' AND side IN ('DOWN','SELL') THEN 1 ELSE 0 END) as down_fills,
            SUM(CASE WHEN event_type='fill' THEN size ELSE 0 END) as total_size,
            SUM(CASE WHEN event_type='fill' THEN size * price ELSE 0 END) as total_cost,
            SUM(CASE WHEN event_type='pause' THEN 1 ELSE 0 END) as pauses,
            MIN(created_at) as first_event,
            MAX(created_at) as last_event
        FROM mm_events
        GROUP BY session_id
        ORDER BY MAX(created_at) DESC
        LIMIT 50
    """).fetchall()

    # Recent fills (last 20)
    recent_fills = conn.execute(f"""
        SELECT session_id, side, price, size, created_at
        FROM mm_events WHERE event_type='fill'
        ORDER BY created_at DESC LIMIT 20
    """).fetchall()

    conn.close()
    return {
        "total_fills": fills,
        "pauses": {r[0]: {"count": r[1], "avg_duration": round(r[2] or 0, 1)}
                   for r in pauses},
        "by_session": [{
            "session_id": r[0], "fills": r[1],
            "up_fills": r[2], "down_fills": r[3],
            "total_size": round(r[4] or 0, 1),
            "total_cost": round(r[5] or 0, 2),
            "pauses": r[6],
            "first_event": r[7], "last_event": r[8],
        } for r in per_session],
        "recent_fills": [{
            "session_id": r[0], "side": r[1],
            "price": r[2], "size": r[3], "created_at": r[4],
        } for r in recent_fills],
    }


def save_mm_window(stats_dict):
    """Save completed window stats from MarketMaker.on_window_change."""
    conn = get_conn()
    conn.execute("""INSERT INTO mm_window_stats
        (window_id, buys_count, sells_count, buys_shares, sells_shares,
         buys_usdc, sells_usdc, volume_usdc, net_position_shares, rebates_est)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (stats_dict.get("window_id", ""),
         stats_dict.get("buys", 0), stats_dict.get("sells", 0),
         stats_dict.get("buys_shares", 0), stats_dict.get("sells_shares", 0),
         stats_dict.get("buys_usdc", 0), stats_dict.get("sells_usdc", 0),
         stats_dict.get("volume_usdc", 0),
         stats_dict.get("net_position_shares", 0),
         stats_dict.get("rebates_est", 0)))
    conn.commit()
    conn.close()


def get_mm_window_stats(limit=50):
    """Get recent mm window stats for API."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT window_id, buys_count, sells_count,
               buys_shares, sells_shares, buys_usdc, sells_usdc,
               volume_usdc, net_position_shares, rebates_est, created_at
        FROM mm_window_stats ORDER BY id DESC LIMIT ?
    """, (limit,)).fetchall()

    totals = conn.execute("""
        SELECT COUNT(*) as windows,
               SUM(buys_count) as buys, SUM(sells_count) as sells,
               SUM(buys_shares) as bs, SUM(sells_shares) as ss,
               SUM(volume_usdc) as vol, SUM(rebates_est) as reb
        FROM mm_window_stats
    """).fetchone()
    conn.close()

    return {
        "totals": {
            "windows_count": totals[0] or 0,
            "buys_total": totals[1] or 0,
            "sells_total": totals[2] or 0,
            "buys_shares": round(totals[3] or 0, 2),
            "sells_shares": round(totals[4] or 0, 2),
            "volume_usdc_total": round(totals[5] or 0, 4),
            "rebates_est_total": round(totals[6] or 0, 6),
        },
        "windows": [{
            "window_id": r[0],
            "buys": r[1], "sells": r[2],
            "buys_shares": round(r[3] or 0, 2),
            "sells_shares": round(r[4] or 0, 2),
            "buys_usdc": round(r[5] or 0, 4),
            "sells_usdc": round(r[6] or 0, 4),
            "volume_usdc": round(r[7] or 0, 4),
            "net_position_shares": round(r[8] or 0, 2),
            "rebates_est": round(r[9] or 0, 6),
            "created_at": r[10],
        } for r in rows],
    }


def save_mm_window_result(result: dict) -> None:
    """Save MM window result (P&L, split, fills, merge)."""
    conn = get_conn()
    fields = [
        "window_id", "window_question", "started_at", "completed_at",
        "split_amount_usdc", "split_done",
        "yes_buys_count", "yes_sells_count",
        "no_buys_count", "no_sells_count",
        "yes_buys_usdc", "yes_sells_usdc",
        "no_buys_usdc", "no_sells_usdc",
        "total_volume_usdc", "spread_pnl_usdc",
        "merge_amount_usdc", "yes_remaining", "no_remaining",
        "rebates_est", "resolved_outcome", "redeem_amount_usdc",
        "net_pnl_usdc",
    ]
    cols = ", ".join(fields)
    placeholders = ", ".join(f":{f}" for f in fields)
    safe = {f: result.get(f, 0 if "usdc" in f or "count" in f or "remaining" in f else None) for f in fields}
    conn.execute(
        f"INSERT OR REPLACE INTO mm_window_results ({cols}) VALUES ({placeholders})",
        safe,
    )
    conn.commit()
    conn.close()


def get_mm_window_results(limit: int = 50) -> list:
    """Last N MM windows."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM mm_window_results ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_mm_summary() -> dict:
    """Aggregate stats across all MM windows."""
    conn = get_conn()
    row = conn.execute("""
        SELECT
            COUNT(*) as total_windows,
            COALESCE(SUM(split_amount_usdc), 0) as total_split,
            COALESCE(SUM(total_volume_usdc), 0) as total_volume,
            COALESCE(SUM(spread_pnl_usdc), 0) as total_spread_pnl,
            COALESCE(SUM(merge_amount_usdc), 0) as total_merge,
            COALESCE(SUM(rebates_est), 0) as total_rebates_est,
            COALESCE(SUM(net_pnl_usdc), 0) as total_net_pnl,
            COALESCE(AVG(spread_pnl_usdc), 0) as avg_spread_per_window,
            COALESCE(SUM(
                yes_buys_count + yes_sells_count +
                no_buys_count + no_sells_count
            ), 0) as total_fills
        FROM mm_window_results
    """).fetchone()
    conn.close()
    return dict(row) if row else {}


def save_maker_buy_trade(trade: dict) -> None:
    """Save a MAKER BUY fill."""
    conn = get_conn()
    safe = {
        "window_id": trade.get("window_id", ""),
        "condition_id": trade.get("condition_id", ""),
        "order_id": trade.get("order_id", ""),
        "token_label": trade.get("token_label", ""),
        "price": trade.get("price", 0),
        "size": trade.get("size", 0),
        "usdc_spent": trade.get("usdc_spent", 0),
        "seconds_to_expiry": trade.get("seconds_to_expiry"),
        "filled_at": trade.get("filled_at", ""),
    }
    conn.execute(
        """INSERT OR IGNORE INTO maker_buy_trades
           (window_id, condition_id, order_id, token_label,
            price, size, usdc_spent, seconds_to_expiry, filled_at)
           VALUES (:window_id, :condition_id, :order_id, :token_label,
                   :price, :size, :usdc_spent, :seconds_to_expiry,
                   :filled_at)""",
        safe,
    )
    conn.commit()
    conn.close()


def get_maker_buy_windows_to_redeem() -> list:
    """Distinct (window_id, condition_id) pairs where outcome is set
    but redeem not done yet. Grouped so we redeem once per window.
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT DISTINCT window_id, condition_id
        FROM maker_buy_trades
        WHERE outcome IS NOT NULL
          AND redeemed = 0
          AND condition_id IS NOT NULL
          AND condition_id != ''
    """).fetchall()
    conn.close()
    return [(r["window_id"], r["condition_id"]) for r in rows]


def mark_maker_buy_redeemed(window_id: str) -> None:
    """Mark all trades in this window as redeemed."""
    conn = get_conn()
    conn.execute(
        "UPDATE maker_buy_trades SET redeemed = 1 WHERE window_id = ?",
        (window_id,),
    )
    conn.commit()
    conn.close()


def update_maker_buy_outcome(window_id: str, outcome: str) -> None:
    """Set outcome and compute pnl considering token_label (YES or NO).
    YES wins when outcome=UP, NO wins when outcome=DOWN.
    """
    conn = get_conn()
    conn.execute(
        """UPDATE maker_buy_trades
           SET outcome = ?,
               won = CASE
                   WHEN token_label = 'YES' AND ? = 'UP' THEN 1
                   WHEN token_label = 'NO'  AND ? = 'DOWN' THEN 1
                   ELSE 0
               END,
               pnl = CASE
                   WHEN (token_label = 'YES' AND ? = 'UP')
                     OR (token_label = 'NO'  AND ? = 'DOWN')
                   THEN (1.0 - price) * size
                   ELSE -(price * size)
               END
           WHERE window_id = ? AND outcome IS NULL""",
        (outcome, outcome, outcome, outcome, outcome, window_id),
    )
    conn.commit()
    conn.close()


def get_maker_buy_windows_pending_outcome() -> list:
    """Distinct window_ids where maker_buy trades exist but outcome is NULL."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT DISTINCT window_id
        FROM maker_buy_trades
        WHERE outcome IS NULL
          AND window_id IS NOT NULL
          AND window_id != ''
    """).fetchall()
    conn.close()
    return [r["window_id"] for r in rows]


async def settle_pending_maker_buy(aio_session) -> int:
    """Independent settlement for maker_buy trades.
    Queries Gamma API for each pending window and updates outcome.
    Does NOT depend on `bets` table. Returns count of windows settled.
    """
    import aiohttp
    windows = get_maker_buy_windows_pending_outcome()
    if not windows:
        return 0
    settled = 0
    for slug in windows:
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            async with aio_session.get(
                url, timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status != 200:
                    continue
                events = await r.json()
                if not events or not events[0].get("closed"):
                    continue
                m = events[0].get("markets", [{}])[0]
                op = json.loads(m.get("outcomePrices", "[0,0]"))
                resolved_outcome = "UP" if float(op[0]) > 0.5 else "DOWN"
                update_maker_buy_outcome(slug, resolved_outcome)
                settled += 1
        except Exception:
            pass
    return settled


def get_maker_buy_stats() -> dict:
    """Aggregate maker_buy stats across resolved trades."""
    conn = get_conn()
    row = conn.execute("""
        SELECT
            COUNT(*) as total_trades,
            COALESCE(SUM(won), 0) as wins,
            CASE WHEN COUNT(*) > 0
                 THEN ROUND(AVG(won) * 100, 1)
                 ELSE 0 END as winrate_pct,
            ROUND(COALESCE(SUM(usdc_spent), 0), 2) as total_invested,
            ROUND(COALESCE(SUM(pnl), 0), 4) as total_pnl,
            ROUND(COALESCE(AVG(price), 0), 4) as avg_price
        FROM maker_buy_trades
        WHERE outcome IS NOT NULL
    """).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_maker_buy_trades(limit: int = 50) -> list:
    """Recent maker_buy trades."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM maker_buy_trades ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_maker_buy_dates_needing_rebates(tz_offset_hours: float = 3.0) -> list:
    """Return YYYY-MM-DD (UTC dates, which is how CLOB /rebates/current groups)
    where at least one trade has maker_rebate_usdc == 0.
    Limited to non-today dates because today's rebates are not yet posted.
    """
    from datetime import datetime, timezone
    conn = get_conn()
    rows = conn.execute("""
        SELECT DISTINCT substr(filled_at, 1, 10) AS d
        FROM maker_buy_trades
        WHERE COALESCE(maker_rebate_usdc, 0) = 0
          AND filled_at IS NOT NULL AND filled_at != ''
    """).fetchall()
    conn.close()
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return [r["d"] for r in rows if r["d"] and r["d"] < today_utc]


def apply_rebate_entries(date_str: str, entries: list) -> int:
    """Distribute rebates from /rebates/current across trades.
    Each entry: {condition_id, rebated_fees_usdc, ...}
    For each condition, split rebate across that day's trades proportionally
    by usdc_spent.
    Returns number of rows updated.
    """
    if not entries:
        return 0
    conn = get_conn()
    updated = 0
    for e in entries:
        if not isinstance(e, dict):
            continue
        cid = e.get("condition_id", "")
        try:
            rebate = float(e.get("rebated_fees_usdc", 0) or 0)
        except Exception:
            rebate = 0.0
        if not cid or rebate <= 0:
            continue
        rows = conn.execute("""
            SELECT id, usdc_spent
            FROM maker_buy_trades
            WHERE condition_id = ?
              AND substr(filled_at, 1, 10) = ?
              AND COALESCE(maker_rebate_usdc, 0) = 0
        """, (cid, date_str)).fetchall()
        if not rows:
            continue
        total = sum(float(r["usdc_spent"] or 0) for r in rows)
        if total <= 0:
            continue
        for r in rows:
            share = float(r["usdc_spent"] or 0) / total
            conn.execute(
                "UPDATE maker_buy_trades SET maker_rebate_usdc = ? WHERE id = ?",
                (round(rebate * share, 6), r["id"]),
            )
            updated += 1
    conn.commit()
    conn.close()
    return updated


def get_maker_buy_claimable_summary() -> dict:
    """Positions where outcome is set, won, but not yet redeemed.
    Payout = size (each winning share = $1)."""
    conn = get_conn()
    row = conn.execute("""
        SELECT
            COUNT(DISTINCT window_id) as windows,
            COUNT(*) as trades,
            COALESCE(SUM(size), 0) as payout_usdc,
            COALESCE(SUM(usdc_spent), 0) as cost_usdc
        FROM maker_buy_trades
        WHERE won = 1 AND COALESCE(redeemed, 0) = 0
    """).fetchone()
    conn.close()
    if not row:
        return {"windows": 0, "trades": 0, "payout_usdc": 0, "cost_usdc": 0}
    return dict(row)


def get_maker_buy_report(tz_offset_hours: float = 3.0) -> dict:
    """Hierarchical report: months → days → windows.
    Days are bucketed in the caller's local timezone (default UTC+3 Kiev).
    Returns a dict with `months` list sorted newest-first.
    """
    from datetime import datetime, timezone, timedelta
    tz = timezone(timedelta(hours=tz_offset_hours))

    conn = get_conn()
    rows = conn.execute("""
        SELECT
            window_id, condition_id, token_label,
            price, size, usdc_spent,
            COALESCE(maker_rebate_usdc, 0) as rebate,
            COALESCE(fee_usdc, 0) as fee,
            outcome, won, pnl,
            seconds_to_expiry,
            COALESCE(redeemed, 0) as redeemed,
            filled_at
        FROM maker_buy_trades
        ORDER BY filled_at ASC
    """).fetchall()
    conn.close()

    # Group by (month, day, window)
    months_map = {}  # YYYY-MM -> {days: {YYYY-MM-DD: {windows: {slug: {...}}}}}

    def _new_agg():
        return {
            "trades": 0,
            "size": 0.0,
            "invested": 0.0,
            "rebate": 0.0,
            "fee": 0.0,
            "pnl_gross": 0.0,
            "wins": 0,
            "losses": 0,
            "pending": 0,
            "yes_trades": 0,
            "no_trades": 0,
            "yes_invested": 0.0,
            "no_invested": 0.0,
            "price_x_size": 0.0,   # for weighted avg
            "sec_x_size": 0.0,
            "sec_size_total": 0.0,  # denom for avg sec (only rows with sec set)
            "first_filled": None,
            "last_filled": None,
        }

    def _fold(a, r):
        size = float(r["size"] or 0)
        inv = float(r["usdc_spent"] or 0)
        pr = float(r["price"] or 0)
        reb = float(r["rebate"] or 0)
        fee = float(r["fee"] or 0)
        pnl = float(r["pnl"] or 0) if r["pnl"] is not None else 0.0
        sec = r["seconds_to_expiry"]
        lbl = (r["token_label"] or "").upper()
        a["trades"] += 1
        a["size"] += size
        a["invested"] += inv
        a["rebate"] += reb
        a["fee"] += fee
        a["pnl_gross"] += pnl
        a["price_x_size"] += pr * size
        if sec is not None:
            a["sec_x_size"] += float(sec) * size
            a["sec_size_total"] += size
        if r["outcome"] is None:
            a["pending"] += 1
        elif r["won"]:
            a["wins"] += 1
        else:
            a["losses"] += 1
        if lbl == "YES":
            a["yes_trades"] += 1
            a["yes_invested"] += inv
        elif lbl == "NO":
            a["no_trades"] += 1
            a["no_invested"] += inv
        fa = r["filled_at"] or ""
        if fa:
            if a["first_filled"] is None or fa < a["first_filled"]:
                a["first_filled"] = fa
            if a["last_filled"] is None or fa > a["last_filled"]:
                a["last_filled"] = fa

    def _finalize(a, window_count=None):
        size = a["size"]
        trades = a["trades"]
        settled = a["wins"] + a["losses"]
        avg_price = (a["price_x_size"] / size) if size > 0 else 0.0
        avg_odds = (1.0 / avg_price) if avg_price > 0 else 0.0
        avg_sec = (
            a["sec_x_size"] / a["sec_size_total"]
            if a["sec_size_total"] > 0 else None
        )
        pnl_net = a["pnl_gross"] + a["rebate"] - a["fee"]
        roi = (pnl_net / a["invested"] * 100.0) if a["invested"] > 0 else 0.0
        out = {
            "trades": trades,
            "size": round(size, 4),
            "invested_usdc": round(a["invested"], 4),
            "rebate_usdc": round(a["rebate"], 6),
            "fee_usdc": round(a["fee"], 6),
            "pnl_gross_usdc": round(a["pnl_gross"], 4),
            "pnl_net_usdc": round(pnl_net, 4),
            "roi_pct": round(roi, 2),
            "wins": a["wins"],
            "losses": a["losses"],
            "pending": a["pending"],
            "winrate_pct": round(a["wins"] / settled * 100.0, 1) if settled else None,
            "avg_price": round(avg_price, 4),
            "avg_decimal_odds": round(avg_odds, 3),
            "avg_seconds_to_expiry": round(avg_sec, 1) if avg_sec is not None else None,
            "yes_trades": a["yes_trades"],
            "no_trades": a["no_trades"],
            "yes_invested_usdc": round(a["yes_invested"], 4),
            "no_invested_usdc": round(a["no_invested"], 4),
            "first_filled": a["first_filled"],
            "last_filled": a["last_filled"],
        }
        if window_count is not None:
            # Use "window_count" (not "windows") to avoid colliding with the
            # array key `windows` used for nested day->windows rendering.
            out["window_count"] = window_count
            if window_count > 0:
                out["avg_invested_per_window"] = round(a["invested"] / window_count, 4)
                out["avg_size_per_window"] = round(size / window_count, 4)
                out["avg_trades_per_window"] = round(trades / window_count, 2)
        return out

    for r in rows:
        fa_iso = r["filled_at"] or ""
        if not fa_iso:
            continue
        # filled_at is stored as UTC ISO (naive). Treat as UTC.
        try:
            dt_utc = datetime.fromisoformat(fa_iso.replace("Z", ""))
            if dt_utc.tzinfo is None:
                dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        dt_local = dt_utc.astimezone(tz)
        m_key = dt_local.strftime("%Y-%m")
        d_key = dt_local.strftime("%Y-%m-%d")
        slug = r["window_id"] or ""

        mth = months_map.setdefault(m_key, {"agg": _new_agg(), "days": {}})
        day = mth["days"].setdefault(d_key, {"agg": _new_agg(), "windows": {}})
        win = day["windows"].setdefault(slug, {
            "agg": _new_agg(),
            "condition_id": r["condition_id"],
            "outcome": r["outcome"],
            "redeemed": bool(r["redeemed"]),
        })
        _fold(mth["agg"], r)
        _fold(day["agg"], r)
        _fold(win["agg"], r)
        # keep latest outcome/redeem on window
        win["outcome"] = r["outcome"]
        win["redeemed"] = bool(r["redeemed"])

    # Build output
    months_out = []
    for m_key in sorted(months_map.keys(), reverse=True):
        mth = months_map[m_key]
        days_out = []
        for d_key in sorted(mth["days"].keys(), reverse=True):
            day = mth["days"][d_key]
            windows_out = []
            for slug, win in sorted(
                day["windows"].items(),
                key=lambda kv: kv[1]["agg"]["last_filled"] or "",
                reverse=True,
            ):
                windows_out.append({
                    "window_id": slug,
                    "condition_id": win["condition_id"],
                    "outcome": win["outcome"],
                    "redeemed": win["redeemed"],
                    **_finalize(win["agg"]),
                })
            days_out.append({
                "date": d_key,
                **_finalize(day["agg"], window_count=len(day["windows"])),
                "windows": windows_out,
            })
        months_out.append({
            "month": m_key,
            **_finalize(
                mth["agg"],
                window_count=sum(len(d["windows"]) for d in mth["days"].values()),
            ),
            "days": days_out,
        })
    return {
        "tz_offset_hours": tz_offset_hours,
        "months": months_out,
        "claimable": get_maker_buy_claimable_summary(),
    }


if __name__ == "__main__":
    init_db()
