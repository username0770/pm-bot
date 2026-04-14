"""
Local API server — reads btc_lab.db, serves stats to agent-hq site.
Run: python btc_lab_api.py
"""
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import btc_lab_db as db

db.init_db()

app = FastAPI(title="BTC Lab API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


from pathlib import Path

TARGET_FILE = Path(__file__).parent / "target.txt"


@app.get("/health")
def health():
    from dotenv import load_dotenv
    load_dotenv()
    target = None
    try:
        if TARGET_FILE.exists():
            t = TARGET_FILE.read_text().strip()
            if t:
                target = float(t)
    except Exception:
        pass
    return {
        "status": "ok",
        "db": str(db.DB_PATH),
        "manualTarget": target,
        "makerMode": os.getenv("MAKER_MODE", "false").lower() == "true",
        "makerOrderTTL": int(os.getenv("MAKER_ORDER_TTL", "30")),
        "realBetting": os.getenv("REAL_BETTING_ENABLED", "false").lower() == "true",
        "betAmount": os.getenv("BET_AMOUNT_USDC", "10"),
    }


@app.post("/target")
def set_target(data: dict):
    """Set manual target price. Send {"price": 67123.45} or {"price": null} to clear."""
    price = data.get("price")
    if price is not None and price > 1000:
        TARGET_FILE.write_text(str(price))
        return {"target": price, "mode": "manual"}
    else:
        if TARGET_FILE.exists():
            TARGET_FILE.write_text("")
        return {"target": None, "mode": "auto"}


@app.get("/target")
def get_target():
    try:
        if TARGET_FILE.exists():
            t = TARGET_FILE.read_text().strip()
            if t:
                return {"target": float(t), "mode": "manual"}
    except Exception:
        pass
    return {"target": None, "mode": "auto"}


# --- Strategies ---

def _camel(d):
    """Convert snake_case dict keys to camelCase for frontend."""
    if not isinstance(d, dict):
        return d
    rmap = {
        "is_active": "isActive", "min_edge": "minEdge",
        "bet_amount_usdc": "betAmountUSDC", "autobet_phase": "autobetPhase",
        "timer_min": "timerMin", "timer_max": "timerMax",
        "price_min": "priceMin", "price_max": "priceMax",
        "fair_min": "fairMin", "max_bets_per_window": "maxBetsPerWindow",
        "total_bets": "totalBets", "total_real_bets": "totalRealBets",
        "total_paper_bets": "totalPaperBets", "total_pnl": "totalPnl",
        "total_fees": "totalFees", "avg_edge": "avgEdge",
        "created_at": "createdAt", "updated_at": "updatedAt",
        "event_start_time": "eventStartTime", "end_date": "endDate",
        "target_price": "targetPrice", "target_price_source": "targetSource",
        "resolved_outcome": "resolvedOutcome", "completed_at": "completedAt",
        "up_token_id": "upTokenId", "down_token_id": "downTokenId",
    }
    return {rmap.get(k, k): v for k, v in d.items()}


@app.get("/strategies")
def strategies():
    return [_camel(s) for s in db.list_strategies()]


@app.post("/strategies")
def create_strategy(data: dict):
    mapped = {FIELD_MAP.get(k, k): v for k, v in data.items()}
    return _camel(db.create_strategy(**mapped))


# camelCase → snake_case mapping for frontend compatibility
FIELD_MAP = {
    "isActive": "is_active", "minEdge": "min_edge",
    "betAmountUSDC": "bet_amount_usdc", "autobetPhase": "autobet_phase",
    "timerMin": "timer_min", "timerMax": "timer_max",
    "priceMin": "price_min", "priceMax": "price_max",
    "fairMin": "fair_min", "maxBetsPerWindow": "max_bets_per_window",
}

@app.patch("/strategies/{sid}")
def update_strategy(sid: str, data: dict):
    mapped = {FIELD_MAP.get(k, k): v for k, v in data.items()}
    return _camel(db.update_strategy(sid, **mapped))


@app.delete("/strategies/{sid}")
def delete_strategy(sid: str):
    db.delete_strategy(sid)
    return {"ok": True}


def _camel_bet(b):
    """Convert bet snake_case to camelCase for frontend."""
    if not b:
        return b
    m = {
        "session_id": "sessionId", "strategy_id": "strategyId",
        "strategy_name": "strategyName", "bet_type": "betType",
        "market_question": "marketQuestion", "market_slug": "marketSlug",
        "up_token_id": "upTokenId", "down_token_id": "downTokenId",
        "event_start_time": "eventStartTime", "event_end_time": "eventEndTime",
        "amount_usdc": "amount", "intended_price": "price",
        "executed_price": "executedPrice", "shares_received": "sharesReceived",
        "fee_rate": "feeRate", "fee_calculated": "feeCalculated",
        "fee_actual": "feeActual", "target_price": "targetPrice",
        "target_price_source": "targetSource",
        "cex_median_at_bet": "cexMedianAtBet",
        "binance_at_bet": "binanceAtBet",
        "pm_up_price_at_bet": "pmUpPriceAtBet",
        "pm_down_price_at_bet": "pmDownPriceAtBet",
        "pm_bid_at_bet": "pmBidAtBet", "pm_ask_at_bet": "pmAskAtBet",
        "fair_probability": "fairProbability",
        "seconds_left_at_bet": "secondsLeftAtBet",
        "timer_phase": "timerPhase", "placed_at": "placedAt",
        "tx_hash": "txHash", "order_id": "orderId",
        "order_status": "orderStatus", "resolved_price": "resolvedPrice",
        "resolved_at": "resolvedAt", "gross_pnl": "grossPnl",
        "net_pnl": "netPnl", "auto_placed": "autoPlaced",
        "signal_type": "signalType", "created_at": "createdAt",
        "move_from_target": "moveFromTarget", "move_percent": "movePercent",
    }
    result = {}
    for k, v in b.items():
        result[m.get(k, k)] = v
    # Ensure key display fields exist
    result.setdefault("fee", result.get("feeCalculated") or 0)
    result.setdefault("pnl", result.get("netPnl"))
    return result


# --- Bets ---

@app.get("/bets")
def bets(
    type: str = None, strategy: str = None,
    outcome: str = None, phase: str = None,
    side: str = None, limit: int = 50, offset: int = 0,
):
    raw = db.get_bets(
        bet_type=type, strategy_id=strategy,
        outcome=outcome, timer_phase=phase,
        side=side, limit=limit, offset=offset,
    )
    return [_camel_bet(b) for b in raw]


@app.post("/bets")
def create_bet(data: dict):
    bid = db.save_bet(data)
    return {"id": bid, "ok": True}


@app.delete("/bets/{bid}")
def delete_bet(bid: str):
    db.delete_bet(bid)
    return {"ok": True}


@app.patch("/bets/{bid}")
def update_bet(bid: str, data: dict):
    if data.get("outcome") and data["outcome"] != "PENDING":
        db.settle_bet(
            bid, data["outcome"],
            data.get("resolved_price", 0),
            data.get("gross_pnl", 0),
            data.get("net_pnl", 0),
            data.get("roi", 0),
        )
    return {"ok": True}


@app.get("/sessions/{sid}/bets")
def session_bets(sid: str):
    raw = db.get_bets(limit=200)
    filtered = [b for b in raw if b.get("session_id") == sid]
    return [_camel_bet(b) for b in filtered]


@app.get("/analytics")
def analytics():
    conn = db.get_conn()
    # Edge buckets
    edge_rows = conn.execute("""
        SELECT
            CASE
                WHEN ABS(edge) < 0.05 THEN '0-5%'
                WHEN ABS(edge) < 0.10 THEN '5-10%'
                WHEN ABS(edge) < 0.15 THEN '10-15%'
                WHEN ABS(edge) < 0.20 THEN '15-20%'
                ELSE '20%+'
            END as bucket,
            COUNT(*) as total,
            SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
            COALESCE(AVG(net_pnl), 0) as avg_pnl
        FROM bets WHERE outcome IN ('WIN', 'LOSS')
        GROUP BY bucket ORDER BY bucket
    """).fetchall()
    by_edge = [{"bucket": r[0], "total": r[1], "wins": r[2],
                "winrate": round(r[2]/max(r[1],1)*100, 1),
                "avgPnl": round(r[3], 2)} for r in edge_rows]

    # By hour
    hour_rows = conn.execute("""
        SELECT CAST(SUBSTR(placed_at, 12, 2) AS INTEGER) as hour,
            COUNT(*) as total,
            SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
            COALESCE(SUM(net_pnl), 0) as pnl
        FROM bets WHERE outcome IN ('WIN', 'LOSS')
        GROUP BY hour ORDER BY hour
    """).fetchall()
    by_hour = [{"hour": r[0], "total": r[1], "wins": r[2],
                "winrate": round(r[2]/max(r[1],1)*100, 1),
                "pnl": round(r[3], 2)} for r in hour_rows]

    # By side
    side_rows = conn.execute("""
        SELECT side, COUNT(*) as total,
            SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
            COALESCE(SUM(net_pnl), 0) as pnl
        FROM bets WHERE outcome IN ('WIN', 'LOSS')
        GROUP BY side
    """).fetchall()
    by_side = {r[0]: {"total": r[1], "wins": r[2],
                      "winrate": round(r[2]/max(r[1],1)*100, 1),
                      "pnl": round(r[3], 2)} for r in side_rows}

    conn.close()
    return {"byEdge": by_edge, "byHour": by_hour, "bySide": by_side}


# --- Stats ---

@app.get("/stats")
def stats():
    return {
        "paper": db.get_stats(bet_type="paper"),
        "real": db.get_stats(bet_type="real"),
        "all": db.get_stats(),
    }


@app.get("/stats/{strategy_id}")
def strategy_stats(strategy_id: str):
    return db.get_stats(strategy_id=strategy_id)


# --- Sessions ---

@app.get("/sessions")
def sessions(limit: int = 50):
    return [_camel(s) for s in db.list_sessions(limit=limit)]


@app.get("/sessions/{sid}/ticks")
def session_ticks(sid: str, limit: int = 500):
    ticks = db.get_session_ticks(sid, limit=limit)
    # Map to camelCase for frontend
    return [{
        "ts": t.get("ts"), "binance": t.get("binance"),
        "coinbase": t.get("coinbase"), "okx": t.get("okx"),
        "bybit": t.get("bybit"), "cexMedian": t.get("cex_median"),
        "chainlink": t.get("chainlink"),
        "pmUpPrice": t.get("pm_up_price"),
        "pmBid": t.get("pm_bid"), "pmAsk": t.get("pm_ask"),
        "secondsLeft": t.get("seconds_left"),
    } for t in ticks]


# --- Polymarket real positions ---

_clob = None

def _get_clob():
    global _clob
    if _clob:
        return _clob
    try:
        from dotenv import load_dotenv
        load_dotenv()
        import os
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.constants import POLYGON
        key = os.getenv("MM2_PRIVATE_KEY")
        funder = os.getenv("MM2_SAFE")
        if not key or not funder:
            return None
        creds = ApiCreds(
            api_key=os.getenv("MM2_CLOB_API_KEY", ""),
            api_secret=os.getenv("MM2_CLOB_SECRET", ""),
            api_passphrase=os.getenv("MM2_CLOB_PASSPHRASE", ""),
        )
        c = ClobClient(
            "https://clob.polymarket.com", key=key,
            chain_id=POLYGON, creds=creds,
            funder=funder, signature_type=2,
        )
        _clob = c
        return c
    except Exception:
        return None


@app.get("/polymarket/trades")
def pm_trades(limit: int = 20):
    """Real trades from Polymarket CLOB."""
    c = _get_clob()
    if not c:
        return {"error": "CLOB not configured"}
    try:
        trades = c.get_trades()
        if not isinstance(trades, list):
            return []
        result = []
        for t in trades[:limit]:
            result.append({
                "id": t.get("id"),
                "side": t.get("side"),
                "size": float(t.get("size", 0)),
                "price": float(t.get("price", 0)),
                "outcome": t.get("outcome"),
                "status": t.get("status"),
                "matchTime": t.get("match_time"),
                "txHash": t.get("transaction_hash"),
                "orderId": t.get("taker_order_id"),
                "feeRateBps": int(t.get("fee_rate_bps", 0)),
                "market": t.get("market"),
            })
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/polymarket/balance")
def pm_balance():
    """Account summary from recent trades."""
    c = _get_clob()
    if not c:
        return {"error": "CLOB not configured"}
    try:
        trades = c.get_trades()
        if not isinstance(trades, list):
            return {"totalTrades": 0}
        # Summarize recent BTC 5min trades
        total_spent = 0
        total_shares = 0
        by_outcome = {"Up": {"spent": 0, "shares": 0, "count": 0},
                      "Down": {"spent": 0, "shares": 0, "count": 0}}
        for t in trades[:50]:
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            cost = size * price
            outcome = t.get("outcome", "?")
            total_spent += cost
            total_shares += size
            if outcome in by_outcome:
                by_outcome[outcome]["spent"] += cost
                by_outcome[outcome]["shares"] += size
                by_outcome[outcome]["count"] += 1
        return {
            "totalTrades": min(len(trades), 50),
            "totalSpent": round(total_spent, 2),
            "totalShares": round(total_shares, 2),
            "byOutcome": by_outcome,
            "wallet": c.get_address(),
        }
    except Exception as e:
        return {"error": str(e)}


# --- Config / Control ---

@app.get("/control/status")
def control_status():
    mm_cfg = db.get_kv("mm_config") or {}
    mm_st = db.get_kv("mm_status") or {}
    auto = db.get_kv("auto_bets") or {"enabled": True}
    return {
        "mm_config": mm_cfg,
        "mm_status": mm_st,
        "auto_bets": auto,
    }


@app.post("/control/command")
def control_command(data: dict):
    cmd = data.get("command")
    if cmd == "update_mm_config":
        cfg = data.get("config", {})
        db.save_kv("mm_config", cfg)
        return {"ok": True, "config": cfg}
    elif cmd == "set_auto_bets":
        enabled = data.get("enabled", True)
        db.save_kv("auto_bets", {"enabled": enabled})
        return {"ok": True, "auto_bets": enabled}
    elif cmd == "set_mm":
        enabled = data.get("enabled", False)
        existing = db.get_kv("mm_config") or {}
        existing["enabled"] = enabled
        db.save_kv("mm_config", existing)
        return {"ok": True, "mm_enabled": enabled}
    return {"error": "unknown command"}


@app.get("/mm-status")
def mm_status():
    return db.get_kv("mm_status") or {
        "enabled": False, "running": False,
        "pause_reason": "", "active_orders": 0,
        "total_fills": 0, "rebates_est": 0,
    }


@app.get("/mm-stats")
def mm_stats():
    """MM window stats + current window from kv store."""
    window_data = db.get_mm_window_stats(limit=50)
    current = db.get_kv("mm_status") or {}
    window_data["current_window"] = current.get("window", {})
    return window_data


@app.get("/mm-windows")
def mm_windows(limit: int = 50):
    """Last N MM window results with P&L."""
    return db.get_mm_window_results(limit=limit)


@app.get("/mm-summary")
def mm_summary():
    """Aggregate MM stats across all windows."""
    return db.get_mm_summary()


@app.get("/maker-buy-stats")
def maker_buy_stats_api():
    """Aggregate stats for maker BUY strategy."""
    return db.get_maker_buy_stats()


@app.get("/maker-buy-trades")
def maker_buy_trades_api(limit: int = 50):
    """Recent maker BUY trades."""
    return db.get_maker_buy_trades(limit=limit)


@app.get("/maker-buy-report")
def maker_buy_report(tz_offset_hours: float = 3.0):
    """Hierarchical maker_buy report: months → days → windows.
    Days are bucketed in the given timezone offset (default Europe/Kiev +3).
    """
    return db.get_maker_buy_report(tz_offset_hours=tz_offset_hours)


@app.get("/maker-buy-report.html", include_in_schema=False)
def maker_buy_report_page():
    from fastapi.responses import FileResponse
    p = Path(__file__).parent / "maker_buy_report.html"
    return FileResponse(str(p))


@app.get("/rebates")
async def get_rebates(check_date: str = None):
    import httpx
    from datetime import date
    if not check_date:
        check_date = date.today().isoformat()

    safe = "0xe3c40212E4FDbCB3a287d4749f116D0887A6cd5c"

    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://clob.polymarket.com/rebates/current",
                params={"date": check_date, "maker_address": safe},
                timeout=10,
            )
            data = r.json() if r.status_code == 200 else []
    except Exception:
        data = []

    if data is None or not isinstance(data, list):
        data = []
    total = sum(float(x.get("rebated_fees_usdc", 0)) for x in data
                if isinstance(x, dict))

    return {
        "date": check_date,
        "total_usdc": round(total, 6),
        "count": len(data),
        "entries": data,
    }


if __name__ == "__main__":
    import uvicorn
    print("BTC Lab API: http://localhost:8765")
    uvicorn.run(app, host="0.0.0.0", port=8765)
