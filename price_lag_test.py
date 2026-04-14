# Force UTF-8 stdout/stderr on Windows to avoid UnicodeEncodeError
import sys as _sys_init
import io as _io_init
if _sys_init.platform == "win32":
    try:
        _sys_init.stdout = _io_init.TextIOWrapper(
            _sys_init.stdout.buffer, encoding="utf-8", errors="replace")
        _sys_init.stderr = _io_init.TextIOWrapper(
            _sys_init.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

from dotenv import load_dotenv
load_dotenv()


import asyncio
import aiohttp
import websockets
import csv
import json
import math
import time
import os
import sys
from datetime import datetime, timezone
from statistics import median
from rich.console import Console, Group
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from collections import deque
from pathlib import Path

import btc_lab_db as labdb
from strategies.market_maker import MarketMaker, MMConfig, load_mm_config_from_env
from strategies.maker_buy_strategy import (
    MakerBuyStrategy, load_maker_buy_config
)
# Eager-import relayer_client so web3 is loaded into sys.modules at startup.
# Lazy-importing from inside an asyncio worker previously failed with
# "No module named 'web3'" in some runtime conditions — this guarantees
# the module is cached once and the worker can reuse it without re-import.
from strategies.relayer_client import create_safe_relayer_client as _create_safe_relayer_client
from strategies.telegram_notifier import (
    create_telegram_notifier, TelegramNotifier
)

console = Console()

# --- Config ---
DURATION = 300

CHAINLINK_FEED_ID = (
    "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
)
CHAINLINK_URL = (
    "https://data.chain.link/api/query-timescale"
    "?query=LIVE_STREAM_REPORTS_QUERY"
    f"&variables=%7B%22feedId%22%3A%22{CHAINLINK_FEED_ID}%22%7D"
)

BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
BYBIT_WS = "wss://stream.bybit.com/v5/public/spot"

EXCHANGES = ["binance", "coinbase", "okx", "bybit"]
EXCHANGE_COLORS = {
    "binance": "#10b981", "coinbase": "#3b82f6",
    "okx": "#f59e0b", "bybit": "#ec4899",
}

# Polymarket fee: crypto category
# Crypto markets: feeRate=0.072 per docs
# Fee in shares: fee = size * 0.072 * p * (1-p)
# fee_rate_bps=1000 in orders is just a signature field, not the actual rate
PM_FEE_RATE = 0.072

# Agent HQ API
AGENT_HQ_URL = os.getenv("AGENT_HQ_URL", "https://agent-hq-puce.vercel.app")
AGENT_HQ_API = os.getenv("AGENT_HQ_API",
                          f"{AGENT_HQ_URL}/api/btc-lab")
AGENT_HQ_TOKEN = os.getenv("AGENT_HQ_TOKEN", "")

# Real betting
REAL_BETTING_ENABLED = os.getenv("REAL_BETTING_ENABLED", "false").lower() == "true"
BET_AMOUNT_USDC = float(os.getenv("BET_AMOUNT_USDC", "10"))
DEFAULT_STRATEGY_ID = os.getenv("DEFAULT_STRATEGY_ID", "")
clob_client = None
AUTO_BETS_ENABLED = True

# Market Maker config
mm_config = load_mm_config_from_env()
mm = None  # initialized after clob_client
maker_buy_cfg = load_maker_buy_config()
maker_buy = MakerBuyStrategy(maker_buy_cfg)

# Telegram notifier (None if disabled / no credentials)
tg = create_telegram_notifier()

# Market-making constants
SETTLEMENT_DELAY = 128
HEARTBEAT_INTERVAL = 5
GAMMA_RISK = float(os.getenv("GAMMA_RISK", "0.01"))
MAKER_SPREAD = float(os.getenv("MAKER_SPREAD", "0.02"))
DUST_THRESHOLD = float(os.getenv("DUST_THRESHOLD", "0.01"))
STRADDLE_FREQ = 0.009
MAX_LATENCY_ARB_PRICE = 0.85

# Polymarket WebSocket
POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_USER_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

# Live PM data from WebSocket
latest_pm_bid = None
latest_pm_ask = None
_seen_trade_ids: set = set()  # dedup MATCHED events
_script_start_ts = time.time()  # ignore WS replay fills before this
latest_pm_mid = None
latest_pm_last_trade = None
pm_ws_connected = False
user_ws_connected = False
last_heartbeat_ts = 0

# Inventory
inventory = {"yes_tokens": 0.0, "no_tokens": 0.0, "net_exposure": 0.0}

# CLOB API creds (filled on init)
CLOB_API_KEY = ""
CLOB_SECRET = ""
CLOB_PASSPHRASE = ""

# --- CLI ---
MANUAL_TARGET = None
TARGET_FILE = Path(__file__).parent / "target.txt"
for i, arg in enumerate(sys.argv):
    if arg == "--target" and i + 1 < len(sys.argv):
        MANUAL_TARGET = float(sys.argv[i + 1])
    if arg == "--duration" and i + 1 < len(sys.argv):
        DURATION = int(sys.argv[i + 1])


def check_target_file():
    """Read manual target from target.txt.
    - File with number → manual target
    - Empty/missing file → auto
    Works while script is running — just edit target.txt.
    """
    try:
        if TARGET_FILE.exists():
            text = TARGET_FILE.read_text().strip()
            if text:
                val = float(text.replace(",", ".").replace("$", "").replace(" ", ""))
                if val > 1000:
                    return val
    except Exception:
        pass
    return None

# --- State ---
prices = {ex: None for ex in EXCHANGES}
timestamps = {ex: None for ex in EXCHANGES}
lags = {ex: deque(maxlen=200) for ex in EXCHANGES}

chainlink = {"price": None, "ts": None, "raw_ts": None}
lags_cl = deque(maxlen=200)
polymarket = {"price": None, "ts": None}
lags_poly = deque(maxlen=200)

cex_median = None
target_price = MANUAL_TARGET
target_source = "manual" if MANUAL_TARGET else None
auto_target_price = None  # always holds the Chainlink-derived auto target
target_mode = "auto"      # "auto" or "manual" — synced from dashboard

market_info = {}
poly_token_id = None
poly_question = None
current_window_slug = None

# Spread tracking: binance-chainlink and median-chainlink
spread_history = deque(maxlen=600)  # {ts, bn_cl, med_cl, bn, cl, med}

moves = deque(maxlen=50)
data_rows = []
start_time = None
raw_market_dumped = False  # dump raw once

last_order_book = {"bids": [], "asks": [], "spread": 0}  # latest order book
hq_session_id = None    # current session ID sent to HQ
hq_bet_cooldown = 0     # prevent rapid fire bets

OUTPUT_CSV = "price_lag_results.csv"
OUTPUT_HTML = "price_lag_report.html"


def is_done():
    return start_time and time.time() - start_time > DURATION


def current_5min_slug():
    now = int(time.time())
    return f"btc-updown-5m-{(now // 300) * 300}"


def pm_fee(price, shares=1.0):
    """Polymarket taker fee for crypto: fee = C * rate * p * (1-p)"""
    p = max(0.01, min(0.99, price))
    return shares * PM_FEE_RATE * p * (1 - p)


def pm_fee_pct(price):
    """Fee as percentage of bet amount at price p"""
    p = max(0.01, min(0.99, price))
    return PM_FEE_RATE * (1 - p) * 100  # fee per $1 of shares


# --- Chainlink ---


async def fetch_chainlink_price(session):
    """Fetch Chainlink BTC/USD. Divisor 1e18. Returns Chainlink's own ts."""
    try:
        async with session.get(CHAINLINK_URL,
                               timeout=aiohttp.ClientTimeout(total=2)) as r:
            if r.status == 200:
                data = await r.json()
                nodes = (data.get("data", {})
                         .get("liveStreamReports", {})
                         .get("nodes", []))
                if nodes:
                    node = nodes[0]
                    price = int(node["price"]) / 1e18
                    raw_ts = node.get("validFromTimestamp", "")
                    try:
                        dt = datetime.fromisoformat(
                            raw_ts.replace("Z", "+00:00"))
                        cl_ms = int(dt.timestamp() * 1000)
                    except Exception:
                        cl_ms = int(time.time() * 1000)
                    return price, cl_ms, raw_ts
    except Exception:
        pass
    return None, None, None


async def fetch_chainlink_at_timestamp(session, target_ts):
    """Find Chainlink price closest to unix timestamp (within 60s window)."""
    try:
        async with session.get(CHAINLINK_URL,
                               timeout=aiohttp.ClientTimeout(total=3)) as r:
            if r.status == 200:
                data = await r.json()
                nodes = (data.get("data", {})
                         .get("liveStreamReports", {})
                         .get("nodes", []))
                best_price, best_diff = None, 999999
                for n in nodes:
                    try:
                        dt = datetime.fromisoformat(
                            n["validFromTimestamp"].replace("Z", "+00:00"))
                        diff = abs(int(dt.timestamp()) - target_ts)
                    except Exception:
                        continue
                    if diff < best_diff:
                        best_diff = diff
                        best_price = int(n["price"]) / 1e18
                if best_diff <= 10 and best_price:
                    return best_price
    except Exception:
        pass
    return None


async def get_price_to_beat(session, slug):
    """Get priceToBeat from Gamma API eventMetadata.
    Available for the CURRENT active window (once trading starts)
    and all closed windows."""
    try:
        async with session.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug},
            timeout=aiohttp.ClientTimeout(total=3),
        ) as r:
            events = await r.json()
            if events:
                meta = events[0].get("eventMetadata")
                if meta and isinstance(meta, dict):
                    ptb = meta.get("priceToBeat")
                    if ptb is not None:
                        return float(ptb)
    except Exception:
        pass
    return None


# --- Polymarket ---


async def get_active_btc_market(session):
    """Find current BTC 5min market via slug pattern."""
    try:
        ev_url = "https://gamma-api.polymarket.com/events"
        now = int(time.time())
        for offset in range(0, 6):
            ts = ((now // 300) + offset) * 300
            slug = f"btc-updown-5m-{ts}"
            async with session.get(ev_url, params={"slug": slug},
                                   timeout=aiohttp.ClientTimeout(total=5)) as r:
                found = await r.json()
                if found and not found[0].get("closed"):
                    markets = found[0].get("markets", [])
                    if not markets:
                        continue
                    m = next((x for x in markets
                              if x.get("acceptingOrders")), markets[0])
                    token_ids = m.get("clobTokenIds", "[]")
                    if isinstance(token_ids, str):
                        token_ids = json.loads(token_ids)
                    if not token_ids:
                        continue
                    async with session.get(
                        "https://clob.polymarket.com/price",
                        params={"token_id": token_ids[0], "side": "buy"},
                        timeout=aiohttp.ClientTimeout(total=3),
                    ) as pr:
                        if pr.status != 200:
                            continue
                    return {
                        "question": m.get("question",
                                          found[0].get("title", "")),
                        "endDate": m.get("endDate", found[0].get("endDate")),
                        "eventStartTime": m.get("eventStartTime",
                                                found[0].get("startTime")),
                        "up_token": token_ids[0],
                        "down_token": (token_ids[1]
                                       if len(token_ids) > 1 else None),
                        "conditionId": m.get("conditionId", ""),
                        "outcomePrices": m.get("outcomePrices", "[0.5,0.5]"),
                        "lastTradePrice": m.get("lastTradePrice"),
                        "bestBid": m.get("bestBid"),
                        "bestAsk": m.get("bestAsk"),
                        "acceptingOrders": m.get("acceptingOrders"),
                        "slug": slug,
                        "feeSchedule": m.get("feeSchedule", {}),
                        "volume": m.get("volume"),
                        "liquidity": m.get("liquidity"),
                        "spread": m.get("spread"),
                        # Full raw for one-time dump
                        "_raw_market": m,
                        "_raw_event": {k: v for k, v in found[0].items()
                                       if k != "markets"},
                    }
    except Exception as e:
        console.print(f"[red]Market fetch: {e}[/red]")
    return None


async def fetch_polymarket_price(session):
    if not poly_token_id:
        return None, None
    try:
        async with session.get(
            "https://clob.polymarket.com/price",
            params={"token_id": poly_token_id, "side": "buy"},
            timeout=aiohttp.ClientTimeout(total=2),
        ) as r:
            if r.status == 200:
                data = await r.json()
                price = float(data.get("price", 0))
                if price > 0:
                    return price, int(time.time() * 1000)
    except Exception:
        pass
    return None, None


def get_timer(end_date_str):
    if not end_date_str:
        return None
    try:
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return max(0, int((end - datetime.now(timezone.utc)).total_seconds()))
    except Exception:
        return None


def calculate_fair_probability(btc_now, target, seconds_left, vol_1min=15.0):
    if not target or not btc_now or seconds_left <= 0:
        return {"fair_up": 0.5, "fair_down": 0.5, "edge_up": 0,
                "edge_up_after_fee": 0, "std_move": 0, "d": 0}
    T = seconds_left / 60.0
    std = vol_1min * math.sqrt(T)
    if std == 0:
        return {"fair_up": 0.5, "fair_down": 0.5, "edge_up": 0,
                "edge_up_after_fee": 0, "std_move": 0, "d": 0}
    d = (target - btc_now) / std

    def phi(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    fair_up = max(0.01, min(0.99, 1 - phi(d)))  # clamp to 1-99%
    try:
        mkt = float(market_info.get("bestAsk")
                     or market_info.get("lastTradePrice") or 0.5)
    except (ValueError, TypeError):
        mkt = 0.5
    edge = fair_up - mkt
    fee = pm_fee(mkt)  # fee per $1 of shares
    return {
        "fair_up": round(fair_up, 3), "fair_down": round(1 - fair_up, 3),
        "edge_up": round(edge, 3),
        "edge_up_after_fee": round(edge - fee, 3),
        "fee": round(fee, 4),
        "std_move": round(std, 2), "d": round(d, 3),
    }


def compute_cex_median():
    global cex_median
    now_ms = int(time.time() * 1000)
    fresh = [prices[ex] for ex in EXCHANGES
             if prices[ex] and timestamps[ex]
             and (now_ms - timestamps[ex]) < 2000]
    if len(fresh) >= 3:
        cex_median = median(fresh)


def record_spread():
    """Record Binance-Chainlink and Median-Chainlink spreads."""
    bn = prices.get("binance")
    cl = chainlink.get("price")
    med = cex_median
    if bn and cl:
        spread_history.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "bn_cl": round(bn - cl, 2),
            "med_cl": round(med - cl, 2) if med else None,
            "bn": bn, "cl": cl, "med": med,
        })


# --- Agent HQ API ---


def _hq_headers():
    return {"Authorization": f"Bearer {AGENT_HQ_TOKEN}",
            "Content-Type": "application/json"}


async def hq_create_session(aio_session):
    """Create a session in Agent HQ dashboard."""
    global hq_session_id
    if not AGENT_HQ_TOKEN or not market_info.get("slug"):
        return
    sid = market_info["slug"]
    hq_session_id = sid
    try:
        await aio_session.post(
            f"{AGENT_HQ_URL}/api/btc-lab/sessions",
            json={
                "id": sid,
                "question": market_info.get("question", ""),
                "eventStartTime": market_info.get("eventStartTime", ""),
                "endDate": market_info.get("endDate", ""),
                "targetPrice": target_price,
                "targetSource": target_source if target_source == "manual" else "auto",
            },
            headers=_hq_headers(),
            timeout=aiohttp.ClientTimeout(total=3),
        )
    except Exception:
        pass


async def hq_send_tick(aio_session):
    """Send a tick to Agent HQ."""
    if not AGENT_HQ_TOKEN or not hq_session_id:
        return
    try:
        up_price = polymarket.get("price")
        tick = {
            "ts": int(time.time() * 1000),
            "binance": prices.get("binance"),
            "coinbase": prices.get("coinbase"),
            "okx": prices.get("okx"),
            "bybit": prices.get("bybit"),
            "cexMedian": cex_median,
            "chainlink": chainlink.get("price"),
            "pmUpPrice": up_price,
            "pmBid": market_info.get("bestBid"),
            "pmAsk": market_info.get("bestAsk"),
            "secondsLeft": get_timer(market_info.get("endDate")) or 0,
        }
        await aio_session.patch(
            f"{AGENT_HQ_URL}/api/btc-lab/sessions/{hq_session_id}",
            json={"tick": tick},
            headers=_hq_headers(),
            timeout=aiohttp.ClientTimeout(total=2),
        )
    except Exception:
        pass


async def hq_send_orderbook(aio_session, ob):
    """Send orderbook snapshot to Agent HQ."""
    if not AGENT_HQ_TOKEN or not hq_session_id or not ob:
        return
    try:
        await aio_session.patch(
            f"{AGENT_HQ_URL}/api/btc-lab/sessions/{hq_session_id}",
            json={"orderBook": {**ob, "ts": int(time.time() * 1000)}},
            headers=_hq_headers(),
            timeout=aiohttp.ClientTimeout(total=2),
        )
    except Exception:
        pass


async def hq_place_bet(aio_session, side, price, edge, fair_prob,
                       seconds_left, strat=None):
    """Place a paper bet on Agent HQ."""
    if not AGENT_HQ_TOKEN or not hq_session_id:
        return
    strat_id = strat.get("id", "default") if strat else "default"
    strat_name = strat.get("name", "Default") if strat else "Default"
    cooldown = strat.get("cooldown", 30) if strat else 30
    amount = strat.get("betAmount", 100) if strat else 100

    now = time.time()
    if now - bet_cooldowns.get(strat_id, 0) < cooldown:
        return
    bet_cooldowns[strat_id] = now

    try:
        m = seconds_left // 60
        s = seconds_left % 60
        bet = {
            "side": side,
            "amount": amount,
            "price": price,
            "cexMedianAtBet": cex_median or 0,
            "targetPrice": target_price or 0,
            "targetSource": target_source if target_source == "manual" else "auto",
            "strategyId": strat_id,
            "strategyName": strat_name,
            "moveAtBet": ((cex_median or 0) - (target_price or 0)),
            "fairProbability": fair_prob,
            "edge": edge,
            "secondsLeftAtBet": seconds_left,
            "timerAtBet": f"{m:02d}:{s:02d}",
        }
        async with aio_session.post(
            f"{AGENT_HQ_URL}/api/btc-lab/sessions/{hq_session_id}/bets",
            json=bet,
            headers=_hq_headers(),
            timeout=aiohttp.ClientTimeout(total=3),
        ) as r:
            if r.status == 200:
                console.print(
                    f"[bold green]BET [{strat_name}]: {side} @{price:.2f} "
                    f"edge={edge:+.1%} ${amount}[/bold green]")
    except Exception:
        pass


# Strategies — only v2 from SQLite, no hardcoded v1
strategies = []  # populated from btc_lab_db at runtime
bets_this_window = {}
bet_cooldowns = {}
bet_settings = {}


async def hq_fetch_v2_strategies(aio_session):
    """Fetch v2 strategies from local SQLite DB.
    Falls back to FastAPI, then to direct DB read."""
    # Try direct DB read first (fastest, no HTTP)
    try:
        db_strats = labdb.list_active_autobet_strategies()
        if db_strats:
            return db_strats
    except Exception:
        pass
    # Fallback to local FastAPI
    try:
        async with aio_session.get(
            "http://localhost:8765/strategies",
            timeout=aiohttp.ClientTimeout(total=2),
        ) as r:
            if r.status == 200:
                v2_strats = await r.json()
                result = []
                for s in v2_strats:
                    if not s.get("isActive"):
                        continue
                    if not s.get("autobet"):
                        continue
                    # Convert v2 format to v1 format used by auto-bet loop
                    result.append({
                        "id": s["id"],
                        "name": s.get("name", ""),
                        "enabled": True,
                        "mirror": s.get("mirror", False),
                        "minEdge": s.get("minEdge", 7),
                        "timerMin": s.get("timerMin", 0),
                        "timerMax": s.get("timerMax", 300),
                        "betAmount": s.get("betAmountUSDC", 10),
                        "maxBetsPerWindow": s.get("maxBetsPerWindow", 5),
                        "cooldown": s.get("cooldown", 30),
                        "priceMin": s.get("priceMin", 0.01),
                        "priceMax": s.get("priceMax", 0.99),
                        "fairMin": s.get("fairMin", 0),
                        "_v2": True,  # marker to use v2 API for recording
                    })
                return result
    except Exception:
        pass
    return None


async def hq_check_control(aio_session):
    """Check control state. Returns (running, manual_target, target_mode)."""
    global strategies, bet_settings
    running = True
    manual_tgt = None
    tgt_mode = "auto"

    # 1. Try Vercel control API (for start/stop, manual target only)
    if AGENT_HQ_TOKEN:
        try:
            async with aio_session.get(
                f"{AGENT_HQ_URL}/api/btc-lab/control",
                headers=_hq_headers(),
                timeout=aiohttp.ClientTimeout(total=2),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    running = data.get("running", True)
                    manual_tgt = data.get("manualTarget")
                    tgt_mode = data.get("targetMode", "auto")
        except Exception:
            pass

    # 2. Load strategies from local SQLite only (no v1)
    v2_strats = await hq_fetch_v2_strategies(aio_session)
    if v2_strats:
        strategies = v2_strats

    return running, manual_tgt, tgt_mode


async def fetch_order_book(aio_session):
    """Fetch Polymarket CLOB order book for UP token."""
    global last_order_book
    if not poly_token_id:
        return None
    try:
        async with aio_session.get(
            "https://clob.polymarket.com/book",
            params={"token_id": poly_token_id},
            timeout=aiohttp.ClientTimeout(total=3),
        ) as r:
            if r.status == 200:
                data = await r.json()
                bids = [[float(b["price"]), float(b["size"])]
                        for b in (data.get("bids") or [])[:5]]
                asks = [[float(a["price"]), float(a["size"])]
                        for a in (data.get("asks") or [])[:5]]
                spread = (float(asks[0][0]) - float(bids[0][0])
                          if bids and asks else 0)
                last_order_book = {
                    "bids": bids, "asks": asks,
                    "spread": round(spread, 4),
                }
                return last_order_book
    except Exception:
        pass
    return None


# --- Real Betting ---


def init_clob_client():
    """Initialize Polymarket CLOB client for MM2 Safe account."""
    global clob_client
    if not REAL_BETTING_ENABLED:
        return
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.constants import POLYGON

        key = os.getenv("MM2_PRIVATE_KEY")
        safe = os.getenv("MM2_SAFE")
        api_key = os.getenv("MM2_CLOB_API_KEY")
        api_secret = os.getenv("MM2_CLOB_SECRET")
        api_pass = os.getenv("MM2_CLOB_PASSPHRASE")

        if not key or not safe:
            console.print("[yellow]MM2_PRIVATE_KEY/MM2_SAFE not set, "
                          "real betting disabled[/yellow]")
            return

        host = "https://clob.polymarket.com"

        if api_key and api_secret and api_pass:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_pass,
            )
            client = ClobClient(
                host, key=key, chain_id=POLYGON,
                creds=creds, signature_type=2, funder=safe,
            )
        else:
            client = ClobClient(
                host, key=key, chain_id=POLYGON,
                signature_type=2, funder=safe,
            )
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(creds)
            console.print(
                f"[yellow]Generated new CLOB creds. Add to .env:\n"
                f"  MM2_CLOB_API_KEY={creds.api_key}\n"
                f"  MM2_CLOB_SECRET={creds.api_secret}\n"
                f"  MM2_CLOB_PASSPHRASE={creds.api_passphrase}[/yellow]"
            )

        clob_client = client
        global CLOB_API_KEY, CLOB_SECRET, CLOB_PASSPHRASE
        CLOB_API_KEY = creds.api_key
        CLOB_SECRET = creds.api_secret
        CLOB_PASSPHRASE = creds.api_passphrase

        addr = client.get_address()
        console.print(f"[green]+ CLOB client ready (MM2 Safe)[/green]")
        console.print(f"[dim]  EOA:    {addr}[/dim]")
        console.print(f"[dim]  Funder: {safe} (Safe)[/dim]")
    except Exception as e:
        console.print(f"[red]CLOB init failed: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()[-500:]}[/dim]")


# --- Heartbeat ---


async def heartbeat_loop():
    """POST /heartbeats every 5s for maker orders."""
    global last_heartbeat_ts
    if not MAKER_MODE:
        return  # only needed for limit orders
    async with aiohttp.ClientSession() as s:
        while not is_done():
            try:
                async with s.post(
                    "https://clob.polymarket.com/heartbeats",
                    timeout=aiohttp.ClientTimeout(total=3),
                ) as r:
                    if r.status == 200:
                        last_heartbeat_ts = time.time()
            except Exception:
                pass
            await asyncio.sleep(HEARTBEAT_INTERVAL)


# --- Polymarket WebSocket ---


async def polymarket_ws_stream():
    """Market WebSocket — full real-time data per docs.
    Events: book, price_change, best_bid_ask, last_trade_price, market_resolved.
    """
    global latest_pm_bid, latest_pm_ask, latest_pm_mid
    global latest_pm_last_trade, pm_ws_connected, last_order_book

    while not is_done():
        try:
            up_token = market_info.get("up_token", "")
            if not up_token:
                await asyncio.sleep(2)
                continue

            down_token = market_info.get("down_token", "")

            async with websockets.connect(
                POLYMARKET_MARKET_WS, ping_interval=None
            ) as ws:
                # Subscribe with custom_feature_enabled for best_bid_ask + market_resolved
                await ws.send(json.dumps({
                    "assets_ids": [up_token, down_token],
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                pm_ws_connected = True
                console.print("[green]+ PM WS connected (custom features)[/green]")

                while not is_done():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        try:
                            await ws.send("PING")
                        except Exception:
                            break
                        continue

                    if msg in ("PONG", "PING"):
                        continue
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue

                    events = data if isinstance(data, list) else [data]
                    for evt in events:
                        etype = evt.get("event_type", "")
                        asset = evt.get("asset_id", "")

                        # ── book: full orderbook snapshot ──
                        if etype == "book":
                            # ONLY use UP token book, ignore DOWN token book
                            if asset and asset != up_token:
                                continue
                            try:
                                if not isinstance(last_order_book, dict):
                                    last_order_book = {"bids": [], "asks": [], "spread": 0}
                                bids_raw = evt.get("bids", [])
                                asks_raw = evt.get("asks", [])
                                if bids_raw:
                                    last_order_book["bids"] = [
                                        [float(b["price"]), float(b["size"])]
                                        for b in bids_raw[:10]
                                        if isinstance(b, dict)
                                    ]
                                if asks_raw:
                                    last_order_book["asks"] = [
                                        [float(a["price"]), float(a["size"])]
                                        for a in asks_raw[:10]
                                        if isinstance(a, dict)
                                    ]
                                if last_order_book["bids"] and last_order_book["asks"]:
                                    latest_pm_bid = last_order_book["bids"][0][0]
                                    latest_pm_ask = last_order_book["asks"][0][0]
                                    latest_pm_mid = (latest_pm_bid + latest_pm_ask) / 2
                                    last_order_book["spread"] = round(latest_pm_ask - latest_pm_bid, 4)
                                    market_info["bestBid"] = latest_pm_bid
                                    market_info["bestAsk"] = latest_pm_ask
                            except Exception:
                                pass

                        # ── best_bid_ask: quick update without full book ──
                        elif etype == "best_bid_ask":
                            # ONLY use UP token prices
                            if asset and asset != up_token:
                                continue
                            try:
                                bb = float(evt.get("best_bid") or 0)
                                ba = float(evt.get("best_ask") or 0)
                                sp = float(evt.get("spread") or 0)
                                if bb > 0:
                                    latest_pm_bid = bb
                                    market_info["bestBid"] = bb
                                if ba > 0:
                                    latest_pm_ask = ba
                                    market_info["bestAsk"] = ba
                                    polymarket["price"] = ba
                                if bb > 0 and ba > 0:
                                    latest_pm_mid = (bb + ba) / 2
                                    if isinstance(last_order_book, dict):
                                        last_order_book["spread"] = sp or round(ba - bb, 4)
                            except Exception:
                                pass

                        # ── price_change: order placed/cancelled ──
                        elif etype == "price_change":
                            try:
                                changes = evt.get("price_changes", evt.get("changes", []))
                                for ch in changes:
                                    if isinstance(ch, dict):
                                        aid = ch.get("asset_id", "")
                                        bb = ch.get("best_bid")
                                        ba = ch.get("best_ask")
                                        if aid == up_token:
                                            if ba:
                                                polymarket["price"] = float(ba)
                                            if bb:
                                                latest_pm_bid = float(bb)
                                                market_info["bestBid"] = float(bb)
                                            if ba:
                                                latest_pm_ask = float(ba)
                                                market_info["bestAsk"] = float(ba)
                                    elif isinstance(ch, (list, tuple)) and len(ch) >= 2:
                                        if ch[0] == up_token:
                                            polymarket["price"] = float(ch[1])
                            except Exception:
                                pass

                        # ── last_trade_price: trade executed ──
                        elif etype == "last_trade_price":
                            try:
                                latest_pm_last_trade = float(evt.get("price") or 0)
                                # Also has side, size, fee_rate_bps
                            except Exception:
                                pass

                        # ── market_resolved: market settled ──
                        elif etype == "market_resolved":
                            winner = evt.get("winning_outcome", "")
                            console.print(
                                f"[bold cyan]Market resolved: "
                                f"{winner}[/bold cyan]")
                            # Trigger settlement
                            try:
                                async with aiohttp.ClientSession() as s:
                                    await labdb.auto_settle_pending(s)
                            except Exception:
                                pass

                        # ── tick_size_change ──
                        elif etype == "tick_size_change":
                            new_tick = evt.get("new_tick_size", "0.01")
                            console.print(
                                f"[dim]Tick size changed: {new_tick}[/dim]")

                    # Reconnect if market changed
                    if market_info.get("up_token", "") != up_token:
                        break

        except Exception as e:
            console.print(f"[yellow]PM WS reconnect: {e}[/yellow]")
        pm_ws_connected = False
        await asyncio.sleep(3)


async def polymarket_user_ws():
    """User channel — order placements, fills, cancellations, trade statuses."""
    global user_ws_connected

    if not CLOB_API_KEY:
        return

    while not is_done():
        try:
            async with websockets.connect(
                POLYMARKET_USER_WS, ping_interval=None
            ) as ws:
                await ws.send(json.dumps({
                    "auth": {
                        "apiKey": CLOB_API_KEY,
                        "secret": CLOB_SECRET,
                        "passphrase": CLOB_PASSPHRASE,
                    },
                    "markets": [],
                    "type": "user",
                }))
                user_ws_connected = True
                console.print("[green]+ PM User WS connected[/green]")

                while not is_done():
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        try:
                            await ws.send("PING")
                        except Exception:
                            break
                        continue

                    if msg in ("PONG", "PING"):
                        continue
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue

                    events = data if isinstance(data, list) else [data]
                    for evt in events:
                        etype = evt.get("event_type", "")

                        # ── trade: order matched/mined/confirmed/failed ──
                        if etype == "trade":
                            status = evt.get("status", "")
                            sz = float(evt.get("size") or 0)
                            pr = float(evt.get("price") or 0)
                            sd = evt.get("side", "")
                            outcome = evt.get("outcome", "")
                            asset_id = evt.get("asset_id", "")
                            trade_id = evt.get("id", "")[:12]

                            # Only process trades for CURRENT market tokens
                            up_token = market_info.get("up_token", "")
                            down_token = market_info.get("down_token", "")
                            if asset_id == up_token:
                                token_side = "UP"
                            elif asset_id == down_token:
                                token_side = "DOWN"
                            else:
                                # Different market — ignore completely
                                if status == "MATCHED":
                                    console.print(
                                        f"[dim]Ignoring fill from other market: "
                                        f"{asset_id[:12]}[/dim]")
                                continue

                            if status == "MATCHED":
                                # Dedup
                                full_trade_id = evt.get("id", "")
                                if full_trade_id in _seen_trade_ids:
                                    continue
                                _seen_trade_ids.add(full_trade_id)
                                if len(_seen_trade_ids) > 500:
                                    _seen_trade_ids.clear()

                                # ── Process maker_orders fills (MM and/or MakerBuy) ──
                                # Each maker_orders entry has OUR order data
                                # Top-level sd/sz/pr are TAKER's perspective
                                mm_processed = False
                                maker_list = evt.get("maker_orders") or []
                                for mo in maker_list:
                                    if not isinstance(mo, dict):
                                        continue
                                    oid = mo.get("order_id", "")
                                    if not oid:
                                        continue

                                    is_mm_order = bool(
                                        mm and mm.active_orders and oid in mm.active_orders
                                    )
                                    # is_tracked covers active orders AND
                                    # orders cancelled in the last N seconds
                                    # (grace window for race-condition fills).
                                    is_mb_order = maker_buy.is_tracked(oid)
                                    if not (is_mm_order or is_mb_order):
                                        continue

                                    mo_side = mo.get("side", sd)
                                    mo_price = float(mo.get("price", pr))
                                    mo_size = float(mo.get("matched_amount", 0) or mo.get("size", sz))
                                    mo_asset = mo.get("asset_id", asset_id)

                                    # Determine token (YES=up_token, NO=down_token)
                                    if mo_asset == up_token:
                                        token_label = "YES"
                                    elif mo_asset == down_token:
                                        token_label = "NO"
                                    else:
                                        continue

                                    tag = "MM" if is_mm_order else "MB"
                                    console.print(
                                        f"[bold green]FILL [{tag}]: {mo_side} {token_label} "
                                        f"{mo_size}sh @ {mo_price:.2f} "
                                        f"[{oid[:10]}][/bold green]")

                                    if is_mm_order:
                                        mm.on_fill(mo_side, mo_price, mo_size,
                                                   order_id=oid, token=token_label)
                                        labdb.save_mm_event({
                                            "session_id": market_info.get("slug"),
                                            "event_type": "fill",
                                            "side": f"{token_label}_{mo_side}",
                                            "price": mo_price,
                                            "size": mo_size,
                                        })
                                        mm_processed = True

                                    if is_mb_order:
                                        maker_buy.on_fill(oid, mo_size, mo_price)
                                        try:
                                            from datetime import datetime
                                            sec_left = get_timer(
                                                market_info.get("endDate")
                                            )
                                            # The trade event id is the
                                            # on-chain trade hash; carry it
                                            # so reconcile loop can dedup.
                                            tx_h = (
                                                evt.get("transaction_hash")
                                                or evt.get("transactionHash")
                                                or full_trade_id or None
                                            )
                                            labdb.save_maker_buy_trade({
                                                "window_id": market_info.get("slug", ""),
                                                "condition_id": market_info.get("conditionId", ""),
                                                "order_id": oid,
                                                "token_label": token_label,
                                                "price": mo_price,
                                                "size": mo_size,
                                                "usdc_spent": round(mo_price * mo_size, 4),
                                                "seconds_to_expiry": sec_left,
                                                # tz-naive UTC iso to keep DB format consistent
                                                "filled_at": datetime.now(timezone.utc)
                                                                .replace(tzinfo=None).isoformat(),
                                                "tx_hash": tx_h,
                                                "source": "ws",
                                            })
                                        except Exception as e:
                                            console.print(
                                                f"[yellow]save_maker_buy_trade: {e}[/yellow]")
                                        mm_processed = True

                                # ── Process non-MM fills (auto-bets) ──
                                # Top-level event data = our taker fill
                                if not mm_processed:
                                    console.print(
                                        f"[bold green]FILL [BET]: {sd} {token_side} "
                                        f"{sz}sh @ {pr:.2f} [{trade_id}][/bold green]")

                                    # Update global inventory (auto-bets only)
                                    if sd == "BUY":
                                        if token_side == "UP":
                                            inventory["yes_tokens"] += sz
                                        else:
                                            inventory["no_tokens"] += sz
                                    elif sd == "SELL":
                                        if token_side == "UP":
                                            inventory["yes_tokens"] -= sz
                                        else:
                                            inventory["no_tokens"] -= sz
                                    inventory["net_exposure"] = (
                                        inventory["yes_tokens"]
                                        - inventory["no_tokens"])

                                    # Save auto-bet to bets table
                                    if sz > 0 and pr > 0:
                                        timer_left = get_timer(market_info.get("endDate")) or 0
                                        phase = "early" if timer_left > 180 else "mid" if timer_left > 60 else "late"
                                        labdb.save_bet({
                                            "sessionId": market_info.get("slug", ""),
                                            "strategyId": "autobet",
                                            "strategyName": "Auto-bet",
                                            "betType": "real" if REAL_BETTING_ENABLED else "paper",
                                            "side": token_side,
                                            "amountUSDC": round(sz * pr, 2),
                                            "intendedPrice": pr,
                                            "executedPrice": pr,
                                            "sharesReceived": sz,
                                            "feeRate": 0,
                                            "feeCalculated": 0,
                                            "targetPrice": target_price,
                                            "targetPriceSource": target_source,
                                            "cexMedianAtBet": cex_median,
                                            "secondsLeftAtBet": timer_left,
                                            "timerPhase": phase,
                                            "autoPlaced": True,
                                            "signalType": "autobet",
                                            "note": f"Auto fill {token_side}",
                                            "orderId": evt.get("taker_order_id", ""),
                                        })

                            elif status == "CONFIRMED":
                                console.print(
                                    f"[dim]Trade confirmed: {sd} {sz} "
                                    f"@ {pr:.2f} [{trade_id}][/dim]")

                            elif status == "FAILED":
                                console.print(
                                    f"[bold red]Trade FAILED: {sd} {sz} "
                                    f"@ {pr:.2f} [{trade_id}][/bold red]")

                            elif status == "RETRYING":
                                console.print(
                                    f"[yellow]Trade retrying: "
                                    f"[{trade_id}][/yellow]")

                        # ── order: placement/update/cancellation ──
                        elif etype == "order":
                            otype = evt.get("type", "")
                            full_oid = evt.get("id", "") or ""
                            oid = full_oid[:12]
                            side = evt.get("side", "")
                            price = evt.get("price", "")
                            orig = evt.get("original_size", "")
                            matched = evt.get("size_matched", "0")

                            if otype == "PLACEMENT":
                                console.print(
                                    f"[dim]Order placed: {side} "
                                    f"{orig}sh @ {price} [{oid}][/dim]")

                            elif otype == "UPDATE":
                                console.print(
                                    f"[cyan]Order update: {side} "
                                    f"filled {matched}/{orig} "
                                    f"@ {price} [{oid}][/cyan]")
                                # Fully filled -> remove from tracker
                                try:
                                    if (full_oid
                                            and float(matched or 0) >= float(orig or 0) > 0
                                            and full_oid in maker_buy._active_order_ids):
                                        maker_buy.on_order_expired(full_oid)
                                except Exception:
                                    pass

                            elif otype == "CANCELLATION":
                                reason = evt.get("cancel_reason", "")
                                console.print(
                                    f"[yellow]Order cancelled: {side} "
                                    f"@ {price} [{oid}]"
                                    f"{' reason=' + reason if reason else ''}[/yellow]")
                                if "liveness" in (reason or "").lower():
                                    console.print(
                                        "[bold red]HEARTBEAT ISSUE![/bold red]")
                                # CRITICAL: drop cancelled/expired orders
                                # from MakerBuy tracker so future ladders
                                # aren't blocked by phantom entries.
                                # Also mark the recently-cancelled entry
                                # as server-confirmed (shorter grace window).
                                if full_oid:
                                    if full_oid in maker_buy._active_order_ids:
                                        maker_buy.on_order_expired(full_oid)
                                    maker_buy.confirm_cancel(full_oid)

        except Exception as e:
            console.print(f"[yellow]User WS reconnect: {e}[/yellow]")
        user_ws_connected = False
        await asyncio.sleep(3)


# --- Inventory ---


def update_inventory(token_side: str, size: float):
    """Update inventory. token_side is 'UP' or 'DOWN'."""
    if token_side == "UP":
        inventory["yes_tokens"] += size
    elif token_side == "DOWN":
        inventory["no_tokens"] += size
    inventory["net_exposure"] = (inventory["yes_tokens"]
                                 - inventory["no_tokens"])


def calculate_skewed_price(base_price: float) -> float:
    I = inventory["net_exposure"]
    return max(0.01, min(0.99, base_price - GAMMA_RISK * I))


def get_maker_quotes(fair_price: float) -> dict:
    skewed = calculate_skewed_price(fair_price)
    half = MAKER_SPREAD / 2
    return {
        "bid": round(skewed - half, 2),
        "ask": round(skewed + half, 2),
        "skew": round(skewed - fair_price, 4),
        "inventory": inventory["net_exposure"],
    }


def is_dust(amount: float, price: float) -> bool:
    return amount * price < DUST_THRESHOLD


def reset_tick_cache():
    if clob_client:
        for attr in ("_tick_sizes", "tick_sizes"):
            if hasattr(clob_client, attr):
                setattr(clob_client, attr, {})


def check_straddle(ask_yes: float, ask_no: float) -> dict | None:
    total = ask_yes + ask_no
    if total < 1.0:
        profit = 1.0 - total
        return {"cost": total, "profit": profit, "pct": profit * 100}
    return None


MAKER_MODE = os.getenv("MAKER_MODE", "false").lower() == "true"
MAKER_ORDER_TTL = int(os.getenv("MAKER_ORDER_TTL", "30"))  # seconds


async def place_real_bet_clob(side, amount_shares, token_id,
                              intended_price, aio_session):
    """Place order on Polymarket CLOB.
    MAKER_MODE=false: taker order at best ask (instant fill, pays fee)
    MAKER_MODE=true:  maker order at best bid+1tick (waits for fill, no fee)
    """
    if not clob_client:
        return {"success": False, "error": "CLOB client not initialized"}
    try:
        # Fetch book for THIS specific token (not default UP token)
        try:
            async with aio_session.get(
                "https://clob.polymarket.com/book",
                params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    book = {
                        "bids": [[float(b["price"]), float(b["size"])]
                                 for b in (data.get("bids") or [])[:5]],
                        "asks": [[float(a["price"]), float(a["size"])]
                                 for a in (data.get("asks") or [])[:5]],
                    }
                else:
                    return {"success": False, "error": f"Book fetch {r.status}"}
        except Exception as e:
            return {"success": False, "error": f"Book fetch: {e}"}
        if not book.get("asks") and not book.get("bids"):
            return {"success": False, "error": "Empty order book"}

        from py_clob_client.clob_types import OrderArgs, OrderType

        if MAKER_MODE:
            return await _place_maker_order(
                side, amount_shares, token_id, intended_price,
                book, aio_session)
        else:
            return await _place_taker_order(
                side, amount_shares, token_id, intended_price, book)

    except Exception as e:
        console.print(f"[bold red]BET FAILED: {type(e).__name__}: {e}[/bold red]")
        if hasattr(e, 'status_code'):
            console.print(f"[red]  Status: {e.status_code}[/red]")
        if hasattr(e, 'error_msg'):
            console.print(f"[red]  Message: {e.error_msg}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()[-300:]}[/dim]")
        return {"success": False, "error": str(e),
                "intendedPrice": intended_price}


async def _place_taker_order(side, shares, token_id, intended_price, book):
    """Taker: buy at best ask, instant fill, pays fee."""
    from py_clob_client.clob_types import OrderArgs, OrderType

    # Use the book that was passed (already for correct token from caller)
    asks = book.get("asks", [])
    if not asks:
        return {"success": False, "error": "No asks"}
    actual_ask = asks[0][0]

    order_args = OrderArgs(
        price=actual_ask,
        size=round(shares, 1),
        side="BUY",
        token_id=token_id,
    )
    signed = clob_client.create_order(order_args)
    resp = clob_client.post_order(signed, OrderType.GTC)

    order_id = resp.get("orderID", "")
    making = float(resp.get("makingAmount") or 0)
    shares_received = making
    executed_price = actual_ask
    usdc_spent = round(shares_received * executed_price, 2)

    console.print(f"[bold green]TAKER: {side} "
                  f"{shares_received:.1f} sh @ {executed_price:.0%} "
                  f"= ${usdc_spent:.2f}[/bold green]")
    console.print(f"  ID: {order_id[:16]}")

    return {
        "success": shares_received > 0,
        "intendedPrice": intended_price,
        "executedPrice": executed_price,
        "slippage": 0,
        "sharesReceived": shares_received,
        "usdcSpent": usdc_spent,
        "orderId": order_id,
        "orderStatus": resp.get("status", ""),
        "orderType": "taker",
    }


async def _place_maker_order(side, shares, token_id, intended_price,
                             book, aio_session):
    """Maker: place limit at best bid + 1 tick. Wait TTL. Cancel unfilled."""
    from py_clob_client.clob_types import OrderArgs, OrderType

    # Place at intended_price - 1 tick (cheaper = maker, no fee)
    tick = 0.01
    maker_price = round(intended_price - tick, 2)

    maker_price = max(0.01, min(0.99, maker_price))

    order_args = OrderArgs(
        price=maker_price,
        size=round(shares, 1),
        side="BUY",
        token_id=token_id,
    )
    signed = clob_client.create_order(order_args)
    resp = clob_client.post_order(signed, OrderType.GTC)

    order_id = resp.get("orderID", "")
    status = resp.get("status", "")
    initial_making = float(resp.get("makingAmount") or 0)

    console.print(f"[bold cyan]MAKER: {side} "
                  f"{shares:.1f} sh @ {maker_price:.0%} "
                  f"(bid+1tick) TTL={MAKER_ORDER_TTL}s[/bold cyan]")
    console.print(f"  ID: {order_id[:16]}  "
                  f"Instant fill: {initial_making:.1f} sh")

    # Wait for fills
    total_filled = initial_making
    if status == "matched":
        # Fully filled immediately
        pass
    elif order_id and total_filled < shares:
        # Wait TTL seconds, checking periodically
        wait_end = time.time() + MAKER_ORDER_TTL
        while time.time() < wait_end:
            await asyncio.sleep(3)
            try:
                order_info = clob_client.get_order(order_id)
                if order_info:
                    filled = float(order_info.get("size_matched", 0))
                    if filled > total_filled:
                        total_filled = filled
                        console.print(
                            f"[cyan]  Fill: {total_filled:.1f}/{shares:.1f} sh[/cyan]")
                    if order_info.get("status") in ("MATCHED", "CANCELLED"):
                        break
            except Exception:
                pass

        # Cancel remaining if not fully filled
        if total_filled < shares:
            try:
                clob_client.cancel(order_id)
                console.print(
                    f"[yellow]  Cancelled unfilled "
                    f"{shares - total_filled:.1f} sh[/yellow]")
            except Exception:
                pass

    shares_received = total_filled
    usdc_spent = round(shares_received * maker_price, 2)

    if shares_received > 0:
        console.print(f"[bold green]MAKER DONE: "
                      f"{shares_received:.1f} sh @ {maker_price:.0%} "
                      f"= ${usdc_spent:.2f} (NO FEE)[/bold green]")
    else:
        console.print(f"[yellow]MAKER: 0 fills, order expired[/yellow]")

    return {
        "success": shares_received > 0,
        "intendedPrice": intended_price,
        "executedPrice": maker_price,
        "slippage": round(maker_price - intended_price, 4),
        "sharesReceived": shares_received,
        "usdcSpent": usdc_spent,
        "orderId": order_id,
        "orderStatus": "filled" if shares_received >= shares else "partial",
        "orderType": "maker",
        "feeActual": 0,  # makers pay no fee
    }


async def record_bet_to_api(bet_type, side, amount, strategy_id,
                            bet_result, aio_session):
    """Record a bet (real or paper) to local SQLite."""
    timer = get_timer(market_info.get("endDate")) or 0
    timer_phase = "early" if timer > 180 else "mid" if timer > 60 else "late"
    # amount = shares ordered. Real USDC = shares × price
    price = bet_result.get("intendedPrice") or 0.5
    price = max(0.01, min(0.99, price))
    shares = bet_result.get("sharesReceived") or amount
    usdc_actual = bet_result.get("usdcSpent") or round(shares * price, 2)
    # Fee: calculated in USDC, collected in shares on BUY
    # fee_usdc = shares_ordered * rate * p * (1-p)
    # fee_shares = fee_usdc / p (deducted from received shares)
    shares_ordered = amount  # amount = shares count
    fee_usdc = round(shares_ordered * PM_FEE_RATE * price * (1 - price), 5)
    fee_shares = round(fee_usdc / price, 4) if price > 0 else 0
    # Net shares = shares_ordered - fee_shares
    fee_calc = fee_usdc

    ref = prices.get("coinbase") or chainlink.get("price") or cex_median
    fv = (calculate_fair_probability(ref, target_price, timer)
          if (ref and target_price and timer > 0) else
          {"fair_up": 0.5, "fair_down": 0.5})
    pm_up = polymarket.get("price") or 0.5
    edge = (fv["fair_up"] - pm_up if side == "UP"
            else fv["fair_down"] - (1 - pm_up))

    # Look up strategy name
    strat_name = ""
    sid = strategy_id or DEFAULT_STRATEGY_ID
    for s in strategies:
        if s.get("id") == sid:
            strat_name = s.get("name", "")
            break
    if not strat_name and sid:
        try:
            s = labdb.get_strategy(sid)
            if s:
                strat_name = s.get("name", "")
        except Exception:
            pass

    bet_data = {
        "sessionId": market_info.get("slug", ""),
        "strategyId": sid,
        "strategyName": strat_name,
        "betType": bet_type,
        "marketQuestion": market_info.get("question", ""),
        "marketSlug": market_info.get("slug", ""),
        "upTokenId": market_info.get("up_token", ""),
        "downTokenId": market_info.get("down_token", ""),
        "eventStartTime": market_info.get("eventStartTime", ""),
        "eventEndTime": market_info.get("endDate", ""),
        "side": side,
        "amountUSDC": usdc_actual,
        "intendedPrice": bet_result.get("intendedPrice"),
        "executedPrice": bet_result.get("executedPrice"),
        "slippage": bet_result.get("slippage"),
        "sharesReceived": bet_result.get("sharesReceived"),
        "feeRate": PM_FEE_RATE,
        "feeCalculated": fee_calc,
        "feeActual": bet_result.get("feeActual"),
        "targetPrice": target_price,
        "targetPriceSource": target_source if target_source == "manual" else "auto",
        "cexMedianAtBet": cex_median,
        "binancePriceAtBet": prices.get("binance"),
        "coinbasePriceAtBet": prices.get("coinbase"),
        "okxPriceAtBet": prices.get("okx"),
        "bybitPriceAtBet": prices.get("bybit"),
        "moveFromTarget": (round(cex_median - target_price, 2)
                           if cex_median and target_price else None),
        "movePercent": (round((cex_median - target_price) / target_price * 100, 4)
                        if cex_median and target_price else None),
        "pmUpPriceAtBet": pm_up,
        "pmDownPriceAtBet": round(1 - pm_up, 3),
        "pmBidAtBet": market_info.get("bestBid"),
        "pmAskAtBet": market_info.get("bestAsk"),
        "pmSpreadAtBet": None,
        "orderBookBids": (last_order_book or {}).get("bids", [])[:3],
        "orderBookAsks": (last_order_book or {}).get("asks", [])[:3],
        "fairProbability": bet_result.get("signalFair") or fv.get("fair_up", 0.5),
        "edge": round(bet_result.get("signalEdge") or edge, 4),
        "modelVolatility": 15.0,
        "secondsLeftAtBet": timer,
        "timerPhase": timer_phase,
        "orderId": bet_result.get("orderId"),
        "orderStatus": bet_result.get("orderStatus"),
        "autoPlaced": bet_result.get("autoPlaced", False),
        "signalType": bet_result.get("signalType", "manual"),
    }
    try:
        # Save locally to SQLite (no HTTP, no limits)
        bet_id = labdb.save_bet(bet_data)
        console.print(f"[cyan]Saved: {bet_id} ({bet_type})[/cyan]")
        return bet_id
    except Exception as e:
        console.print(f"[red]Record error: {e}[/red]")
    return None


async def settlement_loop():
    """Every 5 minutes, settle pending bets via Gamma API + local DB."""
    while not is_done():
        await asyncio.sleep(300)
        try:
            async with aiohttp.ClientSession() as s:
                settled = await labdb.auto_settle_pending(s)
                if settled > 0:
                    console.print(
                        f"[cyan]Settlement: {settled} bets closed[/cyan]")
        except Exception:
            pass


# --- Exchange Streams ---


async def binance_stream():
    try:
        async with websockets.connect(BINANCE_WS) as ws:
            console.print("[green]+ Binance connected[/green]")
            async for msg in ws:
                if is_done():
                    break
                recv_ms = int(time.time() * 1000)
                trade = json.loads(msg)
                price = float(trade["p"])
                prev = prices["binance"]
                prices["binance"] = price
                timestamps["binance"] = recv_ms
                if prev:
                    delta = abs(price - prev)
                    if delta > 1:
                        moves.append({"ts": recv_ms, "price": price,
                                      "dir": "UP" if price > prev else "DOWN",
                                      "delta": delta})
    except Exception as e:
        console.print(f"[red]Binance: {e}[/red]")


async def coinbase_stream():
    try:
        async with websockets.connect(COINBASE_WS) as ws:
            await ws.send(json.dumps({
                "type": "subscribe", "product_ids": ["BTC-USD"],
                "channel": "ticker",
            }))
            console.print("[green]+ Coinbase connected[/green]")
            async for msg in ws:
                if is_done():
                    break
                data = json.loads(msg)
                if data.get("channel") == "ticker" and "events" in data:
                    for ev in data["events"]:
                        tickers = ev.get("tickers", [])
                        if tickers and tickers[0].get("price"):
                            prices["coinbase"] = float(tickers[0]["price"])
                            timestamps["coinbase"] = int(time.time() * 1000)
    except Exception as e:
        console.print(f"[yellow]Coinbase: {e}[/yellow]")


async def okx_stream():
    try:
        async with websockets.connect(OKX_WS) as ws:
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": [{"channel": "tickers", "instId": "BTC-USDT"}],
            }))
            console.print("[green]+ OKX connected[/green]")
            async for msg in ws:
                if is_done():
                    break
                data = json.loads(msg)
                if "data" in data and data["data"]:
                    p = data["data"][0].get("last")
                    if p:
                        prices["okx"] = float(p)
                        timestamps["okx"] = int(time.time() * 1000)
    except Exception as e:
        console.print(f"[yellow]OKX: {e}[/yellow]")


async def bybit_stream():
    try:
        async with websockets.connect(BYBIT_WS) as ws:
            await ws.send(json.dumps({
                "op": "subscribe", "args": ["tickers.BTCUSDT"],
            }))
            console.print("[green]+ Bybit connected[/green]")
            async for msg in ws:
                if is_done():
                    break
                data = json.loads(msg)
                if "data" in data and data["data"].get("lastPrice"):
                    prices["bybit"] = float(data["data"]["lastPrice"])
                    timestamps["bybit"] = int(time.time() * 1000)
    except Exception as e:
        console.print(f"[yellow]Bybit: {e}[/yellow]")


# --- Async Loops ---


RTDS_WS = "wss://ws-live-data.polymarket.com"


async def polymarket_rtds_stream():
    """Polymarket Real-Time Data Socket — Chainlink + Binance prices via WS.
    Much faster than REST polling (sub-second updates)."""
    global auto_target_price

    while not is_done():
        try:
            async with websockets.connect(RTDS_WS, ping_interval=None) as ws:
                # Subscribe to Chainlink BTC/USD + Binance BTC/USDT
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": '{"symbol":"btc/usd"}',
                        },
                        {
                            "topic": "crypto_prices",
                            "type": "update",
                            "filters": "btcusdt",
                        },
                    ],
                }))
                console.print("[green]+ RTDS WS connected "
                              "(Chainlink + Binance)[/green]")

                ping_ts = time.time()
                while not is_done():
                    # Send PING every 5s
                    if time.time() - ping_ts > 5:
                        try:
                            await ws.send("PING")
                        except Exception:
                            break
                        ping_ts = time.time()

                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=6)
                    except asyncio.TimeoutError:
                        continue

                    if msg in ("PONG", "PING"):
                        continue
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue

                    topic = data.get("topic", "")
                    payload = data.get("payload", {})

                    if topic == "crypto_prices_chainlink":
                        sym = payload.get("symbol", "")
                        if sym == "btc/usd":
                            val = float(payload.get("value", 0))
                            ts_ms = int(payload.get("timestamp", 0))
                            if val > 0:
                                chainlink["price"] = val
                                chainlink["ts"] = ts_ms
                                record_spread()

                    elif topic == "crypto_prices":
                        sym = payload.get("symbol", "")
                        if sym == "btcusdt":
                            val = float(payload.get("value", 0))
                            # This is Binance price — compare with our WS
                            # Useful as backup / validation
                            pass  # prices["binance"] already from WS

        except Exception as e:
            console.print(f"[yellow]RTDS reconnect: {e}[/yellow]")
        await asyncio.sleep(3)


async def chainlink_loop():
    """Chainlink poller every 2s. Maintains auto_target_price always."""
    global target_price, target_source, auto_target_price, target_mode

    prev_cl_price = None
    cl_window_slug = current_5min_slug()

    async with aiohttp.ClientSession() as session:
        # Set initial auto target
        cl_init, _, _ = await fetch_chainlink_price(session)
        if cl_init:
            auto_target_price = cl_init
            if target_mode == "auto":
                target_price = auto_target_price
                target_source = "Chainlink (auto)"

        while not is_done():
            now_ms = int(time.time() * 1000)
            cl_price, cl_data_ts, cl_raw_ts = await fetch_chainlink_price(
                session)
            if cl_price and cl_data_ts:
                chainlink["price"] = cl_price
                chainlink["ts"] = cl_data_ts
                chainlink["raw_ts"] = cl_raw_ts
                lags_cl.append(now_ms - cl_data_ts)
                record_spread()

            # Window transition — set new auto target, force auto mode
            new_slug = current_5min_slug()
            if new_slug != cl_window_slug:
                # Use Coinbase as target (closest to Chainlink)
                new_auto = (prices.get("coinbase") or cl_price or
                            prev_cl_price or cex_median)
                if new_auto:
                    auto_target_price = new_auto
                    src = ("Coinbase" if prices.get("coinbase")
                           else "Chainlink" if (cl_price or prev_cl_price)
                           else "CEX Median")
                else:
                    src = "unchanged"
                # Reset to auto on new window (unless target.txt has value)
                file_tgt = check_target_file()
                if file_tgt:
                    target_price = file_tgt
                    target_mode = "manual"
                    target_source = "manual (target.txt)"
                    console.print(
                        f"[yellow]New window, manual target: "
                        f"${file_tgt:,.2f}[/yellow]")
                elif auto_target_price:
                    target_mode = "auto"
                    target_price = auto_target_price
                    target_source = f"{src} (auto @ 0:00)"
                    console.print(
                        f"[bold magenta]New window -> auto target: "
                        f"${auto_target_price:,.2f} ({src})[/bold magenta]")
                else:
                    target_mode = "auto"
                    console.print("[yellow]New window, waiting for price[/yellow]")
                # Tell dashboard
                try:
                    await session.post(
                        f"{AGENT_HQ_URL}/api/btc-lab/control",
                        json={"manualTarget": None, "targetMode": "auto"},
                        headers=_hq_headers(),
                        timeout=aiohttp.ClientTimeout(total=3),
                    )
                except Exception:
                    pass
                cl_window_slug = new_slug

            # Check if priceToBeat became available (confirms our target)
            if (target_source and "auto" in target_source
                    and cl_window_slug):
                ptb = await get_price_to_beat(session, cl_window_slug)
                if ptb:
                    target_price = ptb
                    target_source = "Polymarket API (priceToBeat)"
                    console.print(
                        f"[magenta]Target updated: ${ptb:,.2f} "
                        f"(priceToBeat now available)[/magenta]")

            # Last ~2 seconds of window → snapshot Coinbase as target
            # for the NEXT window (most accurate, closest to settlement)
            timer_now = get_timer(market_info.get("endDate"))
            if timer_now is not None and timer_now <= 2:
                cb = prices.get("coinbase")
                if cb and cb != auto_target_price:
                    auto_target_price = cb  # save for next window
                    # Only apply if not manual
                    if target_mode == "auto":
                        target_price = auto_target_price
                        target_source = "Coinbase (auto @ last sec)"

            # Always keep prev_cl_price fresh
            if cl_price:
                prev_cl_price = cl_price
            # Safety: if auto_target_price is still None, try to set it
            if not auto_target_price:
                fallback = (prices.get("coinbase") or
                            chainlink.get("price") or cex_median)
                if fallback:
                    auto_target_price = fallback
                    if target_mode == "auto":
                        target_price = auto_target_price
                        target_source = "auto (fallback)"
            await asyncio.sleep(2)


async def maker_buy_reconcile_loop():
    """Periodically reconcile our DB against Polymarket Data API.
    Catches fills lost to WS race conditions / reconnects.

    - Runs every 10 minutes.
    - Fetches recent trades for our Safe address from Data API.
    - For each trade, checks if it's already in maker_buy_trades
      (dedup by tx_hash, then fuzzy by condition+label+price+size+time).
    - Inserts missing trades with source='reconcile'.
    - Read-only against Polymarket; only INSERTs into our local DB.
    """
    import os
    from datetime import datetime, timezone
    safe_addr = os.getenv("MM2_SAFE", "").strip().lower()
    if not safe_addr:
        console.print("[yellow]Reconcile: MM2_SAFE not set — skip[/yellow]")
        return

    CYCLE_SEC = 600       # 10 minutes
    STARTUP_DELAY = 45    # let WS settle first
    PAGE_SIZE = 500
    MAX_PAGES = 6         # 3000 trades = several days back
    # Only reconcile trades older than this many seconds. Gives WS
    # plenty of time to deliver naturally so we don't double-count.
    MIN_AGE_SEC = 120

    await asyncio.sleep(STARTUP_DELAY)
    console.print("[bold cyan]MakerBuyReconcile loop started[/bold cyan]")

    cycle = 0
    async with aiohttp.ClientSession() as session:
        while not is_done():
            cycle += 1
            started = time.time()
            inserted = 0
            checked = 0
            try:
                # Pull last MAX_PAGES * PAGE_SIZE trades, page by page
                for page in range(MAX_PAGES):
                    offset = page * PAGE_SIZE
                    url = (
                        "https://data-api.polymarket.com/trades"
                        f"?user={safe_addr}&takerOnly=false"
                        f"&limit={PAGE_SIZE}&offset={offset}"
                    )
                    try:
                        async with session.get(
                            url, timeout=aiohttp.ClientTimeout(total=20)
                        ) as r:
                            if r.status != 200:
                                break
                            data = await r.json()
                    except Exception:
                        break
                    if not isinstance(data, list) or not data:
                        break

                    cutoff = time.time() - MIN_AGE_SEC
                    for t in data:
                        if not isinstance(t, dict):
                            continue
                        ts = int(t.get("timestamp") or 0)
                        if ts == 0 or ts > cutoff:
                            continue  # too fresh, may still arrive via WS
                        side = (t.get("side") or "").upper()
                        if side != "BUY":
                            continue  # maker_buy is BUY-only
                        outcome = (t.get("outcome") or "")
                        # Polymarket: outcome="Up" -> YES, "Down" -> NO
                        if outcome.lower() == "up":
                            label = "YES"
                        elif outcome.lower() == "down":
                            label = "NO"
                        else:
                            continue
                        cid = t.get("conditionId", "")
                        slug = t.get("slug", "") or t.get("eventSlug", "")
                        size = float(t.get("size") or 0)
                        price = float(t.get("price") or 0)
                        if not (cid and size > 0 and price > 0):
                            continue
                        # ISO timestamp from unix
                        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                        filled_iso = dt.replace(tzinfo=None).isoformat()
                        tx_hash = t.get("transactionHash", "") or None
                        # Synthetic order_id for reconcile inserts
                        synth_oid = (
                            f"reconcile_{tx_hash[:16]}_{cid[:8]}_{label}"
                            if tx_hash else
                            f"reconcile_{ts}_{cid[:8]}_{label}_{int(price*1000)}"
                        )
                        checked += 1
                        try:
                            ok = await asyncio.to_thread(
                                labdb.save_reconcile_trade,
                                {
                                    "window_id": slug,
                                    "condition_id": cid,
                                    "order_id": synth_oid,
                                    "token_label": label,
                                    "price": price,
                                    "size": size,
                                    "filled_at": filled_iso,
                                    "tx_hash": tx_hash,
                                },
                            )
                            if ok:
                                inserted += 1
                        except Exception:
                            pass
                    if len(data) < PAGE_SIZE:
                        break

                if inserted > 0 or cycle <= 3:
                    console.print(
                        f"[dim cyan]MakerBuyReconcile #{cycle}: "
                        f"checked={checked} inserted={inserted}[/dim cyan]"
                    )
            except Exception as e:
                console.print(f"[red]reconcile cycle err: {e}[/red]")

            elapsed = time.time() - started
            wait = max(60, CYCLE_SEC - elapsed)
            chunk = 10
            while wait > 0 and not is_done():
                await asyncio.sleep(min(chunk, wait))
                wait -= chunk


async def maker_buy_rebate_loop():
    """Periodically backfill maker_rebate_usdc per trade from
    Polymarket CLOB /rebates/current endpoint.
    Runs every 30 min. Rebates for day D are posted after D ends.
    """
    import os
    safe_addr = os.getenv("MM2_SAFE", "").strip()
    if not safe_addr:
        console.print("[yellow]MakerBuy rebate loop: MM2_SAFE not set[/yellow]")
        return
    # Initial delay so we don't race startup
    await asyncio.sleep(120)
    async with aiohttp.ClientSession() as session:
        while not is_done():
            try:
                dates = labdb.get_maker_buy_dates_needing_rebates()
                for d in dates:
                    try:
                        url = (
                            "https://clob.polymarket.com/rebates/current"
                            f"?date={d}&maker_address={safe_addr}"
                        )
                        async with session.get(
                            url, timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            if r.status != 200:
                                continue
                            data = await r.json()
                        if not isinstance(data, list):
                            continue
                        n = labdb.apply_rebate_entries(d, data)
                        if n > 0:
                            console.print(
                                f"[cyan]MakerBuy rebates backfilled: "
                                f"{n} trade(s) on {d}[/cyan]"
                            )
                    except Exception as e:
                        console.print(
                            f"[yellow]rebate fetch {d}: {e}[/yellow]"
                        )
                await asyncio.sleep(1800)  # 30 min
            except Exception as e:
                console.print(f"[red]rebate_loop err: {e}[/red]")
                await asyncio.sleep(120)


async def maker_buy_redeem_loop():
    """Outer self-healing wrapper around the redeem worker.
    If _maker_buy_redeem_worker dies for any reason, restart it.
    """
    console.print(
        "[bold cyan]MakerBuyRedeem loop spawning worker[/bold cyan]"
    )
    while not is_done():
        try:
            await _maker_buy_redeem_worker()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            console.print(
                f"[red]MakerBuyRedeem worker crashed: {e}, "
                f"restarting in 15s[/red]"
            )
            await asyncio.sleep(15)
        else:
            # Worker returned normally (shouldn't happen while running)
            await asyncio.sleep(15)


async def _maker_buy_redeem_worker():
    """Settle outcomes + redeem winners. Runs every 5 minutes with a
    hard timeout on each relayer call to prevent hangs.

    Two redeem sources:
      A) Local DB (maker_buy_trades with outcome set, redeemed=0) —
         fast, covers fills we successfully tracked.
      B) Polymarket on-chain positions (/positions?redeemable=true) —
         ground truth, catches positions even if we missed the fill in
         the WS pipeline.
    """
    import os
    create_safe_relayer_client = _create_safe_relayer_client
    CYCLE_SEC = 300  # 5 minutes
    STARTUP_DELAY = 8
    REDEEM_TIMEOUT = 25
    # Don't retry the same condition more often than this (avoid spam
    # if a specific redeem keeps failing for server-side reasons).
    RETRY_COOLDOWN_SEC = 1800  # 30 min
    # Hard cap per cycle (combined across DB + on-chain paths).
    # Polymarket Relayer is rate-limited by Cloudflare (~few req/sec).
    MAX_PER_CYCLE = 15
    # Sleep between consecutive relayer calls to avoid 429.
    REDEEM_DELAY_SEC = 0.8
    # Stop cycle early after N consecutive failures (likely rate-limited).
    CIRCUIT_BREAKER_FAILS = 3

    safe_addr = os.getenv("MM2_SAFE", "").strip().lower()
    relayer = None
    cycle = 0
    # condition_id -> last_attempt_ts
    recently_tried: dict = {}
    console.print(
        f"[bold cyan]MakerBuyRedeem worker started "
        f"(cycle={CYCLE_SEC}s, startup={STARTUP_DELAY}s)[/bold cyan]"
    )
    await asyncio.sleep(STARTUP_DELAY)

    async with aiohttp.ClientSession() as session:
        while not is_done():
            cycle += 1
            started = time.time()

            # 1. Settle outcomes for pending maker_buy windows via Gamma.
            # Now returns {"settled": N, "losses_by_window": {slug: [rows]}}
            settled = 0
            losses_by_window = {}
            try:
                settle_result = await asyncio.wait_for(
                    labdb.settle_pending_maker_buy(session), timeout=30,
                )
                if isinstance(settle_result, dict):
                    settled = settle_result.get("settled", 0)
                    losses_by_window = settle_result.get(
                        "losses_by_window", {}
                    )
                else:
                    settled = settle_result or 0
            except asyncio.TimeoutError:
                console.print("[yellow]MakerBuy settle timeout[/yellow]")
            except Exception as e:
                console.print(f"[red]MakerBuy settle err: {e}[/red]")

            # 1b. Telegram notifications for losing windows
            if tg is not None and losses_by_window:
                for slug, losers in losses_by_window.items():
                    try:
                        asyncio.create_task(
                            tg.notify_losses_batch(losers)
                        )
                    except Exception:
                        pass

            # 2. Get windows ready to redeem
            windows = []
            try:
                windows = labdb.get_maker_buy_windows_to_redeem()
            except Exception as e:
                console.print(
                    f"[red]MakerBuy get_windows err: {e}[/red]"
                )

            # 3. Heartbeat — always print so we can see loop is alive
            console.print(
                f"[dim cyan]MakerBuyRedeem #{cycle}: "
                f"settled={settled} pending={len(windows)}[/dim cyan]"
            )

            # 3b. Hourly Telegram digest (internally throttled to 1/h)
            if tg is not None:
                try:
                    asyncio.create_task(
                        _build_and_send_hourly_report(session)
                    )
                except Exception:
                    pass

            # Combined counters + circuit breaker (shared between DB
            # and on-chain paths within ONE cycle).
            attempted_this_cycle = 0
            consecutive_fails = 0
            cycle_aborted = False

            async def _try_redeem(cid, label_for_log, mark_slug=None):
                """One redeem call with circuit-breaker + rate-limit.
                Returns ('ok' | 'fail' | 'abort'). Updates outer counters
                via nonlocal."""
                nonlocal attempted_this_cycle, consecutive_fails, cycle_aborted
                if cycle_aborted:
                    return "abort"
                if attempted_this_cycle >= MAX_PER_CYCLE:
                    cycle_aborted = True
                    return "abort"
                attempted_this_cycle += 1
                try:
                    ok = await asyncio.wait_for(
                        relayer.redeem(
                            condition_id=cid,
                            session=session,
                            wait=False,
                        ),
                        timeout=REDEEM_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    ok = False
                except Exception as e:
                    console.print(
                        f"[red]redeem err {label_for_log}: {e}[/red]"
                    )
                    ok = False
                if ok:
                    consecutive_fails = 0
                    if mark_slug:
                        try: labdb.mark_maker_buy_redeemed(mark_slug)
                        except Exception: pass
                    return "ok"
                consecutive_fails += 1
                if consecutive_fails >= CIRCUIT_BREAKER_FAILS:
                    console.print(
                        f"[yellow]redeem circuit-breaker tripped "
                        f"(>={CIRCUIT_BREAKER_FAILS} consecutive fails), "
                        f"pausing until next cycle[/yellow]"
                    )
                    cycle_aborted = True
                return "fail"

            if windows:
                if relayer is None:
                    try:
                        relayer = create_safe_relayer_client()
                    except Exception as e:
                        console.print(
                            f"[red]MakerBuy relayer init err: {e}[/red]"
                        )
                        relayer = None
                    if relayer is None:
                        console.print(
                            "[yellow]MakerBuy: relayer unavailable, "
                            "retry next cycle[/yellow]"
                        )

                if relayer is not None:
                    ok_count = 0
                    fail_count = 0
                    for window_id, condition_id in windows:
                        res = await _try_redeem(
                            condition_id, window_id[:25], mark_slug=window_id)
                        if res == "ok":
                            ok_count += 1
                        elif res == "fail":
                            fail_count += 1
                        else:  # abort
                            break
                        await asyncio.sleep(REDEEM_DELAY_SEC)
                    if ok_count or fail_count:
                        console.print(
                            f"[cyan]MakerBuyRedeem DB: "
                            f"ok={ok_count} fail={fail_count}"
                            f"{' [aborted]' if cycle_aborted else ''}[/cyan]"
                        )

            # 4. ON-CHAIN FALLBACK: fetch /positions and redeem anything
            #    marked redeemable=true that we haven't tried recently.
            #    Shares the per-cycle cap and circuit breaker with DB path.
            if safe_addr and not cycle_aborted:
                try:
                    on_chain_pending = await _fetch_onchain_redeemables(
                        session, safe_addr
                    )
                except Exception as e:
                    console.print(
                        f"[yellow]on-chain positions err: {e}[/yellow]"
                    )
                    on_chain_pending = []

                now = time.time()
                for cid in list(recently_tried.keys()):
                    if now - recently_tried[cid] > RETRY_COOLDOWN_SEC:
                        recently_tried.pop(cid, None)
                fresh = [
                    (cid, slug, sz) for (cid, slug, sz) in on_chain_pending
                    if cid not in recently_tried
                ]

                if fresh:
                    if relayer is None:
                        try:
                            relayer = create_safe_relayer_client()
                        except Exception as e:
                            console.print(
                                f"[red]relayer init err: {e}[/red]"
                            )
                    if relayer is not None:
                        console.print(
                            f"[cyan]MakerBuyRedeem on-chain: "
                            f"{len(fresh)} redeemable[/cyan]"
                        )
                        ok_oc = 0
                        fail_oc = 0
                        for cid, slug, sz in fresh:
                            recently_tried[cid] = now
                            res = await _try_redeem(
                                cid, cid[:16], mark_slug=slug)
                            if res == "ok":
                                ok_oc += 1
                            elif res == "fail":
                                fail_oc += 1
                            else:
                                break
                            await asyncio.sleep(REDEEM_DELAY_SEC)
                        if ok_oc or fail_oc:
                            console.print(
                                f"[cyan]OnChain redeem: "
                                f"ok={ok_oc} fail={fail_oc}"
                                f"{' [aborted]' if cycle_aborted else ''}[/cyan]"
                            )

            # Sleep remainder in small chunks so is_done() is checked often
            elapsed = time.time() - started
            wait = max(5, CYCLE_SEC - elapsed)
            chunk = 5
            while wait > 0 and not is_done():
                await asyncio.sleep(min(chunk, wait))
                wait -= chunk


async def _build_and_send_hourly_report(session):
    """Collect stats + portfolio, send to Telegram. Throttled internally."""
    if tg is None:
        return
    # Early exit if it's too soon — saves the stats query
    if time.time() - tg._last_hourly < 3600:
        return

    import os
    safe_addr = os.getenv("MM2_SAFE", "").strip().lower()

    # Today's stats from DB
    try:
        st = labdb.get_maker_buy_stats_today(tz_offset_hours=3.0)
    except Exception:
        st = {}

    # Portfolio snapshot — reuse the /portfolio logic
    portfolio = {"cash_usdc": 0, "winnings_redeemable_usdc": 0,
                 "total_portfolio_usdc": 0}
    if safe_addr:
        try:
            url = (
                "https://data-api.polymarket.com/positions"
                f"?user={safe_addr}&sizeThreshold=0.01&limit=500"
            )
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json() or []
                    red = sum(float(p.get("currentValue", 0) or 0)
                              for p in data if isinstance(p, dict)
                              and p.get("redeemable"))
                    open_val = sum(float(p.get("currentValue", 0) or 0)
                                   for p in data if isinstance(p, dict)
                                   and not p.get("redeemable"))
                    portfolio["winnings_redeemable_usdc"] = red
                    portfolio["total_portfolio_usdc"] = red + open_val
        except Exception:
            pass
    # Cash balance via RPC (reuse /portfolio env var)
    try:
        import os as _os
        rpc = _os.getenv("POLYGON_RPC",
                         "https://polygon.gateway.tenderly.co")
        call_data = "0x70a08231" + "000000000000000000000000" + safe_addr[2:]
        payload = {
            "jsonrpc": "2.0", "method": "eth_call", "id": 1,
            "params": [{
                "to": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                "data": call_data,
            }, "latest"],
        }
        async with session.post(
            rpc, json=payload, timeout=aiohttp.ClientTimeout(total=6)
        ) as r:
            j = await r.json()
            res = j.get("result", "")
            if res:
                portfolio["cash_usdc"] = int(res, 16) / 1e6
                portfolio["total_portfolio_usdc"] += portfolio["cash_usdc"]
    except Exception:
        pass

    # Compute minutes-since-last-fill
    last_min = 0.0
    last_iso = st.get("last_filled_at")
    if last_iso:
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(last_iso.replace("Z", ""))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_min = (datetime.now(timezone.utc) - dt).total_seconds() / 60
        except Exception:
            pass

    stats = {
        "winrate_pct": st.get("winrate_pct", 0),
        "total_pnl_today": st.get("total_pnl", 0),
        "trades_today": st.get("total_trades", 0),
        "wins_today": st.get("wins", 0),
        "cash_usdc": portfolio["cash_usdc"],
        "winnings_usdc": portfolio["winnings_redeemable_usdc"],
        "portfolio_value": portfolio["total_portfolio_usdc"],
        "bot_alive": True,
        "last_trade_minutes_ago": last_min,
    }
    await tg.send_hourly_report(stats)


# ── Telegram command handlers + pause flags ─────────────────

# Pause flags in kv_store override .env enable flags at runtime.
# Cached for 5 sec so we don't hit DB every polling tick.
_tg_pause_cache = {"mm": False, "mb": False, "ts": 0.0}

def _is_paused(key: str) -> bool:
    """True if 'mm' or 'mb' was /stop'd via Telegram. Cached 5 sec."""
    now = time.time()
    if now - _tg_pause_cache["ts"] > 5:
        try:
            _tg_pause_cache["mm"] = bool(labdb.get_kv("mm_paused"))
            _tg_pause_cache["mb"] = bool(labdb.get_kv("maker_buy_paused"))
            _tg_pause_cache["ts"] = now
        except Exception:
            pass
    return bool(_tg_pause_cache.get(key, False))


def _fmt_dollar(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "$—"


async def _cmd_help(_args: str) -> str:
    return (
        "<b>Команды PM Bot:</b>\n"
        "/status — текущее состояние окна + активные ордера\n"
        "/stats — статистика за сегодня\n"
        "/balance — портфель on-chain\n"
        "/last [N] — последние N ставок (default 5)\n"
        "/stop — пауза MM и MakerBuy (новые ордера не ставятся)\n"
        "/start — возобновить\n"
        "/cancel — отменить все активные MakerBuy ордера\n"
        "/help — эта справка"
    )


async def _cmd_status(_args: str) -> str:
    try:
        secs = get_timer(market_info.get("endDate")) or 0
    except Exception:
        secs = 0
    slug = market_info.get("slug", "?")
    cap = maker_buy_cfg.max_usdc_per_window or 0
    spent = maker_buy._window_usdc_spent
    line_mm = "🔴 PAUSED" if _is_paused("mm") else (
        "✅ enabled" if mm_config.enabled else "⚫ disabled (cfg)"
    )
    line_mb = "🔴 PAUSED" if _is_paused("mb") else (
        "✅ enabled" if maker_buy_cfg.enabled else "⚫ disabled (cfg)"
    )
    return (
        f"<b>Status</b>\n"
        f"Window: <code>{slug}</code>\n"
        f"Осталось: {secs:.0f}с\n\n"
        f"MM: {line_mm}\n"
        f"MakerBuy: {line_mb}\n\n"
        f"Active MB orders: {len(maker_buy._active_order_ids)}\n"
        f"Side: {maker_buy._active_side or '—'}\n"
        f"Top price: {maker_buy._active_max_price or '—'}\n"
        f"Spent this window: {_fmt_dollar(spent)}"
        f"{' / ' + _fmt_dollar(cap) if cap else ''}\n"
        f"Window bets: {maker_buy._window_bets}/{maker_buy_cfg.max_bets_per_window}"
    )


async def _cmd_stats(_args: str) -> str:
    try:
        today = labdb.get_maker_buy_stats_today(tz_offset_hours=3.0)
    except Exception as e:
        return f"Ошибка: {e}"
    try:
        claimable = labdb.get_maker_buy_claimable_summary()
    except Exception:
        claimable = {"windows": 0, "payout_usdc": 0, "trades": 0}
    return (
        f"<b>Stats today</b>\n"
        f"Trades: {today.get('total_trades', 0)}\n"
        f"Wins: {today.get('wins', 0)}\n"
        f"Winrate: {today.get('winrate_pct', 0)}%\n"
        f"Invested: {_fmt_dollar(today.get('total_invested', 0))}\n"
        f"P&L: {_fmt_dollar(today.get('total_pnl', 0))}\n\n"
        f"Claimable now: {_fmt_dollar(claimable.get('payout_usdc', 0))} "
        f"({claimable.get('windows', 0)} окон, {claimable.get('trades', 0)} тр)"
    )


async def _cmd_balance(_args: str) -> str:
    """Мини-версия /portfolio для Telegram."""
    safe_addr = os.getenv("MM2_SAFE", "").strip().lower()
    if not safe_addr:
        return "MM2_SAFE not set"
    cash = None; winnings = 0.0; open_val = 0.0; total_pos = 0
    try:
        async with aiohttp.ClientSession() as sess:
            # Cash via Polygon RPC
            rpc = os.getenv("POLYGON_RPC",
                            "https://polygon.gateway.tenderly.co")
            call_data = "0x70a08231" + "000000000000000000000000" + safe_addr[2:]
            try:
                async with sess.post(rpc, json={
                    "jsonrpc":"2.0","method":"eth_call","id":1,
                    "params":[{
                        "to":"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                        "data":call_data}, "latest"],
                }, timeout=aiohttp.ClientTimeout(total=6)) as r:
                    j = await r.json()
                    if j.get("result"):
                        cash = int(j["result"], 16) / 1e6
            except Exception:
                pass
            # Positions
            try:
                async with sess.get(
                    "https://data-api.polymarket.com/positions",
                    params={"user": safe_addr,
                            "sizeThreshold": 0.01, "limit": 500},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    if r.status == 200:
                        data = await r.json() or []
                        total_pos = len(data)
                        for p in data:
                            cv = float(p.get("currentValue", 0) or 0)
                            if p.get("redeemable"):
                                winnings += cv
                            else:
                                open_val += cv
            except Exception:
                pass
    except Exception as e:
        return f"Ошибка: {e}"
    total = (cash or 0) + winnings + open_val
    return (
        f"<b>Balance</b>\n"
        f"Cash USDC: {_fmt_dollar(cash) if cash is not None else '— (RPC down)'}\n"
        f"Winnings (claimable): {_fmt_dollar(winnings)}\n"
        f"Open positions: {_fmt_dollar(open_val)} ({total_pos - int(winnings>0):.0f} поз)\n"
        f"────────\n"
        f"Portfolio: <b>{_fmt_dollar(total)}</b>"
    )


async def _cmd_last(args: str) -> str:
    n = 5
    try:
        n = int(args.strip()) if args.strip() else 5
    except Exception:
        n = 5
    n = max(1, min(20, n))
    try:
        rows = labdb.get_maker_buy_trades(limit=n)
    except Exception as e:
        return f"Ошибка: {e}"
    if not rows:
        return "Пусто"
    lines = [f"<b>Last {len(rows)}:</b>"]
    for r in rows:
        outcome = (r.get("outcome") or "—")
        pnl = r.get("pnl")
        pnl_s = f" pnl={_fmt_dollar(pnl)}" if pnl is not None else ""
        lines.append(
            f"{r.get('token_label') or '?'} "
            f"{float(r.get('size') or 0):.1f}sh @ "
            f"{float(r.get('price') or 0):.3f} → {outcome}{pnl_s}"
        )
    return "\n".join(lines)


async def _cmd_stop(_args: str) -> str:
    try:
        labdb.save_kv("mm_paused", True)
        labdb.save_kv("maker_buy_paused", True)
    except Exception as e:
        return f"Ошибка: {e}"
    # Invalidate cache so new state applies immediately
    _tg_pause_cache["ts"] = 0
    return (
        "🔴 Стратегии поставлены на паузу.\n"
        "Новые ордера не ставятся.\n"
        "Существующие резидентные ордера останутся активными до TTL "
        "(либо используй /cancel чтобы отменить их сейчас)."
    )


async def _cmd_start(_args: str) -> str:
    try:
        labdb.save_kv("mm_paused", False)
        labdb.save_kv("maker_buy_paused", False)
    except Exception as e:
        return f"Ошибка: {e}"
    _tg_pause_cache["ts"] = 0
    return "✅ Стратегии возобновлены."


async def _cmd_cancel(_args: str) -> str:
    """Отменяет все активные MakerBuy ордера прямо сейчас."""
    if clob_client is None:
        return "Клиент не готов"
    stale = list(maker_buy._active_order_ids)
    if not stale:
        return "Нет активных ордеров"
    try:
        await asyncio.to_thread(clob_client.cancel_orders, stale)
    except Exception as e:
        return f"Cancel err: {e}"
    maker_buy.clear_active_orders()
    return f"🔁 Отменено {len(stale)} ордеров"


def _register_tg_commands():
    if tg is None:
        return
    for name, h in (
        ("help", _cmd_help),
        ("start", _cmd_start),
        ("stop", _cmd_stop),
        ("status", _cmd_status),
        ("stats", _cmd_stats),
        ("balance", _cmd_balance),
        ("last", _cmd_last),
        ("cancel", _cmd_cancel),
    ):
        tg.register_command(name, h)


async def _sync_maker_buy_onchain_spent(session):
    """Reconcile MakerBuy in-memory `_window_usdc_spent` with the real
    on-chain position for the CURRENT window. Protects the per-window
    spend cap from being fooled by lost WS fills.

    Logic:
      - Look up current condition_id from market_info.
      - Fetch /positions for our Safe, find position with matching
        conditionId on the side we're trading (YES/NO).
      - Use `initialValue` (USDC committed) as on-chain truth.
      - If it's larger than in-memory _window_usdc_spent, upgrade it.
        Never downgrade (fills always add, never subtract).
    """
    import os
    safe_addr = os.getenv("MM2_SAFE", "").strip().lower()
    if not safe_addr:
        return None
    cid = market_info.get("conditionId", "") or ""
    side = (maker_buy._active_side or "").upper()
    if not cid or side not in ("YES", "NO"):
        return None
    try:
        url = (
            "https://data-api.polymarket.com/positions"
            f"?user={safe_addr}&sizeThreshold=0.01&limit=100"
        )
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=6)
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
    except Exception:
        return None
    if not isinstance(data, list):
        return None
    # Find matching conditionId AND side (Polymarket "outcome": "Up"/"Down")
    want_outcome = "Up" if side == "YES" else "Down"
    for p in data:
        if not isinstance(p, dict):
            continue
        if p.get("conditionId") != cid:
            continue
        if (p.get("outcome") or "") != want_outcome:
            continue
        # initialValue = total USDC committed to this position
        try:
            on_chain_cost = float(p.get("initialValue") or 0)
        except Exception:
            continue
        if on_chain_cost <= 0:
            return 0.0
        if on_chain_cost > maker_buy._window_usdc_spent:
            diff = on_chain_cost - maker_buy._window_usdc_spent
            console.print(
                f"[yellow]MAKER_BUY cap sync: in-memory "
                f"${maker_buy._window_usdc_spent:.2f} -> on-chain "
                f"${on_chain_cost:.2f} (+${diff:.2f} recovered)[/yellow]"
            )
            maker_buy._window_usdc_spent = on_chain_cost
        return on_chain_cost
    return 0.0  # no matching position found


async def _fetch_onchain_redeemables(session, safe_addr_lc: str):
    """Return list of (condition_id, slug, size_shares) tuples for
    positions where redeemable=True on Polymarket. Grouped by
    condition_id (one entry per market even if YES+NO both redeemable).
    """
    url = (
        "https://data-api.polymarket.com/positions"
        f"?user={safe_addr_lc}&sizeThreshold=0.01&limit=500"
    )
    async with session.get(
        url, timeout=aiohttp.ClientTimeout(total=15)
    ) as r:
        if r.status != 200:
            return []
        data = await r.json()
    if not isinstance(data, list):
        return []
    # Dedup by condition_id, keep largest size
    by_cid = {}
    for p in data:
        if not isinstance(p, dict) or not p.get("redeemable"):
            continue
        cid = p.get("conditionId") or ""
        if not cid:
            continue
        sz = float(p.get("size", 0) or 0)
        slug = p.get("slug") or p.get("eventSlug") or ""
        existing = by_cid.get(cid)
        if existing is None or sz > existing[2]:
            by_cid[cid] = (cid, slug, sz)
    return list(by_cid.values())


async def polling_loop():
    global poly_token_id, poly_question, current_window_slug
    global target_price, target_source, auto_target_price, target_mode
    global raw_market_dumped, bets_this_window
    global latest_pm_bid, latest_pm_ask, latest_pm_mid, last_order_book

    poll_counter = 0

    async with aiohttp.ClientSession() as session:
        nm = await get_active_btc_market(session)
        if nm and nm.get("up_token"):
            market_info.update(nm)
            poly_token_id = nm["up_token"]
            poly_question = nm["question"]
            current_window_slug = nm["slug"]
            await hq_create_session(session)
            labdb.upsert_session(
                nm["slug"],
                question=nm.get("question"),
                event_start_time=nm.get("eventStartTime"),
                end_date=nm.get("endDate"),
                target_price=target_price,
                up_token_id=nm.get("up_token"),
                down_token_id=nm.get("down_token"),
            )

            # One-time raw dump
            raw_market_dumped = True  # disabled raw dump

        # Initial target — chainlink_loop handles auto_target_price
        # Here we just try to get a better initial value
        if not auto_target_price and current_window_slug:
            ptb = await get_price_to_beat(session, current_window_slug)
            if ptb:
                auto_target_price = ptb
                target_price = ptb
                target_source = "Polymarket API (priceToBeat)"
                console.print(
                    f"[bold magenta]Target: ${ptb:,.2f} "
                    f"(priceToBeat from API)[/bold magenta]")

        if not auto_target_price and market_info.get("eventStartTime"):
            est = market_info["eventStartTime"]
            try:
                est_dt = datetime.fromisoformat(est.replace("Z", "+00:00"))
                ws_ts = int(est_dt.timestamp())
            except Exception:
                ws_ts = int(current_window_slug.split("-")[-1])
            cl_at_start = await fetch_chainlink_at_timestamp(session, ws_ts)
            if cl_at_start:
                auto_target_price = cl_at_start
                target_price = cl_at_start
                target_source = "Chainlink (window start)"
            else:
                cl_p, _, _ = await fetch_chainlink_price(session)
                if cl_p:
                    auto_target_price = cl_p
                    target_price = cl_p
                    target_source = "Chainlink (approx)"

        while not is_done():
            now_ms = int(time.time() * 1000)
            binance_ts = timestamps["binance"]

            poll_counter += 1

            # Check target.txt EVERY iteration (file read = instant)
            file_target = check_target_file()
            if file_target:
                if file_target != target_price or target_mode != "manual":
                    target_price = file_target
                    target_source = "manual (target.txt)"
                    target_mode = "manual"
                    console.print(
                        f"[bold yellow]Manual target: "
                        f"${file_target:,.2f}[/bold yellow]")
            elif target_mode == "manual":
                target_mode = "auto"
                if auto_target_price:
                    target_price = auto_target_price
                    target_source = "Coinbase (auto)"
                    console.print(
                        f"[magenta]Auto restored: "
                        f"${auto_target_price:,.2f}[/magenta]")

            # Check control state every 10 iterations (~5s)
            # NOTE: stop signal disabled — use Ctrl+C to stop
            if poll_counter % 10 == 0:
                await hq_check_control(session)

            new_slug = current_5min_slug()
            if new_slug != current_window_slug:
                current_window_slug = new_slug
                nm = await get_active_btc_market(session)
                if nm and nm.get("up_token"):
                    market_info.update(nm)
                    poly_token_id = nm["up_token"]
                    poly_question = nm["question"]
                    console.print(
                        f"[bold cyan]── New window: "
                        f"{nm['question'][:55]} ──[/bold cyan]")
                    bets_this_window = {}
                    reset_tick_cache()
                    # Reset inventory for new window
                    inventory["yes_tokens"] = 0.0
                    inventory["no_tokens"] = 0.0
                    inventory["net_exposure"] = 0.0
                    # Reset PM prices (old token data is stale)
                    latest_pm_bid = None
                    latest_pm_ask = None
                    latest_pm_mid = None
                    last_order_book = {"bids": [], "asks": [], "spread": 0}
                    _seen_trade_ids.clear()
                    if mm:
                        # Save completed window stats to DB
                        if mm.window_stats.total_fills > 0:
                            labdb.save_mm_window(mm.window_stats.to_dict())
                        mm.on_window_change(new_window_id=nm.get("slug", ""))
                        # Cleanup previous window in background (merge + redeem)
                        if mm.cfg.split_enabled:
                            asyncio.create_task(mm.cleanup_prev_window(session))
                    # Maker-buy window reset
                    maker_buy.on_window_change(nm.get("slug", ""))
                    await hq_create_session(session)
                    labdb.upsert_session(
                        nm["slug"],
                        question=nm.get("question"),
                        event_start_time=nm.get("eventStartTime"),
                        end_date=nm.get("endDate"),
                        target_price=target_price,
                        up_token_id=nm.get("up_token"),
                        down_token_id=nm.get("down_token"),
                    )

            compute_cex_median()

            if binance_ts:
                for ex in EXCHANGES:
                    if ex != "binance" and timestamps[ex]:
                        lags[ex].append(timestamps[ex] - binance_ts)

            poly_price, poly_ts = await fetch_polymarket_price(session)
            if poly_price:
                polymarket["price"] = poly_price
                polymarket["ts"] = poly_ts
                if binance_ts:
                    lags_poly.append(poly_ts - binance_ts)

            if prices["binance"]:
                row = {"time": datetime.now().strftime("%H:%M:%S.%f")[:-3]}
                for ex in EXCHANGES:
                    row[f"{ex}_price"] = prices[ex]
                    row[f"{ex}_ts"] = timestamps[ex]
                row["cex_median"] = cex_median
                row["chainlink_price"] = chainlink["price"]
                row["poly_midpoint"] = polymarket["price"]
                row["target_price"] = target_price
                # Spreads
                if chainlink["price"] and prices["binance"]:
                    row["spread_bn_cl"] = round(
                        prices["binance"] - chainlink["price"], 2)
                if chainlink["price"] and cex_median:
                    row["spread_med_cl"] = round(
                        cex_median - chainlink["price"], 2)
                for ex in EXCHANGES:
                    if ex != "binance":
                        row[f"lag_{ex}_ms"] = (lags[ex][-1]
                                               if lags[ex] else None)
                row["lag_cl_ms"] = lags_cl[-1] if lags_cl else None
                row["lag_poly_ms"] = lags_poly[-1] if lags_poly else None
                data_rows.append(row)

            # Send tick to HQ every 4 iterations (~2s)
            if poll_counter % 4 == 0:
                asyncio.create_task(hq_send_tick(session))

            # Save tick to local DB every 20 iterations (~10s)
            if poll_counter % 20 == 0 and market_info.get("slug"):
                # Use best available PM price
                pm_p = polymarket.get("price")
                if not pm_p or pm_p == 0.5:
                    # Fallback to bestAsk from market_info
                    try:
                        pm_p = float(market_info.get("bestAsk") or 0.5)
                    except (ValueError, TypeError):
                        pm_p = 0.5
                labdb.save_tick(market_info["slug"], {
                    "ts": now_ms,
                    "binance": prices.get("binance"),
                    "coinbase": prices.get("coinbase"),
                    "okx": prices.get("okx"),
                    "bybit": prices.get("bybit"),
                    "cexMedian": cex_median,
                    "chainlink": chainlink.get("price"),
                    "pmUpPrice": pm_p,
                    "pmBid": market_info.get("bestBid"),
                    "pmAsk": market_info.get("bestAsk"),
                    "secondsLeft": get_timer(market_info.get("endDate")) or 0,
                })

            # Fetch orderbook every 10 iterations (~5s)
            if poll_counter % 10 == 0:
                ob = await fetch_order_book(session)
                if ob:
                    asyncio.create_task(hq_send_orderbook(session, ob))

            # Write MM status to DB every 10 iterations (~5s).
            # Config is read from .env only at startup (no DB override).
            if mm and poll_counter % 10 == 0:
                labdb.save_kv("mm_status", mm.get_status())
                auto_ctrl = labdb.get_kv("auto_bets")
                if auto_ctrl and isinstance(auto_ctrl, dict):
                    global AUTO_BETS_ENABLED
                    AUTO_BETS_ENABLED = auto_ctrl.get("enabled", True)

            # Market Maker cycle (every ~2s)
            if mm and mm.cfg.enabled and not _is_paused("mm") and poll_counter % 4 == 0:
                # Build book from WS live data (more accurate than CLOB /book)
                mm_book = last_order_book if isinstance(last_order_book, dict) else {}
                # If WS has live bid/ask, use those as primary
                ws_bid = latest_pm_bid or market_info.get("bestBid")
                ws_ask = latest_pm_ask or market_info.get("bestAsk")
                if ws_bid and ws_ask:
                    try:
                        b = float(ws_bid)
                        a = float(ws_ask)
                        if 0.01 < b < 0.99 and 0.01 < a < 0.99:
                            mm_book = {
                                "bids": [[b, 100]],
                                "asks": [[a, 100]],
                                "spread": round(a - b, 4),
                            }
                    except (ValueError, TypeError):
                        pass
                await mm.run_cycle(
                    market_info=market_info,
                    book=mm_book,
                    coinbase_price=prices.get("coinbase", 0),
                    inventory=inventory,
                    target_price=target_price or 0,
                    seconds_remaining=get_timer(market_info.get("endDate")) or 0,
                    aio_session=session,
                )

            # ── MAKER BUY: on-chain cap sync (every ~10 sec) ────
            # Correct _window_usdc_spent against real on-chain position
            # so the per-window USDC cap can't be fooled by lost WS fills.
            # poll_counter ticks every 0.5s; % 20 -> every ~10 seconds.
            if (maker_buy_cfg.enabled
                    and maker_buy_cfg.max_usdc_per_window > 0
                    and poll_counter % 20 == 0
                    and maker_buy._active_side):
                try:
                    await _sync_maker_buy_onchain_spent(session)
                except Exception as e:
                    console.print(
                        f"[dim yellow]MakerBuy cap sync err: {e}[/dim yellow]"
                    )

            # ── MAKER BUY STRATEGY (independent of MM) ──────────
            # poll_counter ticks every 0.5s; % 2 -> runs every ~1 second.
            if (maker_buy_cfg.enabled and clob_client
                    and not _is_paused("mb") and poll_counter % 2 == 0):
                mb_up_token = market_info.get("up_token", "")
                mb_down_token = market_info.get("down_token", "")
                if mb_up_token:
                    # Fetch BOTH order books directly from CLOB so we see
                    # the real current state including empty-book scenarios.
                    # Polymarket CLOB returns asks in DESCENDING price order
                    # and bids in ASCENDING price order, so `levels[0]` is
                    # the WORST price on both sides — we must pick min ask
                    # / max bid explicitly to be robust.
                    def _top(raw, side):
                        """Return best price on a side ('asks' or 'bids')
                        or 0 if empty/missing. Robust to sort order."""
                        if not raw:
                            return 0.0
                        levels = getattr(raw, side, None) or (
                            raw.get(side, []) if isinstance(raw, dict) else [])
                        if not levels:
                            return 0.0
                        prices = []
                        for lv in levels:
                            try:
                                p = float(
                                    lv.price if hasattr(lv, "price") else lv[0]
                                )
                                if p > 0:
                                    prices.append(p)
                            except Exception:
                                continue
                        if not prices:
                            return 0.0
                        # best ask = lowest price, best bid = highest price
                        return min(prices) if side == "asks" else max(prices)

                    yes_ask = 0.0
                    yes_bid = 0.0
                    try:
                        raw_yes = await asyncio.to_thread(
                            clob_client.get_order_book, mb_up_token)
                        yes_ask = _top(raw_yes, "asks")
                        yes_bid = _top(raw_yes, "bids")
                    except Exception:
                        # Fallback to WS-cached on network error
                        try:
                            yes_ask = float(
                                market_info.get("bestAsk") or 0
                            ) or (latest_pm_ask or 0)
                        except (ValueError, TypeError):
                            yes_ask = 0.0
                        try:
                            yes_bid = float(
                                market_info.get("bestBid") or 0
                            ) or (latest_pm_bid or 0)
                        except (ValueError, TypeError):
                            yes_bid = 0.0

                    no_ask = 0.0
                    no_bid = 0.0
                    if mb_down_token:
                        try:
                            raw_no = await asyncio.to_thread(
                                clob_client.get_order_book, mb_down_token)
                            no_ask = _top(raw_no, "asks")
                            no_bid = _top(raw_no, "bids")
                        except Exception:
                            pass
                    mb_secs = get_timer(market_info.get("endDate")) or 0

                    # Defensive: drop phantom order IDs whose TTL has
                    # already passed. Protects should_enter from being
                    # blocked by stale entries if a CANCELLATION WS
                    # event was lost.
                    try:
                        pruned = maker_buy.prune_expired_orders()
                        if pruned:
                            console.print(
                                f"[yellow]MAKER_BUY: pruned {pruned} "
                                f"phantom order(s)[/yellow]"
                            )
                    except Exception:
                        pass

                    async def _mb_do_refresh(side, target_ask, label):
                        """Cancel active orders then re-ladder. target_ask
                        forwarded to build_and_place (0 for empty-ask,
                        real ask for chase)."""
                        stale_ids = list(maker_buy._active_order_ids)
                        try:
                            await asyncio.to_thread(
                                clob_client.cancel_orders, stale_ids)
                        except Exception as e:
                            console.print(
                                f"[yellow]MakerBuy cancel stale: {e}[/yellow]")
                        maker_buy.clear_active_orders()
                        tok = mb_up_token if side == "YES" else mb_down_token
                        try:
                            tk = float(
                                await asyncio.to_thread(
                                    clob_client.get_tick_size, tok)
                            )
                        except Exception:
                            tk = 0.01
                        try:
                            placed = await asyncio.to_thread(
                                maker_buy.build_and_place,
                                tok, side, target_ask, tk, clob_client,
                            )
                            if placed:
                                console.print(
                                    f"[cyan]MAKER_BUY {label}: {placed} "
                                    f"{side} orders[/cyan]"
                                )
                        except Exception as e:
                            console.print(
                                f"[red]MakerBuy {label} err: {e}[/red]")

                    # 1) Empty-ask refresh: our side ask book empty AND
                    #    opposite side looks like certain loser.
                    refresh, r_reason, r_side = maker_buy.wants_top_refresh(
                        yes_ask, no_ask, mb_secs)
                    if refresh:
                        console.print(f"[cyan]MAKER_BUY {r_reason}[/cyan]")
                        await _mb_do_refresh(r_side, 0.0, "refresh @0.99")
                    else:
                        # 2) Chase refresh: someone outbid us, re-ladder
                        #    to the new top of book.
                        try:
                            chase_tick = float(
                                await asyncio.to_thread(
                                    clob_client.get_tick_size,
                                    mb_up_token if maker_buy._active_side == "YES"
                                    else (mb_down_token or mb_up_token),
                                )
                            )
                        except Exception:
                            chase_tick = 0.01
                        chase, c_reason, c_side = maker_buy.wants_chase_refresh(
                            yes_bid, yes_ask, no_bid, no_ask,
                            mb_secs, chase_tick,
                        )
                        if chase:
                            console.print(f"[cyan]MAKER_BUY {c_reason}[/cyan]")
                            target_ask = yes_ask if c_side == "YES" else no_ask
                            await _mb_do_refresh(
                                c_side, target_ask, "chase"
                            )

                    should, reason, side = maker_buy.should_enter(
                        yes_ask, no_ask, mb_secs)
                    if should:
                        console.print(
                            f"[cyan]MAKER_BUY signal: {reason} side={side}[/cyan]"
                        )
                        target_token = (mb_up_token if side == "YES"
                                        else mb_down_token)
                        target_ask = yes_ask if side == "YES" else no_ask
                        # Fetch tick_size for target token
                        try:
                            mb_tick = float(
                                await asyncio.to_thread(
                                    clob_client.get_tick_size, target_token)
                            )
                        except Exception:
                            mb_tick = 0.01
                        try:
                            placed = await asyncio.to_thread(
                                maker_buy.build_and_place,
                                target_token, side, target_ask, mb_tick, clob_client,
                            )
                            if placed:
                                console.print(
                                    f"[cyan]MAKER_BUY: {placed} {side} orders placed[/cyan]"
                                )
                        except Exception as e:
                            console.print(f"[red]MakerBuy err: {e}[/red]")

            # Auto bet — check ALL enabled strategies
            if not AUTO_BETS_ENABLED:
                await asyncio.sleep(0.5)
                continue
            timer = get_timer(market_info.get("endDate"))
            # Use Coinbase (closest to Chainlink) for fair value
            ref = prices.get("coinbase") or chainlink.get("price") or cex_median

            # Get fresh PM price from CLOB (same source as frontend)
            if poly_token_id and poll_counter % 2 == 0:
                fresh_pm, _ = await fetch_polymarket_price(session)
                if fresh_pm and fresh_pm > 0:
                    polymarket["price"] = fresh_pm

            pm_price = polymarket.get("price") or 0.5
            if pm_price == 0.5:
                try:
                    pm_price = float(market_info.get("bestAsk") or 0.5)
                except (ValueError, TypeError):
                    pm_price = 0.5

            if (target_price and ref and timer is not None
                    and timer >= 10 and pm_price > 0.02 and pm_price < 0.98):
                fv = calculate_fair_probability(ref, target_price, timer)
                fee_up = pm_fee(pm_price)
                fee_down = pm_fee(1 - pm_price)
                edge_up = fv["fair_up"] - pm_price - fee_up
                edge_down = fv["fair_down"] - (1 - pm_price) - fee_down

                for strat in strategies:
                    if not strat.get("enabled"):
                        continue
                    sid = strat["id"]
                    min_e = strat.get("minEdge", 7) / 100.0
                    t_min = strat.get("timerMin", 0)
                    t_max = strat.get("timerMax", 300)
                    max_b = strat.get("maxBetsPerWindow", 5)
                    p_min = strat.get("priceMin", 0.01)
                    p_max = strat.get("priceMax", 0.99)

                    if not (t_min <= timer <= t_max):
                        continue
                    if bets_this_window.get(sid, 0) >= max_b:
                        continue
                    # Cooldown check
                    cd = strat.get("cooldown", 30)
                    last_bet_time = bet_cooldowns.get(sid, 0)
                    if time.time() - last_bet_time < cd:
                        continue

                    is_mirror = strat.get("mirror", False)
                    fair_min = strat.get("fairMin", 0)

                    # "Sure Thing" mode: bet when fair >= fairMin
                    # regardless of edge (buys at any market price)
                    up_signal = False
                    down_signal = False
                    if fair_min > 0:
                        if fv["fair_up"] >= fair_min and p_min <= pm_price <= p_max:
                            up_signal = True
                        elif fv["fair_down"] >= fair_min and p_min <= (1-pm_price) <= p_max:
                            down_signal = True
                    else:
                        if edge_up > min_e and p_min <= pm_price <= p_max:
                            up_signal = True
                        elif edge_down > min_e and p_min <= (1-pm_price) <= p_max:
                            down_signal = True

                    if up_signal or down_signal:
                        if up_signal:
                            if is_mirror:
                                bet_side, bet_price = "DOWN", 1 - pm_price
                            else:
                                bet_side, bet_price = "UP", pm_price
                        else:
                            if is_mirror:
                                bet_side, bet_price = "UP", pm_price
                            else:
                                bet_side, bet_price = "DOWN", 1 - pm_price

                        bets_this_window[sid] = bets_this_window.get(sid, 0) + 1
                        bet_cooldowns[sid] = time.time()
                        bet_amt = float(strat.get("betAmount") or
                                        strat.get("betAmountUSDC") or
                                        BET_AMOUNT_USDC)

                        # Signal edge (at trigger time)
                        signal_edge = edge_up if up_signal else edge_down
                        signal_fair = fv["fair_up"] if up_signal else fv["fair_down"]

                        # Determine token ID
                        if bet_side == "UP":
                            token_id = market_info.get("up_token", "")
                        else:
                            token_id = market_info.get("down_token", "")

                        # Place real order or simulate paper
                        if REAL_BETTING_ENABLED and clob_client and token_id:
                            bet_result = await place_real_bet_clob(
                                bet_side, bet_amt, token_id,
                                bet_price, session)
                            bet_result["autoPlaced"] = True
                            bet_result["signalType"] = "edge-threshold"
                            bet_result["signalEdge"] = signal_edge
                            bet_result["signalFair"] = signal_fair
                            bet_mode = "real"
                        else:
                            # bet_amt = shares count (not USDC!)
                            paper_shares = bet_amt
                            paper_usdc = round(paper_shares * bet_price, 2)
                            bet_result = {
                                "success": True,
                                "intendedPrice": bet_price,
                                "executedPrice": bet_price,
                                "slippage": 0,
                                "sharesReceived": paper_shares,
                                "usdcSpent": paper_usdc,
                                "autoPlaced": True,
                                "signalType": "edge-threshold",
                                "signalEdge": signal_edge,
                                "signalFair": signal_fair,
                            }
                            bet_mode = "paper"

                        if bet_result.get("success"):
                            asyncio.create_task(record_bet_to_api(
                                bet_mode, bet_side, bet_amt, sid,
                                bet_result, session))

            await asyncio.sleep(0.5)


# --- Display ---


def lag_color(lag_ms):
    if lag_ms is None:
        return "-"
    if abs(lag_ms) < 100:
        return f"[green]+{lag_ms}ms[/green]"
    elif abs(lag_ms) > 500:
        return f"[red]+{lag_ms}ms[/red]"
    return f"[white]+{lag_ms}ms[/white]"


def make_market_panel():
    mi = market_info
    if not mi:
        return Panel("[dim]Loading...[/dim]", border_style="dim")

    mode_tag = ("[bold red]REAL[/bold red]" if REAL_BETTING_ENABLED
                else "[bold blue]PAPER[/bold blue]")

    timer = get_timer(mi.get("endDate"))
    pct = max(0, min(100, int(100 * (1 - (timer or 0) / 300))))
    filled = int(20 * pct / 100)
    bar = "#" * filled + "-" * (20 - filled)
    t_str = f"{(timer or 0) // 60:02d}:{(timer or 0) % 60:02d}"
    t_col = ("red" if (timer or 300) < 60
             else "yellow" if (timer or 300) < 120 else "green")

    text = Text()
    text.append(f"  >> {mi.get('question', '')[:65]}\n", style="bold white")
    text.append("  Timer: ", style="white")
    text.append(f"{t_str} ", style=f"bold {t_col}")
    text.append(f"[{bar}] {pct}%\n", style=t_col)

    # Target
    if target_price:
        text.append("  Target: ", style="dim")
        text.append(f"${target_price:,.2f}", style="bold cyan")
        if target_source:
            text.append(f"  ({target_source})", style="dim")
        if MANUAL_TARGET:
            text.append("  [manual]", style="yellow")
        text.append("\n")

        cl_now = chainlink["price"]
        ref = cl_now or cex_median
        ref_name = "Chainlink" if cl_now else "CEX Median"
        if ref:
            mv = ref - target_price
            mv_pct = (mv / target_price) * 100
            arrow = "^" if mv > 0 else "v"
            mc = "green" if mv > 0 else "red"
            text.append(f"  {ref_name}: ", style="dim")
            text.append(f"${ref:,.2f}  ", style="bold")
            text.append(f"{arrow} {mv:+.2f} ({mv_pct:+.3f}%)\n",
                        style=f"bold {mc}")
            text.append("  Direction: ", style="dim")
            text.append(f"{'UP ^' if mv >= 0 else 'DOWN v'}\n",
                        style=f"bold {mc}")
    else:
        text.append("  Target: waiting...\n", style="yellow")

    # Spreads
    if spread_history:
        last = spread_history[-1]
        text.append("  Spread Bn-CL: ", style="dim")
        bn_cl = last.get("bn_cl", 0)
        text.append(f"${bn_cl:+.2f}", style="bold")
        med_cl = last.get("med_cl")
        if med_cl is not None:
            text.append(f"  Med-CL: ${med_cl:+.2f}", style="bold")
        # Avg spread
        avg_bn_cl = sum(s["bn_cl"] for s in spread_history) / len(spread_history)
        text.append(f"  (avg: ${avg_bn_cl:+.2f})\n", style="dim")

    # Market prices
    try:
        op = json.loads(mi.get("outcomePrices", "[0.5,0.5]"))
        up_p = float(op[0])
        down_p = float(op[1]) if len(op) > 1 else 1 - up_p
    except Exception:
        up_p = down_p = 0.5
    if polymarket["price"]:
        up_p = polymarket["price"]
        down_p = 1 - up_p

    fee_up = pm_fee(up_p)
    fee_down = pm_fee(down_p)
    acc = "OPEN" if mi.get("acceptingOrders") else "CLOSED"

    # Fair value calculation
    ref = prices.get("coinbase") or chainlink.get("price") or cex_median
    fv = None
    if target_price and ref and timer and timer > 0:
        fv = calculate_fair_probability(ref, target_price, timer)

    # --- UP vs DOWN comparison table ---
    text.append(f"\n  {'':14s} {'UP':>10s}    {'DOWN':>10s}\n", style="bold")
    text.append(f"  {'Market price':14s}", style="dim")
    text.append(f" {up_p:>9.0%} ", style="bold green" if up_p > 0.5 else "bold")
    text.append(f"    {down_p:>9.0%}\n", style="bold red" if down_p > 0.5 else "bold")

    if fv:
        text.append(f"  {'Fair value':14s}", style="dim")
        text.append(f" {fv['fair_up']:>9.1%} ", style="bold cyan")
        text.append(f"    {fv['fair_down']:>9.1%}\n", style="bold cyan")

        edge_up = fv["fair_up"] - up_p
        edge_down = fv["fair_down"] - down_p
        ec_up = "green" if edge_up > 0.03 else "red" if edge_up < -0.03 else "yellow"
        ec_down = "green" if edge_down > 0.03 else "red" if edge_down < -0.03 else "yellow"
        text.append(f"  {'Edge':14s}", style="dim")
        text.append(f" {edge_up:>+9.1%} ", style=f"bold {ec_up}")
        text.append(f"    {edge_down:>+9.1%}\n", style=f"bold {ec_down}")

        edge_up_af = edge_up - fee_up
        edge_down_af = edge_down - fee_down
        ec_up_af = "green" if edge_up_af > 0.03 else "red" if edge_up_af < -0.03 else "yellow"
        ec_down_af = "green" if edge_down_af > 0.03 else "red" if edge_down_af < -0.03 else "yellow"
        text.append(f"  {'After fee':14s}", style="dim")
        text.append(f" {edge_up_af:>+9.1%} ", style=f"bold {ec_up_af}")
        text.append(f"    {edge_down_af:>+9.1%}\n", style=f"bold {ec_down_af}")

    text.append(f"  {'Fee':14s}", style="dim")
    text.append(f" ${fee_up:>8.4f} ", style="dim")
    text.append(f"    ${fee_down:>8.4f}\n", style="dim")

    bid = mi.get('bestBid', '?')
    ask = mi.get('bestAsk', '?')
    text.append(f"  {'Bid/Ask':14s}", style="dim")
    text.append(f" {str(bid):>9s} ", style="dim")
    text.append(f"    {str(ask):>9s}", style="dim")
    text.append(f"  |  {acc}\n", style="dim")

    # Signal
    if fv:
        edge_up_af = fv["fair_up"] - up_p - fee_up
        edge_down_af = fv["fair_down"] - down_p - fee_down
        if edge_up_af > 0.04:
            text.append(f"  >> BET UP!  (edge {edge_up_af:+.1%} after fee)",
                        style="bold green")
        elif edge_down_af > 0.04:
            text.append(f"  >> BET DOWN!  (edge {edge_down_af:+.1%} after fee)",
                        style="bold green")
        elif edge_up_af > 0:
            text.append(f"  ~ slight UP edge ({edge_up_af:+.1%})", style="yellow")
        elif edge_down_af > 0:
            text.append(f"  ~ slight DOWN edge ({edge_down_af:+.1%})", style="yellow")
        else:
            text.append(f"  No edge", style="dim")

    return Panel(text,
                 border_style="red" if REAL_BETTING_ENABLED else "yellow",
                 title=f"[yellow]Polymarket 5min BTC[/yellow]  {mode_tag}  "
                       f"[dim]${BET_AMOUNT_USDC}[/dim]")


def make_table():
    elapsed = int(time.time() - start_time) if start_time else 0
    remaining = max(0, DURATION - elapsed)

    table = Table(
        title=f"BTC Price Lag Monitor  |  {remaining}s remaining",
        border_style="cyan",
    )
    table.add_column("Source", style="bold white", width=14)
    table.add_column("BTC Price", justify="right", style="yellow", width=14)
    table.add_column("Timestamp", justify="right", width=16)
    table.add_column("Lag vs Binance", justify="right", width=40)

    for ex in EXCHANGES:
        p = prices[ex]
        ts = timestamps[ex]
        ts_s = (datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S.%f")[:-3]
                if ts else "-")
        ps = f"${p:,.2f}" if p else "-"
        if ex == "binance":
            table.add_row(ex.title(), ps, ts_s,
                          "[green]<- benchmark[/green]")
        else:
            table.add_row(ex.title(), ps, ts_s,
                          lag_color(lags[ex][-1] if lags[ex] else None))

    table.add_section()
    med_s = f"${cex_median:,.2f}" if cex_median else "-"
    table.add_row("[magenta]CEX Median[/magenta]",
                  f"[magenta]{med_s}[/magenta]", "",
                  "[magenta]~Chainlink approx[/magenta]")

    table.add_section()
    cl_p = chainlink["price"]
    cl_ts = chainlink["ts"]
    cl_ts_s = (datetime.fromtimestamp(cl_ts / 1000).strftime("%H:%M:%S.%f")[:-3]
               if cl_ts else "-")
    cl_age = lags_cl[-1] if lags_cl else None
    cl_age_s = f"[white]age {cl_age}ms[/white]" if cl_age is not None else "-"
    table.add_row("[bold]Chainlink[/bold]",
                  f"[bold]${cl_p:,.2f}[/bold]" if cl_p else "-",
                  cl_ts_s, cl_age_s)

    poly_p = polymarket["price"]
    poly_lag = lags_poly[-1] if lags_poly else None
    table.add_row("PM (UP)" if poly_token_id else "Polymarket",
                  f"{poly_p:.4f}" if poly_p else "-",
                  "-", lag_color(poly_lag))

    # Spreads
    table.add_section()
    bn_p = prices.get("binance")
    if bn_p and cl_p:
        bn_cl = bn_p - cl_p
        sc = "green" if bn_cl > 0 else "red"
        table.add_row("[dim]Bn - CL[/dim]", "",
                      f"[{sc}]${bn_cl:+.2f}[/{sc}]",
                      f"[dim]avg ${sum(s['bn_cl'] for s in spread_history) / max(len(spread_history), 1):+.2f}[/dim]"
                      if spread_history else "")
    if cex_median and cl_p:
        med_cl = cex_median - cl_p
        sc2 = "green" if med_cl > 0 else "red"
        table.add_row("[dim]Med - CL[/dim]", "",
                      f"[{sc2}]${med_cl:+.2f}[/{sc2}]",
                      f"[dim]avg ${sum(s['med_cl'] for s in spread_history if s.get('med_cl') is not None) / max(sum(1 for s in spread_history if s.get('med_cl') is not None), 1):+.2f}[/dim]"
                      if spread_history else "")
    # Coinbase-Chainlink median
    cb_p = prices.get("coinbase")
    if cb_p and cl_p:
        cb_cl_med = (cb_p + cl_p) / 2
        diff = cb_p - cl_p
        sc3 = "green" if diff > 0 else "red"
        table.add_row("[cyan]CB+CL mid[/cyan]",
                      f"[cyan]${cb_cl_med:,.2f}[/cyan]",
                      f"[{sc3}]${diff:+.2f}[/{sc3}]",
                      "[dim]Coinbase-Chainlink avg[/dim]")

    table.add_section()
    avgs = []
    for ex in EXCHANGES:
        if ex != "binance":
            a = int(sum(lags[ex]) / len(lags[ex])) if lags[ex] else 0
            avgs.append(f"{ex[:3]}:{a}ms")
    a_cl = int(sum(lags_cl) / len(lags_cl)) if lags_cl else 0
    a_pm = int(sum(lags_poly) / len(lags_poly)) if lags_poly else 0
    avgs.extend([f"CL:{a_cl}ms", f"PM:{a_pm}ms"])
    table.add_row("[dim]Avg[/dim]", "", "", " ".join(avgs))

    mv = ""
    for m in list(moves)[-5:]:
        mv += f"{'^ ' if m['dir'] == 'UP' else 'v '}${m['delta']:.0f} "
    # WS + Heartbeat + Inventory status
    table.add_section()
    ws_s = "[green]OK[/green]" if pm_ws_connected else "[red]OFF[/red]"
    usr_s = "[green]OK[/green]" if user_ws_connected else "[dim]off[/dim]"
    hb_age = int(time.time() - last_heartbeat_ts) if last_heartbeat_ts else -1
    hb_s = f"[green]{hb_age}s[/green]" if 0 <= hb_age < 10 else "[red]stale[/red]"
    table.add_row("[dim]PM WS[/dim]", ws_s,
                  f"User: {usr_s}", f"HB: {hb_s}")

    # Global inventory (all fills — auto-bets + MM)
    inv = inventory["net_exposure"]
    inv_c = "green" if abs(inv) < 50 else "yellow" if abs(inv) < 200 else "red"
    table.add_row("[dim]Inventory[/dim]", "",
                  f"[{inv_c}]Global: Y{inventory['yes_tokens']:.0f} "
                  f"N{inventory['no_tokens']:.0f} "
                  f"Net:{inv:+.0f}[/{inv_c}]", "")
    # MM position (from window_stats)
    if mm and mm.cfg.enabled:
        mw = mm.window_stats
        mm_net = mw.buys_shares - mw.sells_shares
        mm_ic = "green" if abs(mm_net) < 15 else "yellow" if abs(mm_net) < 30 else "red"
        pos = f"Long +{mm_net:.0f}" if mm_net > 0 else f"Short {mm_net:.0f}" if mm_net < 0 else "Balanced"
        table.add_row("", "",
                      f"[{mm_ic}]MM: {pos} "
                      f"(B{mw.buys_shares:.0f}/S{mw.sells_shares:.0f})[/{mm_ic}]", "")

    # Straddle check
    if latest_pm_ask and latest_pm_bid:
        ask_no = 1 - latest_pm_bid  # cost of NO
        straddle = check_straddle(latest_pm_ask, ask_no)
        if straddle:
            table.add_row(
                "[bold yellow]STRADDLE[/bold yellow]", "",
                f"[yellow]cost={straddle['cost']:.3f} "
                f"profit={straddle['profit']:.4f}[/yellow]", "")

    # Market Maker status
    if mm and mm.cfg.enabled:
        ms = mm.get_status()
        mm_c = "green" if ms["running"] else "yellow"
        w = ms.get("window", {})
        t = ms.get("total", {})
        pause_str = f" | PAUSE {ms['pause_reason'][:20]}" if ms["pause_reason"] else ""
        line1 = (f"{'RUN' if ms['running'] else 'IDLE'} {ms['strategy']}{pause_str}"
                 f" | {ms['active_orders']}ord")
        line2 = (f"  {w.get('buys',0)}B/{w.get('sells',0)}S "
                 f"| vol=${w.get('volume_usdc',0):.2f} "
                 f"| pos={w.get('net_position_shares',0):+.0f}sh "
                 f"| ~reb=${w.get('rebates_est',0):.5f}")
        line3 = (f"  Total: {t.get('windows_count',0)}win "
                 f"| {t.get('buys',0)}B/{t.get('sells',0)}S "
                 f"| vol=${t.get('volume_usdc',0):.2f} "
                 f"| ~reb=${t.get('rebates_est',0):.5f}")
        table.add_row(f"[{mm_c}]MM[/{mm_c}]", "", f"[{mm_c}]{line1}[/{mm_c}]", "")
        table.add_row("", "", f"[dim]{line2}[/dim]", "")
        table.add_row("", "", f"[dim]{line3}[/dim]", "")

    return table, mv


async def display_loop():
    with Live(console=console, refresh_per_second=2) as live:
        while not is_done():
            mp = make_market_panel()
            table, mv = make_table()
            lp = Panel(table, subtitle=f"Moves: {mv}", border_style="blue")
            live.update(Group(mp, lp))
            await asyncio.sleep(0.5)


# --- Output ---


def save_csv():
    if not data_rows:
        return
    # Collect ALL keys across all rows (some rows have extra fields)
    all_keys = dict.fromkeys(k for row in data_rows for k in row)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data_rows)
    console.print(f"[green]CSV: {OUTPUT_CSV} ({len(data_rows)} rows)[/green]")


def save_html_report():
    if not data_rows:
        return

    times = [r["time"] for r in data_rows]
    price_datasets = []
    for ex in EXCHANGES:
        vals = [r.get(f"{ex}_price") or "null" for r in data_rows]
        price_datasets.append({
            "label": ex.title(), "data": vals,
            "borderColor": EXCHANGE_COLORS[ex],
            "borderWidth": 1.5, "pointRadius": 0,
        })
    med_vals = [r.get("cex_median") or "null" for r in data_rows]
    price_datasets.append({
        "label": "CEX Median", "data": med_vals,
        "borderColor": "#a855f7", "borderWidth": 2.5, "pointRadius": 0,
    })
    cl_vals = [r.get("chainlink_price") or "null" for r in data_rows]
    price_datasets.append({
        "label": "Chainlink", "data": cl_vals,
        "borderColor": "#ef4444", "borderWidth": 2,
        "pointRadius": 0, "borderDash": [5, 3],
    })

    # Spread datasets
    bn_cl_vals = [r.get("spread_bn_cl") or "null" for r in data_rows]
    med_cl_vals = [r.get("spread_med_cl") or "null" for r in data_rows]

    lag_datasets = []
    for ex in EXCHANGES:
        if ex != "binance":
            vals = [r.get(f"lag_{ex}_ms") or "null" for r in data_rows]
            lag_datasets.append({
                "label": ex.title(), "data": vals,
                "borderColor": EXCHANGE_COLORS[ex],
                "borderWidth": 1.5, "pointRadius": 0,
            })
    lag_datasets.append({
        "label": "Chainlink age", "data": [r.get("lag_cl_ms") or "null"
                                            for r in data_rows],
        "borderColor": "#ef4444", "borderWidth": 2,
        "pointRadius": 0, "borderDash": [5, 3],
    })
    lag_datasets.append({
        "label": "Polymarket", "data": [r.get("lag_poly_ms") or "null"
                                         for r in data_rows],
        "borderColor": "#60a5fa", "borderWidth": 1.5, "pointRadius": 0,
    })

    stats = {}
    for ex in EXCHANGES:
        if ex != "binance":
            stats[ex] = (int(sum(lags[ex]) / len(lags[ex]))
                         if lags[ex] else 0)
    avg_cl = int(sum(lags_cl) / len(lags_cl)) if lags_cl else 0
    avg_poly = int(sum(lags_poly) / len(lags_poly)) if lags_poly else 0
    stats["chainlink"] = avg_cl
    stats["polymarket"] = avg_poly

    fastest = min(
        (ex for ex in EXCHANGES if ex != "binance"),
        key=lambda x: abs(stats.get(x, 99999)),
    ) if stats else "-"

    avg_spread = (sum(s["bn_cl"] for s in spread_history) / len(spread_history)
                  if spread_history else 0)

    conclusions = [
        f"Fastest vs Binance: <b>{fastest.title()}</b> ({stats.get(fastest, 0)}ms)",
    ]
    for ex in EXCHANGES:
        if ex != "binance":
            conclusions.append(f"{ex.title()}: <b>{stats.get(ex, 0)}ms</b>")
    conclusions.append(f"Chainlink data age: <b>{avg_cl}ms</b>")
    conclusions.append(f"Polymarket CLOB: <b>{avg_poly}ms</b>")
    conclusions.append(f"Avg Binance-Chainlink spread: <b>${avg_spread:+.2f}</b>")
    conclusions.append(f"PM fee rate (crypto): <b>{PM_FEE_RATE}</b> "
                       f"(max {PM_FEE_RATE*0.25:.4f} at 50%)")
    if target_price:
        conclusions.append(f"Target: <b>${target_price:,.2f}</b> ({target_source})")

    step = max(1, len(times) // 300)
    times_ds = json.dumps(times[::step])
    price_ds = [{**d, "data": d["data"][::step]} for d in price_datasets]
    lag_ds = [{**d, "data": d["data"][::step]} for d in lag_datasets]

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>BTC Price Lag Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
body{{font-family:-apple-system,sans-serif;background:#0f0f0f;color:#e0e0e0;padding:20px;max-width:1200px;margin:0 auto}}
h1{{color:#60a5fa}}h2{{color:#94a3b8;font-size:16px}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}}
.stat{{background:#1e1e2e;border-radius:8px;padding:16px;text-align:center}}
.stat .val{{font-size:28px;font-weight:600;color:#f59e0b}}
.stat .lbl{{font-size:12px;color:#64748b;margin-top:4px}}
.chart-box{{background:#1e1e2e;border-radius:8px;padding:16px;margin:12px 0}}
canvas{{max-height:300px}}
.conclusions{{background:#1e1e2e;border-radius:8px;padding:20px;margin:20px 0;border-left:3px solid #60a5fa}}
.conclusions li{{margin:6px 0;line-height:1.6}}
</style></head><body>
<h1>BTC Price Lag + Spread Report</h1>
<p style="color:#64748b">{datetime.now().strftime('%Y-%m-%d %H:%M')} |
{DURATION}s | {len(data_rows)} pts | Fee: {PM_FEE_RATE}</p>
<div class="stats">
<div class="stat"><div class="val">{stats.get(fastest,0)}ms</div><div class="lbl">Fastest ({fastest.title()})</div></div>
<div class="stat"><div class="val">{avg_cl}ms</div><div class="lbl">Chainlink age</div></div>
<div class="stat"><div class="val">${avg_spread:+.1f}</div><div class="lbl">Avg Bn-CL spread</div></div>
<div class="stat"><div class="val">{len(moves)}</div><div class="lbl">BTC moves</div></div>
</div>
<div class="chart-box"><h2>Prices</h2><canvas id="c1"></canvas></div>
<div class="chart-box"><h2>Spread: Binance-Chainlink / Median-Chainlink ($)</h2><canvas id="c2"></canvas></div>
<div class="chart-box"><h2>Lag (ms)</h2><canvas id="c3"></canvas></div>
<div class="conclusions"><h2>Results</h2>
<ul>{"".join(f"<li>{c}</li>" for c in conclusions)}</ul></div>
<script>
const T={times_ds};
new Chart(document.getElementById('c1'),{{type:'line',data:{{labels:T,datasets:{json.dumps(price_ds)}}},
options:{{animation:false,spanGaps:true,plugins:{{legend:{{labels:{{color:'#e0e0e0'}}}}}},
scales:{{x:{{ticks:{{color:'#64748b',maxTicksLimit:12}}}},y:{{ticks:{{color:'#64748b'}}}}}}}}}});
new Chart(document.getElementById('c2'),{{type:'line',data:{{labels:{json.dumps(times[::step])},datasets:[
{{label:'Binance-Chainlink',data:{json.dumps(bn_cl_vals[::step])},borderColor:'#10b981',borderWidth:1.5,pointRadius:0}},
{{label:'Median-Chainlink',data:{json.dumps(med_cl_vals[::step])},borderColor:'#a855f7',borderWidth:1.5,pointRadius:0}}
]}},options:{{animation:false,spanGaps:true,plugins:{{legend:{{labels:{{color:'#e0e0e0'}}}}}},
scales:{{x:{{ticks:{{color:'#64748b',maxTicksLimit:12}}}},y:{{ticks:{{color:'#64748b'}},title:{{display:true,text:'$',color:'#64748b'}}}}}}}}}});
new Chart(document.getElementById('c3'),{{type:'line',data:{{labels:T,datasets:{json.dumps(lag_ds)}}},
options:{{animation:false,spanGaps:true,plugins:{{legend:{{labels:{{color:'#e0e0e0'}}}}}},
scales:{{x:{{ticks:{{color:'#64748b',maxTicksLimit:12}}}},y:{{ticks:{{color:'#64748b'}},title:{{display:true,text:'ms',color:'#64748b'}}}}}}}}}});
</script></body></html>"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    console.print(f"[green]HTML: {OUTPUT_HTML}[/green]")


# --- Main ---


async def main():
    global start_time, poly_token_id, poly_question
    global target_price, target_source, current_window_slug

    labdb.init_db()

    console.print("[bold cyan]BTC Price Lag Monitor - "
                  "Multi-Exchange + Chainlink + Polymarket[/bold cyan]")
    console.print(f"Exchanges: {', '.join(ex.title() for ex in EXCHANGES)}")
    console.print(f"PM fee rate: {PM_FEE_RATE} (crypto)")

    if MANUAL_TARGET:
        console.print(f"[bold yellow]Manual target: "
                      f"${MANUAL_TARGET:,.2f}[/bold yellow]")

    console.print("Searching for BTC 5min market...")
    async with aiohttp.ClientSession() as session:
        nm = await get_active_btc_market(session)
        if nm and nm.get("up_token"):
            market_info.update(nm)
            poly_token_id = nm["up_token"]
            poly_question = nm["question"]
            current_window_slug = nm["slug"]
            console.print(f"[green]+ Polymarket: {poly_question}[/green]")
            timer = get_timer(nm.get("endDate"))
            if timer:
                console.print(f"[dim]  Ends in: {timer // 60}m {timer % 60}s[/dim]")

        if not target_price and current_window_slug:
            ptb = await get_price_to_beat(session, current_window_slug)
            if ptb:
                target_price = ptb
                target_source = "Polymarket API (priceToBeat)"
                console.print(
                    f"[bold magenta]Target: ${ptb:,.2f} "
                    f"(priceToBeat)[/bold magenta]")

        if not target_price:
            est = market_info.get("eventStartTime")
            if est:
                try:
                    est_dt = datetime.fromisoformat(
                        est.replace("Z", "+00:00"))
                    ws_ts = int(est_dt.timestamp())
                    elapsed = int(time.time()) - ws_ts
                    console.print(
                        f"[dim]  eventStartTime: {est} "
                        f"({elapsed}s ago)[/dim]")
                except Exception:
                    ws_ts = None
            else:
                ws_ts = (int(current_window_slug.split("-")[-1])
                         if current_window_slug else None)

            if ws_ts:
                cl_at_start = await fetch_chainlink_at_timestamp(
                    session, ws_ts)
                if cl_at_start:
                    target_price = cl_at_start
                    target_source = "Chainlink (eventStartTime)"
                    console.print(
                        f"[bold magenta]Target: ${cl_at_start:,.2f} "
                        f"(Chainlink at eventStart)[/bold magenta]")
                else:
                    cl_p, _, _ = await fetch_chainlink_price(session)
                    if cl_p:
                        target_price = cl_p
                        target_source = "Chainlink (approx)"
                        console.print(
                            f"[yellow]Target: ${cl_p:,.2f} "
                            f"(approx, eventStart >60s ago)[/yellow]")
        if not target_price:
            console.print("[yellow]Target: will capture at next boundary "
                          "or use --target FLAG[/yellow]")

    # Real betting setup
    if REAL_BETTING_ENABLED:
        console.print("[bold red]REAL BETTING ENABLED "
                      f"(${BET_AMOUNT_USDC} USDC)[/bold red]")
        init_clob_client()
    else:
        console.print("[blue]Paper mode[/blue]")

    # Initialize Market Maker
    global mm
    def _mm_event_cb(evt):
        try:
            labdb.save_mm_event(evt)
        except Exception:
            pass
    mm = MarketMaker(cfg=mm_config, clob_client=clob_client, on_event=_mm_event_cb)
    # Wire Telegram notification for split failures
    if tg is not None:
        def _on_split_failed(condition_id, error):
            try:
                asyncio.create_task(
                    tg.notify_split_failed(condition_id, str(error))
                )
            except Exception:
                pass
        mm.on_split_failed = _on_split_failed

    # Register Telegram command handlers (uses globals `mm`, `maker_buy` etc.)
    _register_tg_commands()

    if mm_config.enabled:
        console.print(f"[cyan]+ MM enabled: {mm_config.strategy} "
                      f"| {mm_config.ladder_levels} levels "
                      f"x {mm_config.level_size_shares} sh[/cyan]")
    else:
        console.print("[dim]MM disabled (set MM_ENABLED=true)[/dim]")

    console.print(
        f"[cyan]MakerBuy: enabled={maker_buy_cfg.enabled} "
        f"entry={maker_buy_cfg.entry_threshold} "
        f"levels={maker_buy_cfg.levels} "
        f"usdc={maker_buy_cfg.usdc_per_level}/level[/cyan]"
    )

    console.print(f"\nDuration: {DURATION}s  "
                  f"(--duration N to change)\n")
    await asyncio.sleep(1)
    start_time = time.time()
    if not current_window_slug:
        current_window_slug = current_5min_slug()

    # Telegram startup notification (async, fire-and-forget)
    if tg is not None:
        mods = []
        if mm_config.enabled: mods.append("✅ MM")
        if maker_buy_cfg.enabled: mods.append("✅ MakerBuy")
        if REAL_BETTING_ENABLED: mods.append("✅ Real betting")
        asyncio.create_task(tg.send_startup(mods))

    async def safe(name, coro):
        """Wrap coroutine to prevent one crash from killing all.
        Reports critical crashes to Telegram if enabled."""
        try:
            await coro
        except Exception as e:
            console.print(f"[bold red]{name} crashed: {e}[/bold red]")
            if tg is not None:
                try:
                    asyncio.create_task(
                        tg.notify_error(f"{name} crashed: {e}",
                                        source=name, cooldown_sec=300)
                    )
                except Exception:
                    pass

    await asyncio.gather(
        safe("Binance", binance_stream()),
        safe("Coinbase", coinbase_stream()),
        safe("OKX", okx_stream()),
        safe("Bybit", bybit_stream()),
        safe("Chainlink", chainlink_loop()),
        safe("Polling", polling_loop()),
        safe("Display", display_loop()),
        safe("Settlement", settlement_loop()),
        safe("Heartbeat", heartbeat_loop()),
        safe("RTDS", polymarket_rtds_stream()),
        safe("PM WS", polymarket_ws_stream()),
        safe("User WS", polymarket_user_ws()),
        safe("MakerBuyRedeem", maker_buy_redeem_loop()),
        safe("MakerBuyRebate", maker_buy_rebate_loop()),
        safe("MakerBuyReconcile", maker_buy_reconcile_loop()),
        safe("TelegramPoll",
             tg.poll_updates_loop() if tg is not None else asyncio.sleep(0)),
    )

    console.print("\n[bold green]Done![/bold green]")
    save_csv()
    save_html_report()

    console.print(f"\n[bold]RESULTS:[/bold]")
    for ex in EXCHANGES:
        if ex != "binance":
            a = int(sum(lags[ex]) / len(lags[ex])) if lags[ex] else 0
            c = "green" if abs(a) < 100 else "red"
            console.print(f"  {ex.title():12s} [{c}]{a}ms[/{c}]")
    a_cl = int(sum(lags_cl) / len(lags_cl)) if lags_cl else 0
    a_pm = int(sum(lags_poly) / len(lags_poly)) if lags_poly else 0
    console.print(f"  {'Chainlink':12s} [red]age {a_cl}ms[/red]")
    console.print(f"  {'Polymarket':12s} [red]{a_pm}ms[/red]")
    if spread_history:
        avg_s = sum(s["bn_cl"] for s in spread_history) / len(spread_history)
        console.print(f"  Avg Bn-CL spread: ${avg_s:+.2f}")
    if target_price:
        console.print(f"  Target: ${target_price:,.2f} ({target_source})")

    console.print(f"\n[cyan]Opening report...[/cyan]")
    import webbrowser
    webbrowser.open(f"file://{os.path.abspath(OUTPUT_HTML)}")


if __name__ == "__main__":
    asyncio.run(main())
