"""
Конфигурация бота — читается из .env файла
"""
import os
from dataclasses import dataclass, field

# ─── Загружаем .env если есть ─────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class Config:
    # ── BetBurger ─────────────────────────────────────────────────────────────
    BETBURGER_TOKEN:              str   = field(default_factory=lambda: os.getenv("BETBURGER_TOKEN", ""))
    BETBURGER_FILTER_ID:          int   = field(default_factory=lambda: int(os.getenv("BETBURGER_FILTER_ID", "0")))
    BETBURGER_FILTER_ID_VALUEBET: int   = field(default_factory=lambda: int(os.getenv("BETBURGER_FILTER_ID_VALUEBET", "0")))
    BETBURGER_EMAIL:              str   = field(default_factory=lambda: os.getenv("BETBURGER_EMAIL", ""))
    BETBURGER_PASSWORD:           str   = field(default_factory=lambda: os.getenv("BETBURGER_PASSWORD", ""))

    # ── Polymarket ────────────────────────────────────────────────────────────
    POLYMARKET_PRIVATE_KEY: str = field(default_factory=lambda: os.getenv("POLYMARKET_PRIVATE_KEY", ""))
    POLYMARKET_FUNDER:      str = field(default_factory=lambda: os.getenv("POLYMARKET_FUNDER", ""))

    # ── PS3838 (Pinnacle) — для арбитража ────────────────────────────────────
    PS3838_USERNAME: str = field(default_factory=lambda: os.getenv("PS3838_USERNAME", ""))
    PS3838_PASSWORD: str = field(default_factory=lambda: os.getenv("PS3838_PASSWORD", ""))

    # ── Настройки ВИЛОК ───────────────────────────────────────────────────────
    MIN_ROI:       float = field(default_factory=lambda: float(os.getenv("MIN_ROI", "0.03")))
    MIN_LIQUIDITY: float = field(default_factory=lambda: float(os.getenv("MIN_LIQUIDITY", "50")))
    MIN_STAKE:     float = field(default_factory=lambda: float(os.getenv("MIN_STAKE", "10")))
    STAKE_PCT:     float = field(default_factory=lambda: float(os.getenv("STAKE_PCT", "0.01")))

    # ── Настройки ВЭЛЬЮ-БЕТОВ ────────────────────────────────────────────────
    VB_MIN_ROI:         float = field(default_factory=lambda: float(os.getenv("VB_MIN_ROI", "0.04")))
    VB_MIN_LIQUIDITY:   float = field(default_factory=lambda: float(os.getenv("VB_MIN_LIQUIDITY", "50")))
    VB_MIN_STAKE:       float = field(default_factory=lambda: float(os.getenv("VB_MIN_STAKE", "2")))
    VB_STAKE_PCT:       float = field(default_factory=lambda: float(os.getenv("VB_STAKE_PCT", "0.01")))
    VB_MAX_STAKE_PCT:   float = field(default_factory=lambda: float(os.getenv("VB_MAX_STAKE_PCT", "0.05")))
    VB_USE_KELLY:       bool  = field(default_factory=lambda: os.getenv("VB_USE_KELLY", "false").lower() == "true")
    VB_MAX_ODDS:        float = field(default_factory=lambda: float(os.getenv("VB_MAX_ODDS", "0")))

    # ── Настройки ЛАЙВ-БЕТОВ ─────────────────────────────────────────────────
    BETBURGER_FILTER_ID_LIVE: int   = field(default_factory=lambda: int(os.getenv("BETBURGER_FILTER_ID_LIVE", "0")))
    LV_MIN_ROI:         float = field(default_factory=lambda: float(os.getenv("LV_MIN_ROI", "0.04")))
    LV_MIN_LIQUIDITY:   float = field(default_factory=lambda: float(os.getenv("LV_MIN_LIQUIDITY", "30")))
    LV_MIN_STAKE:       float = field(default_factory=lambda: float(os.getenv("LV_MIN_STAKE", "2")))
    LV_STAKE_PCT:       float = field(default_factory=lambda: float(os.getenv("LV_STAKE_PCT", "0.01")))
    LV_MAX_STAKE_PCT:   float = field(default_factory=lambda: float(os.getenv("LV_MAX_STAKE_PCT", "0.05")))
    LV_USE_KELLY:       bool  = field(default_factory=lambda: os.getenv("LV_USE_KELLY", "false").lower() == "true")
    LV_MAX_ODDS:        float = field(default_factory=lambda: float(os.getenv("LV_MAX_ODDS", "0")))
    LV_ORDER_TTL_SECS:  int   = field(default_factory=lambda: int(os.getenv("LV_ORDER_TTL_SECS", "30")))
    PM_ORDER_TTL_SECS:  int   = field(default_factory=lambda: int(os.getenv("PM_ORDER_TTL_SECS", "3600")))  # прематч TTL: 1 час

    # ── Авто-продажа (RESELL) — прематч ────────────────────────────────────
    VB_RESELL_ENABLED:  bool  = field(default_factory=lambda: os.getenv("VB_RESELL_ENABLED", "false").lower() == "true")
    VB_RESELL_MARKUP:   float = field(default_factory=lambda: float(os.getenv("VB_RESELL_MARKUP", "2")))      # наценка в % (аддитивная: sell = entry + markup/100)
    VB_RESELL_FALLBACK: str   = field(default_factory=lambda: os.getenv("VB_RESELL_FALLBACK", "keep"))         # keep | market_sell

    # ── Авто-продажа (RESELL) — лайв ──────────────────────────────────────
    LV_RESELL_ENABLED:  bool  = field(default_factory=lambda: os.getenv("LV_RESELL_ENABLED", "false").lower() == "true")
    LV_RESELL_MARKUP:   float = field(default_factory=lambda: float(os.getenv("LV_RESELL_MARKUP", "3")))
    LV_RESELL_FALLBACK: str   = field(default_factory=lambda: os.getenv("LV_RESELL_FALLBACK", "keep"))

    # ── Хедж-бот (дельта-нейтральный) ───────────────────────────────────────
    HEDGE_DB_PATH:        str   = field(default_factory=lambda: os.getenv("HEDGE_DB_PATH", "hedge.db"))
    HEDGE_MIN_ROI:        float = field(default_factory=lambda: float(os.getenv("HEDGE_MIN_ROI", "0.02")))
    HEDGE_MAX_BUDGET:     float = field(default_factory=lambda: float(os.getenv("HEDGE_MAX_BUDGET", "1000")))
    HEDGE_BUDGET_PCT:     float = field(default_factory=lambda: float(os.getenv("HEDGE_BUDGET_PCT", "0.05")))
    HEDGE_SCAN_INTERVAL:  int   = field(default_factory=lambda: int(os.getenv("HEDGE_SCAN_INTERVAL", "300")))
    HEDGE_PRICE_INTERVAL: int   = field(default_factory=lambda: int(os.getenv("HEDGE_PRICE_INTERVAL", "30")))
    HEDGE_AUTO_EXECUTE:   bool  = field(default_factory=lambda: os.getenv("HEDGE_AUTO_EXECUTE", "false").lower() == "true")
    HEDGE_ORDER_TTL_SECS: int   = field(default_factory=lambda: int(os.getenv("HEDGE_ORDER_TTL_SECS", "300")))
    HEDGE_SPORTS:         str   = field(default_factory=lambda: os.getenv("HEDGE_SPORTS", "tennis,nba,nhl,soccer,mlb,mma,nfl,ncaa"))
    HEDGE_CROSS_TOURNEY:  bool  = field(default_factory=lambda: os.getenv("HEDGE_CROSS_TOURNEY", "false").lower() == "true")
    HEDGE_KNOCKOUT_ONLY:  bool  = field(default_factory=lambda: os.getenv("HEDGE_KNOCKOUT_ONLY", "true").lower() == "true")
    HEDGE_MIN_TOURNEY_PRICE: float = field(default_factory=lambda: float(os.getenv("HEDGE_MIN_TOURNEY_PRICE", "0.03")))
    HEDGE_MIN_LIQUIDITY:  float = field(default_factory=lambda: float(os.getenv("HEDGE_MIN_LIQUIDITY", "100")))
    HEDGE_PRICE_CACHE_TTL: int  = field(default_factory=lambda: int(os.getenv("HEDGE_PRICE_CACHE_TTL", "60")))

    # ── Dutching (internal arb: YES+NO < 1.00) ────────────────────────────────
    DUTCH_MIN_SPREAD:      float = field(default_factory=lambda: float(os.getenv("DUTCH_MIN_SPREAD", "0.005")))
    DUTCH_MIN_LIQUIDITY:   float = field(default_factory=lambda: float(os.getenv("DUTCH_MIN_LIQUIDITY", "50")))
    DUTCH_STAKE:           float = field(default_factory=lambda: float(os.getenv("DUTCH_STAKE", "5")))
    DUTCH_MAX_STAKE:       float = field(default_factory=lambda: float(os.getenv("DUTCH_MAX_STAKE", "50")))
    DUTCH_POLL_INTERVAL:   int   = field(default_factory=lambda: int(os.getenv("DUTCH_POLL_INTERVAL", "60")))
    DUTCH_SPORTS:          str   = field(default_factory=lambda: os.getenv("DUTCH_SPORTS", "tennis,nba,nhl,soccer,mlb,mma,nfl"))
    DUTCH_ORDER_TTL_SECS:  int   = field(default_factory=lambda: int(os.getenv("DUTCH_ORDER_TTL_SECS", "120")))

    # ── Market Making ───────────────────────────────────────────────────────
    MM_LEVELS:             int   = field(default_factory=lambda: int(os.getenv("MM_LEVELS", "3")))
    MM_STEP:               int   = field(default_factory=lambda: int(os.getenv("MM_STEP", "1")))
    MM_ORDER_SIZE:         float = field(default_factory=lambda: float(os.getenv("MM_ORDER_SIZE", "20")))
    MM_POLL_INTERVAL:      int   = field(default_factory=lambda: int(os.getenv("MM_POLL_INTERVAL", "30")))
    MM_MAX_MARKETS:        int   = field(default_factory=lambda: int(os.getenv("MM_MAX_MARKETS", "5")))
    MM_REQUOTE_THRESHOLD:  int   = field(default_factory=lambda: int(os.getenv("MM_REQUOTE_THRESHOLD", "1")))
    MM_AUTO_SEARCH:        bool  = field(default_factory=lambda: os.getenv("MM_AUTO_SEARCH", "false").lower() == "true")
    MM_AUTO_SPORTS:        str   = field(default_factory=lambda: os.getenv("MM_AUTO_SPORTS", "soccer,nba,tennis,mma"))
    MM_AUTO_MIN_LIQ:       float = field(default_factory=lambda: float(os.getenv("MM_AUTO_MIN_LIQ", "500")))
    MM_SKEW_STEP:          float = field(default_factory=lambda: float(os.getenv("MM_SKEW_STEP", "50")))   # каждые N shares перекоса = +1 тик сдвига
    MM_SKEW_MAX:           int   = field(default_factory=lambda: int(os.getenv("MM_SKEW_MAX", "3")))       # макс. тиков сдвига
    MM_MAX_POSITION:       float = field(default_factory=lambda: float(os.getenv("MM_MAX_POSITION", "200")))  # макс. shares перекоса, дальше стоп
    MM_SPREAD_PANIC:       int   = field(default_factory=lambda: int(os.getenv("MM_SPREAD_PANIC", "5")))      # если спред > N тиков → cancel всё
    MM_ANCHOR:             str   = field(default_factory=lambda: os.getenv("MM_ANCHOR", "mid"))  # mid | spread | spread1
    MM_SELL_ENABLED:       bool  = field(default_factory=lambda: os.getenv("MM_SELL_ENABLED", "true").lower() in ("true", "1", "yes"))  # SELL existing shares

    # ── Общие ─────────────────────────────────────────────────────────────────
    POLL_INTERVAL: int = field(default_factory=lambda: int(os.getenv("POLL_INTERVAL", "5")))
    LV_POLL_INTERVAL: int = field(default_factory=lambda: int(os.getenv("LV_POLL_INTERVAL", "5")))

    # ── БД ────────────────────────────────────────────────────────────────────
    DB_PATH:          str = field(default_factory=lambda: os.getenv("DB_PATH", "arb_bot.db"))
    DB_PATH_VALUEBET: str = field(default_factory=lambda: os.getenv("DB_PATH_VALUEBET", "valuebets.db"))

    def validate(self) -> list[str]:
        """Проверяет наличие обязательных полей. Возвращает список ошибок."""
        errors = []
        if not self.BETBURGER_TOKEN and not (self.BETBURGER_EMAIL and self.BETBURGER_PASSWORD):
            errors.append("Нужен BETBURGER_TOKEN или (BETBURGER_EMAIL + BETBURGER_PASSWORD)")
        if not self.BETBURGER_FILTER_ID_VALUEBET and not self.BETBURGER_FILTER_ID:
            errors.append("Нужен BETBURGER_FILTER_ID_VALUEBET")
        if not self.POLYMARKET_PRIVATE_KEY:
            errors.append("Нужен POLYMARKET_PRIVATE_KEY")
        if not self.POLYMARKET_FUNDER:
            errors.append("Нужен POLYMARKET_FUNDER")
        return errors

    def print_summary(self):
        """Печатает конфиг без секретов"""
        bb_auth = f"email={self.BETBURGER_EMAIL}" \
                  if self.BETBURGER_EMAIL else f"token={self.BETBURGER_TOKEN[:8]}..."
        print(f"  BetBurger:   {bb_auth}  filter={self.BETBURGER_FILTER_ID_VALUEBET}")
        print(f"  Polymarket:  funder={self.POLYMARKET_FUNDER[:10]}...")
        print(f"  VB_MIN_ROI:  {self.VB_MIN_ROI*100:.1f}%")
        print(f"  MIN_LIQ:     ${self.MIN_LIQUIDITY:.0f}")
        print(f"  STAKE:       {self.VB_STAKE_PCT*100:.1f}% банкролла")
        print(f"  MAX_STAKE:   {self.VB_MAX_STAKE_PCT*100:.1f}% банкролла")
        print(f"  KELLY:       {'да (half-Kelly)' if self.VB_USE_KELLY else 'нет (flat)'}")
        print(f"  POLL:        {self.POLL_INTERVAL}s")
        print(f"  DB:          {self.DB_PATH_VALUEBET}")