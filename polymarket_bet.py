"""
Парсер direct_link из BetBurger для Polymarket ставок.

direct_link пример:
  marketId=1529572&eventId=241565
  &outcomeId=1143...&outcomeName=Under
  &negRisk=false&liquidityNum=9444.672
  &competitive=0.999...&takerBaseFee=0&makerBaseFee=0
  &bestOffers=2.0:1010.5,1.9607843:7388.324

bestOffers формат: price:size,price:size,...
  price  = коэффициент (напр. 2.0 = 50¢)
  size   = доступный объём в USDC

Конвертация odds → Polymarket price:
  polymarket_price = 1 / odds   (напр. odds=2.0 → price=0.50)
"""

from urllib.parse import parse_qs, unquote
from dataclasses import dataclass, field
from typing import Optional
import datetime
import logging

log = logging.getLogger(__name__)


@dataclass
class PriceLevel:
    """Один уровень стакана: коэффициент + доступный объём"""
    odds: float          # коэффициент (1/price)
    price: float         # вероятность 0..1
    size: float          # объём в USDC
    implied_pct: float   # implied probability %

    def __str__(self):
        return f"odds={self.odds:.4f} (price={self.price:.4f} / {self.implied_pct:.1f}%)  size=${self.size:.1f}"


@dataclass
class PolymarketBet:
    """
    Полная информация о Polymarket ставке из BetBurger.
    Источник данных: direct_link + основные поля BetDto.
    """
    # Идентификаторы
    bet_id: str                     # BetBurger bet ID (base64)
    market_id: str                  # Polymarket market ID
    event_id: str                   # Polymarket event ID
    outcome_id: str                 # Polymarket outcome/token ID (для CLOB)
    outcome_name: str               # Название исхода ("Under", "Yes", "Team1 Win", etc.)

    # Матч
    home: str
    away: str
    league: str
    sport_id: int
    started_at: int                 # Unix timestamp

    # Маркет
    market_type: int                # market_and_bet_type (1=Team1Win, 20=TotalUnder, ...)
    market_type_name: str           # человекочитаемое
    market_param: float             # линия (229.5 для тотала)
    neg_risk: bool                  # negRisk флаг Polymarket

    # Коэффициенты и ликвидность
    bb_odds: float                  # коэффициент от BetBurger
    bb_price: float                 # implied probability (1/odds)
    value_pct: float                # велью% = middle_value (напр. 1.99)
    arb_pct: float                  # доходность вилки arbs[].percent (напр. 2.8)
    total_liquidity: float          # рыночная ликвидность (liquidityNum)
    best_ask: float                 # лучший ask-price (минимальная цена покупки)
    best_ask_size: float            # объём на лучшем аске
    depth_at_price: float           # доступный объём по нашей цене (market_depth)
    competitive: float              # конкурентность 0..1

    # Стакан (топ уровни)
    order_book: list = field(default_factory=list)  # list[PriceLevel]

    # Мета
    bk_event_id: int = 0
    ref_event_id: int = 0
    direct_link_raw: str = ""

    @property
    def polymarket_url(self) -> str:
        """Прямая ссылка на событие в Polymarket"""
        if self.market_id:
            return f"https://polymarket.com/event/{self.market_id}"
        return ""

    @property
    def match_name(self) -> str:
        return f"{self.home} vs {self.away}"

    @property
    def start_dt(self) -> str:
        """Время начала матча"""
        try:
            dt = datetime.datetime.fromtimestamp(self.started_at)
            return dt.strftime("%d.%m %H:%M")
        except Exception:
            return "?"

    @property
    def edge(self) -> float:
        """Edge = value% / 100"""
        return self.value_pct / 100

    def display(self, idx: int = 0) -> str:
        """Красивый вывод для консоли"""
        sep = "─" * 70
        lines = [
            f"\n  {'─'*68}",
            f"  #{idx:02d}  🎯 {self.match_name}",
            f"       📅 {self.start_dt}  |  {self.league}",
            f"",
            f"       ИСХОД:     {self.outcome_name}  ({self.market_type_name}{'  линия='+str(self.market_param) if self.market_param else ''})",
            f"       EDGE:      +{self.value_pct:.2f}%",
            f"       КОЭФ:      {self.bb_odds:.4f}  (implied {self.bb_price:.1%})",
            f"",
            f"       ЛИКВИДНОСТЬ:",
            f"         Рынок:   ${self.total_liquidity:,.0f}",
            f"         Стакан:  ${self.depth_at_price:,.2f}  (наш уровень)",
            f"         Best ask:  {self.best_ask:.4f} (${self.best_ask_size:,.0f})",
        ]

        if self.order_book:
            lines.append(f"")
            lines.append(f"       СТАКАН (топ уровни):")
            for lvl in self.order_book[:5]:
                marker = " ◄ наш" if abs(lvl.price - self.bb_price) < 0.02 else ""
                lines.append(f"         {lvl}{marker}")

        lines += [
            f"",
            f"       Polymarket market ID: {self.market_id}",
            f"       Outcome (token) ID:   {self.outcome_id[:24]}...{self.outcome_id[-8:]}",
            f"       🔗 {self.polymarket_url}",
        ]
        return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Маппинг market_and_bet_type
# ──────────────────────────────────────────────────────────────────────────────
SPORT_NAMES = {
    1:  "⚾ Baseball",
    2:  "🏀 Basketball",
    4:  "🤾 Futsal",
    5:  "🤾 Handball",
    6:  "🏒 Hockey",
    7:  "⚽ Soccer",
    8:  "🎾 Tennis",
    9:  "🏐 Volleyball",
    10: "🏈 Am. Football",
    11: "🎱 Snooker",
    12: "🎯 Darts",
    13: "🏓 Table Tennis",
    14: "🏸 Badminton",
    15: "🏉 Rugby League",
    16: "🏊 Water Polo",
    17: "🏒 Bandy",
    18: "🥊 Martial Arts",
    19: "🏑 Field Hockey",
    20: "🏉 AFL",
    21: "🎮 Other E-Sports",
    22: "♟️ Chess",
    23: "🏐 Gaelic Sport",
    24: "🏏 Cricket",
    25: "🏎️ Formula 1",
    27: "🏎️ Motorsport",
    28: "🚴 Cycling",
    29: "🏐 Beach Volleyball",
    30: "🏇 Horse Racing",
    31: "🎿 Biathlon",
    32: "🥌 Curling",
    33: "🎾 Squash",
    34: "🏐 Netball",
    35: "⚽ Beach Soccer",
    36: "🏒 Floorball",
    37: "🏑 Hurling",
    38: "🏐 Kung Volleyball",
    39: "🎮 E-Soccer",
    41: "🎮 E-Basketball",
    43: "🏉 Rugby Union",
    44: "🥊 Boxing",
    45: "🥋 MMA",
    46: "🎮 Dota 2",
    47: "🎮 Counter-Strike",
    48: "🎮 League of Legends",
    49: "⛳ Golf",
    50: "🥍 Lacrosse",
    51: "🎮 Valorant",
    52: "🎮 Overwatch",
    53: "🎮 PUBG",
    54: "🎮 Fortnite",
    55: "🎮 Rainbow Six",
    56: "🎮 Cross Fire",
    57: "🎮 Call of Duty",
    58: "🎮 Apex Legends",
    59: "🎮 Deadlock",
    60: "🎮 Standoff 2",
    61: "🎮 King of Glory",
    62: "🎮 Arena of Valor",
    63: "🎮 Mobile Legends",
    64: "🎮 Heroes of the Storm",
    65: "🎮 StarCraft",
    66: "🎮 Warcraft",
    67: "🎮 Age of Empires",
    68: "🎮 Hearthstone",
    69: "🎮 Rocket League",
    70: "🎮 Brawl Stars",
    71: "🎮 HALO",
}

MARKET_NAMES = {
    1:  "Team1 Win",
    2:  "Team2 Win",
    3:  "Asian HCP1 DNB",
    4:  "Asian HCP2 DNB",
    5:  "Euro HCP1",
    6:  "Euro HCPX",
    7:  "Euro HCP2",
    8:  "Both to Score",
    9:  "One Scoreless",
    11: "1 (Home Win 1X2)",
    12: "X (Draw)",
    13: "2 (Away Win 1X2)",
    14: "1X",
    15: "X2",
    16: "12 (No Draw)",
    17: "Asian HCP1",
    18: "Asian HCP2",
    19: "Total Over",
    20: "Total Under",
    21: "Total Over Team1",
    22: "Total Under Team1",
    23: "Total Over Team2",
    24: "Total Under Team2",
    25: "Odd",
    26: "Even",
}


def parse_direct_link(raw: str) -> dict:
    """
    Парсит строку direct_link из BetBurger.
    Возвращает dict с ключами: market_id, event_id, outcome_id, outcome_name,
    neg_risk, liquidity_num, competitive, best_offers, taker_fee, maker_fee.
    """
    if not raw:
        return {}

    # direct_link может быть уже URL-encoded
    decoded = unquote(raw)
    params = parse_qs(decoded, keep_blank_values=True)

    def _get(key, default=None):
        v = params.get(key, [default])
        return v[0] if v else default

    # Парсим bestOffers: "2.0:1010.5,1.9607843:7388.324"
    best_offers_raw = _get("bestOffers", "")
    order_book = []
    if best_offers_raw:
        for pair in best_offers_raw.split(","):
            try:
                parts = pair.strip().split(":")
                if len(parts) == 2:
                    odds = float(parts[0])
                    size = float(parts[1])
                    price = round(1.0 / odds, 6) if odds > 0 else 0
                    order_book.append(PriceLevel(
                        odds=odds,
                        price=price,
                        size=size,
                        implied_pct=price * 100,
                    ))
            except (ValueError, ZeroDivisionError):
                pass

    return {
        "market_id":     _get("marketId", ""),
        "pm_event_id":   _get("eventId", ""),
        "outcome_id":    _get("outcomeId", ""),
        "outcome_name":  _get("outcomeName", ""),
        "neg_risk":      _get("negRisk", "false").lower() == "true",
        "liquidity_num": _safe_float(_get("liquidityNum", 0)),
        "competitive":   _safe_float(_get("competitive", 0)),
        "taker_fee":     _safe_float(_get("takerBaseFee", 0)),
        "maker_fee":     _safe_float(_get("makerBaseFee", 0)),
        "order_book":    order_book,
    }


def _safe_float(val, default=0.0) -> float:
    """Конвертирует значение в float, обрабатывая 'null', None, '' и прочие строки."""
    if val is None or val == "" or val == "null" or val == "undefined":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def from_betburger(bet: dict, arb_meta: dict) -> "Optional[PolymarketBet]":
    """
    Создаёт PolymarketBet из сырых данных BetBurger (bet + arb_meta).
    Возвращает None если данных недостаточно.
    """
    direct_link = bet.get("direct_link", "") or bet.get("bookmaker_event_direct_link", "")

    # У Polymarket прямая ссылка = direct_link (не bookmaker_event_direct_link)
    # Если в bookmaker_event_direct_link нет outcomeId — пробуем direct_link
    link_to_parse = ""
    raw_dl = bet.get("direct_link", "")
    raw_bdl = bet.get("bookmaker_event_direct_link", "")

    if raw_dl and "outcomeId" in raw_dl:
        link_to_parse = raw_dl
    elif raw_bdl and "outcomeId" in raw_bdl:
        link_to_parse = raw_bdl
    else:
        # direct_link для Polymarket = поле direct_link (строка с параметрами)
        link_to_parse = raw_dl or raw_bdl

    pm = parse_direct_link(link_to_parse)

    # Если нет outcome_id — Polymarket ставку не разместить
    if not pm.get("outcome_id") and not pm.get("market_id"):
        # Иногда данные частичные — всё равно создаём объект
        pass

    koef = _safe_float(bet.get("koef", 0))
    if koef <= 0:
        return None
    bb_price = round(1.0 / koef, 6)

    market_type = int(_safe_float(bet.get("market_and_bet_type", 0)))
    market_param = _safe_float(bet.get("market_and_bet_type_param", 0))

    # Лучший ask из стакана
    ob = pm.get("order_book", [])
    best_ask = ob[0].price if ob else bb_price
    best_ask_size = ob[0].size if ob else 0.0

    # ── РАСЧЁТ ВЕЛЬЮ% ────────────────────────────────────────────────────────
    # Согласно документации и проверке на реальных данных:
    #   arbs[].middle_value        = "Valuebet overvalue percentage" — именно это
    #                                показывает сайт BetBurger как велью%  ← ГЛАВНЫЙ ИСТОЧНИК
    #   source.valueBets[].percent = тоже велью%, совпадает с middle_value
    #                                (боты копируют его в arb_meta["percent"])
    #   arbs[].percent             = доходность шурбета (вилки) — НЕ велью
    #
    # Приоритет: middle_value → source.valueBets.percent → 0
    middle_value = _safe_float(arb_meta.get("middle_value"))
    if middle_value > 0:
        # middle_value уже в % (напр. 1.99 = 1.99%)
        value_pct = middle_value
    else:
        # Fallback: source.valueBets[].percent (боты подставляют в arb_meta["percent"])
        raw_roi   = arb_meta.get("percent") or 0
        value_pct = _safe_float(raw_roi)
        if 0 < abs(value_pct) <= 1:   # дробный формат (0.047) → конвертируем в %
            value_pct *= 100

    # Санити-кап
    if value_pct > 100:
        log.warning("ANOMALY: value_pct=%.1f%% (middle_value=%s) — обнуляем", value_pct, middle_value)
        value_pct = 0.0

    return PolymarketBet(
        bet_id          = str(bet.get("id", "")),
        market_id       = pm.get("market_id", ""),
        event_id        = pm.get("pm_event_id", ""),
        outcome_id      = pm.get("outcome_id", ""),
        outcome_name    = pm.get("outcome_name", MARKET_NAMES.get(market_type, f"type={market_type}")),

        home            = bet.get("home", "") or bet.get("team1_name", ""),
        away            = bet.get("away", "") or bet.get("team2_name", ""),
        league          = bet.get("league_name", bet.get("league", "")),
        sport_id        = int(bet.get("sport_id", 0) or 0),
        started_at      = int(bet.get("started_at", 0) or 0),

        market_type     = market_type,
        market_type_name= MARKET_NAMES.get(market_type, f"type={market_type}"),
        market_param    = market_param,
        neg_risk        = pm.get("neg_risk", False),

        bb_odds         = koef,
        bb_price        = bb_price,
        value_pct       = value_pct,
        arb_pct         = _safe_float(arb_meta.get("percent")),  # arbs[].percent = доходность вилки
        total_liquidity = pm.get("liquidity_num", _safe_float(bet.get("market_depth", 0))),
        best_ask        = best_ask,
        best_ask_size   = best_ask_size,
        depth_at_price  = _safe_float(bet.get("market_depth", 0)),
        competitive     = pm.get("competitive", 0.0),

        order_book      = ob,
        bk_event_id     = int(bet.get("bookmaker_event_id", 0) or 0),
        ref_event_id    = int(bet.get("event_id", 0) or 0),
        direct_link_raw = link_to_parse,
    )