from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ArbLeg:
    bookmaker: str
    event_id: str
    market_id: str
    selection: str
    odds: float
    liquidity: Optional[float] = None
    # Polymarket specific
    token_id: Optional[str] = None
    price: Optional[float] = None  # 0..1 вероятность


@dataclass
class Arb:
    uid: str              # уникальный ID от BetBurger
    event_name: str
    sport: str
    roi: float            # например 0.04 = 4%
    legs: list = field(default_factory=list)

    # Кешированные поля после парсинга
    _ps3838_leg: Optional[ArbLeg] = field(default=None, repr=False)
    _pm_leg: Optional[ArbLeg] = field(default=None, repr=False)

    @property
    def has_polymarket_leg(self) -> bool:
        return self.polymarket_leg is not None

    @property
    def has_ps3838_leg(self) -> bool:
        return self.ps3838_leg is not None

    @property
    def polymarket_leg(self) -> Optional[ArbLeg]:
        if self._pm_leg is None:
            for leg in self.legs:
                if "polymarket" in leg.bookmaker.lower():
                    self._pm_leg = leg
                    break
        return self._pm_leg

    @property
    def ps3838_leg(self) -> Optional[ArbLeg]:
        if self._ps3838_leg is None:
            for leg in self.legs:
                if "ps3838" in leg.bookmaker.lower() or "pinnacle" in leg.bookmaker.lower():
                    self._ps3838_leg = leg
                    break
        return self._ps3838_leg

    # Shortcuts
    @property
    def polymarket_token_id(self) -> str:
        return self.polymarket_leg.token_id

    @property
    def polymarket_price(self) -> float:
        return self.polymarket_leg.price

    @property
    def polymarket_liquidity(self) -> float:
        return self.polymarket_leg.liquidity or 0

    @property
    def ps3838_event_id(self) -> str:
        return self.ps3838_leg.event_id

    @property
    def ps3838_market_id(self) -> str:
        return self.ps3838_leg.market_id

    @property
    def ps3838_selection(self) -> str:
        return self.ps3838_leg.selection

    @property
    def ps3838_odds(self) -> float:
        return self.ps3838_leg.odds

    def calc_stake_ratios(self):
        """
        Считаем доли ставок для арбитража.
        Сумма stake_1 + stake_2 = 1 (нормированные)
        Гарантированный профит = roi
        """
        o1 = self.ps3838_leg.odds
        o2 = 1 / self.polymarket_leg.price  # конвертируем вероятность в коэф
        total = 1 / o1 + 1 / o2
        return 1 / (o1 * total), 1 / (o2 * total)

    @property
    def ps3838_stake_ratio(self) -> float:
        return self.calc_stake_ratios()[0]

    @property
    def polymarket_stake_ratio(self) -> float:
        return self.calc_stake_ratios()[1]


@dataclass
class BetResult:
    success: bool
    bet_id: Optional[str] = None
    error: Optional[str] = None
    filled_odds: Optional[float] = None
    filled_amount: Optional[float] = None
