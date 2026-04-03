# -*- coding: utf-8 -*-
"""
Hedge Calculator — дельта-нейтральное позиционирование.

Рассчитывает оптимальные размеры двух позиций (матч + турнир),
чтобы прибыль была одинакова в любом сценарии.

Математика (два сценария):
  Budget B = s_a * p_a + s_b * p_b
  Profit_1 = s_a * (exit_a1 - p_a) + s_b * (exit_b1 - p_b)
  Profit_2 = s_a * (exit_a2 - p_a) + s_b * (exit_b2 - p_b)
  Delta-neutral: Profit_1 = Profit_2

  Решение:
    ratio = (exit_b2 - exit_b1) / (exit_a1 - exit_a2)
    s_b = B / (p_b + p_a * ratio)
    s_a = s_b * ratio
"""

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class ScenarioResult:
    """Результат одного сценария."""
    name: str = ""
    exit_a: float = 0.0       # цена выхода позиции A
    exit_b: float = 0.0       # цена выхода позиции B
    pnl_a: float = 0.0        # P&L позиции A
    pnl_b: float = 0.0        # P&L позиции B
    total_pnl: float = 0.0    # суммарный P&L


@dataclass
class HedgeResult:
    """Результат расчёта дельта-нейтральной позиции."""
    # Позиции
    size_a: float = 0.0       # shares для позиции A (матч)
    size_b: float = 0.0       # shares для позиции B (турнир)
    cost_a: float = 0.0       # стоимость позиции A (USDC)
    cost_b: float = 0.0       # стоимость позиции B (USDC)
    total_cost: float = 0.0   # общая стоимость

    # Результат
    profit: float = 0.0       # профит (одинаковый во всех сценариях)
    roi: float = 0.0          # ROI = profit / budget
    roi_pct: float = 0.0      # ROI в процентах

    # Сценарии
    scenarios: list = field(default_factory=list)

    # Валидация
    is_profitable: bool = False
    is_delta_neutral: bool = False  # отклонение P&L < 0.1%
    max_pnl_variance: float = 0.0  # макс отклонение между сценариями
    warnings: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "size_a": round(self.size_a, 2),
            "size_b": round(self.size_b, 2),
            "cost_a": round(self.cost_a, 2),
            "cost_b": round(self.cost_b, 2),
            "total_cost": round(self.total_cost, 2),
            "profit": round(self.profit, 2),
            "roi": round(self.roi, 4),
            "roi_pct": round(self.roi_pct, 2),
            "is_profitable": self.is_profitable,
            "is_delta_neutral": self.is_delta_neutral,
            "max_pnl_variance": round(self.max_pnl_variance, 4),
            "warnings": self.warnings,
            "scenarios": [
                {
                    "name": s.name,
                    "exit_a": s.exit_a,
                    "exit_b": s.exit_b,
                    "pnl_a": round(s.pnl_a, 2),
                    "pnl_b": round(s.pnl_b, 2),
                    "total_pnl": round(s.total_pnl, 2),
                }
                for s in self.scenarios
            ],
        }


def calc_delta_neutral(
    price_a: float,
    price_b: float,
    exit_a1: float,
    exit_b1: float,
    exit_a2: float,
    exit_b2: float,
    budget: float,
    scenario_names: tuple[str, str] = ("Scenario 1", "Scenario 2"),
) -> HedgeResult:
    """
    Рассчитать дельта-нейтральные позиции для двух сценариев.

    Args:
        price_a:  цена покупки позиции A (0..1)
        price_b:  цена покупки позиции B (0..1)
        exit_a1:  цена выхода A в сценарии 1 (0..1)
        exit_b1:  цена выхода B в сценарии 1 (0..1)
        exit_a2:  цена выхода A в сценарии 2 (0..1)
        exit_b2:  цена выхода B в сценарии 2 (0..1)
        budget:   бюджет в USDC
        scenario_names: названия сценариев
    """
    result = HedgeResult()
    result.warnings = []

    # Валидация входных данных
    if budget <= 0:
        result.warnings.append("Budget must be > 0")
        return result
    if price_a <= 0 or price_a >= 1:
        result.warnings.append("Price A must be between 0 and 1 (exclusive)")
        return result
    if price_b <= 0 or price_b >= 1:
        result.warnings.append("Price B must be between 0 and 1 (exclusive)")
        return result

    diff_a = exit_a1 - exit_a2  # разница выходных цен A между сценариями
    diff_b = exit_b2 - exit_b1  # разница выходных цен B между сценариями

    if abs(diff_a) < 1e-8:
        result.warnings.append("Exit prices for A are the same in both scenarios — no hedge possible")
        return result
    if abs(diff_b) < 1e-8:
        result.warnings.append("Exit prices for B are the same in both scenarios — no hedge possible")
        return result

    # Ratio: сколько shares A на каждый share B
    ratio = diff_b / diff_a

    if ratio <= 0:
        result.warnings.append("Negative ratio — positions move in same direction, no hedge possible")
        return result

    # Размеры позиций
    denominator = price_b + price_a * ratio
    if abs(denominator) < 1e-8:
        result.warnings.append("Cannot compute position sizes (denominator = 0)")
        return result

    s_b = budget / denominator
    s_a = s_b * ratio

    if s_a <= 0 or s_b <= 0:
        result.warnings.append("Computed negative position size — check exit prices")
        return result

    cost_a = s_a * price_a
    cost_b = s_b * price_b

    # P&L по сценариям
    pnl_1_a = s_a * (exit_a1 - price_a)
    pnl_1_b = s_b * (exit_b1 - price_b)
    pnl_1 = pnl_1_a + pnl_1_b

    pnl_2_a = s_a * (exit_a2 - price_a)
    pnl_2_b = s_b * (exit_b2 - price_b)
    pnl_2 = pnl_2_a + pnl_2_b

    # Заполняем результат
    result.size_a = s_a
    result.size_b = s_b
    result.cost_a = cost_a
    result.cost_b = cost_b
    result.total_cost = cost_a + cost_b
    result.profit = pnl_1  # должен быть ≈ pnl_2
    result.roi = pnl_1 / budget if budget > 0 else 0
    result.roi_pct = result.roi * 100

    result.scenarios = [
        ScenarioResult(
            name=scenario_names[0],
            exit_a=exit_a1, exit_b=exit_b1,
            pnl_a=pnl_1_a, pnl_b=pnl_1_b,
            total_pnl=pnl_1,
        ),
        ScenarioResult(
            name=scenario_names[1],
            exit_a=exit_a2, exit_b=exit_b2,
            pnl_a=pnl_2_a, pnl_b=pnl_2_b,
            total_pnl=pnl_2,
        ),
    ]

    result.is_profitable = pnl_1 > 0
    result.max_pnl_variance = abs(pnl_1 - pnl_2)
    result.is_delta_neutral = result.max_pnl_variance < budget * 0.001  # < 0.1% variance

    if not result.is_profitable:
        result.warnings.append(f"Not profitable: P&L = ${pnl_1:.2f}")
    if not result.is_delta_neutral:
        result.warnings.append(f"Not perfectly delta-neutral: variance = ${result.max_pnl_variance:.2f}")

    return result


def calc_multi_scenario(
    price_a: float,
    price_b: float,
    scenarios: list[dict],
    budget: float,
) -> HedgeResult:
    """
    Рассчитать позиции для N сценариев (минимизация дисперсии P&L).

    scenarios: [{"name": "...", "exit_a": 1.0, "exit_b": 0.45}, ...]

    Для двух сценариев — точное решение (calc_delta_neutral).
    Для N > 2 — оптимизация: ищем ratio, минимизирующий дисперсию P&L.
    """
    if len(scenarios) < 2:
        result = HedgeResult()
        result.warnings = ["Need at least 2 scenarios"]
        return result

    if len(scenarios) == 2:
        return calc_delta_neutral(
            price_a, price_b,
            scenarios[0]["exit_a"], scenarios[0]["exit_b"],
            scenarios[1]["exit_a"], scenarios[1]["exit_b"],
            budget,
            scenario_names=(scenarios[0].get("name", "Scenario 1"),
                            scenarios[1].get("name", "Scenario 2")),
        )

    # N > 2: grid search for optimal ratio
    best_ratio = None
    best_variance = float("inf")

    for r_int in range(1, 10000):
        ratio = r_int / 100.0  # 0.01 to 100.0
        denom = price_b + price_a * ratio
        if abs(denom) < 1e-8:
            continue
        s_b = budget / denom
        s_a = s_b * ratio
        if s_a <= 0 or s_b <= 0:
            continue

        pnls = []
        for sc in scenarios:
            pnl = s_a * (sc["exit_a"] - price_a) + s_b * (sc["exit_b"] - price_b)
            pnls.append(pnl)

        mean_pnl = sum(pnls) / len(pnls)
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)

        if variance < best_variance:
            best_variance = variance
            best_ratio = ratio

    if best_ratio is None:
        result = HedgeResult()
        result.warnings = ["Could not find valid ratio for multi-scenario hedge"]
        return result

    # Compute final result with best ratio
    denom = price_b + price_a * best_ratio
    s_b = budget / denom
    s_a = s_b * best_ratio
    cost_a = s_a * price_a
    cost_b = s_b * price_b

    scenario_results = []
    pnls = []
    for sc in scenarios:
        pnl_a = s_a * (sc["exit_a"] - price_a)
        pnl_b = s_b * (sc["exit_b"] - price_b)
        total = pnl_a + pnl_b
        pnls.append(total)
        scenario_results.append(ScenarioResult(
            name=sc.get("name", f"Scenario {len(scenario_results)+1}"),
            exit_a=sc["exit_a"], exit_b=sc["exit_b"],
            pnl_a=pnl_a, pnl_b=pnl_b, total_pnl=total,
        ))

    mean_pnl = sum(pnls) / len(pnls)
    max_var = max(abs(p - mean_pnl) for p in pnls)

    result = HedgeResult(
        size_a=s_a, size_b=s_b,
        cost_a=cost_a, cost_b=cost_b,
        total_cost=cost_a + cost_b,
        profit=mean_pnl,
        roi=mean_pnl / budget if budget > 0 else 0,
        roi_pct=(mean_pnl / budget * 100) if budget > 0 else 0,
        scenarios=scenario_results,
        is_profitable=mean_pnl > 0,
        is_delta_neutral=max_var < budget * 0.001,
        max_pnl_variance=max_var,
    )

    if not result.is_profitable:
        result.warnings.append(f"Not profitable: avg P&L = ${mean_pnl:.2f}")

    return result


def bayesian_exit_prices(
    match_price: float,
    tourney_player_price: float,
    match_loser_tourney_price: float,
    player_a_name: str = "Player A",
    player_b_name: str = "Player B",
    tourney_player_name: str = "Tournament Player",
) -> list[dict]:
    """
    Рассчитать exit цены турнира по Байесу.

    Когда игрок выбывает из турнира (проигрывает матч),
    его доля вероятности перераспределяется пропорционально
    остальным участникам.

    Args:
        match_price: текущая цена матча (вероятность что A победит)
        tourney_player_price: цена турнирного player в маркете (0..1)
        match_loser_tourney_price: турнирная цена проигравшего матч (0..1)

    Формулы:
        P(T wins tourney | A wins match, B eliminated):
            = P(T) / (1 - P(B_tourney))  — нормализация без B

        P(T wins tourney | B wins match, A eliminated):
            = P(T) / (1 - P(A_tourney))  — если T != A
            = 0                          — если T = A
    """
    # Сценарий 1: A выигрывает матч → B выбывает
    # Турнирные шансы всех кроме B нормализуются
    if match_loser_tourney_price < 1.0:
        exit_b1 = tourney_player_price / (1.0 - match_loser_tourney_price)
    else:
        exit_b1 = tourney_player_price

    # Сценарий 2: B выигрывает матч → A выбывает (если tourney_player = A)
    # Нужно знать, tourney_player совпадает с A или с B
    # Предполагаем tourney_player = отличный от проигравшего
    # Поэтому в обоих сценариях он остаётся в турнире
    exit_b2 = tourney_player_price / (1.0 - match_loser_tourney_price)

    # Clamp to valid range
    exit_b1 = min(max(exit_b1, 0.01), 0.99)
    exit_b2 = min(max(exit_b2, 0.01), 0.99)

    return [
        {
            "name": f"{player_a_name} wins match",
            "exit_a": 1.0,
            "exit_b": exit_b1,
        },
        {
            "name": f"{player_b_name} wins match",
            "exit_a": 0.0,
            "exit_b": exit_b2,
        },
    ]


def analyze_hedge_opportunity(
    match_price_a: float,
    tourney_player_price: float,
    tourney_opponent_price: float,
    budget: float,
    player_a: str = "Player A",
    player_b: str = "Player B",
    tourney_player: str = "Tournament Player",
) -> HedgeResult:
    """
    Полный автоматический анализ хедж-возможности.

    Логика:
    1. Мы ставим на A в матче (buy match @ match_price_a)
    2. Мы ставим на tournament_player в турнире (buy tourney @ tourney_player_price)
    3. Байесовски рассчитываем exit цены:
       - Если A выигрывает матч: B вылетает из турнира →
         tourney_player's price = P(T) / (1 - P(B_tourney))
       - Если B выигрывает матч: A вылетает из турнира →
         tourney_player's price = P(T) / (1 - P(A_tourney))
         (если tourney_player != A, иначе = 0)

    Args:
        match_price_a: цена "A wins match" (0..1)
        tourney_player_price: цена "tourney_player wins tournament" (0..1)
        tourney_opponent_price: цена проигравшего в турнирном маркете (0..1)
            Это цена того из A/B, кто вылетит. Для стратегии с внешним player:
            используем max(P(A_tourney), P(B_tourney)) как worst case.
        budget: бюджет в USDC

    Стратегия работает когда tourney_player != ни A ни B (внешний игрок):
    - Если A выиграет: B вылетает, tournament_player получает долю B
    - Если B выиграет: A вылетает, tournament_player получает долю A
    - В обоих случаях tournament_player остаётся в турнире → его цена растёт

    Или когда tourney_player = A:
    - Если A выиграет матч: его турнирные шансы растут
    - Если A проиграет: его турнирные шансы = 0 (вылетел)
    """
    # Определяем: tourney_player совпадает с A?
    # Для простоты передаём tourney_opponent_price как цену того, кто вылетает

    # Сценарий 1: A выигрывает матч → B вылетает
    # B's tournament share redistributes
    if tourney_opponent_price < 1.0:
        exit_tourney_sc1 = tourney_player_price / (1.0 - tourney_opponent_price)
    else:
        exit_tourney_sc1 = tourney_player_price
    exit_tourney_sc1 = min(exit_tourney_sc1, 0.99)

    # Сценарий 2: B выигрывает матч → A вылетает
    # Здесь зависит от того кто tourney_player
    # Если tourney_player это независимый игрок (не A и не B):
    # его цена тоже пересчитывается по Байесу когда A вылетает
    # Нужна цена A в турнире
    # Предположение: tourney_opponent_price = P(eliminated player in tournament)
    # В сценарии 2 вылетает A, нужна P(A in tournament)
    # Для упрощения: передаём обе цены (A и B) в турнире

    # Упрощённый вариант: exit_tourney_sc2 тоже растёт пропорционально
    # но с другим eliminated player
    # TODO: передать обе турнирные цены для точного расчёта

    # Пока используем симметричный подход:
    # tourney_opponent_price — это цена того кто точно вылетит
    # В сценарии 1 это B, в сценарии 2 — зависит от стратегии
    exit_tourney_sc2 = exit_tourney_sc1  # simplified

    scenarios = [
        {
            "name": f"{player_a} wins match",
            "exit_a": 1.0,
            "exit_b": exit_tourney_sc1,
        },
        {
            "name": f"{player_b} wins match",
            "exit_a": 0.0,
            "exit_b": exit_tourney_sc2,
        },
    ]

    return calc_delta_neutral(
        price_a=match_price_a,
        price_b=tourney_player_price,
        exit_a1=1.0,
        exit_b1=exit_tourney_sc1,
        exit_a2=0.0,
        exit_b2=exit_tourney_sc2,
        budget=budget,
        scenario_names=(scenarios[0]["name"], scenarios[1]["name"]),
    )


def analyze_hedge_full(
    match_price_a: float,
    tourney_player_price: float,
    player_a_tourney_price: float,
    player_b_tourney_price: float,
    budget: float,
    player_a: str = "Player A",
    player_b: str = "Player B",
    tourney_player: str = "Tournament Player",
    tourney_is_a: bool = False,
    tourney_is_b: bool = False,
    is_knockout: bool = True,
    parallel_eliminated_share: float = 0.0,
) -> HedgeResult:
    """
    Полный анализ с обеими турнирными ценами (A и B).

    Три стратегии в зависимости от того, кто tourney_player:

    1) tourney_player = A (ставим на фаворита матча + его победу в турнире):
       - A wins match → A advances, его турнирная цена растёт: P(A) / (1 - P(B))
       - B wins match → A eliminated (knockout) или шансы падают (league)
         exit = 0 (knockout) или P(A) * (1 - penalty) (league)

    2) tourney_player = B (ставим на матч A + победу B в турнире):
       - A wins match → B eliminated/ослабевает, exit_b падает
       - B wins match → B advances, exit_b растёт

    3) tourney_player = третий игрок (не в матче):
       - A wins → B eliminated, вероятность третьего растёт: P(T) / (1 - P(B))
       - B wins → A eliminated, вероятность третьего растёт: P(T) / (1 - P(A))
       Это идеальная дельта-нейтральная стратегия: в обоих случаях T растёт.
    """
    from gamma_client import GammaClient
    gc = GammaClient()

    # Определяем — tourney_player один из игроков матча?
    if not tourney_is_a and not tourney_is_b:
        tourney_is_a = gc._names_match(tourney_player, player_a)
        tourney_is_b = gc._names_match(tourney_player, player_b)

    # Модель exit цен зависит от формата турнира
    # Knockout: проигравший вылетает → его доля = 0, остальные пропорционально растут
    # League: проигрыш одного матча незначительно влияет (~3% снижение)
    league_penalty = 0.97  # множитель при проигрыше в league

    if tourney_is_a:
        # Стратегия: ставим на "A выиграет матч" + "A выиграет турнир"
        # Sc1: A wins → B eliminated/ослабевает → A's tourney price UP
        if player_b_tourney_price < 1.0:
            exit_b1 = tourney_player_price / (1.0 - player_b_tourney_price)
        else:
            exit_b1 = tourney_player_price
        exit_b1 = min(exit_b1, 0.99)

        # Sc2: B wins → A loses
        if is_knockout:
            exit_b2 = 0.0  # eliminated from tournament
        else:
            exit_b2 = tourney_player_price * league_penalty  # marginal drop

    elif tourney_is_b:
        # Стратегия: ставим на "A выиграет матч" + "B выиграет турнир"
        # Sc1: A wins → B loses
        if is_knockout:
            exit_b1 = 0.0  # B eliminated
        else:
            exit_b1 = tourney_player_price * league_penalty

        # Sc2: B wins → A eliminated/ослабевает → B's tourney price UP
        if player_a_tourney_price < 1.0:
            exit_b2 = tourney_player_price / (1.0 - player_a_tourney_price)
        else:
            exit_b2 = tourney_player_price
        exit_b2 = min(exit_b2, 0.99)

    else:
        # Стратегия: третий игрок — дельта-нейтрально
        if is_knockout:
            # Knockout: в обоих случаях кто-то вылетает → T's share grows
            # parallel_eliminated_share = суммарная доля вероятности от
            # ДРУГИХ параллельных матчей (expected elimination)
            # Общая eliminated доля = этот матч + параллельные
            total_elim_sc1 = player_b_tourney_price + parallel_eliminated_share
            total_elim_sc2 = player_a_tourney_price + parallel_eliminated_share
            # Clamp чтобы не делить на 0 или отрицательное
            total_elim_sc1 = min(total_elim_sc1, 0.95)
            total_elim_sc2 = min(total_elim_sc2, 0.95)

            if total_elim_sc1 > 0:
                exit_b1 = tourney_player_price / (1.0 - total_elim_sc1)
            else:
                exit_b1 = tourney_player_price
            if total_elim_sc2 > 0:
                exit_b2 = tourney_player_price / (1.0 - total_elim_sc2)
            else:
                exit_b2 = tourney_player_price
        else:
            # League: минимальное влияние на третьего
            exit_b1 = tourney_player_price * 1.01
            exit_b2 = tourney_player_price * 1.01
        exit_b1 = min(exit_b1, 0.99)
        exit_b2 = min(exit_b2, 0.99)

    return calc_delta_neutral(
        price_a=match_price_a,
        price_b=tourney_player_price,
        exit_a1=1.0, exit_b1=exit_b1,
        exit_a2=0.0, exit_b2=exit_b2,
        budget=budget,
        scenario_names=(f"{player_a} wins match", f"{player_b} wins match"),
    )


def suggest_exit_prices(
    match_side: str = "player1",
) -> list[dict]:
    """
    Предложить дефолтные сценарии с выходными ценами.
    Используется как fallback когда нет турнирных цен для байесовского расчёта.
    """
    return [
        {
            "name": f"{match_side} wins match",
            "exit_a": 1.0,
            "exit_b": 0.45,
        },
        {
            "name": f"{match_side} loses match",
            "exit_a": 0.0,
            "exit_b": 0.79,
        },
    ]


def validate_hedge_opportunity(
    result: HedgeResult,
    min_roi: float = 0.01,
    max_budget: float = 10000,
    min_size: float = 1.0,
) -> list[str]:
    """Валидация хедж-возможности. Возвращает список проблем."""
    issues = []

    if result.warnings:
        issues.extend(result.warnings)

    if result.roi < min_roi:
        issues.append(f"ROI {result.roi_pct:.1f}% below minimum {min_roi*100:.1f}%")

    if result.total_cost > max_budget:
        issues.append(f"Total cost ${result.total_cost:.2f} exceeds max budget ${max_budget:.2f}")

    if result.size_a < min_size:
        issues.append(f"Position A size {result.size_a:.1f} below minimum {min_size}")

    if result.size_b < min_size:
        issues.append(f"Position B size {result.size_b:.1f} below minimum {min_size}")

    return issues
