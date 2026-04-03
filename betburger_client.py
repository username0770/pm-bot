# -*- coding: utf-8 -*-
"""
BetBurger REST API клиент
Документация: https://rest-api-pr.betburger.com/swagger-ui/index.html

Структура ответа:
  {
    "bets":  [ BetDto, ... ]      ← отдельные ставки
    "arbs":  [ ValuebetArbsDto, ... ]  ← велью-пары; arbs[i].bet1_id = bets[j].id
                                         arbs[i].percent = % велью
  }
"""

import aiohttp
import logging
from models import Arb, ArbLeg
from betburger_auth import BetBurgerAuth

log = logging.getLogger(__name__)

LIVE_ARB_API     = "https://rest-api-lv.betburger.com/api/v1/arbs/bot_pro_search"
PREMATCH_ARB_API = "https://rest-api-pr.betburger.com/api/v1/arbs/bot_pro_search"
LIVE_VB_API      = "https://rest-api-lv.betburger.com/api/v1/valuebets/bot_pro_search"
PREMATCH_VB_API  = "https://rest-api-pr.betburger.com/api/v1/valuebets/bot_pro_search"

POLYMARKET_ID = 483  # bookmaker_id Polymarket в BetBurger (из betburger.com/api/entity_ids)

# Маппинг bookmaker_id → имя
BOOKMAKER_NAMES = {
    2:   "Bet365",
    3:   "Unibet",
    4:   "Pinnacle",
    9:   "Smarkets",
    10:  "Interwetten",
    12:  "William Hill",
    13:  "Bwin",
    16:  "PS3838",
    26:  "Betsson_SE",
    30:  "Betway",
    31:  "Betfair",
    32:  "Betfair_IT",
    34:  "Betsson",
    39:  "1xBet",
    48:  "Betclic",
    52:  "Boylesports",
    483: "Polymarket",
    57:  "Betvictor",
    60:  "Sportingbet",
    71:  "Nordicbet2",
    74:  "Marathonbet",
    78:  "NordicBet",
    79:  "Expekt",
    80:  "Betfair_exch",
    81:  "Ladbrokes",
    92:  "888sport",
    95:  "BetAtHome",
    128: "Fonbet",
    148: "Mozzartbet",
    150: "BetCity",
    162: "Vbet",
    187: "Betano",
    188: "Betcris",
    200: "Coolbet",
    204: "Matchbook",
    308: "BetBoom",
    314: "Betmaster",
    432: "GGbet",
    458: "Melbet",
    464: "Megapari",
    466: "Betpas",
    469: "Parimatch",
    483: "BetWinner",
    488: "Bwin_AT",
    489: "Mostbet",
    700: "Betandyou",
    702: "Betwinner2",
}

# market_and_bet_type маппинг (из betburger.com/api/entity_ids → Variations)
# Для Polymarket важны: 1=Team1Win, 2=Team2Win, 11=1, 12=X(Draw), 13=2
MARKET_VARIATIONS = {
    1:  "Team1 Win",
    2:  "Team2 Win",
    3:  "Asian HCP1 DNB",
    4:  "Asian HCP2 DNB",
    5:  "Euro HCP1 (%s)",
    6:  "Euro HCPX (%s)",
    7:  "Euro HCP2 (%s)",
    8:  "Both to score",
    9:  "One scoreless",
    11: "1 (Home Win)",
    12: "X (Draw)",
    13: "2 (Away Win)",
    14: "1X",
    15: "X2",
    16: "12",
    17: "Asian HCP1 (%s)",
    18: "Asian HCP2 (%s)",
    19: "Total Over (%s)",
    20: "Total Under (%s)",
    21: "Total Over (%s) Team1",
    22: "Total Under (%s) Team1",
    23: "Total Over (%s) Team2",
    24: "Total Under (%s) Team2",
    25: "Odd",
    26: "Even",
}

# Для basketball (sport_id=2) маппинг на Polymarket исходы:
# market_type=1 → "Team1 Win" → ищем YES токен для home team
# market_type=2 → "Team2 Win" → ищем YES токен для away team
POLY_OUTCOME_MAP = {
    1:  "home_win",   # Team1 Win
    2:  "away_win",   # Team2 Win
    11: "home_win",   # 1 (1X2)
    12: "draw",       # X
    13: "away_win",   # 2
    14: "home_or_draw",
    15: "away_or_draw",
    16: "home_or_away",
}


class BetBurgerClient:
    def __init__(self, token: str, filter_id: int, live: bool = False,
                 polymarket_only: bool = True,
                 auth: BetBurgerAuth | None = None):
        self._static_token   = token
        self.filter_id       = filter_id
        self.live            = live
        self.polymarket_only = polymarket_only
        self._auth           = auth  # если задан — токен обновляется автоматически

    async def _get_token(self) -> str:
        """Возвращает актуальный токен (обновляет через auth если задан)."""
        if self._auth:
            return await self._auth.get_token()
        return self._static_token

    async def get_arbs(self) -> list:
        url = LIVE_ARB_API if self.live else PREMATCH_ARB_API
        return await self._fetch(url)

    async def get_valuebets(self) -> list:
        url = LIVE_VB_API if self.live else PREMATCH_VB_API
        return await self._fetch(url)

    async def _fetch(self, url: str) -> list:
        token   = await self._get_token()
        qparams = {"access_token": token, "locale": "en"}
        fdata   = aiohttp.FormData()
        fdata.add_field("search_filter[]", str(self.filter_id))
        fdata.add_field("per_page", "100")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, params=qparams, data=fdata,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 403:
                        log.error("BetBurger 403: нет API подписки")
                        return []
                    if resp.status != 200:
                        log.error("BetBurger API ошибка: %d", resp.status)
                        return []
                    raw = await resp.json()
                    return self._parse(raw)
        except Exception as e:
            log.error("BetBurger запрос упал: %s", e)
            return []

    def _parse(self, data) -> list:
        # Сохраняем последний сырой ответ для диагностики
        try:
            import json as _json, pathlib, datetime
            _out = pathlib.Path(__file__).parent / "betburger_last_raw.json"
            _out.write_text(_json.dumps({
                "saved_at": datetime.datetime.now().isoformat(),
                "data": data
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

        if isinstance(data, list):
            # Старый формат — просто список бетов без arbs
            return self._parse_bets_only(data)

        if not isinstance(data, dict):
            return []

        bets_raw = data.get("bets", [])
        arbs_raw = data.get("arbs", [])

        # Строим индекс bet_id → bet
        bet_index = {b["id"]: b for b in bets_raw if "id" in b}

        # Строим индекс bet_id → arb (велью%)
        arb_by_bet = {}
        for arb in arbs_raw:
            for key in ("bet1_id", "bet2_id", "bet3_id"):
                bid = arb.get(key)
                if bid:
                    arb_by_bet[bid] = arb

        results = []
        for bet in bets_raw:
            try:
                bid    = bet.get("id")
                bk_id  = bet.get("bookmaker_id")

                # Фильтр — только Polymarket
                if self.polymarket_only and bk_id != POLYMARKET_ID:
                    continue

                arb_meta = arb_by_bet.get(bid, {})
                obj = self._parse_item(bet, arb_meta)
                if obj:
                    results.append(obj)
            except Exception as e:
                log.debug("Парсинг бета упал: %s", e)

        return results

    def _parse_bets_only(self, items: list) -> list:
        """Fallback если ответ — просто список без arbs"""
        results = []
        for item in items:
            if self.polymarket_only and item.get("bookmaker_id") != POLYMARKET_ID:
                continue
            try:
                obj = self._parse_item(item, {})
                if obj:
                    results.append(obj)
            except Exception as e:
                log.debug("Парсинг fallback упал: %s", e)
        return results

    def _parse_item(self, bet: dict, arb_meta: dict):
        uid       = str(bet.get("id", ""))
        home      = bet.get("home", "") or bet.get("team1_name", "")
        away      = bet.get("away", "") or bet.get("team2_name", "")
        league    = bet.get("league_name", bet.get("league", ""))
        sport     = str(bet.get("sport_id", ""))
        koef      = float(bet.get("koef", 1) or 1)
        price     = round(1 / koef, 6) if koef > 1 else None
        liq       = float(bet.get("market_depth", 0) or 0) or None
        bk_id     = bet.get("bookmaker_id")
        bk_name   = BOOKMAKER_NAMES.get(bk_id, f"ID={bk_id}") if bk_id else "unknown"
        event_id  = str(bet.get("bookmaker_event_id", bet.get("event_id", "")))
        market_id = str(bet.get("market_and_bet_type", ""))
        selection = str(bet.get("market_and_bet_type_param", ""))
        link      = bet.get("bookmaker_event_direct_link") or bet.get("direct_link", "")

        # ROI из arb_meta
        roi = 0.0
        if arb_meta:
            raw_roi = arb_meta.get("roi") or arb_meta.get("percent") or 0
            roi = float(raw_roi) / 100 if abs(float(raw_roi or 0)) > 1 else float(raw_roi or 0)

        leg = ArbLeg(
            bookmaker = bk_name,
            event_id  = event_id,
            market_id = market_id,
            selection = selection,
            odds      = koef,
            liquidity = liq,
            token_id  = None,
            price     = price,
        )

        return Arb(
            uid        = uid,
            event_name = f"{home} vs {away} [{league}]",
            sport      = sport,
            roi        = roi,
            legs       = [leg]
        )