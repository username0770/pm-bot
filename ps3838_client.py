"""
PS3838 (Pinnacle) API клиент
Документация: https://www.ps3838.com/api
"""

import aiohttp
import logging
from models import BetResult

log = logging.getLogger(__name__)

BASE_URL = "https://api.ps3838.com"


class PS3838Client:
    def __init__(self, username: str, password: str):
        self.auth = aiohttp.BasicAuth(username, password)
        self.session = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(auth=self.auth)
        return self.session

    async def place_bet(
        self,
        event_id: str,
        market_id: str,
        selection: str,
        odds: float,
        stake: float,
        currency: str = "USD"
    ) -> BetResult:
        """
        Размещение ставки через PS3838 API
        
        Сначала нужно получить line_id через /v1/line (актуальные коэффициенты),
        затем сделать ставку через /v1/bets/straight
        """
        # Шаг 1: получаем актуальную линию и line_id
        line = await self._get_line(event_id, market_id, selection, odds)
        if not line:
            return BetResult(success=False, error="Не удалось получить линию")

        # Проверяем что коэффициент не упал ниже допустимого
        actual_odds = line.get("price", 0)
        if actual_odds < odds * 0.995:  # допуск 0.5%
            return BetResult(
                success=False,
                error=f"Коэф упал: ожидали {odds:.3f}, получили {actual_odds:.3f}"
            )

        # Шаг 2: размещаем ставку
        return await self._post_bet(line, stake, currency)

    async def _get_line(self, event_id: str, market_id: str, selection: str, odds: float) -> dict | None:
        """Получить актуальную линию для ставки"""
        # Парсим тип ставки из selection (1X2, handicap, totals и т.д.)
        bet_type, team, handicap = self._parse_selection(selection)

        params = {
            "sportId": 29,  # будет перезаписан реальным sport_id
            "leagueIds": [],
            "eventId": int(event_id),
            "betType": bet_type,
            "team": team,
            "side": None,
            "handicap": handicap,
            "oddsFormat": "Decimal",
        }

        try:
            session = self._get_session()
            async with session.get(
                f"{BASE_URL}/v1/line",
                params={k: v for k, v in params.items() if v is not None},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                data = await resp.json()
                if data.get("status") == "SUCCESS":
                    return {
                        "line_id": data.get("lineId"),
                        "alt_line_id": data.get("altLineId"),
                        "price": data.get("price"),
                        "event_id": event_id,
                        "bet_type": bet_type,
                        "team": team,
                        "handicap": handicap,
                    }
                log.warning("PS3838 get_line: %s", data)
                return None
        except Exception as e:
            log.error("PS3838 get_line ошибка: %s", e)
            return None

    async def _post_bet(self, line: dict, stake: float, currency: str) -> BetResult:
        """Отправить ставку"""
        payload = {
            "oddsFormat": "Decimal",
            "uniqueRequestId": f"arb_{line['event_id']}_{line['line_id']}",
            "acceptBetterLine": True,  # принять если коэф улучшился
            "bets": [{
                "lineId": line["line_id"],
                "altLineId": line.get("alt_line_id"),
                "eventId": int(line["event_id"]),
                "price": line["price"],
                "stake": round(stake, 2),
                "teamType": line["team"],
                "betType": line["bet_type"],
                "handicap": line.get("handicap"),
            }]
        }

        try:
            session = self._get_session()
            async with session.post(
                f"{BASE_URL}/v1/bets/straight",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                bet = data.get("bets", [{}])[0]
                status = bet.get("status", "")

                if status == "ACCEPTED":
                    return BetResult(
                        success=True,
                        bet_id=str(bet.get("betId")),
                        filled_odds=bet.get("price"),
                        filled_amount=stake
                    )
                else:
                    return BetResult(
                        success=False,
                        error=f"PS3838 статус: {status} | {bet.get('errorCode', '')}"
                    )
        except Exception as e:
            log.error("PS3838 place_bet ошибка: %s", e)
            return BetResult(success=False, error=str(e))

    @staticmethod
    def _parse_selection(selection: str) -> tuple[str, str | None, float | None]:
        """
        Конвертируем строку selection из BetBurger в параметры PS3838
        Примеры:
          "TU(4.5)"  → TOTAL_POINTS, OVER, 4.5
          "TO(3.5)"  → TOTAL_POINTS, UNDER, 3.5
          "Team1Win" → MONEYLINE, TEAM1, None
          "Team2Win" → MONEYLINE, TEAM2, None
        """
        s = selection.upper()

        if s.startswith("TU(") or s.startswith("OVER"):
            val = s.replace("TU(", "").replace("OVER(", "").replace(")", "")
            return "TOTAL_POINTS", "OVER", float(val) if val else None

        if s.startswith("TO(") or s.startswith("UNDER"):
            val = s.replace("TO(", "").replace("UNDER(", "").replace(")", "")
            return "TOTAL_POINTS", "UNDER", float(val) if val else None

        if "TEAM1" in s or s in ("1", "HOME"):
            return "MONEYLINE", "TEAM1", None

        if "TEAM2" in s or s in ("2", "AWAY"):
            return "MONEYLINE", "TEAM2", None

        if "DRAW" in s or s == "X":
            return "MONEYLINE", "DRAW", None

        # По умолчанию
        return "MONEYLINE", "TEAM1", None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
