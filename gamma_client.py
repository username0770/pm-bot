# -*- coding: utf-8 -*-
"""
Gamma API клиент — поиск рынков на Polymarket.

Используется для обнаружения пар матч + турнирный winner
для дельта-нейтральных хеджей.

Gamma API docs: https://docs.polymarket.com/#gamma-markets-api
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote

log = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CACHE_TTL = 120  # seconds


@dataclass
class HedgeMarket:
    """Один рынок Polymarket (матч или турнирный winner)."""
    condition_id: str = ""
    token_id_yes: str = ""
    token_id_no: str = ""
    question: str = ""
    end_date: str = ""
    neg_risk: bool = False
    market_type: str = ""       # 'match' | 'tournament'
    event_slug: str = ""
    slug: str = ""
    outcome_prices: str = ""    # "[0.55, 0.45]"
    tags: list = field(default_factory=list)


@dataclass
class HedgePair:
    """Пара: матч + турнирный рынок для хеджирования."""
    pair_id: str = ""
    sport: str = ""
    event_name: str = ""
    player_a: str = ""          # игрок из матча (на которого ставим)
    player_b: str = ""          # его оппонент в матче
    match_market: Optional[HedgeMarket] = None
    tournament_market: Optional[HedgeMarket] = None
    tournament_player: str = "" # игрок в турнирном рынке
    is_knockout: bool = True    # формат турнира: knockout (стратегия работает) vs league


class GammaClient:
    """Обёртка над Polymarket Gamma API для поиска рынков."""

    KNOCKOUT_PATTERNS = {
        "ncaa tournament", "march madness",
        "grand slam", "wimbledon", "french open", "us open", "australian open",
        "roland garros",
        "champions league knockout", "ucl knockout",
        "playoff", "playoffs", "nba playoffs", "nhl playoffs",
        "stanley cup playoffs", "world cup knockout",
        "copa america knockout", "euro knockout",
    }

    LEAGUE_PATTERNS = {
        "regular season", "nba champion", "nhl champion", "mlb champion",
        "nfl champion", "afc champion", "nfc champion",
        "premier league winner", "la liga winner", "serie a winner",
        "bundesliga winner", "ligue 1 winner", "super lig winner",
        "division winner", "conference champion",
        "mvp", "scoring leader", "player of the year",
        "trophy winner", "award winner",
        "top scorer",
    }

    def __init__(self):
        self._cache: dict = {}   # url -> (data, timestamp)

    def _is_knockout_format(self, event_title: str, sport_tag: str) -> bool:
        """Определить формат турнира: knockout (проигравший вылетает) vs league."""
        title = event_title.lower()
        for pat in self.KNOCKOUT_PATTERNS:
            if pat in title:
                return True
        for pat in self.LEAGUE_PATTERNS:
            if pat in title:
                return False
        # Tennis — всегда knockout (турниры)
        if sport_tag == "tennis":
            return True
        # Командные виды спорта — regular season по умолчанию (не knockout)
        if sport_tag in ("nba", "nhl", "mlb", "nfl", "soccer"):
            return False
        # NCAA — knockout (March Madness)
        if sport_tag in ("ncaa", "march-madness", "college-basketball"):
            return True
        return False

    # ── HTTP ────────────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict = None) -> list | dict:
        """GET запрос к Gamma API с кешированием."""
        url = f"{GAMMA_BASE}{path}"
        if params:
            url += "?" + urlencode(params)

        # Check cache
        cached = self._cache.get(url)
        if cached and time.time() - cached[1] < CACHE_TTL:
            return cached[0]

        req = Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        try:
            with urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                self._cache[url] = (data, time.time())
                return data
        except Exception as e:
            log.error("Gamma API error: %s — %s", url, e)
            return []

    # ── Events ──────────────────────────────────────────────────────────────

    def get_events(self, tag: str = None, active: bool = True,
                   limit: int = 100, offset: int = 0) -> list[dict]:
        """Получить список событий (events). tag: tennis, soccer, nba, nhl, mma..."""
        params = {"active": str(active).lower(), "closed": "false",
                  "limit": limit, "offset": offset,
                  "order": "startDate", "ascending": "false"}
        if tag:
            params["tag_slug"] = tag
        return self._get("/events", params)

    def get_event(self, event_id: str) -> dict:
        """Получить конкретное событие по ID."""
        result = self._get(f"/events/{event_id}")
        return result if isinstance(result, dict) else {}

    # ── Markets ─────────────────────────────────────────────────────────────

    def get_markets(self, **kwargs) -> list[dict]:
        """Получить рынки с фильтрами. kwargs: event_slug, active, closed, etc."""
        params = {k: v for k, v in kwargs.items() if v is not None}
        return self._get("/markets", params)

    def get_market(self, condition_id: str) -> dict:
        """Получить конкретный рынок."""
        result = self._get(f"/markets/{condition_id}")
        return result if isinstance(result, dict) else {}

    def search_markets(self, query: str, active: bool = True) -> list[dict]:
        """Поиск рынков по тексту."""
        return self._get("/markets", {
            "active": str(active).lower(),
            "slug_contains": query.lower().replace(" ", "-"),
        })

    # ── Parsing ─────────────────────────────────────────────────────────────

    def _parse_market(self, raw: dict) -> Optional[HedgeMarket]:
        """Парсинг сырого рынка Gamma API в HedgeMarket."""
        tokens_raw = raw.get("clobTokenIds", "")
        if isinstance(tokens_raw, str):
            try:
                tokens = json.loads(tokens_raw) if tokens_raw else []
            except json.JSONDecodeError:
                tokens = []
        elif isinstance(tokens_raw, list):
            tokens = tokens_raw
        else:
            tokens = []

        if not tokens or len(tokens) < 2:
            return None

        question = raw.get("question", "")
        market_type = self._classify_market(question)

        return HedgeMarket(
            condition_id=raw.get("conditionId", raw.get("condition_id", "")),
            token_id_yes=tokens[0] if tokens else "",
            token_id_no=tokens[1] if len(tokens) > 1 else "",
            question=question,
            end_date=raw.get("endDate", raw.get("end_date_iso", "")),
            neg_risk=raw.get("negRisk", False),
            market_type=market_type,
            event_slug=raw.get("eventSlug", ""),
            slug=raw.get("slug", ""),
            outcome_prices=raw.get("outcomePrices", ""),
            tags=raw.get("tags", []),
        )

    def _classify_market(self, question: str) -> str:
        """Определить тип рынка: match или tournament."""
        q = question.lower()
        # Tournament patterns
        tournament_patterns = [
            r"win\b.*\b(tournament|championship|open|masters|cup|grand slam|atp|wta|slam|wells|wimbledon|roland|us open|australian)",
            r"(tournament|championship|open|masters|cup|grand slam).*winner",
            r"who will win the",
            r"will .+ win the ",
            r"win the \d{4}",
            r"winner of the",
            r"to win the ",
        ]
        for pat in tournament_patterns:
            if re.search(pat, q):
                return "tournament"

        # Match patterns
        match_patterns = [
            r"\bvs\.?\b",
            r"\bversus\b",
            r"\bv\b",
            r"\bmatch\b",
            r"\bbeat\b",
            r"will .+ win .+ round",
            r"will .+ advance",
        ]
        for pat in match_patterns:
            if re.search(pat, q):
                return "match"

        return "other"

    # ── Pair Discovery ──────────────────────────────────────────────────────

    def _extract_players_from_match(self, question: str) -> tuple[str, str]:
        """Извлечь имена игроков из вопроса матча."""
        q = question.strip()
        # "Will Alcaraz beat Medvedev?" → Alcaraz, Medvedev
        # "Alcaraz vs Medvedev" → Alcaraz, Medvedev
        # "Alcaraz vs. Medvedev: Who will win?" → Alcaraz, Medvedev

        # Try "X vs Y" pattern
        vs_match = re.search(r"(.+?)\s+vs\.?\s+(.+?)(?:\s*[:\?\-]|$)", q, re.IGNORECASE)
        if vs_match:
            a = vs_match.group(1).strip().rstrip(":")
            b = vs_match.group(2).strip().rstrip("?:.")
            # Remove "Will" prefix
            a = re.sub(r"^(will|does|can)\s+", "", a, flags=re.IGNORECASE).strip()
            return a, b

        # Try "Will X beat Y" pattern
        beat_match = re.search(r"will\s+(.+?)\s+(?:beat|defeat|win against)\s+(.+?)[\?\.]?$",
                               q, re.IGNORECASE)
        if beat_match:
            return beat_match.group(1).strip(), beat_match.group(2).strip()

        return "", ""

    def _extract_player_from_tournament(self, question: str) -> str:
        """Извлечь имя игрока из вопроса турнирного winner."""
        q = question.strip()
        # "Will Sinner win the Indian Wells?" → Sinner
        # "Sinner to win Indian Wells 2026?" → Sinner

        win_match = re.search(r"will\s+(.+?)\s+win\b", q, re.IGNORECASE)
        if win_match:
            return win_match.group(1).strip()

        to_win = re.search(r"(.+?)\s+to\s+win\b", q, re.IGNORECASE)
        if to_win:
            return to_win.group(1).strip()

        return ""

    def _names_match(self, name1: str, name2: str) -> bool:
        """Проверить совпадение имён игроков (нечёткое)."""
        if not name1 or not name2:
            return False
        n1 = name1.lower().strip()
        n2 = name2.lower().strip()
        if n1 == n2:
            return True
        # Одно из имён содержит другое (фамилия внутри полного имени)
        if n1 in n2 or n2 in n1:
            return True
        # Сравнить фамилии (последнее слово)
        last1 = n1.split()[-1] if n1.split() else ""
        last2 = n2.split()[-1] if n2.split() else ""
        if last1 and last2 and last1 == last2:
            return True
        return False

    def _extract_tourney_keywords(self, tourney_title: str) -> list[str]:
        """Извлечь ключевые слова турнира из названия event.
        '2026 Men\\'s Australian Open Winner' → ['australian', 'open']
        '2026 NBA Champion' → ['nba']
        """
        skip = {"2026", "2025", "2024", "men's", "women's", "winner", "champion",
                "the", "of", "men", "women", "mens", "womens", "men.s", "women.s"}
        words = re.sub(r"['\u2019]s\b", "", tourney_title.lower()).split()
        return [w for w in words if w not in skip and not w.isdigit() and len(w) > 1]

    def _tourney_names_match(self, match_tourney: str, tourney_keywords: list[str]) -> bool:
        """Проверить что турнир матча совпадает с турнирным event.
        match_tourney: 'Miami Open' (из 'Miami Open: X vs Y')
        tourney_keywords: ['miami', 'open'] (из '2026 Miami Open Winner')

        Требуем совпадение всех специфичных слов (не generic типа 'open', 'cup').
        """
        mt = match_tourney.lower()
        # Generic слова — совпадение только по ним недостаточно
        generic = {"open", "cup", "championship", "championships", "series",
                   "tournament", "finals", "classic"}
        # Специфичные ключевые слова (не generic)
        specific = [kw for kw in tourney_keywords if kw not in generic]

        if specific:
            # Все специфичные слова должны быть в названии матча
            return all(kw in mt for kw in specific)
        else:
            # Только generic слова — совпадение всех
            return all(kw in mt for kw in tourney_keywords)

    def find_hedge_pairs(self, sport_tags: list[str] = None,
                         cross_tournament: bool = False,
                         knockout_only: bool = True,
                         min_tourney_price: float = 0.03) -> list[HedgePair]:
        """
        Найти пары матч + турнирный рынок для хеджирования.

        Args:
            sport_tags: теги для поиска (tennis, nba, soccer...)
            cross_tournament: разрешить кросс-турнирные пары
            knockout_only: только knockout-турниры (где стратегия работает)
            min_tourney_price: минимальная цена tournament player (фильтр longshots)
        """
        if sport_tags is None:
            sport_tags = ["tennis", "soccer", "nba", "nhl",
                          "mlb", "mma", "nfl"]

        seen_event_ids = set()

        def _add_events(tag, limit=100):
            events = self.get_events(tag=tag, limit=limit)
            result = []
            if not isinstance(events, list):
                return result
            for e in events:
                eid = e.get("id", "")
                if eid not in seen_event_ids:
                    seen_event_ids.add(eid)
                    e["_tag"] = tag
                    result.append(e)
            return result

        # 1. Загружаем матчевые events по каждому sport tag
        match_events = []
        for tag in sport_tags:
            for e in _add_events(tag, limit=100):
                title = (e.get("title", "") or "").lower()
                if "vs" in title or "beat" in title:
                    match_events.append(e)

        # 2. Загружаем tournament events (без сортировки по дате — иначе они уходят за пределы limit)
        tournament_events = []
        params_tourney = {"active": "true", "closed": "false", "limit": 100, "tag_slug": "sports"}
        tourney_raw = self._get("/events", params_tourney)
        if isinstance(tourney_raw, list):
            for e in tourney_raw:
                title = (e.get("title", "") or "").lower()
                if "winner" in title or "champion" in title or "win the" in title:
                    eid = e.get("id", "")
                    if eid not in seen_event_ids:
                        seen_event_ids.add(eid)
                    e["_tag"] = "sports"
                    tournament_events.append(e)
        # Дополнительно ищем в sport-specific запросах
        for tag in sport_tags:
            for e in _add_events(tag, limit=100):
                title = (e.get("title", "") or "").lower()
                if "winner" in title or "champion" in title or "win the" in title:
                    tournament_events.append(e)

        log.info("Events: %d match, %d tournament (seen %d)",
                 len(match_events), len(tournament_events), len(seen_event_ids))

        # Индекс турнирных маркетов:
        # event_title -> list of (HedgeMarket, player_name, tourney_keywords, is_knockout, te_title_lower)
        tourney_by_event: dict[str, list[tuple[HedgeMarket, str, list[str], bool]]] = {}
        # Карта: event_title -> sport keywords для кросс-спорт фильтрации
        tourney_sport_hints: dict[str, str] = {}  # te_title -> detected sport
        skipped_knockout = 0
        skipped_longshot = 0
        for te in tournament_events:
            te_title = te.get("title", "")
            te_tag = te.get("_tag", "")
            te_keywords = self._extract_tourney_keywords(te_title)
            is_ko = self._is_knockout_format(te_title, te_tag)

            # Фильтр: пропускаем league-формат если knockout_only
            if knockout_only and not is_ko:
                skipped_knockout += 1
                continue

            for m in te.get("markets", []):
                parsed = self._parse_market(m)
                if not parsed:
                    continue

                # Фильтр longshots: пропускаем если цена < min_tourney_price
                try:
                    prices = json.loads(parsed.outcome_prices) if parsed.outcome_prices else []
                    if prices and float(prices[0]) < min_tourney_price:
                        skipped_longshot += 1
                        continue
                except (json.JSONDecodeError, ValueError, IndexError):
                    pass

                player = self._extract_player_from_tournament(
                    parsed.question or m.get("question", ""))
                if player:
                    if te_title not in tourney_by_event:
                        tourney_by_event[te_title] = []
                    tourney_by_event[te_title].append((parsed, player, te_keywords, is_ko))

        log.info("Tournament index: %d events, skipped %d league + %d longshots",
                 len(tourney_by_event), skipped_knockout, skipped_longshot)

        # Сопоставляем матчи с турнирными рынками
        pairs = []
        seen_pair_ids = set()
        team_sports = {"nba", "nfl", "nhl", "mlb", "soccer", "mma",
                       "ncaa", "college-basketball", "march-madness"}

        for me in match_events:
            me_title = me.get("title", "")
            me_tag = me.get("_tag", "")
            markets_raw = me.get("markets", [])
            is_team_sport = me_tag in team_sports

            # Извлекаем название турнира из матча (до первого ":")
            match_tourney_name = me_title.split(":")[0].strip() if ":" in me_title else ""

            # Ищем ТОЛЬКО match winner маркет
            for m in markets_raw:
                q = (m.get("question", "") or "").lower()
                if any(x in q for x in ["total", "o/u", "set 1", "set 2",
                       "handicap", "spread", "over", "under"]):
                    continue
                if "vs" not in q:
                    continue

                parsed = self._parse_market(m)
                if not parsed:
                    continue
                parsed.market_type = "match"

                player_a, player_b = self._extract_players_from_match(
                    parsed.question or m.get("question", ""))
                if not player_a or not player_b:
                    continue

                # Для каждого tournament event — проверяем совместимость
                for te_title, te_markets in tourney_by_event.items():
                    if not te_markets:
                        continue
                    te_keywords = te_markets[0][2]
                    te_is_ko = te_markets[0][3]

                    # Кросс-спорт фильтр: MLB матч не должен линковаться с NCAA tournament
                    te_lower = te_title.lower()
                    sport_compat = True
                    if me_tag == "mlb" and "ncaa" in te_lower:
                        sport_compat = False
                    elif me_tag == "ncaa" and ("nba" in te_lower or "nhl" in te_lower or "mlb" in te_lower):
                        sport_compat = False
                    elif me_tag == "nba" and ("ncaa" in te_lower or "nhl" in te_lower):
                        sport_compat = False
                    elif me_tag == "nhl" and ("nba" in te_lower or "ncaa" in te_lower):
                        sport_compat = False
                    if not sport_compat:
                        continue

                    # Матчинг: для team sports — проверяем что хотя бы один
                    # матчевый игрок есть в tournament event
                    if is_team_sport:
                        has_match_player = any(
                            self._names_match(player_a, tp) or self._names_match(player_b, tp)
                            for _, tp, _, _ in te_markets
                        )
                        if not has_match_player:
                            continue
                    else:
                        if match_tourney_name and te_keywords:
                            if not self._tourney_names_match(match_tourney_name, te_keywords):
                                if not cross_tournament:
                                    continue

                    # Для каждого tournament player создаём пару
                    for tm, t_player, _, is_ko in te_markets:
                        pair_id = f"{parsed.condition_id}_{tm.condition_id}"
                        if pair_id in seen_pair_ids:
                            continue
                        seen_pair_ids.add(pair_id)

                        pair = HedgePair(
                            pair_id=pair_id,
                            sport=me_tag,
                            event_name=te_title,
                            player_a=player_a,
                            player_b=player_b,
                            match_market=parsed,
                            tournament_market=tm,
                            tournament_player=t_player,
                            is_knockout=is_ko,
                        )
                        pairs.append(pair)

        log.info("Найдено %d hedge-пар по тегам %s", len(pairs), sport_tags)
        return pairs

    def clear_cache(self):
        """Очистить кэш запросов."""
        self._cache.clear()
