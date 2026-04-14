# -*- coding: utf-8 -*-
"""
Вэлью-бет бот — автоматические ставки на Polymarket.

Цикл работы:
  1. Каждые POLL_INTERVAL секунд запрашивает BetBurger API
  2. Фильтрует только Polymarket беты с нужным edge
  3. Проверяет дубли: in-memory кэш + БД (по outcome_id)
  4. Рассчитывает размер ставки (flat % банкролла или half-Kelly)
  5. Размещает GTC лимитный ордер на Polymarket CLOB
  6. Записывает в БД: параметры, order_id, статус
  7. После размещения outcome_id добавляется в кэш — повтор не произойдёт

Дедупликация:
  - Ставка НЕ повторяется если:
    a) outcome_id уже в in-memory кэше этой сессии
    b) outcome_id уже в БД со статусом pending/placed И outcome_result=pending
  - После settle (won/lost/void) — исход снова доступен (для других матчей)
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import aiohttp

log = logging.getLogger(__name__)

POLYMARKET_ID = 483
PREMATCH_URL  = "https://rest-api-pr.betburger.com/api/v1/valuebets/bot_pro_search"
LIVE_URL      = "https://rest-api-lv.betburger.com/api/v1/valuebets/bot_pro_search"


class ValueBetBot:
    def __init__(self):
        import os
        # Перечитываем .env при каждом старте бота
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)
        except ImportError:
            pass

        # Читаем конфиг — поддерживаем любую версию config.py
        # через getattr+os.getenv чтобы не падать на старых файлах
        try:
            import config as cfg_module
            # Сбрасываем кэш модуля чтобы подхватить свежий .env
            import importlib
            importlib.reload(cfg_module)
            cfg = cfg_module.Config()
        except Exception:
            cfg = None

        def _get(attr, env_key, default=""):
            """Читает из cfg или прямо из env"""
            if cfg is not None:
                v = getattr(cfg, attr, None)
                if v is not None and v != "":
                    return v
            return os.getenv(env_key, default)

        def _getf(attr, env_key, default=0.0):
            try:
                return float(_get(attr, env_key, str(default)))
            except Exception:
                return default

        def _getb(attr, env_key, default=False):
            v = _get(attr, env_key, str(default))
            return str(v).lower() in ("true", "1", "yes")

        # Собираем все нужные параметры
        self._bb_email    = _get("BETBURGER_EMAIL",              "BETBURGER_EMAIL")
        self._bb_password = _get("BETBURGER_PASSWORD",           "BETBURGER_PASSWORD")
        self._bb_token    = _get("BETBURGER_TOKEN",              "BETBURGER_TOKEN")
        self._filter_id   = int(_get("BETBURGER_FILTER_ID_VALUEBET", "BETBURGER_FILTER_ID_VALUEBET",
                                     _get("BETBURGER_FILTER_ID", "BETBURGER_FILTER_ID", "0")) or "0")
        self._pm_key      = _get("POLYMARKET_PRIVATE_KEY",       "POLYMARKET_PRIVATE_KEY")
        self._pm_funder   = _get("POLYMARKET_FUNDER",            "POLYMARKET_FUNDER")
        self._db_path     = _get("DB_PATH_VALUEBET",             "DB_PATH_VALUEBET",    "valuebets.db")

        # Настройки торговли
        self.cfg = cfg  # для совместимости с остальным кодом

        class _FallbackCfg:
            pass
        if cfg is None:
            cfg = _FallbackCfg()

        # Гарантируем все нужные атрибуты с дефолтами
        if not hasattr(cfg, "VB_MIN_ROI"):        cfg.VB_MIN_ROI        = _getf("VB_MIN_ROI",        "VB_MIN_ROI",        0.04)
        if not hasattr(cfg, "MIN_LIQUIDITY"):      cfg.MIN_LIQUIDITY     = _getf("MIN_LIQUIDITY",      "MIN_LIQUIDITY",     50.0)
        if not hasattr(cfg, "VB_MIN_STAKE"):       cfg.VB_MIN_STAKE      = _getf("VB_MIN_STAKE",       "VB_MIN_STAKE",      2.0)
        if not hasattr(cfg, "VB_STAKE_PCT"):       cfg.VB_STAKE_PCT      = _getf("VB_STAKE_PCT",       "VB_STAKE_PCT",      0.01)
        if not hasattr(cfg, "VB_MAX_STAKE_PCT"):   cfg.VB_MAX_STAKE_PCT  = _getf("VB_MAX_STAKE_PCT",   "VB_MAX_STAKE_PCT",  0.05)
        if not hasattr(cfg, "VB_USE_KELLY"):       cfg.VB_USE_KELLY      = _getb("VB_USE_KELLY",       "VB_USE_KELLY",      False)
        if not hasattr(cfg, "VB_MAX_ODDS"):        cfg.VB_MAX_ODDS       = _getf("VB_MAX_ODDS",        "VB_MAX_ODDS",        0.0)
        if not hasattr(cfg, "POLL_INTERVAL"):      cfg.POLL_INTERVAL     = int(_getf("POLL_INTERVAL",  "POLL_INTERVAL",     5))
        self.cfg = cfg

        # BetBurger авторизация
        # Приоритет: BETBURGER_TOKEN (статичный) > email+password (автообновление)
        self._auth = None
        if self._bb_token:
            # Есть готовый токен — используем его напрямую, логин не нужен
            log.info("BetBurger: статичный токен %s...", self._bb_token[:8])
        elif self._bb_email and self._bb_password:
            # Нет токена — пробуем автологин
            from betburger_auth import BetBurgerAuth
            self._auth = BetBurgerAuth(self._bb_email, self._bb_password)
            log.info("BetBurger: автообновление токена через %s", self._bb_email)
        else:
            raise ValueError("Нет BETBURGER_TOKEN или BETBURGER_EMAIL+PASSWORD в .env")

        # Polymarket CLOB
        if not self._pm_key or not self._pm_funder:
            raise ValueError("Нет POLYMARKET_PRIVATE_KEY или POLYMARKET_FUNDER в .env")
        from polymarket_client import PolymarketClient
        self.pm = PolymarketClient(self._pm_key, self._pm_funder)

        # БД с дедупликацией
        from db_bets import BetDatabase
        self.db = BetDatabase(self._db_path)

        # in-memory кэш outcome_id → timestamp (для скорости без запросов к БД)
        self._placed_outcomes: dict[str, float] = {}  # outcome_id → placed_at

        # Загружаем активные ставки из БД в кэш при старте
        self._load_active_from_db()

        # Прематч ордера ожидающие отмены по TTL: {order_id: (cancel_at, bet_rec_id, started_at)}
        self._pending_cancel: dict[str, tuple[float, int, int]] = {}

        # ── Resell (авто-продажа с наценкой) ─────────────────────────────────
        # Заполненные BUY ожидающие размещения SELL:
        # {buy_order_id: (cancel_at, rec_id, started_at, token_id, neg_risk, tick_size, sell_price, size)}
        self._pending_resell: dict[str, tuple] = {}
        # Активные SELL ордера:
        # {sell_order_id: (cancel_at, rec_id, buy_order_id, entry_price, sell_price, size, token_id, neg_risk, tick_size)}
        self._active_sells: dict[str, tuple] = {}
        self._load_active_resells()

        # Раннее обнаружение заполнения ордеров
        self._last_fill_poll = 0   # timestamp последней проверки fills
        self._fill_poll_interval = 30  # проверять каждые 30 сек

        # Line movement tracking
        self._last_line_sample = 0
        self._line_sample_interval = 60  # каждые 60 сек

        # Счётчики для лога
        self._ticks          = 0
        self._bets_placed    = 0
        self._bets_skipped   = 0

    def _load_active_from_db(self):
        """При старте загружаем активные ставки в in-memory кэш"""
        active = self.db.get_active_bets()
        for rec in active:
            self._placed_outcomes[rec.outcome_id] = time.time()
        if active:
            log.info("Загружено %d активных ставок из БД в кэш", len(active))

    def _load_active_resells(self):
        """При старте загружаем активные resell из БД"""
        try:
            resells = self.db.get_active_resells()
            for rec in resells:
                started = int(getattr(rec, "started_at", 0) or 0)
                cancel_at = max(time.time() + 60, started - 60) if started > 0 else time.time() + 3600
                oid = getattr(rec, "outcome_id", "")
                soid = getattr(rec, "sell_order_id", "")
                sp = float(getattr(rec, "sell_price_target", 0) or 0)
                stk = float(getattr(rec, "stake", 0) or 0)
                ep = float(getattr(rec, "stake_price", 0) or 0)
                neg = bool(getattr(rec, "neg_risk", 0))
                buy_oid = getattr(rec, "order_id", "")

                if getattr(rec, "resell_status", "") == "selling" and soid:
                    self._active_sells[soid] = (cancel_at, rec.id, buy_oid, ep, sp, stk, oid, neg, "0.01")
                elif getattr(rec, "resell_status", "") == "pending_sell":
                    self._pending_resell[buy_oid] = (cancel_at, rec.id, started, oid, neg, "0.01", sp, stk)
            if resells:
                log.info("Загружено %d активных resell из БД", len(resells))
        except Exception as e:
            log.debug("_load_active_resells: %s", e)

    # ── Resell хелперы ────────────────────────────────────────────────────────

    _env_ts = 0  # timestamp последнего чтения .env

    def _reload_env(self):
        """Перечитывает .env файл (макс раз в 5 сек) чтобы подхватить изменения из дашборда."""
        now = time.time()
        if now - self._env_ts < 5:
            return
        self._env_ts = now
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)
        except ImportError:
            pass

    def _should_resell(self) -> bool:
        """Проверяет включён ли resell (перечитывает .env для runtime toggle)."""
        import os
        self._reload_env()
        return os.getenv("VB_RESELL_ENABLED", "false").lower() in ("true", "1", "yes")

    def _get_resell_markup(self) -> float:
        """Наценка в % (аддитивная)."""
        import os
        self._reload_env()
        try:
            return float(os.getenv("VB_RESELL_MARKUP", "2"))
        except Exception:
            return 2.0

    def _get_resell_fallback(self) -> str:
        """Что делать если SELL не исполнен: keep или market_sell."""
        import os
        self._reload_env()
        return os.getenv("VB_RESELL_FALLBACK", "keep")

    # sport_id киберспорта (для фильтра карт)
    ESPORT_IDS = {21, 39, 41, 46, 47, 48, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59,
                  60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71}

    def _is_excluded_sport(self, pm) -> bool:
        """Проверяет исключён ли спорт/лига/карта по настройкам.
        EXCLUDED_SPORTS: sport_id через запятую (напр. '47,48,21')
        EXCLUDED_LEAGUES: подстроки лиг через запятую (напр. 'Counter-Strike,ESL')
        ESPORT_MAX_MAP: макс. номер карты (по умолч. 3, т.е. карта 4+ блокируется)
        """
        import os
        self._reload_env()

        # Исключение по sport_id
        excluded_ids = os.getenv("EXCLUDED_SPORTS", "").strip()
        if excluded_ids:
            try:
                ids = {int(x.strip()) for x in excluded_ids.split(",") if x.strip().isdigit()}
                if getattr(pm, "sport_id", 0) in ids:
                    log.info("skip [excluded sport_id=%d] %s  %s",
                             pm.sport_id, pm.match_name, pm.league)
                    return True
            except Exception:
                pass

        # Исключение по подстроке в league
        excluded_leagues = os.getenv("EXCLUDED_LEAGUES", "").strip()
        if excluded_leagues:
            league_lower = (getattr(pm, "league", "") or "").lower()
            for pattern in excluded_leagues.split(","):
                pattern = pattern.strip().lower()
                if pattern and pattern in league_lower:
                    log.info("skip [excluded league '%s'] %s  %s",
                             pattern, pm.match_name, pm.league)
                    return True

        # Фильтр карт в киберспорте (cancel arb protection)
        sport_id = getattr(pm, "sport_id", 0)
        if sport_id in self.ESPORT_IDS:
            max_map = int(os.getenv("ESPORT_MAX_MAP", "3") or "3")
            if max_map > 0:
                param = getattr(pm, "market_param", 0) or 0
                outcome = (getattr(pm, "outcome_name", "") or "").lower()
                # market_param = номер карты (4, 5, ...) или
                # outcome_name содержит "map 4", "map 5", "карта 4"
                if param > max_map:
                    log.info("skip [esport map %d > max %d] %s  %s",
                             int(param), max_map, pm.match_name, pm.league)
                    return True
                import re
                map_match = re.search(r'map\s*(\d+)|карт[аеы]\s*(\d+)', outcome)
                if map_match:
                    map_num = int(map_match.group(1) or map_match.group(2))
                    if map_num > max_map:
                        log.info("skip [esport '%s' map %d > max %d] %s  %s",
                                 outcome, map_num, max_map, pm.match_name, pm.league)
                        return True

        return False

    # ──────────────────────────────────────────────────────────────────────────
    # Основной цикл
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self):
        log.info("=" * 60)
        log.info("📈 ValueBet бот запущен")
        log.info("   MIN_EDGE    = %.1f%%", self.cfg.VB_MIN_ROI * 100)
        log.info("   MIN_LIQ     = $%.0f", self.cfg.MIN_LIQUIDITY)
        log.info("   STAKE       = %.1f%% банкролла%s",
                 self.cfg.VB_STAKE_PCT * 100,
                 " (Half Kelly)" if self.cfg.VB_USE_KELLY else "")
        log.info("   POLL        = %ds", self.cfg.POLL_INTERVAL)
        log.info("=" * 60)

        while True:
            try:
                await self.tick()
            except Exception as e:
                log.error("Критическая ошибка в tick: %s", e, exc_info=True)
            await asyncio.sleep(self.cfg.POLL_INTERVAL)

    async def tick(self):
        self._ticks += 1
        await self._poll_order_fills()
        await self._process_pending_cancels()
        await self._process_pending_resells()
        await self._process_active_sells()
        await self._sample_line_movements()
        items = await self._fetch_valuebets()

        if not items:
            log.debug("[tick %d] Нет Polymarket бетов", self._ticks)
            return

        log.info("[tick %d] Получено %d Polymarket бетов", self._ticks, len(items))

        for bet, arb_meta in items:
            try:
                await self._process(bet, arb_meta)
            except Exception as e:
                log.error("process ошибка: %s", e, exc_info=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Загрузка данных с BetBurger
    # ──────────────────────────────────────────────────────────────────────────

    async def _get_token(self) -> str:
        if self._auth:
            return await self._auth.get_token()
        return self._bb_token

    async def _fetch_valuebets(self) -> list[tuple[dict, dict]]:
        """Возвращает список (bet_dict, arb_meta_dict) только для Polymarket"""
        token     = await self._get_token()
        filter_id = self._filter_id

        results = []
        async with aiohttp.ClientSession() as session:
            for url in [PREMATCH_URL]:  # добавить LIVE_URL для live
                try:
                    fdata = aiohttp.FormData()
                    fdata.add_field("search_filter[]", str(filter_id))
                    fdata.add_field("per_page", "100")
                    fdata.add_field("sort_by", "percent")  # сортировка по edge

                    async with session.post(
                        url,
                        params  = {"access_token": token, "locale": "en"},
                        data    = fdata,
                        timeout = aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status == 401:
                            log.error("BetBurger 401 — токен невалиден")
                            if self._auth:
                                self._auth._token = None  # сбрасываем кэш
                            return []
                        if resp.status != 200:
                            log.error("BetBurger HTTP %d", resp.status)
                            continue

                        raw  = await resp.json()
                        bets = raw.get("bets", []) if isinstance(raw, dict) else raw
                        arbs = raw.get("arbs", []) if isinstance(raw, dict) else []

                        # Сохраняем сырой ответ для диагностики
                        try:
                            import json as _json, pathlib, datetime
                            _out = pathlib.Path(self._db_path).parent / "betburger_last_raw.json"
                            _out.write_text(_json.dumps({
                                "saved_at": datetime.datetime.now().isoformat(),
                                "data": raw
                            }, ensure_ascii=False, indent=2), encoding="utf-8")
                        except Exception:
                            pass

                        # source.valueBets[] — правильный источник велью%
                        # (arbs[].percent = доходность шурбета, НЕ велью одного исхода)
                        # source.valueBets[].percent = велью% конкретного outcome
                        source = raw.get("source", {}) if isinstance(raw, dict) else {}
                        source_vb_by_id: dict[str, dict] = {}
                        for vb in (source.get("valueBets") or []):
                            bid = vb.get("bet_id") or vb.get("id")
                            if bid:
                                source_vb_by_id[bid] = vb

                        # Индекс bet_id → arb (для вспомогательных данных: min/max koef, paused и т.д.)
                        arb_by_bet: dict[str, dict] = {}
                        for arb in arbs:
                            for key in ("bet1_id", "bet2_id", "bet3_id"):
                                bid = arb.get(key)
                                if bid:
                                    arb_by_bet[bid] = arb

                        for bet in bets:
                            if bet.get("bookmaker_id") == POLYMARKET_ID:
                                bid = bet.get("id", "")
                                arb = arb_by_bet.get(bid, {})
                                svb = source_vb_by_id.get(bid, {})
                                # Передаём arb_meta с правильным percent из source.valueBets
                                arb_meta = dict(arb)
                                if svb.get("percent") is not None:
                                    arb_meta["percent"] = svb["percent"]
                                    arb_meta["avg_koef"] = svb.get("avg_koef")
                                results.append((bet, arb_meta))

                except aiohttp.ClientError as e:
                    log.error("BetBurger сетевая ошибка: %s", e)
                except Exception as e:
                    log.error("BetBurger fetch: %s", e, exc_info=True)

        return results

    # ──────────────────────────────────────────────────────────────────────────
    # Обработка одного бета
    # ──────────────────────────────────────────────────────────────────────────

    async def _process(self, bet: dict, arb_meta: dict):
        from polymarket_bet import from_betburger

        pm = from_betburger(bet, arb_meta)
        if pm is None:
            return

        # ── ФИЛЬТРЫ ───────────────────────────────────────────────────────────

        # 1. Edge (value%)
        if pm.value_pct / 100 < self.cfg.VB_MIN_ROI:
            log.debug("skip [low edge] %s  edge=%.2f%% < %.1f%%",
                      pm.match_name, pm.value_pct, self.cfg.VB_MIN_ROI * 100)
            return

        # 1b. Макс. edge — защита от cancel-арбитража и ошибок линий
        try:
            import os as _os
            max_edge = float(_os.getenv("VB_MAX_EDGE", "0") or "0")
        except Exception:
            max_edge = 0
        if max_edge > 0 and pm.value_pct > max_edge:
            log.info("skip [HIGH edge] %s  edge=%.2f%% > max=%.1f%%  (возможный cancel-арб)",
                     pm.match_name, pm.value_pct, max_edge)
            return

        # 1c. Исключение спортов / лиг
        if self._is_excluded_sport(pm):
            return

        # 2. Макс. коэффициент (VB_MAX_ODDS = 0 → без ограничений)
        # Читаем напрямую из env чтобы сразу подхватывать изменения из дашборда
        try:
            import os as _os
            max_odds = float(_os.getenv("VB_MAX_ODDS", "0") or "0")
        except Exception:
            max_odds = getattr(self.cfg, 'VB_MAX_ODDS', 0.0)
        if max_odds and max_odds > 1.0 and pm.bb_odds > max_odds:
            log.debug("skip [high odds] %s  odds=%.2f > max=%.2f",
                      pm.match_name, pm.bb_odds, max_odds)
            return

        # 3. outcome_id обязателен для ставки
        if not pm.outcome_id:
            log.warning("skip [no outcome_id] %s | %s", pm.match_name, pm.outcome_name)
            return

        # 3. Ликвидность
        avail_liq = pm.depth_at_price or pm.total_liquidity or 0
        if avail_liq < self.cfg.MIN_LIQUIDITY:
            log.debug("skip [low liq] %s  $%.0f < $%.0f",
                      pm.match_name, avail_liq, self.cfg.MIN_LIQUIDITY)
            return

        # 4. Матч ещё не начался (буфер 5 минут)
        now_ts = time.time()
        if pm.started_at > 0 and pm.started_at < now_ts - 300:
            log.debug("skip [started] %s  started=%s",
                      pm.match_name, pm.start_dt)
            return

        # ── ДЕДУПЛИКАЦИЯ ──────────────────────────────────────────────────────

        # 4a. In-memory кэш (быстро)
        if pm.outcome_id in self._placed_outcomes:
            log.debug("skip [in-memory dup] %s | %s", pm.match_name, pm.outcome_name)
            return

        # 4b. БД (персистентно — защита после рестарта)
        existing = self.db.already_bet(pm.outcome_id)
        if existing:
            log.debug("skip [db dup] %s | %s | #%d %s",
                      pm.match_name, pm.outcome_name, existing.id, existing.status)
            # Добавляем в in-memory чтобы не запрашивать БД снова
            self._placed_outcomes[pm.outcome_id] = time.time()
            return

        # ── РАСЧЁТ СТАВКИ ─────────────────────────────────────────────────────

        stake = self._calc_stake(pm)
        min_stake = getattr(self.cfg, "VB_MIN_STAKE", 1.0)
        if stake < min_stake:
            log.info("skip [stake too small] $%.2f < $%.2f  %s",
                     stake, min_stake, pm.match_name)
            return

        # ── ЛОГИРУЕМ ВОЗМОЖНОСТЬ ──────────────────────────────────────────────

        log.info("")
        log.info("━" * 60)
        log.info("🎯  %s", pm.match_name)
        log.info("    %s  |  %s", pm.league, pm.start_dt)
        log.info("    ИСХОД:   %s  (%s%s)",
                 pm.outcome_name, pm.market_type_name,
                 f"  линия={pm.market_param}" if pm.market_param else "")
        log.info("    EDGE:    +%.2f%%", pm.value_pct)
        log.info("    КОЭФ:    %.4f   implied %.1f%%", pm.bb_odds, pm.bb_price * 100)
        log.info("    ЛИК:     $%.0f (рынок)  $%.0f (стакан)",
                 pm.total_liquidity, pm.depth_at_price)
        if pm.order_book:
            log.info("    СТАКАН:  %s", "  |  ".join(
                f"{lvl.odds:.3f}→${lvl.size:.0f}" for lvl in pm.order_book[:4]
            ))
        log.info("    СТАВКА:  $%.2f @ price=%.4f", stake, pm.bb_price)
        log.info("    TOKEN:   %s...%s", pm.outcome_id[:16], pm.outcome_id[-8:])

        # ── ЗАПИСЫВАЕМ НАМЕРЕНИЕ В БД ─────────────────────────────────────────

        rec_id = self.db.insert_bet(pm, stake=stake, stake_price=pm.bb_price,
                                    status="pending")

        # ── СРАЗУ ДОБАВЛЯЕМ В КЭШИ (до ответа PM — чтобы параллельный tick не дублировал) ──

        self._placed_outcomes[pm.outcome_id] = time.time()

        # ── РАЗМЕЩАЕМ НА POLYMARKET ───────────────────────────────────────────

        # Определяем tick_size:
        # Polymarket использует 0.01 для большинства маркетов
        # Для negRisk маркетов и некоторых спортивных тоже 0.01
        tick_size = "0.01"

        result = await self.pm.place_order(
            token_id  = pm.outcome_id,
            price     = pm.bb_price,
            size      = stake,
            neg_risk  = pm.neg_risk,
            tick_size = tick_size,
        )

        # ── ОБНОВЛЯЕМ БД ──────────────────────────────────────────────────────

        if result.success:
            self.db.update_placed(
                rec_id,
                order_id    = result.bet_id or "",
                status      = "placed",
                stake_price = pm.bb_price,
            )
            # Записываем fee rate
            try:
                fee_rate = self.pm.get_fee_rate(pm.outcome_id)
                if fee_rate > 0:
                    fee_usdc = round(stake * pm.bb_price * fee_rate, 4)
                    self.db.conn.execute(
                        "UPDATE bets SET fee_rate=?, fee_usdc=? WHERE id=?",
                        (fee_rate, fee_usdc, rec_id))
                    self.db.conn.commit()
                    log.info("[fee] Ставка #%d: fee_rate=%.4f (%.2f%%), fee=$%.4f",
                             rec_id, fee_rate, fee_rate * 100, fee_usdc)
            except Exception:
                pass
            # Вычитаем cost_usdc из свободного баланса
            cost_usdc = round(stake * pm.bb_price, 2)
            self.db.adjust_free_usdc(-cost_usdc)
            self._bets_placed += 1
            # Планируем автоотмену
            # TTL = min(PM_ORDER_TTL_SECS, время до начала матча - 60сек)
            base_ttl = self.cfg.PM_ORDER_TTL_SECS
            if pm.started_at and pm.started_at > 0:
                secs_to_start = int(pm.started_at) - int(time.time())
                if secs_to_start > 120:
                    # Оставляем буфер 60 сек до начала
                    effective_ttl = min(base_ttl, secs_to_start - 60)
                else:
                    effective_ttl = max(30, secs_to_start - 10)
            else:
                effective_ttl = base_ttl
            cancel_at = time.time() + effective_ttl
            self._pending_cancel[result.bet_id] = (cancel_at, rec_id, pm.started_at or 0)
            log.info("\u2705  ПОСТАВЛЕНО  $%.2f @ %.4f | order=%s | отмена через %ds",
                     stake, pm.bb_price,
                     (result.bet_id or "")[:24], effective_ttl)
        else:
            self.db.update_failed(rec_id, error_msg=result.error or "unknown")
            # Если ошибка временная — убираем из кэша чтобы попробовать снова
            if self._is_retryable_error(result.error):
                del self._placed_outcomes[pm.outcome_id]
                log.warning("❌  ОШИБКА (retry) %s: %s", pm.match_name, result.error)
            else:
                log.error("❌  ОШИБКА (final) %s: %s", pm.match_name, result.error)

        self._bets_skipped = 0  # сброс для статистики

        log.info("━" * 60)

    # ──────────────────────────────────────────────────────────────────────────
    # Line Movement — мониторинг движения цены после ставки
    # ──────────────────────────────────────────────────────────────────────────

    async def _sample_line_movements(self):
        """Раз в 60с снимает best ask для всех placed прематч ставок.
        Best ask = цена по которой можно купить прямо сейчас (реальная рыночная цена)."""
        now = time.time()
        if now - self._last_line_sample < self._line_sample_interval:
            return
        self._last_line_sample = now

        try:
            bets_to_track = self.db.line_get_bets_to_track()
        except Exception:
            return

        if not bets_to_track:
            return

        for bet in bets_to_track:
            try:
                price = self.pm.get_best_ask(bet["outcome_id"])
                if price is None or price <= 0:
                    # fallback на midpoint
                    price = self.pm.get_midpoint(bet["outcome_id"])
                if price is None or price <= 0:
                    continue
                mid = price  # переменная для совместимости
                placed_at_str = bet.get("placed_at", "")
                if placed_at_str:
                    from datetime import datetime, timezone
                    try:
                        pa = datetime.fromisoformat(placed_at_str.replace("Z", "+00:00"))
                        minutes_after = int((now - pa.timestamp()) / 60)
                    except Exception:
                        minutes_after = 0
                else:
                    minutes_after = 0

                self.db.line_record_snapshot(bet["id"], now, mid, minutes_after)
            except Exception:
                pass  # не ломаем бот из-за tracking

    # ──────────────────────────────────────────────────────────────────────────
    # Раннее обнаружение заполненных ордеров (до TTL)
    # ──────────────────────────────────────────────────────────────────────────

    async def _poll_order_fills(self):
        """Проверяет все pending ордера на заполнение каждые N сек.
        Без этого resell trigger срабатывает только при истечении TTL."""
        now = time.time()
        if now - self._last_fill_poll < self._fill_poll_interval:
            return
        self._last_fill_poll = now

        if not self._pending_cancel:
            return

        filled_orders = []
        for order_id, (cancel_at, rec_id, started_at) in list(self._pending_cancel.items()):
            try:
                order_info = self._get_order_info(order_id)
                status_raw = (order_info.get("status") or "").upper()
                size_matched = float(order_info.get("size_matched") or order_info.get("sizeMatched") or 0)
                size_remaining = float(order_info.get("size_remaining") or order_info.get("sizeRemaining") or 0)
                original_size = float(order_info.get("original_size") or order_info.get("originalSize") or 0)
                price = float(order_info.get("price") or 0)

                # Только MATCHED/FILLED статус = надёжный полный fill
                if status_raw in ("MATCHED", "FILLED"):
                    log.info("[poll] ✅ Ордер %s полностью заполнен (%.4f shares) — обнаружен до TTL",
                             order_id[:16], size_matched)
                    filled_orders.append((order_id, size_matched, price, False))
                # LIVE с partial fill — НЕ трогаем, ждём TTL или полный fill
                elif status_raw == "LIVE" and size_matched > 0:
                    log.debug("[poll] ⏳ Ордер %s LIVE partial (%.4f/%.4f) — ждём",
                              order_id[:16], size_matched, original_size)
            except Exception as e:
                log.debug("[poll] Ошибка проверки %s: %s", order_id[:16], e)

        for order_id, size_matched, price, partial in filled_orders:
            self._pending_cancel.pop(order_id, None)
            self._update_bet_filled(order_id, size_matched, price, partial=partial)

    # ──────────────────────────────────────────────────────────────────────────
    # Автоотмена прематч ордеров по TTL
    # ──────────────────────────────────────────────────────────────────────────

    async def _process_pending_cancels(self):
        """Проверяет и отменяет прематч ордера у которых истёк TTL"""
        if not self._pending_cancel:
            return

        now = time.time()
        to_cancel = [oid for oid, (cancel_at, _, _) in self._pending_cancel.items()
                     if now >= cancel_at]

        for order_id in to_cancel:
            cancel_at, rec_id, started_at = self._pending_cancel.pop(order_id)
            try:
                # Получаем статус ордера из CLOB
                order_info     = self._get_order_info(order_id)
                status_raw     = (order_info.get("status") or "").upper()
                size_matched   = float(order_info.get("size_matched")   or order_info.get("sizeMatched")   or 0)
                size_remaining = float(order_info.get("size_remaining") or order_info.get("sizeRemaining") or 0)
                price          = float(order_info.get("price") or 0)

                log.info("[PM] Ордер %s...%s: status=%s matched=%.4f remaining=%.4f",
                         order_id[:12], order_id[-6:], status_raw, size_matched, size_remaining)

                # Полностью исполнен — не трогаем
                if status_raw in ("MATCHED", "FILLED") or (size_matched > 0 and size_remaining == 0):
                    log.info("[PM] ✅ Ордер %s полностью исполнен (%.4f shares) — оставляем",
                             order_id[:16], size_matched)
                    self._update_bet_filled(order_id, size_matched, price, partial=False)
                    continue

                # Отменяем ордер (или остаток при частичном исполнении)
                cancelled = self.pm.cancel_order(order_id)

                if size_matched > 0:
                    # Частичное исполнение — фиксируем реальный объём
                    log.info("[PM] ⏱ Ордер %s частично исполнен (%.4f shares) — остаток отменён",
                             order_id[:16], size_matched)
                    self._update_bet_filled(order_id, size_matched, price, partial=True)
                elif cancelled:
                    # Ничего не исполнилось — отмена полная
                    log.info("[PM] ⏱ Ордер %s отменён по TTL (0 исполнено) — ставка void",
                             order_id[:16])
                    self.db.conn.execute(
                        "UPDATE bets SET status='cancelled', outcome_result='void', "
                        "error_msg='PM_TTL_expired' WHERE order_id=? AND status='placed'",
                        (order_id,)
                    )
                    self.db.conn.commit()
                    # Возвращаем весь cost в баланс
                    row = self.db.conn.execute(
                        "SELECT stake, stake_price FROM bets WHERE order_id=?", (order_id,)
                    ).fetchone()
                    if row:
                        refund = round(float(row["stake"] or 0) * float(row["stake_price"] or 0), 2)
                        if refund > 0:
                            self.db.adjust_free_usdc(refund)
                            log.info("[PM] 💰 Возвращено $%.2f в баланс (полная отмена TTL)", refund)
                else:
                    # cancel_order вернул False — проверяем снова
                    log.warning("[PM] Не удалось отменить ордер %s — перечитываем статус", order_id[:16])
                    order_info2  = self._get_order_info(order_id)
                    size_matched2 = float(order_info2.get("size_matched") or order_info2.get("sizeMatched") or 0)
                    if size_matched2 > 0:
                        self._update_bet_filled(order_id, size_matched2, price, partial=False)

            except Exception as e:
                log.error("[PM] Ошибка при отмене ордера %s: %s", order_id[:16], e)

    def _get_order_info(self, order_id: str) -> dict:
        """Получает статус ордера из CLOB API"""
        try:
            client = self.pm._get_client()
            resp = client.get_order(order_id)
            return resp if isinstance(resp, dict) else {}
        except Exception as e:
            log.debug("[PM] get_order_info %s: %s", order_id[:16], e)
            return {}

    def _update_bet_filled(self, order_id: str, size_matched: float, price: float, partial: bool):
        """
        Обновляет ставку по реальному объёму исполнения.
        partial=True  → частично исполнена, пересчитываем stake и возвращаем остаток в баланс
        partial=False → полностью исполнена, просто обновляем stake если надо
        """
        try:
            row = self.db.conn.execute(
                "SELECT id, stake, stake_price FROM bets WHERE order_id=?", (order_id,)
            ).fetchone()
            if not row:
                return

            bet_id     = row["id"]
            orig_stake = float(row["stake"] or 0)
            orig_price = float(row["stake_price"] or price or 0)
            entry_price = price if price > 0 else orig_price
            orig_cost   = round(orig_stake * orig_price, 2)
            real_cost   = round(size_matched * entry_price, 2)
            refund      = round(orig_cost - real_cost, 2)

            self.db.conn.execute(
                "UPDATE bets SET stake=?, stake_price=? WHERE id=?",
                (size_matched, entry_price, bet_id)
            )
            self.db.conn.commit()

            if refund > 0.01 and partial:
                self.db.adjust_free_usdc(refund)
                log.info("[PM] 💰 Ордер %s: возврат $%.2f (исполнено %.4f из %.4f shares)",
                         order_id[:16], refund, size_matched, orig_stake)

            # ── Resell: после заполнения BUY → запланировать SELL ──────────
            self._trigger_resell(order_id, bet_id, size_matched, entry_price)

        except Exception as e:
            log.error("[PM] _update_bet_filled %s: %s", order_id[:16], e)

    # ──────────────────────────────────────────────────────────────────────────
    # Resell — логика авто-продажи с наценкой
    # ──────────────────────────────────────────────────────────────────────────

    PM_MIN_SELL_SIZE = 5.0  # Polymarket минимальный размер SELL ордера

    def _trigger_resell(self, buy_order_id: str, bet_id: int,
                        size_matched: float, entry_price: float):
        """Планирует SELL ордер для заполненного BUY, если resell включён."""
        if not self._should_resell():
            return
        if size_matched < 0.01 or entry_price <= 0:
            return
        if size_matched < self.PM_MIN_SELL_SIZE:
            log.info("[resell] Размер %.2f < минимум PM %.0f shares — пропуск resell для #%d (позиция останется для расчёта)",
                     size_matched, self.PM_MIN_SELL_SIZE, bet_id)
            return
        if buy_order_id in self._pending_resell:
            return  # уже запланирован

        markup = self._get_resell_markup()
        sell_price = entry_price + markup / 100.0

        # Округляем до tick_size
        tick_size = "0.01"
        decimals = len(tick_size.split(".")[-1])
        sell_price = round(sell_price, decimals)
        max_price = 1.0 - float(tick_size)
        sell_price = min(sell_price, max_price)

        if sell_price <= entry_price:
            log.warning("[resell] sell_price=%.4f <= entry=%.4f — пропуск", sell_price, entry_price)
            return

        # Получаем данные ставки
        row = self.db.conn.execute(
            "SELECT outcome_id, started_at, neg_risk FROM bets WHERE id=?", (bet_id,)
        ).fetchone()
        if not row:
            return

        token_id = row["outcome_id"]
        started_at = int(row["started_at"] or 0)
        neg_risk = bool(row["neg_risk"] if "neg_risk" in row.keys() else 0)

        # TTL для SELL: до начала матча - 60 сек
        if started_at > 0:
            time_to_start = started_at - time.time()
            if time_to_start < 120:
                log.info("[resell] До матча < 120 сек — пропуск resell для #%d", bet_id)
                return
            cancel_at = started_at - 60
        else:
            import os
            ttl = int(os.getenv("PM_ORDER_TTL_SECS", "3600"))
            cancel_at = time.time() + ttl

        self._pending_resell[buy_order_id] = (
            cancel_at, bet_id, started_at, token_id,
            neg_risk, tick_size, sell_price, size_matched
        )
        self.db.update_resell_placed(bet_id, sell_order_id="",
                                     sell_price_target=sell_price,
                                     resell_status="pending_sell")
        log.info("[resell] 📤 Запланирован SELL для #%d: %.4f → %.4f (+%.1f%%) × %.2f shares",
                 bet_id, entry_price, sell_price, markup, size_matched)

    async def _process_pending_resells(self):
        """Размещает SELL ордера для заполненных BUY."""
        if not self._pending_resell:
            return

        to_process = list(self._pending_resell.items())
        for buy_order_id, data in to_process:
            cancel_at, rec_id, started_at, token_id, neg_risk, tick_size, sell_price, size = data

            # Если до отмены < 30 сек — не размещать
            if time.time() >= cancel_at - 30:
                self._pending_resell.pop(buy_order_id, None)
                self.db.update_resell_placed(rec_id, resell_status="expired")
                log.info("[resell] ⏱ Истекло время для SELL #%d — пропуск", rec_id)
                continue

            try:
                result = await self.pm.place_sell_order(
                    token_id=token_id,
                    price=sell_price,
                    size=size,
                    neg_risk=neg_risk,
                    tick_size=tick_size,
                )
                if result.success and result.bet_id:
                    sell_order_id = result.bet_id
                    self._pending_resell.pop(buy_order_id, None)

                    # Получаем entry_price
                    row = self.db.conn.execute(
                        "SELECT stake_price FROM bets WHERE id=?", (rec_id,)
                    ).fetchone()
                    entry_price = float(row["stake_price"] or 0) if row else 0

                    self._active_sells[sell_order_id] = (
                        cancel_at, rec_id, buy_order_id, entry_price,
                        sell_price, size, token_id, neg_risk, tick_size
                    )
                    self.db.update_resell_placed(rec_id, sell_order_id=sell_order_id,
                                                sell_price_target=sell_price,
                                                resell_status="selling")
                    log.info("[resell] ✅ SELL размещён: %s для #%d @ %.4f × %.2f",
                             sell_order_id[:16], rec_id, sell_price, size)
                else:
                    err = result.error or ""
                    # allowance ошибка — retry на след. tick (allowance уже обновится автоматически)
                    if "allowance" in err.lower() or "balance" in err.lower():
                        log.warning("[resell] ⚠️ SELL #%d allowance error — retry next tick: %s", rec_id, err)
                        # Оставляем в _pending_resell для retry
                    else:
                        self._pending_resell.pop(buy_order_id, None)
                        self.db.update_resell_placed(rec_id, resell_status="cancelled")
                        log.warning("[resell] ❌ SELL не принят для #%d: %s", rec_id, err)

            except Exception as e:
                log.error("[resell] Ошибка размещения SELL для #%d: %s", rec_id, e)

    async def _process_active_sells(self):
        """Мониторит активные SELL ордера: проверяет исполнение и TTL."""
        if not self._active_sells:
            return

        now = time.time()
        to_check = list(self._active_sells.items())

        for sell_order_id, data in to_check:
            cancel_at, rec_id, buy_order_id, entry_price, sell_price, size, token_id, neg_risk, tick_size = data

            try:
                order_info = self._get_order_info(sell_order_id)
                status_raw = (order_info.get("status") or "").upper()
                size_matched = float(order_info.get("size_matched") or order_info.get("sizeMatched") or 0)
                size_remaining = float(order_info.get("size_remaining") or order_info.get("sizeRemaining") or 0)

                # Полностью или частично исполнен (remaining == 0 → ордер закрыт)
                if status_raw in ("MATCHED", "FILLED") or (size_matched > 0 and size_remaining == 0):
                    self._active_sells.pop(sell_order_id, None)
                    self._finalize_sell(rec_id, size_matched, size, entry_price, sell_price,
                                        token_id, neg_risk, tick_size, ttl_expired=False)
                    continue

                # TTL истёк — отменяем SELL
                if now >= cancel_at:
                    self._active_sells.pop(sell_order_id, None)
                    self.pm.cancel_order(sell_order_id)

                    if size_matched > 0:
                        self._finalize_sell(rec_id, size_matched, size, entry_price, sell_price,
                                            token_id, neg_risk, tick_size, ttl_expired=True)
                    else:
                        # Ничего не продано — применяем fallback
                        fallback = self._get_resell_fallback()
                        if fallback == "market_sell":
                            log.info("[resell] ⏱ SELL TTL #%d — market_sell fallback", rec_id)
                            try:
                                mkt_result = await self.pm.place_market_sell(
                                    token_id, size, neg_risk, tick_size)
                                if mkt_result.success:
                                    # Берём фактическую цену из BetResult.price (best_bid)
                                    actual_price = mkt_result.price or entry_price
                                    proceeds = round(size * actual_price, 2)
                                    cost = round(size * entry_price, 2)
                                    profit = round(proceeds - cost, 2)
                                    self.db.update_resell_result(rec_id, resell_status="sold",
                                                                profit_actual=profit,
                                                                sell_price=actual_price)
                                    self.db.adjust_free_usdc(proceeds)
                                    self._unlock_outcome_after_resell(rec_id)
                                    log.info("[resell] 💰 Market SELL #%d @ %.4f: P&L %+.2f$",
                                             rec_id, actual_price, profit)
                                else:
                                    self.db.update_resell_placed(rec_id, resell_status="expired")
                                    log.warning("[resell] ❌ Market SELL failed #%d: %s",
                                                rec_id, mkt_result.error)
                            except Exception as me:
                                self.db.update_resell_placed(rec_id, resell_status="expired")
                                log.error("[resell] market_sell error #%d: %s", rec_id, me)
                        else:
                            # keep: позиция остаётся для обычного расчёта
                            self.db.update_resell_placed(rec_id, resell_status="expired")
                            log.info("[resell] ⏱ SELL TTL #%d — keep (позиция для обычного расчёта)",
                                     rec_id)

            except Exception as e:
                log.error("[resell] Ошибка мониторинга SELL %s: %s", sell_order_id[:16], e)

    def _finalize_sell(self, rec_id: int, size_matched: float, size_total: float,
                       entry_price: float, sell_price: float,
                       token_id: str = "", neg_risk: bool = False, tick_size: str = "0.01",
                       ttl_expired: bool = False):
        """Корректно завершает SELL ордер (полный или частичный).

        Ключевой принцип:
        - P&L считается ТОЛЬКО от проданной части (size_matched)
        - Непроданная часть остаётся как обычная ставка (ожидает результат матча)
        - Stake в БД обновляется до непроданного остатка
        """
        sold_proceeds  = round(size_matched * sell_price, 2)
        sold_cost      = round(size_matched * entry_price, 2)
        resell_profit  = round(sold_proceeds - sold_cost, 2)
        unsold         = round(size_total - size_matched, 2)

        if unsold < 0.01:
            # ── Полная продажа: вся позиция закрыта ──────────────────────
            self.db.update_resell_result(rec_id, resell_status="sold",
                                        profit_actual=resell_profit, sell_price=sell_price)
            self.db.adjust_free_usdc(sold_proceeds)
            self._unlock_outcome_after_resell(rec_id)
            log.info("[resell] 💰 SOLD #%d: %.2f shares @ %.4f → $%.2f, P&L %+.2f$",
                     rec_id, size_matched, sell_price, sold_proceeds, resell_profit)
        else:
            # ── Частичная продажа: проданная часть фиксируется, остаток ждёт результат ──
            # 1) Возвращаем proceeds проданной части
            self.db.adjust_free_usdc(sold_proceeds)

            # 2) Обновляем ставку: stake = непроданный остаток (он ждёт результат матча)
            self.db.conn.execute(
                "UPDATE bets SET stake = ?, resell_status = 'partial_sold', "
                "profit_actual = ?, sell_price = ?, "
                "outcome_result = 'pending', status = 'placed', "
                "notes = COALESCE(notes,'') || ? WHERE id = ?",
                (unsold, resell_profit, sell_price,
                 f" partial_resell: sold {size_matched:.2f}/{size_total:.2f} @ {sell_price}, P&L {resell_profit:+.2f}",
                 rec_id)
            )
            self.db.conn.commit()
            log.info("[resell] ⚡ Частичный SELL #%d: продано %.2f/%.2f @ %.4f, P&L %+.2f$ | "
                     "остаток %.2f shares ждёт результат матча",
                     rec_id, size_matched, size_total, sell_price, resell_profit, unsold)

    def _unlock_outcome_after_resell(self, rec_id: int):
        """Убирает outcome_id из кэша дубликатов, разрешая повторную покупку
        после завершённого resell цикла (buy→sell)."""
        try:
            row = self.db.conn.execute(
                "SELECT outcome_id FROM bets WHERE id=?", (rec_id,)
            ).fetchone()
            if row and row["outcome_id"]:
                oid = row["outcome_id"]
                if oid in self._placed_outcomes:
                    del self._placed_outcomes[oid]
                    log.info("[resell] 🔓 Outcome %s разблокирован для повторной покупки (bet #%d)",
                             oid[:20], rec_id)
        except Exception as e:
            log.debug("[resell] _unlock_outcome err: %s", e)

    # ──────────────────────────────────────────────────────────────────────────
    # Расчёт размера ставки
    # ──────────────────────────────────────────────────────────────────────────

    def _calc_stake(self, pm) -> float:
        bankroll = self.db.get_bankroll()

        if self.cfg.VB_USE_KELLY:
            stake = self._half_kelly(pm, bankroll)
        else:
            stake = bankroll * self.cfg.VB_STAKE_PCT

        # Ограничения
        avail_liq = pm.depth_at_price or pm.total_liquidity or 9999

        # VB_FULL_LIMIT=true → лимитка на всю сумму (не урезаем по ликвидности)
        import os
        full_limit = os.getenv("VB_FULL_LIMIT", "false").lower() in ("true", "1", "yes")
        if not full_limit:
            stake = min(stake, avail_liq * 0.9)  # не больше 90% доступной ликвидности
        stake = min(stake, bankroll * getattr(self.cfg, "VB_MAX_STAKE_PCT", 0.05))  # макс % банкролла

        return round(stake, 2)

    def _half_kelly(self, pm, bankroll: float) -> float:
        """
        Half-Kelly criterion:
          f* = (b*p - q) / b  * 0.5
        где b = odds-1, p = наша prob, q = 1-p
        """
        odds = pm.bb_odds
        b = odds - 1.0
        # Наша вероятность: p = (1 + edge) / odds
        p = (1.0 + pm.edge) / odds
        q = 1.0 - p

        if b <= 0 or p <= 0 or p >= 1:
            return bankroll * self.cfg.VB_STAKE_PCT

        kelly_f = (b * p - q) / b
        kelly_f = max(0.0, kelly_f) * 0.5   # half-Kelly
        kelly_f = min(kelly_f, 0.03)          # максимум 3% даже по Kelly

        return bankroll * kelly_f

    # ──────────────────────────────────────────────────────────────────────────
    # Вспомогательные
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _is_retryable_error(error: str | None) -> bool:
        """Временные ошибки при которых имеет смысл повторить попытку"""
        if not error:
            return False
        retryable_keywords = [
            "timeout", "connection", "network", "rate limit",
            "503", "502", "429", "timed out",
        ]
        err_lower = error.lower()
        return any(k in err_lower for k in retryable_keywords)

    def print_session_stats(self):
        """Статистика текущей сессии"""
        log.info("📊 Сессия: тиков=%d  поставлено=%d  кэш=%d исходов",
                 self._ticks, self._bets_placed, len(self._placed_outcomes))
        self.db.print_stats()


# ──────────────────────────────────────────────────────────────────────────────
# Точка входа
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt= "%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("valuebet_bot.log", encoding="utf-8"),
        ]
    )
    # Меньше шума от aiohttp
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    bot = ValueBetBot()
    try:
        await bot.run()
    except KeyboardInterrupt:
        log.info("Бот остановлен пользователем")
        bot.print_session_stats()


if __name__ == "__main__":
    asyncio.run(main())