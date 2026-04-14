# -*- coding: utf-8 -*-
"""
Live Value-Bet бот — автоматические ставки на Polymarket в режиме ЛАЙВ.

Отличия от прематч-бота (valuebet_bot.py):
  - Использует BetBurger LIVE API (rest-api-lv.betburger.com)
  - Фильтр: BETBURGER_FILTER_ID_LIVE (default env var)
  - Настройки: LV_* вместо VB_* (отдельные лимиты для лайва)
  - Ордер GTC + автоотмена через LV_ORDER_TTL_SECS секунд (default 30)
  - bet_mode = 'live' во всех записях БД
  - Сырой ответ сохраняется в betburger_last_raw_live.json
  - Дедупликация по outcome_id — общая с прематчем (одна БД)

Цикл работы:
  1. Каждые LV_POLL_INTERVAL секунд запрашивает BetBurger Live API
  2. Фильтрует только Polymarket беты с нужным edge
  3. Проверяет дубли: in-memory кэш + БД (по outcome_id, любой режим)
  4. Рассчитывает размер ставки (flat % банкролла или half-Kelly)
  5. Размещает GTC лимитный ордер на Polymarket CLOB
  6. Запускает таймер: через LV_ORDER_TTL_SECS отменяет если не исполнен
  7. Записывает в БД с bet_mode='live'
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import aiohttp

log = logging.getLogger(__name__)

POLYMARKET_ID = 483
LIVE_VB_URL   = "https://rest-api-lv.betburger.com/api/v1/valuebets/bot_pro_search"


class LiveValueBetBot:
    def __init__(self):
        import os
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)
        except ImportError:
            pass

        try:
            import config as cfg_module
            import importlib
            importlib.reload(cfg_module)
            cfg = cfg_module.Config()
        except Exception:
            cfg = None

        def _get(attr, env_key, default=""):
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

        # Учётные данные (общие с прематчем)
        self._bb_email    = _get("BETBURGER_EMAIL",    "BETBURGER_EMAIL")
        self._bb_password = _get("BETBURGER_PASSWORD", "BETBURGER_PASSWORD")
        self._bb_token    = _get("BETBURGER_TOKEN",    "BETBURGER_TOKEN")
        self._pm_key      = _get("POLYMARKET_PRIVATE_KEY", "POLYMARKET_PRIVATE_KEY")
        self._pm_funder   = _get("POLYMARKET_FUNDER",      "POLYMARKET_FUNDER")
        self._db_path     = _get("DB_PATH_VALUEBET",       "DB_PATH_VALUEBET", "valuebets.db")

        # Live-специфичный filter_id
        self._filter_id = int(
            os.getenv("BETBURGER_FILTER_ID_LIVE", "0") or
            _get("BETBURGER_FILTER_ID_LIVE", "BETBURGER_FILTER_ID_LIVE", "0") or
            "0"
        )
        if not self._filter_id:
            raise ValueError("Нужен BETBURGER_FILTER_ID_LIVE в .env (пример: 1308405)")

        # ── Live настройки торговли ─────────────────────────────────────────
        class _Cfg:
            pass
        lv = _Cfg()
        lv.LV_MIN_ROI       = _getf("LV_MIN_ROI",       "LV_MIN_ROI",       0.04)
        lv.MIN_LIQUIDITY    = _getf("LV_MIN_LIQUIDITY",  "LV_MIN_LIQUIDITY", 30.0)   # лайв = меньший порог
        lv.VB_MIN_STAKE     = _getf("LV_MIN_STAKE",      "LV_MIN_STAKE",     2.0)
        lv.VB_STAKE_PCT     = _getf("LV_STAKE_PCT",      "LV_STAKE_PCT",     0.01)
        lv.VB_MAX_STAKE_PCT = _getf("LV_MAX_STAKE_PCT",  "LV_MAX_STAKE_PCT", 0.05)
        lv.VB_USE_KELLY     = _getb("LV_USE_KELLY",      "LV_USE_KELLY",     False)
        lv.VB_MAX_ODDS      = _getf("LV_MAX_ODDS",       "LV_MAX_ODDS",      0.0)
        lv.POLL_INTERVAL    = int(_getf("LV_POLL_INTERVAL", "LV_POLL_INTERVAL", 5))
        lv.ORDER_TTL        = int(_getf("LV_ORDER_TTL_SECS", "LV_ORDER_TTL_SECS", 30))
        self.cfg = lv

        # BetBurger авторизация
        self._auth = None
        if self._bb_token:
            log.info("BetBurger Live: статичный токен %s...", self._bb_token[:8])
        elif self._bb_email and self._bb_password:
            from betburger_auth import BetBurgerAuth
            self._auth = BetBurgerAuth(self._bb_email, self._bb_password)
            log.info("BetBurger Live: автообновление токена через %s", self._bb_email)
        else:
            raise ValueError("Нет BETBURGER_TOKEN или BETBURGER_EMAIL+PASSWORD в .env")

        # Polymarket CLOB
        if not self._pm_key or not self._pm_funder:
            raise ValueError("Нет POLYMARKET_PRIVATE_KEY или POLYMARKET_FUNDER в .env")
        from polymarket_client import PolymarketClient
        self.pm = PolymarketClient(self._pm_key, self._pm_funder)

        # БД (общая с прематчем — одна таблица bets)
        from db_bets import BetDatabase
        self.db = BetDatabase(self._db_path)

        # in-memory кэш outcome_id → timestamp
        self._placed_outcomes: dict[str, float] = {}
        self._load_active_from_db()

        # Счётчики
        self._ticks       = 0
        self._bets_placed = 0
        self._bets_skipped = 0

        # Активные ордера ожидающие отмены: {order_id: cancel_at_timestamp}
        self._pending_cancel: dict[str, float] = {}
        self._last_fill_poll = 0
        self._fill_poll_interval = 30

        # Line movement tracking
        self._last_line_sample = 0
        self._line_sample_interval = 60  # каждые 60 сек

        # ── Resell (авто-продажа с наценкой) ─────────────────────────────────
        self._pending_resell: dict[str, tuple] = {}
        self._active_sells: dict[str, tuple] = {}
        self._load_active_resells()

    def _load_active_from_db(self):
        """При старте загружаем активные live-ставки в in-memory кэш"""
        active = self.db.get_active_bets()
        for rec in active:
            self._placed_outcomes[rec.outcome_id] = time.time()
        if active:
            log.info("[LIVE] Загружено %d активных ставок из БД в кэш", len(active))

    def _load_active_resells(self):
        """При старте загружаем активные resell из БД"""
        try:
            resells = self.db.get_active_resells()
            # Фильтруем только live ставки
            for rec in resells:
                if getattr(rec, "bet_mode", "") != "live":
                    continue
                started = int(getattr(rec, "started_at", 0) or 0)
                cancel_at = max(time.time() + 30, started - 60) if started > 0 else time.time() + 300
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
            live_resells = sum(1 for r in resells if getattr(r, "bet_mode", "") == "live")
            if live_resells:
                log.info("[LIVE] Загружено %d активных resell из БД", live_resells)
        except Exception as e:
            log.debug("[LIVE] _load_active_resells: %s", e)

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
        import os
        self._reload_env()
        return os.getenv("LV_RESELL_ENABLED", "false").lower() in ("true", "1", "yes")

    def _get_resell_markup(self) -> float:
        import os
        self._reload_env()
        try:
            return float(os.getenv("LV_RESELL_MARKUP", "3"))
        except Exception:
            return 3.0

    def _get_resell_fallback(self) -> str:
        import os
        self._reload_env()
        return os.getenv("LV_RESELL_FALLBACK", "keep")

    ESPORT_IDS = {21, 39, 41, 46, 47, 48, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59,
                  60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71}

    def _is_excluded_sport(self, pm) -> bool:
        """Проверяет исключён ли спорт/лига/карта по настройкам."""
        import os
        self._reload_env()

        excluded_ids = os.getenv("EXCLUDED_SPORTS", "").strip()
        if excluded_ids:
            try:
                ids = {int(x.strip()) for x in excluded_ids.split(",") if x.strip().isdigit()}
                if getattr(pm, "sport_id", 0) in ids:
                    log.info("[LIVE] skip [excluded sport_id=%d] %s  %s",
                             pm.sport_id, pm.match_name, pm.league)
                    return True
            except Exception:
                pass

        excluded_leagues = os.getenv("EXCLUDED_LEAGUES", "").strip()
        if excluded_leagues:
            league_lower = (getattr(pm, "league", "") or "").lower()
            for pattern in excluded_leagues.split(","):
                pattern = pattern.strip().lower()
                if pattern and pattern in league_lower:
                    log.info("[LIVE] skip [excluded league '%s'] %s  %s",
                             pattern, pm.match_name, pm.league)
                    return True

        # Фильтр карт в киберспорте
        sport_id = getattr(pm, "sport_id", 0)
        if sport_id in self.ESPORT_IDS:
            max_map = int(os.getenv("ESPORT_MAX_MAP", "3") or "3")
            if max_map > 0:
                param = getattr(pm, "market_param", 0) or 0
                outcome = (getattr(pm, "outcome_name", "") or "").lower()
                if param > max_map:
                    log.info("[LIVE] skip [esport map %d > max %d] %s  %s",
                             int(param), max_map, pm.match_name, pm.league)
                    return True
                import re
                map_match = re.search(r'map\s*(\d+)|карт[аеы]\s*(\d+)', outcome)
                if map_match:
                    map_num = int(map_match.group(1) or map_match.group(2))
                    if map_num > max_map:
                        log.info("[LIVE] skip [esport map %d > max %d] %s  %s",
                                 map_num, max_map, pm.match_name, pm.league)
                        return True

        return False

    # ──────────────────────────────────────────────────────────────────────────
    # Основной цикл
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self):
        log.info("=" * 60)
        log.info("⚡ LIVE ValueBet бот запущен")
        log.info("   FILTER_ID   = %d", self._filter_id)
        log.info("   MIN_EDGE    = %.1f%%", self.cfg.LV_MIN_ROI * 100)
        log.info("   MIN_LIQ     = $%.0f", self.cfg.MIN_LIQUIDITY)
        log.info("   STAKE       = %.1f%% банкролла%s",
                 self.cfg.VB_STAKE_PCT * 100,
                 " (Half Kelly)" if self.cfg.VB_USE_KELLY else "")
        log.info("   ORDER_TTL   = %ds (GTC + автоотмена)", self.cfg.ORDER_TTL)
        log.info("   POLL        = %ds", self.cfg.POLL_INTERVAL)
        log.info("=" * 60)

        while True:
            try:
                await self.tick()
                await self._poll_order_fills()
                await self._process_pending_cancels()
                await self._process_pending_resells()
                await self._process_active_sells()
                await self._sample_line_movements()
            except Exception as e:
                log.error("[LIVE] Критическая ошибка в tick: %s", e, exc_info=True)
            await asyncio.sleep(self.cfg.POLL_INTERVAL)

    async def tick(self):
        self._ticks += 1
        items = await self._fetch_live_valuebets()

        if not items:
            log.debug("[LIVE tick %d] Нет Polymarket бетов", self._ticks)
            return

        log.debug("[LIVE tick %d] Получено %d Polymarket бетов", self._ticks, len(items))

        for bet, arb_meta in items:
            try:
                await self._process(bet, arb_meta)
            except Exception as e:
                log.error("[LIVE] Ошибка обработки бета: %s", e, exc_info=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Fetch Live BetBurger
    # ──────────────────────────────────────────────────────────────────────────

    async def _fetch_live_valuebets(self) -> list:
        """Запрашивает BetBurger Live API. Возвращает [(bet_dict, arb_meta), ...]"""
        import pathlib, json as _json, datetime as _dt

        token = self._bb_token
        if self._auth:
            token = await self._auth.get_token()
            if not token:
                log.error("[LIVE] BetBurger: не удалось получить токен")
                return []

        results = []
        async with aiohttp.ClientSession() as session:
            try:
                fdata = aiohttp.FormData()
                fdata.add_field("search_filter[]", str(self._filter_id))
                fdata.add_field("per_page", "100")
                fdata.add_field("sort_by", "percent")

                async with session.post(
                    LIVE_VB_URL,
                    params  = {"access_token": token},
                    data    = fdata,
                    timeout = aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 401:
                        log.warning("[LIVE] BetBurger: 401 Unauthorized — токен устарел")
                        if self._auth:
                            self._auth._token = None
                        return []
                    if resp.status != 200:
                        log.error("[LIVE] BetBurger HTTP %d", resp.status)
                        return []

                    raw  = await resp.json()
                    bets = raw.get("bets", []) if isinstance(raw, dict) else raw
                    arbs = raw.get("arbs", []) if isinstance(raw, dict) else []

                    # Сохраняем сырой ответ для диагностики
                    try:
                        out = pathlib.Path(self._db_path).parent / "betburger_last_raw_live.json"
                        out.write_text(_json.dumps({
                            "saved_at": _dt.datetime.now().isoformat(),
                            "data": raw
                        }, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception:
                        pass

                    # source.valueBets[] — правильный источник велью%
                    # (arbs[].percent = доходность шурбета, НЕ велью одного исхода)
                    source = raw.get("source", {}) if isinstance(raw, dict) else {}
                    source_vb_by_id: dict[str, dict] = {}
                    for vb in (source.get("valueBets") or []):
                        bid = vb.get("bet_id") or vb.get("id")
                        if bid:
                            source_vb_by_id[bid] = vb

                    # Индекс bet_id → arb (для вспомогательных данных)
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
                            arb_meta = dict(arb)
                            if svb.get("percent") is not None:
                                arb_meta["percent"] = svb["percent"]
                                arb_meta["avg_koef"] = svb.get("avg_koef")
                            results.append((bet, arb_meta))

            except aiohttp.ClientError as e:
                log.error("[LIVE] BetBurger сетевая ошибка: %s", e)
            except Exception as e:
                log.error("[LIVE] BetBurger fetch: %s", e, exc_info=True)

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

        # 1. Edge
        if pm.value_pct / 100 < self.cfg.LV_MIN_ROI:
            log.debug("[LIVE] skip [low edge] %s  edge=%.2f%% < %.1f%%",
                      pm.match_name, pm.value_pct, self.cfg.LV_MIN_ROI * 100)
            return

        # 1b. Макс. edge — защита от cancel-арбитража
        try:
            import os as _os
            max_edge = float(_os.getenv("LV_MAX_EDGE", "0") or "0")
        except Exception:
            max_edge = 0
        if max_edge > 0 and pm.value_pct > max_edge:
            log.info("[LIVE] skip [HIGH edge] %s  edge=%.2f%% > max=%.1f%%  (cancel-арб)",
                     pm.match_name, pm.value_pct, max_edge)
            return

        # 1c. Исключение спортов / лиг
        if self._is_excluded_sport(pm):
            return

        # 2. Макс. коэффициент
        max_odds = self.cfg.VB_MAX_ODDS
        if max_odds and max_odds > 1.0 and pm.bb_odds > max_odds:
            log.debug("[LIVE] skip [high odds] %s  odds=%.2f > max=%.2f",
                      pm.match_name, pm.bb_odds, max_odds)
            return

        # 3. outcome_id обязателен
        if not pm.outcome_id:
            log.warning("[LIVE] skip [no outcome_id] %s | %s", pm.match_name, pm.outcome_name)
            return

        # 4. Ликвидность
        avail_liq = pm.depth_at_price or pm.total_liquidity or 0
        if avail_liq < self.cfg.MIN_LIQUIDITY:
            log.debug("[LIVE] skip [low liq] %s  $%.0f < $%.0f",
                      pm.match_name, avail_liq, self.cfg.MIN_LIQUIDITY)
            return

        # 5. В лайве НЕ проверяем started_at — матч уже идёт, это норма

        # ── ДЕДУПЛИКАЦИЯ ──────────────────────────────────────────────────────

        if pm.outcome_id in self._placed_outcomes:
            log.debug("[LIVE] skip [in-memory dup] %s | %s", pm.match_name, pm.outcome_name)
            return

        existing = self.db.already_bet(pm.outcome_id)
        if existing:
            log.debug("[LIVE] skip [db dup] %s | %s | #%d %s",
                      pm.match_name, pm.outcome_name, existing.id, existing.status)
            self._placed_outcomes[pm.outcome_id] = time.time()
            return

        # ── РАСЧЁТ СТАВКИ ─────────────────────────────────────────────────────

        stake = self._calc_stake(pm)
        min_stake = self.cfg.VB_MIN_STAKE
        if stake < min_stake:
            log.info("[LIVE] skip [stake too small] $%.2f < $%.2f  %s",
                     stake, min_stake, pm.match_name)
            return

        # ── ЛОГИРУЕМ ──────────────────────────────────────────────────────────

        log.info("")
        log.info("⚡ " + "━" * 58)
        log.info("⚡ LIVE  %s", pm.match_name)
        log.info("   %s  |  %s", pm.league, pm.start_dt)
        log.info("   ИСХОД:   %s  (%s%s)",
                 pm.outcome_name, pm.market_type_name,
                 f"  линия={pm.market_param}" if pm.market_param else "")
        log.info("   EDGE:    +%.2f%%", pm.value_pct)
        log.info("   КОЭФ:    %.4f   implied %.1f%%", pm.bb_odds, pm.bb_price * 100)
        log.info("   ЛИК:     $%.0f (рынок)  $%.0f (стакан)",
                 pm.total_liquidity, pm.depth_at_price)
        log.info("   СТАВКА:  $%.2f @ price=%.4f", stake, pm.bb_price)
        log.info("   TTL:     %d сек (автоотмена если не исполнен)", self.cfg.ORDER_TTL)
        log.info("   TOKEN:   %s...%s", pm.outcome_id[:16], pm.outcome_id[-8:])

        # ── ЗАПИСЫВАЕМ В БД ───────────────────────────────────────────────────

        rec_id = self.db.insert_bet(pm, stake=stake, stake_price=pm.bb_price,
                                    status="pending", bet_mode="live")

        # Добавляем в кэш ДО размещения (защита от параллельных тиков)
        self._placed_outcomes[pm.outcome_id] = time.time()

        # ── РАЗМЕЩАЕМ ОРДЕР ───────────────────────────────────────────────────

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
            cost_usdc = round(stake * pm.bb_price, 2)
            self.db.adjust_free_usdc(-cost_usdc)
            self._bets_placed += 1

            # Планируем автоотмену через ORDER_TTL секунд
            if result.bet_id:
                cancel_at = time.time() + self.cfg.ORDER_TTL
                self._pending_cancel[result.bet_id] = cancel_at
                log.info("✅ [LIVE] ПОСТАВЛЕНО  $%.2f @ %.4f | order=%s | отмена через %ds",
                         stake, pm.bb_price, (result.bet_id or "")[:24], self.cfg.ORDER_TTL)
            else:
                log.info("✅ [LIVE] ПОСТАВЛЕНО  $%.2f @ %.4f (нет order_id для отмены)",
                         stake, pm.bb_price)
        else:
            self.db.update_failed(rec_id, error_msg=result.error or "unknown")
            if self._is_retryable_error(result.error):
                del self._placed_outcomes[pm.outcome_id]
                log.warning("❌ [LIVE] ОШИБКА (retry) %s: %s", pm.match_name, result.error)
            else:
                log.error("❌ [LIVE] ОШИБКА (final) %s: %s", pm.match_name, result.error)

        log.info("⚡ " + "━" * 58)

    # ──────────────────────────────────────────────────────────────────────────
    # Автоотмена ордеров по TTL
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
        for order_id, cancel_at in list(self._pending_cancel.items()):
            try:
                order_info = self._get_order_info(order_id)
                status_raw = (order_info.get("status") or "").upper()
                size_matched = float(order_info.get("size_matched") or order_info.get("sizeMatched") or 0)
                price = float(order_info.get("price") or 0)
                original_size = float(order_info.get("original_size") or order_info.get("originalSize") or 0)

                if status_raw in ("MATCHED", "FILLED"):
                    log.info("[LIVE poll] ✅ Ордер %s полностью заполнен (%.4f shares)",
                             order_id[:16], size_matched)
                    filled_orders.append((order_id, size_matched, price, False))
                elif status_raw == "LIVE" and size_matched > 0:
                    log.debug("[LIVE poll] ⏳ Ордер %s LIVE partial (%.4f/%.4f)",
                              order_id[:16], size_matched, original_size)
            except Exception as e:
                log.debug("[LIVE poll] Ошибка проверки %s: %s", order_id[:16], e)

        for order_id, size_matched, price, partial in filled_orders:
            self._pending_cancel.pop(order_id, None)
            self._update_bet_filled(order_id, size_matched, price, partial=partial)

    async def _process_pending_cancels(self):
        """Проверяет и отменяет ордера у которых истёк TTL"""
        if not self._pending_cancel:
            return

        now = time.time()
        to_cancel = [oid for oid, cancel_at in self._pending_cancel.items()
                     if now >= cancel_at]

        for order_id in to_cancel:
            del self._pending_cancel[order_id]
            try:
                # Получаем полный статус ордера из CLOB
                order_info = self._get_order_info(order_id)
                status_raw  = (order_info.get("status") or "").lower()
                size_matched = float(order_info.get("size_matched") or
                                     order_info.get("sizeMatched") or 0)
                size_remaining = float(order_info.get("size_remaining") or
                                       order_info.get("sizeRemaining") or 0)
                price = float(order_info.get("price") or 0)

                log.info("[LIVE] Ордер %s...%s: status=%s matched=%.4f remaining=%.4f",
                         order_id[:12], order_id[-6:], status_raw, size_matched, size_remaining)

                # Полностью или частично исполнен — не отменяем
                # Обновляем БД с реальным размером заполнения
                if status_raw in ("matched", "filled") or (size_matched > 0 and size_remaining == 0):
                    log.info("[LIVE] ✅ Ордер %s...%s полностью исполнен (%.4f shares)",
                             order_id[:12], order_id[-6:], size_matched)
                    self._update_bet_filled(order_id, size_matched, price, partial=False)
                    continue

                if size_matched > 0:
                    # Частичное исполнение — фиксируем реальный объём, остаток отменяем
                    log.info("[LIVE] ⚡ Ордер %s...%s частично исполнен (%.4f из %.4f shares)",
                             order_id[:12], order_id[-6:], size_matched, size_matched + size_remaining)
                    self._update_bet_filled(order_id, size_matched, price, partial=True)

                # Отменяем (остаток или весь ордер)
                cancelled = self.pm.cancel_order(order_id)
                if cancelled or size_matched > 0:
                    if size_matched == 0:
                        # Полная отмена — ничего не сыграло
                        log.info("[LIVE] ⏱ Ордер %s...%s отменён по TTL (%ds) — 0 исполнено",
                                 order_id[:12], order_id[-6:], self.cfg.ORDER_TTL)
                        self.db.conn.execute(
                            "UPDATE bets SET status='cancelled', error_msg='TTL expired' "
                            "WHERE order_id=? AND status='placed' AND bet_mode='live'",
                            (order_id,)
                        )
                        self.db.conn.commit()
                        # Возвращаем cost (stake * stake_price)
                        row = self.db.conn.execute(
                            "SELECT stake, stake_price FROM bets WHERE order_id=?", (order_id,)
                        ).fetchone()
                        if row:
                            refund = round(float(row["stake"] or 0) * float(row["stake_price"] or 0), 2)
                            if refund > 0:
                                self.db.adjust_free_usdc(refund)
                                log.info("[LIVE] Возвращено $%.2f (полная отмена TTL)", refund)
                    else:
                        log.info("[LIVE] ⏱ Ордер %s...%s остаток отменён (%.4f shares исполнено)",
                                 order_id[:12], order_id[-6:], size_matched)
                else:
                    log.warning("[LIVE] Не удалось отменить ордер %s...%s (возможно уже исполнен)",
                                order_id[:12], order_id[-6:])
                    # Принудительно перечитываем статус ещё раз
                    order_info2 = self._get_order_info(order_id)
                    size_matched2 = float(order_info2.get("size_matched") or
                                          order_info2.get("sizeMatched") or 0)
                    if size_matched2 > 0:
                        self._update_bet_filled(order_id, size_matched2, price, partial=False)

            except Exception as e:
                log.error("[LIVE] Ошибка при отмене ордера %s: %s", order_id[:12], e)

    def _get_order_info(self, order_id: str) -> dict:
        """Получает полный статус ордера из CLOB API (status, size_matched, size_remaining, price)"""
        try:
            client = self.pm._get_client()
            resp = client.get_order(order_id)
            return resp if isinstance(resp, dict) else {}
        except Exception as e:
            log.debug("[LIVE] get_order_info %s: %s", order_id[:16], e)
            return {}

    def _update_bet_filled(self, order_id: str, size_matched: float, price: float, partial: bool):
        """
        Обновляет запись ставки по реальному объёму исполнения.
        partial=True  → частично исполнена, пересчитываем stake и возвращаем остаток в баланс
        partial=False → полностью исполнена, просто обновляем stake если надо
        """
        try:
            row = self.db.conn.execute(
                "SELECT id, stake, stake_price FROM bets WHERE order_id=?",
                (order_id,)
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
                log.info("[LIVE] 💰 Ордер %s: возврат $%.2f (исполнено %.4f из %.4f shares)",
                         order_id[:16], refund, size_matched, orig_stake)

            # Resell trigger
            self._trigger_resell(order_id, bet_id, size_matched, entry_price)

        except Exception as e:
            log.error("[LIVE] _update_bet_filled %s: %s", order_id[:16], e)

    # ──────────────────────────────────────────────────────────────────────────
    # Resell — логика авто-продажи (live)
    # ──────────────────────────────────────────────────────────────────────────

    PM_MIN_SELL_SIZE = 5.0  # Polymarket минимальный размер SELL ордера

    def _trigger_resell(self, buy_order_id: str, bet_id: int,
                        size_matched: float, entry_price: float):
        if not self._should_resell():
            return
        if size_matched < 0.01 or entry_price <= 0:
            return
        if size_matched < self.PM_MIN_SELL_SIZE:
            log.info("[LIVE resell] Размер %.2f < минимум PM %.0f — пропуск resell #%d",
                     size_matched, self.PM_MIN_SELL_SIZE, bet_id)
            return
        if buy_order_id in self._pending_resell:
            return

        markup = self._get_resell_markup()
        sell_price = entry_price + markup / 100.0
        tick_size = "0.01"
        decimals = len(tick_size.split(".")[-1])
        sell_price = round(sell_price, decimals)
        sell_price = min(sell_price, 1.0 - float(tick_size))

        if sell_price <= entry_price:
            return

        row = self.db.conn.execute(
            "SELECT outcome_id, started_at, neg_risk FROM bets WHERE id=?", (bet_id,)
        ).fetchone()
        if not row:
            return

        token_id = row["outcome_id"]
        started_at = int(row["started_at"] or 0)
        neg_risk = bool(row["neg_risk"] if "neg_risk" in row.keys() else 0)

        # SELL TTL для лайва: используем PM_ORDER_TTL_SECS (не LV_ORDER_TTL_SECS который для BUY)
        # Матч уже идёт → started_at в прошлом → нельзя привязывать к нему
        import os
        sell_ttl = int(os.getenv("PM_ORDER_TTL_SECS", "3600"))
        cancel_at = time.time() + sell_ttl

        self._pending_resell[buy_order_id] = (
            cancel_at, bet_id, started_at, token_id,
            neg_risk, tick_size, sell_price, size_matched
        )
        self.db.update_resell_placed(bet_id, sell_order_id="",
                                     sell_price_target=sell_price,
                                     resell_status="pending_sell")
        log.info("[LIVE resell] 📤 SELL для #%d: %.4f → %.4f (+%.1f%%) × %.2f",
                 bet_id, entry_price, sell_price, markup, size_matched)

    async def _process_pending_resells(self):
        if not self._pending_resell:
            return
        for buy_order_id, data in list(self._pending_resell.items()):
            cancel_at, rec_id, started_at, token_id, neg_risk, tick_size, sell_price, size = data
            if time.time() >= cancel_at - 10:
                self._pending_resell.pop(buy_order_id, None)
                self.db.update_resell_placed(rec_id, resell_status="expired")
                continue
            try:
                result = await self.pm.place_sell_order(token_id, sell_price, size, neg_risk, tick_size)
                if result.success and result.bet_id:
                    sell_order_id = result.bet_id
                    self._pending_resell.pop(buy_order_id, None)
                    row = self.db.conn.execute("SELECT stake_price FROM bets WHERE id=?", (rec_id,)).fetchone()
                    entry_price = float(row["stake_price"] or 0) if row else 0
                    self._active_sells[sell_order_id] = (
                        cancel_at, rec_id, buy_order_id, entry_price,
                        sell_price, size, token_id, neg_risk, tick_size
                    )
                    self.db.update_resell_placed(rec_id, sell_order_id=sell_order_id,
                                                sell_price_target=sell_price, resell_status="selling")
                    log.info("[LIVE resell] ✅ SELL: %s для #%d @ %.4f", sell_order_id[:16], rec_id, sell_price)
                else:
                    err = result.error or ""
                    if "allowance" in err.lower() or "balance" in err.lower():
                        log.warning("[LIVE resell] ⚠️ SELL #%d allowance error — retry: %s", rec_id, err)
                    else:
                        self._pending_resell.pop(buy_order_id, None)
                        self.db.update_resell_placed(rec_id, resell_status="cancelled")
                        log.warning("[LIVE resell] ❌ SELL fail #%d: %s", rec_id, err)
            except Exception as e:
                log.error("[LIVE resell] place_sell error #%d: %s", rec_id, e)

    async def _process_active_sells(self):
        if not self._active_sells:
            return
        now = time.time()
        for sell_order_id, data in list(self._active_sells.items()):
            cancel_at, rec_id, buy_order_id, entry_price, sell_price, size, token_id, neg_risk, tick_size = data
            try:
                info = self._get_order_info(sell_order_id)
                status = (info.get("status") or "").upper()
                matched = float(info.get("size_matched") or info.get("sizeMatched") or 0)
                remaining = float(info.get("size_remaining") or info.get("sizeRemaining") or 0)

                if status in ("MATCHED", "FILLED") or (matched > 0 and remaining == 0):
                    self._active_sells.pop(sell_order_id, None)
                    self._finalize_sell(rec_id, matched, size, entry_price, sell_price,
                                        token_id, neg_risk, tick_size, ttl_expired=False)
                    continue

                if now >= cancel_at:
                    self._active_sells.pop(sell_order_id, None)
                    self.pm.cancel_order(sell_order_id)
                    if matched > 0:
                        self._finalize_sell(rec_id, matched, size, entry_price, sell_price,
                                            token_id, neg_risk, tick_size, ttl_expired=True)
                    else:
                        fallback = self._get_resell_fallback()
                        if fallback == "market_sell":
                            try:
                                r = await self.pm.place_market_sell(token_id, size, neg_risk, tick_size)
                                if r.success:
                                    # Берём фактическую цену из BetResult.price (best_bid)
                                    actual_price = r.price or entry_price
                                    proceeds = round(size * actual_price, 2)
                                    profit = round(proceeds - round(size * entry_price, 2), 2)
                                    self.db.update_resell_result(rec_id, "sold", profit, actual_price)
                                    self.db.adjust_free_usdc(proceeds)
                                    self._unlock_outcome_after_resell(rec_id)
                                    log.info("[LIVE resell] 💰 Market SELL #%d @ %.4f: P&L %+.2f$",
                                             rec_id, actual_price, profit)
                                else:
                                    self.db.update_resell_placed(rec_id, resell_status="expired")
                            except Exception:
                                self.db.update_resell_placed(rec_id, resell_status="expired")
                        else:
                            self.db.update_resell_placed(rec_id, resell_status="expired")
                            log.info("[LIVE resell] ⏱ SELL TTL #%d — keep", rec_id)
            except Exception as e:
                log.error("[LIVE resell] monitor error %s: %s", sell_order_id[:16], e)

    def _finalize_sell(self, rec_id: int, size_matched: float, size_total: float,
                       entry_price: float, sell_price: float,
                       token_id: str = "", neg_risk: bool = False, tick_size: str = "0.01",
                       ttl_expired: bool = False):
        """Корректно завершает SELL ордер (полный или частичный).

        P&L считается ТОЛЬКО от проданной части.
        Непроданная часть остаётся как обычная ставка (ожидает результат).
        """
        sold_proceeds = round(size_matched * sell_price, 2)
        sold_cost     = round(size_matched * entry_price, 2)
        resell_profit = round(sold_proceeds - sold_cost, 2)
        unsold        = round(size_total - size_matched, 2)

        if unsold < 0.01:
            self.db.update_resell_result(rec_id, resell_status="sold",
                                        profit_actual=resell_profit, sell_price=sell_price)
            self.db.adjust_free_usdc(sold_proceeds)
            self._unlock_outcome_after_resell(rec_id)
            log.info("[LIVE resell] 💰 SOLD #%d: %.2f shares @ %.4f → $%.2f, P&L %+.2f$",
                     rec_id, size_matched, sell_price, sold_proceeds, resell_profit)
        else:
            self.db.adjust_free_usdc(sold_proceeds)
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
            log.info("[LIVE resell] ⚡ Частичный SELL #%d: продано %.2f/%.2f @ %.4f, P&L %+.2f$ | "
                     "остаток %.2f shares ждёт результат",
                     rec_id, size_matched, size_total, sell_price, resell_profit, unsold)

    def _unlock_outcome_after_resell(self, rec_id: int):
        """Убирает outcome_id из кэша дубликатов после завершённого resell цикла."""
        try:
            row = self.db.conn.execute(
                "SELECT outcome_id FROM bets WHERE id=?", (rec_id,)
            ).fetchone()
            if row and row["outcome_id"]:
                oid = row["outcome_id"]
                if oid in self._placed_outcomes:
                    del self._placed_outcomes[oid]
                    log.info("[LIVE resell] 🔓 Outcome %s разблокирован (bet #%d)", oid[:20], rec_id)
        except Exception as e:
            log.debug("[LIVE resell] _unlock_outcome err: %s", e)

    # ──────────────────────────────────────────────────────────────────────────
    # Расчёт ставки (аналог прематча но с LV_ настройками)
    # ──────────────────────────────────────────────────────────────────────────

    def _calc_stake(self, pm) -> float:
        bankroll = self.db.get_bankroll()

        if self.cfg.VB_USE_KELLY:
            stake = self._half_kelly(pm, bankroll)
        else:
            stake = bankroll * self.cfg.VB_STAKE_PCT

        avail_liq = pm.depth_at_price or pm.total_liquidity or 9999
        stake = min(stake, avail_liq * 0.9)
        stake = min(stake, bankroll * self.cfg.VB_MAX_STAKE_PCT)
        stake = max(stake, 0.0)
        return round(stake, 2)

    def _half_kelly(self, pm, bankroll: float) -> float:
        """Half-Kelly критерий для лайва"""
        b = pm.bb_odds - 1.0
        p = pm.bb_price
        q = 1.0 - p
        kelly_f = (b * p - q) / b if b > 0 else 0.0
        half_kelly = kelly_f / 2.0
        return round(max(0.0, bankroll * half_kelly), 2)

    def _is_retryable_error(self, error: str | None) -> bool:
        """Временные ошибки при которых имеет смысл повторить в следующем тике"""
        if not error:
            return False
        retryable = ["timeout", "connection", "network", "temporarily", "503", "502", "429"]
        return any(r in error.lower() for r in retryable)

    # ──────────────────────────────────────────────────────────────────────────
    # Line Movement — мониторинг движения цены для live ставок
    # ──────────────────────────────────────────────────────────────────────────

    async def _sample_line_movements(self):
        """Раз в 60с снимает best ask для всех placed live ставок."""
        now = time.time()
        if now - self._last_line_sample < self._line_sample_interval:
            return
        self._last_line_sample = now

        try:
            bets_to_track = self.db.line_get_live_bets_to_track()
        except Exception:
            return

        if not bets_to_track:
            return

        for bet in bets_to_track:
            try:
                price = self.pm.get_best_ask(bet["outcome_id"])
                if price is None or price <= 0:
                    price = self.pm.get_midpoint(bet["outcome_id"])
                if price is None or price <= 0:
                    continue

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

                self.db.line_record_snapshot(bet["id"], now, price, minutes_after)
            except Exception:
                pass  # не ломаем бот из-за tracking