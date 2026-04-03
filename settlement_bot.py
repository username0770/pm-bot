# -*- coding: utf-8 -*-
"""
Settlement Sniper Bot — покупка near-settled маркетов.

Стратегия: находить маркеты где результат фактически известен (price >= 0.95),
выставлять лимитку на покупку winning side за 0.95-0.99,
ждать settlement → получить $1.00 за share.

Profit = (1.00 - buy_price) × shares
Risk = void/dispute маркета

Использует ТОЛЬКО Polymarket CLOB API (без внешних источников на первом этапе).
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from config import Config
from db_bets import BetDatabase
from gamma_client import GammaClient
from polymarket_client import PolymarketClient

log = logging.getLogger(__name__)


class SettlementSniper:
    """Settlement Sniper — покупка winning side перед settlement."""

    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv(override=True)

        self.cfg = Config()
        self.gamma = GammaClient()
        self.pm = PolymarketClient(
            private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
            funder_address=os.getenv("POLYMARKET_FUNDER", ""),
        )
        self.db = BetDatabase(os.getenv("DB_PATH_VALUEBET", "valuebet.db"))

        self._running = True
        self._ticks = 0

        # Настройки
        self._scan_interval = int(os.getenv("SNIPE_SCAN_INTERVAL", "30"))  # секунд
        self._min_price = float(os.getenv("SNIPE_MIN_PRICE", "0.95"))     # мин. цена для покупки
        self._max_price = float(os.getenv("SNIPE_MAX_PRICE", "0.995"))    # макс. цена (не покупать за 1.00)
        self._order_size = float(os.getenv("SNIPE_ORDER_SIZE", "5"))      # $ на ордер
        self._max_positions = int(os.getenv("SNIPE_MAX_POSITIONS", "10")) # макс. позиций одновременно
        self._max_days_to_end = float(os.getenv("SNIPE_MAX_DAYS_TO_END", "3"))  # макс. дней до окончания события
        self._tick_size = "0.01"

        # Теги/категории для сканирования. Специальное значение "all" — сканировать без тега (все категории).
        # SNIPE_TAGS имеет приоритет над устаревшим SNIPE_SPORTS.
        _default_tags = "nba,nhl,tennis,soccer,mlb,mma,nfl,golf,politics,crypto,pop-culture"
        raw_tags = os.getenv("SNIPE_TAGS") or os.getenv("SNIPE_SPORTS") or _default_tags
        self._tags: list[str] = [t.strip() for t in raw_tags.split(",") if t.strip()]

        # Режим: manual (по умолчанию) или auto
        self._auto_mode = os.getenv("SNIPE_AUTO_MODE", "false").lower() in ("true", "1", "yes")

        # Active snipes: {condition_id: snipe_state}
        self._active: dict[str, dict] = {}
        self._scanned_cids: set = set()  # уже проверенные condition_id (чтоб не дублировать)
        self._candidates: list[dict] = []  # кандидаты для manual mode (ждут одобрения)
        self._rejected_cids: set = set()  # отклонённые condition_id
        self._load_from_db()

        # Stats
        self._total_scanned = 0
        self._total_sniped = 0
        self._total_settled = 0
        self._total_profit = 0.0

    def _load_from_db(self):
        """Загрузить активные snipes из БД."""
        try:
            rows = self.db.conn.execute(
                "SELECT * FROM settlement_snipes WHERE status IN ('watching','ordered','filled')"
            ).fetchall()
            for r in rows:
                self._active[r["condition_id"]] = {
                    "id": r["id"],
                    "condition_id": r["condition_id"],
                    "token_id": r["token_id"],
                    "question": r["market_question"],
                    "tag": r["sport"],  # колонка называется sport в БД (legacy)
                    "side": r["side"],
                    "target_price": r["snipe_price"],
                    "order_id": r["order_id"] or "",
                    "status": r["status"],
                    "amount": r["snipe_amount"],
                    "cost": r["snipe_cost_usdc"],
                }
            if self._active:
                log.info("[snipe] Загружено %d активных snipes из БД", len(self._active))
        except Exception as e:
            log.debug("[snipe] load_from_db: %s", e)

    # ──────────────────────────────────────────────────────────────────────────
    # Main loop
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self):
        log.info("=" * 60)
        log.info("[snipe] Settlement Sniper запущен")
        log.info("[snipe]   Min price: %.3f, Max price: %.3f", self._min_price, self._max_price)
        log.info("[snipe]   Order size: $%.0f, Max positions: %d", self._order_size, self._max_positions)
        log.info("[snipe]   Tags: %s", ", ".join(self._tags))
        log.info("[snipe]   Scan interval: %ds", self._scan_interval)
        log.info("=" * 60)

        while self._running:
            try:
                self._ticks += 1

                # 1. Проверить статус активных snipes (fills, settlements)
                await self._check_active_snipes()

                # 2. Сканировать новые возможности (каждые N тиков)
                if self._ticks % max(1, self._scan_interval) == 1:
                    await self._scan_markets()

            except Exception as e:
                log.error("[snipe] tick error: %s", e, exc_info=True)

            await asyncio.sleep(1)

    # ──────────────────────────────────────────────────────────────────────────
    # Scanning — поиск near-settled маркетов
    # ──────────────────────────────────────────────────────────────────────────

    async def _scan_markets(self):
        """Сканировать маркеты на Polymarket через Gamma API."""
        if len(self._active) >= self._max_positions:
            return

        client = self.pm._get_client()
        found = 0

        # Строим список запросов: "all" → один запрос без тега, иначе по одному на тег
        tag_queries: list[tuple[str, Optional[str]]] = []
        if len(self._tags) == 1 and self._tags[0].lower() == "all":
            tag_queries = [("all", None)]
        else:
            # Если "all" входит в список вместе с другими — берём только "all"
            if "all" in [t.lower() for t in self._tags]:
                tag_queries = [("all", None)]
            else:
                tag_queries = [(t, t) for t in self._tags]

        for tag_label, tag_param in tag_queries:
            try:
                limit = 200 if tag_param is None else 50
                events = self.gamma.get_events(tag=tag_param, limit=limit)
            except Exception as e:
                log.debug("[snipe] scan %s error: %s", tag_label, e)
                continue

            for ev in events:
                for raw_mkt in ev.get("markets", []):
                    cid = raw_mkt.get("conditionId", "")
                    if not cid or cid in self._active or cid in self._rejected_cids:
                        continue
                    # В manual mode: не пропускать уже просканированные (обновляем цены)
                    if self._auto_mode and cid in self._scanned_cids:
                        continue
                    # В manual mode: не дублировать в candidates
                    if not self._auto_mode and any(c["condition_id"] == cid for c in self._candidates):
                        continue

                    self._scanned_cids.add(cid)
                    self._total_scanned += 1

                    # Проверяем цены из Gamma
                    prices_raw = raw_mkt.get("outcomePrices", "")
                    try:
                        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
                    except Exception:
                        continue

                    if len(prices) < 2:
                        continue

                    p_yes = float(prices[0])
                    p_no = float(prices[1])

                    # Ищем сторону с ценой >= min_price (winning side)
                    winning_side = None
                    winning_price = 0
                    token_id = ""
                    tokens_raw = raw_mkt.get("clobTokenIds", "")
                    try:
                        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else (tokens_raw or [])
                    except Exception:
                        continue

                    if len(tokens) < 2:
                        continue

                    if p_yes >= self._min_price:
                        winning_side = "YES"
                        winning_price = p_yes
                        token_id = tokens[0]
                    elif p_no >= self._min_price:
                        winning_side = "NO"
                        winning_price = p_no
                        token_id = tokens[1]
                    else:
                        continue

                    if winning_price > self._max_price:
                        continue

                    # Извлекаем дату окончания события
                    end_date_raw = (raw_mkt.get("endDate") or raw_mkt.get("end_date_iso")
                                    or ev.get("endDate") or "")
                    end_ts: Optional[float] = None
                    hours_to_end: Optional[float] = None
                    if end_date_raw:
                        try:
                            end_dt = datetime.fromisoformat(
                                end_date_raw.replace("Z", "+00:00"))
                            end_ts = end_dt.timestamp()
                        except Exception:
                            pass

                    if end_ts is not None:
                        now_ts = time.time()
                        days_to_end = (end_ts - now_ts) / 86400
                        # Пропускаем слишком далёкие события
                        if days_to_end > self._max_days_to_end:
                            continue
                        # Пропускаем события, которые давно закончились (>24h назад)
                        if days_to_end < -1:
                            continue
                        hours_to_end = round((end_ts - now_ts) / 3600, 1)

                    # Проверяем через CLOB API — маркет ещё принимает ордера?
                    try:
                        mkt_info = client.get_market(condition_id=cid)
                        if not mkt_info.get("accepting_orders"):
                            continue
                        if mkt_info.get("closed"):
                            continue
                        # Если endDate нет из Gamma — попробуем взять из CLOB
                        if end_ts is None:
                            clob_end = mkt_info.get("end_date_iso", "")
                            if clob_end:
                                try:
                                    end_dt = datetime.fromisoformat(
                                        clob_end.replace("Z", "+00:00"))
                                    end_ts = end_dt.timestamp()
                                    now_ts = time.time()
                                    days_to_end = (end_ts - now_ts) / 86400
                                    if days_to_end > self._max_days_to_end:
                                        continue
                                    if days_to_end < -1:
                                        continue
                                    hours_to_end = round((end_ts - now_ts) / 3600, 1)
                                except Exception:
                                    pass
                    except Exception:
                        continue

                    # Проверяем реальную цену через CLOB
                    try:
                        price_resp = client.get_price(token_id, "BUY")
                        real_price = float(price_resp.get("price", 0)) if isinstance(price_resp, dict) else 0
                    except Exception:
                        real_price = winning_price

                    if real_price < self._min_price or real_price > self._max_price:
                        continue

                    # Нашли кандидата!
                    question = raw_mkt.get("question", "")
                    event_title = ev.get("title", "")
                    profit_per_share = round(1.0 - real_price, 4)
                    shares = round(self._order_size / real_price, 2)
                    expected_profit = round(shares * profit_per_share, 2)
                    profit_pct = round(profit_per_share / real_price * 100, 2)

                    # Определить категорию из тегов события (если сканируем "all")
                    ev_tags = ev.get("tags", [])
                    if isinstance(ev_tags, list) and ev_tags:
                        category = ev_tags[0].get("slug", tag_label) if isinstance(ev_tags[0], dict) else str(ev_tags[0])
                    else:
                        category = tag_label

                    candidate = {
                        "condition_id": cid,
                        "token_id": token_id,
                        "question": question,
                        "event": event_title,
                        "tag": tag_label,
                        "category": category,
                        "side": winning_side,
                        "price": real_price,
                        "profit_per_share": profit_per_share,
                        "expected_profit": expected_profit,
                        "profit_pct": profit_pct,
                        "shares": shares,
                        "found_at": time.time(),
                        "end_ts": end_ts,
                        "hours_to_end": hours_to_end,
                        "end_date_raw": end_date_raw,
                    }

                    if self._auto_mode:
                        # AUTO: размещаем сразу
                        log.info("[snipe] 🎯 AUTO [%s]: %s | %s @ %.3f | +$%.2f (%.1f%%)",
                                 category, question[:40], winning_side, real_price, expected_profit, profit_pct)
                        await self._place_snipe(
                            cid=cid, token_id=token_id, side=winning_side,
                            price=real_price, question=question, event=event_title,
                            tag=category,
                        )
                    else:
                        # MANUAL: добавляем в candidates, ждём одобрения
                        self._candidates.append(candidate)
                        log.info("[snipe] 🔍 CANDIDATE [%s]: %s | %s @ %.3f | +%.1f%% | awaiting approval",
                                 category, question[:40], winning_side, real_price, profit_pct)

                    found += 1

                    if self._auto_mode and len(self._active) >= self._max_positions:
                        break
                if self._auto_mode and len(self._active) >= self._max_positions:
                    break
            if self._auto_mode and len(self._active) >= self._max_positions:
                break

        if self._ticks % 60 == 1:
            log.info("[snipe tick %d] scanned=%d, active=%d, sniped=%d, settled=%d, profit=$%.2f",
                     self._ticks, self._total_scanned, len(self._active),
                     self._total_sniped, self._total_settled, self._total_profit)

    # ──────────────────────────────────────────────────────────────────────────
    # Order placement
    # ──────────────────────────────────────────────────────────────────────────

    async def _place_snipe(self, cid: str, token_id: str, side: str,
                           price: float, question: str, event: str, tag: str = "",
                           size_override: float = 0):
        """Размещает лимитный ордер на покупку winning side."""
        order_size = size_override if size_override > 0 else self._order_size
        try:
            result = await self.pm.place_order(
                token_id=token_id,
                price=price,
                size=order_size,
                tick_size=self._tick_size,
            )

            if result.success and result.bet_id:
                shares = round(order_size / price, 2)
                cost = round(shares * price, 2)
                profit_target = round(shares * (1.0 - price), 2)

                # Записываем в БД
                self.db.conn.execute("""
                    INSERT INTO settlement_snipes
                    (condition_id, token_id, market_question, sport, side,
                     snipe_price, snipe_amount, snipe_cost_usdc, order_id,
                     status, profit_target)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (cid, token_id, question, tag, side,
                      price, shares, cost, result.bet_id,
                      "ordered", profit_target))
                self.db.conn.commit()

                rec_id = self.db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                self._active[cid] = {
                    "id": rec_id,
                    "condition_id": cid,
                    "token_id": token_id,
                    "question": question,
                    "tag": tag,
                    "side": side,
                    "target_price": price,
                    "order_id": result.bet_id,
                    "status": "ordered",
                    "amount": shares,
                    "cost": cost,
                }
                self._total_sniped += 1

                log.info("[snipe] ✅ ORDER: %s | %s @ %.3f | $%.2f | target profit $%.2f | oid=%s",
                         question[:30], side, price, cost, profit_target, result.bet_id[:16])
            else:
                log.warning("[snipe] ❌ Order failed: %s | %s", question[:30], result.error)

        except Exception as e:
            log.error("[snipe] place_snipe error: %s", e)

    # ──────────────────────────────────────────────────────────────────────────
    # Manual mode — approve / reject candidates
    # ──────────────────────────────────────────────────────────────────────────

    async def approve_candidate(self, condition_id: str, custom_size: float = 0) -> str:
        """Одобрить кандидата — разместить ордер."""
        c = None
        for i, cand in enumerate(self._candidates):
            if cand["condition_id"] == condition_id:
                c = self._candidates.pop(i)
                break
        if not c:
            return "candidate not found"

        size = custom_size if custom_size > 0 else self._order_size
        await self._place_snipe(
            cid=c["condition_id"], token_id=c["token_id"], side=c["side"],
            price=c["price"], question=c["question"], event=c["event"],
            tag=c.get("category", c.get("tag", "")), size_override=size,
        )
        return None  # success

    def reject_candidate(self, condition_id: str):
        """Отклонить кандидата — больше не показывать."""
        self._candidates = [c for c in self._candidates if c["condition_id"] != condition_id]
        self._rejected_cids.add(condition_id)

    def set_auto_mode(self, enabled: bool):
        self._auto_mode = enabled
        log.info("[snipe] Mode: %s", "AUTO" if enabled else "MANUAL")

    # ──────────────────────────────────────────────────────────────────────────
    # Active snipe monitoring
    # ──────────────────────────────────────────────────────────────────────────

    async def _check_active_snipes(self):
        """Проверяет активные snipes — fills и settlements."""
        if not self._active:
            return

        client = self.pm._get_client()

        for cid, snipe in list(self._active.items()):
            try:
                status = snipe["status"]

                if status == "ordered":
                    # Проверяем заполнение ордера
                    info = client.get_order(snipe["order_id"])
                    order_status = (info.get("status") or "").upper()
                    matched = float(info.get("size_matched") or info.get("sizeMatched") or 0)

                    if order_status in ("MATCHED", "FILLED") or matched > 0:
                        snipe["status"] = "filled"
                        snipe["amount"] = matched
                        snipe["cost"] = round(matched * snipe["target_price"], 2)
                        self.db.conn.execute(
                            "UPDATE settlement_snipes SET status='filled', "
                            "snipe_amount=?, snipe_cost_usdc=?, executed_at=datetime('now') "
                            "WHERE id=?",
                            (matched, snipe["cost"], snipe["id"]))
                        self.db.conn.commit()
                        log.info("[snipe] 📥 FILLED: %s | %.0f shares @ %.3f | cost=$%.2f",
                                 snipe["question"][:30], matched, snipe["target_price"], snipe["cost"])

                    # Проверяем — не лучше ли переставить ордер
                    elif order_status == "LIVE":
                        try:
                            price_resp = client.get_price(snipe["token_id"], "BUY")
                            current_ask = float(price_resp.get("price", 0)) if isinstance(price_resp, dict) else 0
                            # Если текущий ask лучше нашего ордера — переставляем
                            if 0 < current_ask < snipe["target_price"] and current_ask >= self._min_price:
                                self.pm.cancel_order(snipe["order_id"])
                                result = await self.pm.place_order(
                                    token_id=snipe["token_id"],
                                    price=current_ask,
                                    size=self._order_size,
                                    tick_size=self._tick_size,
                                )
                                if result.success and result.bet_id:
                                    snipe["order_id"] = result.bet_id
                                    snipe["target_price"] = current_ask
                                    self.db.conn.execute(
                                        "UPDATE settlement_snipes SET order_id=?, snipe_price=? WHERE id=?",
                                        (result.bet_id, current_ask, snipe["id"]))
                                    self.db.conn.commit()
                                    log.info("[snipe] 🔄 REQUOTE: %s | new price %.3f",
                                             snipe["question"][:30], current_ask)
                        except Exception:
                            pass

                elif status == "filled":
                    # Проверяем settlement маркета
                    try:
                        mkt_info = client.get_market(condition_id=cid)
                        if mkt_info.get("closed"):
                            # Маркет settled! Проверяем результат
                            tokens = mkt_info.get("tokens", [])
                            winner_token = None
                            for t in tokens:
                                if t.get("winner"):
                                    winner_token = t.get("token_id")
                                    break

                            if winner_token == snipe["token_id"]:
                                # Мы выиграли!
                                payout = round(snipe["amount"] * 1.0, 2)
                                profit = round(payout - snipe["cost"], 2)
                                snipe["status"] = "settled_won"
                                self._total_settled += 1
                                self._total_profit += profit
                                self.db.conn.execute(
                                    "UPDATE settlement_snipes SET status='settled_won', "
                                    "profit_actual=?, settled_at=datetime('now') WHERE id=?",
                                    (profit, snipe["id"]))
                                self.db.conn.commit()
                                log.info("[snipe] 💰 WON: %s | profit=$%.2f (%.1f%%)",
                                         snipe["question"][:30], profit,
                                         profit / snipe["cost"] * 100 if snipe["cost"] > 0 else 0)
                                del self._active[cid]
                            elif winner_token:
                                # Мы проиграли
                                profit = -snipe["cost"]
                                snipe["status"] = "settled_lost"
                                self._total_settled += 1
                                self._total_profit += profit
                                self.db.conn.execute(
                                    "UPDATE settlement_snipes SET status='settled_lost', "
                                    "profit_actual=?, settled_at=datetime('now') WHERE id=?",
                                    (profit, snipe["id"]))
                                self.db.conn.commit()
                                log.info("[snipe] ❌ LOST: %s | loss=$%.2f",
                                         snipe["question"][:30], abs(profit))
                                del self._active[cid]
                    except Exception:
                        pass

            except Exception as e:
                log.debug("[snipe] check error %s: %s", cid[:12], e)

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        return {
            "auto_mode": self._auto_mode,
            "active": len(self._active),
            "candidates": len(self._candidates),
            "total_scanned": self._total_scanned,
            "total_sniped": self._total_sniped,
            "total_settled": self._total_settled,
            "total_profit": round(self._total_profit, 2),
            "settings": {
                "min_price": self._min_price,
                "max_price": self._max_price,
                "order_size": self._order_size,
                "max_positions": self._max_positions,
                "scan_interval": self._scan_interval,
                "max_days_to_end": self._max_days_to_end,
                "tags": ",".join(self._tags),
            },
            "snipes": [
                {
                    "id": s["id"],
                    "question": s["question"],
                    "side": s["side"],
                    "price": s["target_price"],
                    "amount": s["amount"],
                    "cost": s["cost"],
                    "status": s["status"],
                    "tag": s.get("tag", ""),
                    "profit_target": round(s["amount"] * (1.0 - s["target_price"]), 2),
                }
                for s in self._active.values()
            ],
            "candidate_list": [
                {
                    "condition_id": c["condition_id"],
                    "question": c["question"],
                    "event": c["event"],
                    "side": c["side"],
                    "price": c["price"],
                    "profit_pct": c["profit_pct"],
                    "expected_profit": c["expected_profit"],
                    "tag": c.get("category", c.get("tag", "")),
                    "age_min": round((time.time() - c["found_at"]) / 60, 0),
                    "hours_to_end": c.get("hours_to_end"),
                }
                for c in self._candidates
            ],
        }

    def stop(self):
        """Остановить бот. Cancel все открытые ордера."""
        self._running = False
        for cid, snipe in self._active.items():
            if snipe["status"] == "ordered" and snipe.get("order_id"):
                try:
                    self.pm.cancel_order(snipe["order_id"])
                except Exception:
                    pass
        log.info("[snipe] Settlement Sniper остановлен")
