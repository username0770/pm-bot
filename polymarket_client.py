"""
Polymarket CLOB API клиент
Использует официальный py-clob-client

pip install py-clob-client
"""

import logging
import asyncio
from models import BetResult

log = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137  # Polygon mainnet


class PolymarketClient:
    def __init__(self, private_key: str, funder_address: str):
        self.private_key    = private_key
        self.funder         = funder_address
        self.funder_address = funder_address  # alias
        self._client     = None

    def _get_client(self):
        """Ленивая инициализация CLOB клиента"""
        if self._client is None:
            from py_clob_client.client import ClobClient
            client = ClobClient(
                host       = CLOB_HOST,
                key        = self.private_key,
                chain_id   = CHAIN_ID,
                signature_type = 1,   # L2 (funder кошелёк)
                funder     = self.funder,
            )
            client.set_api_creds(client.create_or_derive_api_creds())
            self._client = client
            log.info("Polymarket CLOB клиент готов. Funder: %s", self.funder[:10])
        return self._client

    # ──────────────────────────────────────────────────────────────────────────
    # Fee rate
    # ──────────────────────────────────────────────────────────────────────────

    _fee_cache: dict = {}  # token_id → (fee_rate, cached_at)

    def get_fee_rate(self, token_id: str) -> float:
        """Получить fee rate (0..1) для токена. Кеширует на 5 мин."""
        import time
        cached = self._fee_cache.get(token_id)
        if cached and time.time() - cached[1] < 300:
            return cached[0]
        try:
            from py_clob_client.http_helpers.helpers import get as clob_get
            client = self._get_client()
            resp = clob_get(f"{client.host}/fee-rate?token_id={token_id}")
            fee_bps = float(resp.get("base_fee", 0) if isinstance(resp, dict) else 0)
            # base_fee может быть в BPS (0.01 = 1%) или в decimal
            fee_rate = fee_bps / 10000 if fee_bps > 1 else fee_bps
            self._fee_cache[token_id] = (fee_rate, time.time())
            return fee_rate
        except Exception as e:
            log.debug("get_fee_rate error for %s: %s", token_id[:16], e)
            return 0.0

    # ──────────────────────────────────────────────────────────────────────────
    # Allowance для продажи conditional tokens
    # ──────────────────────────────────────────────────────────────────────────

    _approved_tokens: set = set()  # кэш уже одобренных token_id

    def _ensure_sell_allowance(self, client, token_id: str):
        """Проверяет и обновляет allowance для SELL (conditional token).
        Без этого SELL ордера возвращают 'not enough balance / allowance'."""
        if token_id in self._approved_tokens:
            return
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            resp = client.update_balance_allowance(params)
            log.info("SELL allowance обновлён для token %s...%s: %s",
                     token_id[:12], token_id[-8:], resp)
            self._approved_tokens.add(token_id)
        except Exception as e:
            log.warning("Не удалось обновить allowance для %s: %s", token_id[:12], e)

    # ──────────────────────────────────────────────────────────────────────────
    # Основной метод — размещение ставки
    # ──────────────────────────────────────────────────────────────────────────

    async def place_order(
        self,
        token_id: str,
        price: float,       # вероятность 0..1  (напр. 0.49 = коэф 2.04)
        size: float,        # объём в USDC
        neg_risk: bool = False,
        tick_size: str = "0.01",
    ) -> BetResult:
        """
        Размещает GTC лимитный ордер на покупку YES/NO токена.

        token_id  — outcome/token ID из Polymarket (из BetBurger direct_link → outcomeId)
        price     — implied probability (1 / odds)
        size      — сумма ставки в USDC
        neg_risk  — negRisk флаг маркета (из direct_link)
        tick_size — шаг цены (обычно "0.01", иногда "0.001")
        """
        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import BUY
            from py_clob_client.constants import ZERO_ADDRESS

            client = self._get_client()

            # Округляем цену до tick_size
            decimals = len(tick_size.split(".")[-1])
            price_rounded = round(price, decimals)

            # Защита от крайних значений
            min_price = float(tick_size)
            max_price = 1.0 - float(tick_size)
            price_rounded = max(min_price, min(max_price, price_rounded))

            log.info(
                "PM ордер: token=%s...%s  price=%.4f  size=%.2f  negRisk=%s",
                token_id[:12], token_id[-8:], price_rounded, size, neg_risk
            )

            # Создаём OrderArgs
            order_args = OrderArgs(
                token_id = token_id,
                price    = price_rounded,
                size     = round(size, 2),
                side     = BUY,
            )

            # Размещаем — options должен быть объектом с атрибутом tick_size
            # Пробуем PartialCreateOrderOptions, падаем на SimpleNamespace
            try:
                from py_clob_client.clob_types import PartialCreateOrderOptions
                options = PartialCreateOrderOptions(
                    tick_size = tick_size,
                    neg_risk  = neg_risk,
                )
            except (ImportError, TypeError):
                import types
                options = types.SimpleNamespace(
                    tick_size = tick_size,
                    neg_risk  = neg_risk,
                )

            resp = client.create_and_post_order(order_args, options)

            log.debug("PM ответ: %s", resp)

            order_id = resp.get("orderID") or resp.get("id") or resp.get("order_id", "")
            status   = resp.get("status", "")
            error_msg = resp.get("errorMsg") or resp.get("error", "")

            # Статусы Polymarket: matched/live/delayed/unmatched/cancelled
            if status in ("matched", "live", "delayed", "unmatched") or order_id:
                filled = resp.get("sizeFilled") or resp.get("size_filled") or 0
                return BetResult(
                    success      = True,
                    bet_id       = order_id,
                    filled_odds  = round(1.0 / price_rounded, 4) if price_rounded > 0 else 0,
                    filled_amount= float(filled) if filled else size,
                )
            else:
                msg = error_msg or f"status={status}"
                log.warning("PM ордер не принят: %s | resp=%s", msg, resp)
                return BetResult(success=False, error=msg)

        except Exception as e:
            log.error("PM place_order исключение: %s", e, exc_info=True)
            return BetResult(success=False, error=str(e))

    # ──────────────────────────────────────────────────────────────────────────
    # Размещение SELL ордера
    # ──────────────────────────────────────────────────────────────────────────

    async def place_sell_order(
        self,
        token_id: str,
        price: float,       # цена продажи (0..1)
        size: float,        # кол-во shares для продажи
        neg_risk: bool = False,
        tick_size: str = "0.01",
    ) -> BetResult:
        """
        Размещает GTC лимитный SELL ордер для продажи позиции.

        token_id  — ID токена (outcome_id)
        price     — цена продажи (entry_price + markup)
        size      — количество shares для продажи
        neg_risk  — negRisk флаг маркета
        tick_size — шаг цены
        """
        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import SELL

            client = self._get_client()

            # Ensure allowance for conditional token before selling
            self._ensure_sell_allowance(client, token_id)

            decimals = len(tick_size.split(".")[-1])
            price_rounded = round(price, decimals)
            min_price = float(tick_size)
            max_price = 1.0 - float(tick_size)
            price_rounded = max(min_price, min(max_price, price_rounded))

            log.info(
                "PM SELL: token=%s...%s  price=%.4f  size=%.2f  negRisk=%s",
                token_id[:12], token_id[-8:], price_rounded, size, neg_risk
            )

            order_args = OrderArgs(
                token_id = token_id,
                price    = price_rounded,
                size     = round(size, 2),
                side     = SELL,
            )

            try:
                from py_clob_client.clob_types import PartialCreateOrderOptions
                options = PartialCreateOrderOptions(
                    tick_size = tick_size,
                    neg_risk  = neg_risk,
                )
            except (ImportError, TypeError):
                import types
                options = types.SimpleNamespace(
                    tick_size = tick_size,
                    neg_risk  = neg_risk,
                )

            resp = client.create_and_post_order(order_args, options)
            log.debug("PM SELL ответ: %s", resp)

            order_id = resp.get("orderID") or resp.get("id") or resp.get("order_id", "")
            status   = resp.get("status", "")
            error_msg = resp.get("errorMsg") or resp.get("error", "")

            if status in ("matched", "live", "delayed", "unmatched") or order_id:
                filled = resp.get("sizeFilled") or resp.get("size_filled") or 0
                return BetResult(
                    success      = True,
                    bet_id       = order_id,
                    filled_odds  = round(1.0 / price_rounded, 4) if price_rounded > 0 else 0,
                    filled_amount= float(filled) if filled else size,
                )
            else:
                msg = error_msg or f"status={status}"
                log.warning("PM SELL не принят: %s | resp=%s", msg, resp)
                return BetResult(success=False, error=msg)

        except Exception as e:
            log.error("PM place_sell_order исключение: %s", e, exc_info=True)
            return BetResult(success=False, error=str(e))

    async def place_market_sell(
        self,
        token_id: str,
        size: float,
        neg_risk: bool = False,
        tick_size: str = "0.01",
    ) -> BetResult:
        """
        Продаёт позицию по рыночной цене (best bid - 1 tick).
        Используется как fallback когда SELL ордер не исполнен.
        """
        try:
            client = self._get_client()
            self._ensure_sell_allowance(client, token_id)
            book = client.get_order_book(token_id)
            bids = book.bids if hasattr(book, "bids") else []

            if not bids:
                log.warning("PM market_sell: нет bids для %s...%s", token_id[:12], token_id[-8:])
                return BetResult(success=False, error="no bids in order book")

            # Best bid — первый (самый высокий)
            best_bid = float(bids[0].price if hasattr(bids[0], "price") else bids[0].get("price", 0))
            sell_price = best_bid  # Продаём по best bid для моментального исполнения

            if sell_price <= 0:
                return BetResult(success=False, error="best bid is 0")

            log.info("PM market_sell: best_bid=%.4f, selling %s...%s × %.2f",
                     sell_price, token_id[:12], token_id[-8:], size)

            return await self.place_sell_order(token_id, sell_price, size, neg_risk, tick_size)

        except Exception as e:
            log.error("PM place_market_sell исключение: %s", e, exc_info=True)
            return BetResult(success=False, error=str(e))

    # ──────────────────────────────────────────────────────────────────────────
    # Вспомогательные методы
    # ──────────────────────────────────────────────────────────────────────────

    def get_midpoint(self, token_id: str) -> float | None:
        """Текущая mid-цена токена"""
        try:
            client = self._get_client()
            mid = client.get_midpoint(token_id)
            return float(mid.get("mid", 0)) if mid else None
        except Exception as e:
            log.debug("PM get_midpoint: %s", e)
            return None

    def get_best_ask(self, token_id: str) -> float | None:
        """Текущий best ask — цена по которой можно купить прямо сейчас."""
        try:
            client = self._get_client()
            resp = client.get_price(token_id, "SELL")
            return float(resp.get("price", 0)) if resp else None
        except Exception as e:
            log.debug("PM get_best_ask: %s", e)
            return None

    def get_order_book(self, token_id: str) -> dict:
        """Стакан ордеров"""
        try:
            client = self._get_client()
            return client.get_order_book(token_id) or {}
        except Exception as e:
            log.debug("PM get_order_book: %s", e)
            return {}

    def get_balance(self) -> float:
        """Баланс USDC на funder кошельке"""
        try:
            client = self._get_client()
            # py-clob-client не предоставляет баланс напрямую,
            # используем get_collateral_balance если доступен
            if hasattr(client, "get_collateral_balance"):
                bal = client.get_collateral_balance()
                return float(bal.get("balance", 0)) if bal else 0.0
            return 0.0
        except Exception as e:
            log.debug("PM get_balance: %s", e)
            return 0.0

    def get_open_orders(self) -> list:
        """Открытые ордера"""
        try:
            client = self._get_client()
            orders = client.get_orders()
            return orders if isinstance(orders, list) else []
        except Exception as e:
            log.debug("PM get_orders: %s", e)
            return []

    def get_order_status(self, order_id: str) -> dict:
        """Статус конкретного ордера"""
        try:
            client = self._get_client()
            return client.get_order(order_id) or {}
        except Exception as e:
            log.debug("PM get_order_status: %s", e)
            return {}

    def cancel_order(self, order_id: str) -> bool:
        """Отменить ордер"""
        try:
            client = self._get_client()
            resp = client.cancel(order_id)
            return bool(resp)
        except Exception as e:
            log.warning("PM cancel_order %s: %s", order_id[:16], e)
            return False

    def get_trades(self, asset_id: str = None) -> list:
        """
        Получает историю сделок пользователя через CLOB API.
        Фильтрует по asset_id (token_id) если указан.
        Возвращает список трейдов с полями: side, price, size, asset_id, matchTime и др.
        """
        try:
            from py_clob_client.clob_types import TradeParams
            client = self._get_client()
            params = TradeParams(asset_id=asset_id) if asset_id else None
            trades = client.get_trades(params)
            return trades if isinstance(trades, list) else []
        except Exception as e:
            log.error("PM get_trades: %s", e, exc_info=True)
            return []

    def get_portfolio(self) -> dict:
        """
        Получает портфель с Polymarket.
        Использует Data API для позиций (официальная документация).
        Для баланса USDC использует py-clob-client.
        """
        import urllib.request as _ureq
        import json as _json

        result = {
            "ok":               True,
            "cash":             0.0,
            "open_orders_value":0.0,
            "portfolio_value":  0.0,
            "total":            0.0,
            "positions":        [],
            "prices_available": False,
            "error":            None,
        }

        # ── 1. Cash balance ────────────────────────────────────────────────────
        # py-clob-client: get_collateral_balance() or get_balance_allowance()
        # Согласно документации — это USDC allowance/balance через контракт
        try:
            client = self._get_client()
            # Официальный метод через py-clob-client
            for method_name in ("get_balance", "get_collateral_balance"):
                if hasattr(client, method_name):
                    try:
                        bal = getattr(client, method_name)()
                        if isinstance(bal, dict):
                            v = float(bal.get("balance", bal.get("USDC", bal.get("collateral", 0))) or 0)
                        else:
                            v = float(bal or 0)
                        if v > 0:
                            result["cash"] = v
                            break
                    except Exception:
                        pass
        except Exception as e:
            log.debug("get_portfolio cash client: %s", e)

        # ── 2. Открытые ордера ─────────────────────────────────────────────────
        try:
            orders = self.get_open_orders()
            frozen = sum(
                float(o.get("sizeRemaining", o.get("size", 0))) * float(o.get("price", 0))
                for o in (orders or [])
                if isinstance(o, dict)
            )
            result["open_orders_value"] = round(frozen, 2)
        except Exception as e:
            log.debug("get_portfolio orders: %s", e)

        # ── 3. Позиции через Data API (официальная документация) ──────────────
        # Поля: size, avgPrice, curPrice, currentValue, cashPnl, percentPnl,
        #        initialValue, totalBought, title, outcome
        funder = self.funder_address.lower()
        positions = []
        total_current_value = 0.0
        total_initial_value = 0.0

        urls_to_try = [
            f"https://data-api.polymarket.com/positions?user={funder}&sizeThreshold=0.01&limit=500",
            f"https://data-api.polymarket.com/positions?proxyWallet={funder}&sizeThreshold=0.01&limit=500",
        ]

        for url in urls_to_try:
            try:
                req = _ureq.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; PolyBot/1.0)",
                    "Accept": "application/json",
                })
                with _ureq.urlopen(req, timeout=20) as r:
                    raw = _json.loads(r.read())

                items = raw if isinstance(raw, list) else raw.get("positions", [])
                if not items:
                    continue

                for p in items:
                    # Точные имена полей из документации Data API
                    size          = float(p.get("size", 0) or 0)
                    avg_price     = float(p.get("avgPrice", 0) or 0)
                    cur_price     = float(p.get("curPrice", 0) or 0)
                    current_value = float(p.get("currentValue", 0) or 0)
                    initial_value = float(p.get("initialValue", 0) or 0)  # = totalBought в $
                    cash_pnl      = float(p.get("cashPnl", 0) or 0)
                    pct_pnl       = float(p.get("percentPnl", 0) or 0)
                    title         = p.get("title", p.get("slug", "?"))
                    outcome       = p.get("outcome", "Yes")
                    redeemable    = p.get("redeemable", False)
                    end_date      = p.get("endDate", "")

                    if size < 0.01:
                        continue

                    # Если cur_price есть — используем currentValue, иначе считаем из initialValue
                    value = current_value if current_value > 0 else (size * avg_price if avg_price > 0 else 0)
                    cost  = initial_value if initial_value > 0 else (size * avg_price)
                    pnl   = cash_pnl if cash_pnl != 0 else (value - cost)

                    total_current_value += value
                    total_initial_value += cost

                    positions.append({
                        "question":   title[:70],
                        "outcome":    outcome,
                        "size":       round(size, 2),           # токенов куплено
                        "avg_price":  round(avg_price, 4),      # средняя цена входа
                        "cur_price":  round(cur_price, 4),      # текущая рыночная цена
                        "value":      round(value, 2),          # текущая стоимость $
                        "cost":       round(cost, 2),           # потрачено $
                        "pnl":        round(pnl, 2),            # P&L $
                        "pnl_pct":   round(pct_pnl, 1),        # P&L %
                        "redeemable": redeemable,               # можно получить выплату
                        "end_date":   end_date[:10] if end_date else "",
                    })

                if positions:
                    break  # нашли данные — не пробуем другие URL

            except Exception as e:
                log.debug("get_portfolio positions [%s...]: %s", url[:60], e)
                result["error"] = str(e)
                continue

        # Сортируем: сначала redeemable (ждут выплаты), потом по стоимости
        positions.sort(key=lambda x: (-int(x.get("redeemable", False)), -(x["value"] or x["cost"])))

        has_prices = any(p["cur_price"] > 0 for p in positions)
        result["positions"]        = positions
        result["portfolio_value"]  = round(total_current_value, 2)
        result["total"]            = round(result["cash"] + total_current_value, 2)
        result["prices_available"] = has_prices
        if positions and result["error"]:
            result["error"] = None  # есть позиции — ошибка не критична

        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Вспомогательный JSON-RPC (не требует web3)
    # ──────────────────────────────────────────────────────────────────────────

    _POLYGON_RPCS = [
        "https://1rpc.io/matic",
        "https://rpc-mainnet.matic.quiknode.pro",
        "https://polygon-bor-rpc.publicnode.com",
    ]

    def _rpc(self, method: str, params: list, timeout: int = 10) -> dict:
        """Один JSON-RPC вызов к Polygon."""
        import urllib.request as _ureq, json as _json
        rpc_url = os.getenv("POLYGON_RPC", "") or self._POLYGON_RPCS[0]
        payload = _json.dumps({"jsonrpc": "2.0", "method": method,
                               "params": params, "id": 1}).encode()
        req = _ureq.Request(rpc_url, data=payload,
                            headers={"Content-Type": "application/json",
                                     "User-Agent": "PolyBot/1.0"})
        with _ureq.urlopen(req, timeout=timeout) as r:
            return _json.loads(r.read())

    def _usdc_balance(self, address: str) -> float:
        """USDC.e баланс кошелька (6 decimals). Без web3."""
        # balanceOf(address) = 0x70a08231 + адрес выровненный до 32 байт
        addr_pad = address.lower().replace("0x", "").zfill(64)
        data = "0x70a08231" + addr_pad
        USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e на Polygon
        USDC_N = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native USDC
        total = 0.0
        for token in (USDC_E, USDC_N):
            try:
                res = self._rpc("eth_call", [{"to": token, "data": data}, "latest"])
                raw = res.get("result", "0x0") or "0x0"
                total += int(raw, 16) / 1_000_000  # 6 decimals
            except Exception as e:
                log.debug("_usdc_balance %s: %s", token[:10], e)
        return total

    def _matic_balance(self, address: str) -> float:
        """MATIC баланс (wei → ether). Без web3."""
        try:
            res = self._rpc("eth_getBalance", [address, "latest"])
            return int(res.get("result", "0x0"), 16) / 1e18
        except Exception as e:
            log.debug("_matic_balance: %s", e)
            return 0.0

    def _get_nonce(self, address: str) -> int:
        res = self._rpc("eth_getTransactionCount", [address, "latest"])
        return int(res.get("result", "0x0"), 16)

    def _get_gas_price(self) -> int:
        res = self._rpc("eth_gasPrice", [])
        return int(int(res.get("result", "0x0"), 16) * 1.3)

    def _send_raw_tx(self, signed_hex: str) -> str:
        """Отправляет подписанную транзакцию. Возвращает txhash."""
        res = self._rpc("eth_sendRawTransaction", [signed_hex])
        if "error" in res:
            raise RuntimeError(res["error"].get("message", str(res["error"])))
        return res.get("result", "")

    # ──────────────────────────────────────────────────────────────────────────
    # Быстрый wallet snapshot (для хедера, кешируется снаружи)
    # ──────────────────────────────────────────────────────────────────────────

    def get_wallet_snapshot(self) -> dict:
        """
        Лёгкий снэпшот: cash + portfolio_value + redeemable_amount.
        Cash = USDC внутри Polymarket Exchange (не на кошельке напрямую).
        Берётся через CLOB API get_balance_allowance(COLLATERAL).
        Позиции — через Polymarket Data API.
        """
        import urllib.request as _ureq
        import json as _json

        funder = self.funder_address.lower()

        # ── 1. USDC cash — через CLOB API (баланс внутри биржи Polymarket) ─────
        # Polymarket хранит USDC в Exchange контракте, а не на proxy-кошельке.
        # _usdc_balance(funder) всегда даёт ~0 по этой причине.
        cash = 0.0
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_client()
            result = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            # result: {"balance": "10040000", "allowance": "..."} — 6 decimals (USDC)
            raw_bal = result.get("balance", 0) if isinstance(result, dict) else 0
            cash = float(raw_bal) / 1_000_000  # micro-USDC → USDC
            log.debug("wallet_snapshot cash via CLOB: %s → %.2f", raw_bal, cash)
        except Exception as e:
            log.debug("wallet_snapshot CLOB balance: %s", e)
            # Fallback 1: другие методы py-clob-client
            try:
                client = self._get_client()
                for method_name in ("get_balance", "get_collateral_balance"):
                    if hasattr(client, method_name):
                        bal = getattr(client, method_name)()
                        raw = (bal.get("balance", bal.get("USDC", bal.get("collateral", 0)))
                               if isinstance(bal, dict) else (bal or 0))
                        v = float(raw)
                        # если вернуло в микро-единицах (> 1000 для $1) — конвертируем
                        v = v / 1_000_000 if v > 1000 else v
                        if v > 0:
                            cash = v
                            break
            except Exception:
                pass
            # Fallback 2: прямой blockchain запрос (работает если USDC реально на кошельке)
            if cash == 0.0:
                try:
                    cash = self._usdc_balance(funder)
                except Exception:
                    pass

        # ── 2. Позиции (portfolio_value + redeemable) ──────────────────────────
        portfolio_value = 0.0
        redeemable_amount = 0.0
        redeemable_count = 0

        for url in [
            f"https://data-api.polymarket.com/positions?user={funder}&sizeThreshold=0.01&limit=500",
            f"https://data-api.polymarket.com/positions?proxyWallet={funder}&sizeThreshold=0.01&limit=500",
        ]:
            try:
                req = _ureq.Request(url, headers={
                    "User-Agent": "PolyBot/1.0", "Accept": "application/json"})
                with _ureq.urlopen(req, timeout=15) as r:
                    items = _json.loads(r.read())
                    if not isinstance(items, list):
                        items = items.get("positions", [])
                    if not items:
                        continue
                    log.debug("wallet_snapshot: %d positions from %s", len(items), url.split("?")[0])
                    if items:
                        sample = items[0]
                        log.debug("wallet_snapshot sample keys: %s", list(sample.keys()))
                    for p in items:
                        size = float(p.get("size", 0) or 0)
                        if size < 0.01:
                            continue
                        # Порядок приоритетов: currentValue > size*curPrice > size*avgPrice
                        current_value = float(p.get("currentValue", 0) or 0)
                        cur_price = float(p.get("curPrice", p.get("currentPrice", 0)) or 0)
                        avg_price  = float(p.get("avgPrice", 0) or 0)
                        if current_value > 0:
                            value = current_value
                        elif cur_price > 0:
                            value = size * cur_price
                        elif avg_price > 0:
                            value = size * avg_price
                        else:
                            value = 0.0
                        portfolio_value += value
                        if p.get("redeemable"):
                            # Settled позиция: redeemable = size shares × $1.00
                            # Берём currentValue если есть, иначе size (settled = $1 per share)
                            redeem_val = current_value if current_value > 0 else size
                            redeemable_amount += redeem_val
                            redeemable_count += 1
                    break
            except Exception as e:
                log.debug("wallet_snapshot positions: %s", e)

        return {
            "ok": True,
            "cash": round(cash, 2),
            "portfolio_value": round(portfolio_value, 2),
            "total": round(cash + portfolio_value, 2),
            "redeemable": round(redeemable_amount, 2),
            "redeemable_count": redeemable_count,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # On-chain redemption (без web3, только eth_account + eth_abi + JSON-RPC)
    # ──────────────────────────────────────────────────────────────────────────

    def redeem_positions(self) -> dict:
        """
        Redeems all settled positions on-chain.
        Использует eth_account (уже установлен с py-clob-client) + raw JSON-RPC.
        Не требует web3.
        """
        import urllib.request as _ureq
        import json as _json

        # ── 1. Получаем redeemable позиции ────────────────────────────────────
        funder = self.funder_address.lower()
        redeemable = []
        for url in [
            f"https://data-api.polymarket.com/positions?user={funder}&sizeThreshold=0.01&limit=500",
            f"https://data-api.polymarket.com/positions?proxyWallet={funder}&sizeThreshold=0.01&limit=500",
        ]:
            try:
                req = _ureq.Request(url, headers={
                    "User-Agent": "PolyBot/1.0", "Accept": "application/json"})
                with _ureq.urlopen(req, timeout=15) as r:
                    items = _json.loads(r.read())
                    if not isinstance(items, list):
                        items = items.get("positions", [])
                    redeemable = [p for p in items
                                  if p.get("redeemable") and float(p.get("size", 0)) > 0.01]
                    if items:
                        break
            except Exception as e:
                log.warning("[redeem] fetch positions: %s", e)

        if not redeemable:
            return {"ok": True, "redeemed": 0, "amount": 0.0, "msg": "Nothing to redeem"}

        log.info("[redeem] Found %d redeemable positions", len(redeemable))

        # ── 2. eth_account для подписи (уже в зависимостях py-clob-client) ────
        try:
            from eth_account import Account
            from eth_abi import encode as abi_encode
        except ImportError as e:
            return {"ok": False, "error": f"eth_account/eth_abi not available: {e}"}

        eoa = Account.from_key(self.private_key).address

        # ── 3. Проверяем MATIC баланс EOA ─────────────────────────────────────
        matic = self._matic_balance(eoa)
        if matic < 0.001:
            return {
                "ok": False,
                "error": (f"Нет MATIC для газа. На EOA {eoa[:12]}... только "
                          f"{matic:.6f} MATIC. Пополни минимум на 0.01 MATIC."),
                "eoa": eoa,
                "matic": round(matic, 8),
            }

        # ── 4. Константы ──────────────────────────────────────────────────────
        CTF     = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        USDC_E  = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        # redeemPositions(address,bytes32,bytes32,uint256[]) → selector 0x01b7037c
        REDEEM_SEL   = bytes.fromhex("01b7037c")
        # executeCall(address,uint256,bytes)               → selector 0x9e5d4c49
        EXEC_SEL     = bytes.fromhex("9e5d4c49")
        PARENT_COLL  = b"\x00" * 32
        CHAIN_ID     = 137  # Polygon

        gas_price = self._get_gas_price()
        funder_cs = self.funder_address  # proxy wallet

        # ── 5. Редим каждую позицию ────────────────────────────────────────────
        redeemed_count = 0
        redeemed_amount = 0.0
        errors = []

        for pos in redeemable:
            cid_hex = (pos.get("conditionId") or "").replace("0x", "")
            outcome_index = int(pos.get("outcomeIndex", 0))
            size = float(pos.get("size", 0))

            if not cid_hex or len(cid_hex) != 64:
                errors.append(f"Bad conditionId: {cid_hex[:16]}")
                continue

            cid_bytes = bytes.fromhex(cid_hex)
            index_set = 1 << outcome_index  # YES=1, NO=2

            try:
                # Кодируем calldata для CTF.redeemPositions
                redeem_args = abi_encode(
                    ["address", "bytes32", "bytes32", "uint256[]"],
                    [USDC_E, PARENT_COLL, cid_bytes, [index_set]],
                )
                redeem_data = REDEEM_SEL + redeem_args

                # Кодируем calldata для proxy.executeCall(CTF, 0, redeem_data)
                exec_args = abi_encode(
                    ["address", "uint256", "bytes"],
                    [CTF, 0, redeem_data],
                )
                tx_data = EXEC_SEL + exec_args

                nonce = self._get_nonce(eoa)

                # Собираем и подписываем транзакцию
                tx = {
                    "to": funder_cs,
                    "nonce": nonce,
                    "gasPrice": gas_price,
                    "gas": 400_000,
                    "value": 0,
                    "data": "0x" + tx_data.hex(),
                    "chainId": CHAIN_ID,
                }
                signed = Account.sign_transaction(tx, self.private_key)
                tx_hash = self._send_raw_tx("0x" + signed.raw_transaction.hex())

                log.info("[redeem] ✅ cid=...%s size=%.2f tx=%s",
                         cid_hex[-8:], size, tx_hash[:20] if tx_hash else "?")
                redeemed_count += 1
                redeemed_amount += size

            except Exception as e:
                err_msg = str(e)[:150]
                log.warning("[redeem] ❌ cid=...%s: %s", cid_hex[-8:], err_msg)
                errors.append(err_msg)

        return {
            "ok": True,
            "redeemed": redeemed_count,
            "amount": round(redeemed_amount, 2),
            "errors": errors,
        }