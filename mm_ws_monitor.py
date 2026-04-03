"""
WebSocket монитор ликвидности для Market Making.

Подключается к Polymarket WS и отслеживает:
- price_change events (добавление/удаление ордеров)
- Массовое снятие ордеров (много price_change с size=0 за короткий период)
- Резкие изменения best_bid_ask

Вызывает callback при обнаружении опасных паттернов.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional

log = logging.getLogger(__name__)

WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class LiquidityMonitor:
    """Мониторит ликвидность через WebSocket и вызывает callback при danger."""

    def __init__(self, on_danger: Callable[[str, str, dict], None]):
        """
        on_danger(condition_id, reason, details) — вызывается при обнаружении опасности.
        reason: 'mass_cancel' | 'bba_jump' | 'volume_drop'
        """
        self._on_danger = on_danger
        self._tokens: dict[str, str] = {}  # token_id → condition_id
        self._ws = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Per-token state
        self._cancel_counts: dict[str, list] = {}   # token → [(ts, side), ...] последние cancels
        self._prev_bba: dict[str, tuple] = {}        # token → (best_bid, best_ask)
        self._book_depth: dict[str, dict] = {}       # token → {bid_levels: N, ask_levels: N, bid_volume: X, ask_volume: X}

    def subscribe(self, token_id: str, condition_id: str):
        """Добавить токен для мониторинга."""
        self._tokens[token_id] = condition_id
        if self._ws and self._running:
            asyncio.ensure_future(self._send_subscribe(token_id))

    def unsubscribe(self, token_id: str):
        """Убрать токен."""
        self._tokens.pop(token_id, None)
        self._cancel_counts.pop(token_id, None)
        self._prev_bba.pop(token_id, None)
        self._book_depth.pop(token_id, None)

    async def _send_subscribe(self, token_id: str):
        if self._ws:
            try:
                msg = json.dumps({"assets_ids": [token_id], "operation": "subscribe"})
                await self._ws.send(msg)
            except Exception:
                pass

    async def run(self):
        """Основной цикл — подключение и обработка сообщений."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                log.debug("[mm-ws] Connection error: %s — reconnect in 5s", e)
                await asyncio.sleep(5)

    async def _connect_and_listen(self):
        import websockets
        async with websockets.connect(WS_URI, ping_interval=30, ping_timeout=10) as ws:
            self._ws = ws
            log.info("[mm-ws] Connected to Polymarket WS")

            # Subscribe to all tracked tokens
            if self._tokens:
                sub = {
                    "type": "market",
                    "assets_ids": list(self._tokens.keys()),
                    "custom_feature_enabled": True
                }
                await ws.send(json.dumps(sub))

            async for raw in ws:
                if not self._running:
                    break
                try:
                    data = json.loads(raw)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        self._process_event(item)
                except Exception as e:
                    log.debug("[mm-ws] Event error: %s", e)

    def _process_event(self, event: dict):
        evt_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if asset_id not in self._tokens:
            return

        cid = self._tokens[asset_id]
        now = time.time()

        if evt_type == "book":
            # Initial snapshot — записываем глубину
            bids = event.get("bids", [])
            asks = event.get("asks", [])
            bid_vol = sum(float(b.get("size", 0)) for b in bids)
            ask_vol = sum(float(a.get("size", 0)) for a in asks)
            self._book_depth[asset_id] = {
                "bid_levels": len(bids),
                "ask_levels": len(asks),
                "bid_volume": bid_vol,
                "ask_volume": ask_vol,
                "ts": now,
            }

        elif evt_type == "price_change":
            # Ордер добавлен/изменён/убран на определённом уровне
            changes = event.get("changes", [])
            for change in changes:
                # change = {"price": "0.55", "side": "BUY", "size": "0"}
                # size=0 означает уровень полностью убран
                size = float(change.get("size", "0"))
                side = change.get("side", "")
                price = change.get("price", "")

                if size == 0:
                    # Ордер убран — потенциальный cancel
                    cancels = self._cancel_counts.setdefault(asset_id, [])
                    cancels.append((now, side, price))
                    # Очистка старых (>3 сек)
                    cancels[:] = [(t, s, p) for t, s, p in cancels if now - t < 3]

                    # Если >5 cancels за 3 сек — массовое снятие
                    if len(cancels) >= 5:
                        log.warning("[mm-ws] 🚨 MASS CANCEL detected on %s: %d levels removed in 3s",
                                    asset_id[:16], len(cancels))
                        self._on_danger(cid, "mass_cancel", {
                            "cancels": len(cancels),
                            "token": asset_id,
                        })
                        cancels.clear()

        elif evt_type == "best_bid_ask":
            # Top-of-book update
            bid = float(event.get("best_bid", 0))
            ask = float(event.get("best_ask", 0))
            prev = self._prev_bba.get(asset_id)
            self._prev_bba[asset_id] = (bid, ask)

            if prev:
                pb, pa = prev
                # Спред резко вырос
                old_spread = pa - pb if pa > pb else 0
                new_spread = ask - bid if ask > bid else 0
                if old_spread > 0 and new_spread > old_spread * 3 and new_spread > 0.03:
                    log.warning("[mm-ws] 🚨 BBA SPREAD JUMP on %s: %.2f→%.2f (%.0fx)",
                                asset_id[:16], old_spread, new_spread, new_spread / old_spread)
                    self._on_danger(cid, "bba_jump", {
                        "old_spread": old_spread,
                        "new_spread": new_spread,
                        "token": asset_id,
                    })

    def stop(self):
        self._running = False
        if self._ws:
            asyncio.ensure_future(self._ws.close())
