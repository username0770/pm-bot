"""Telegram notifier — шлёт уведомления о работе pm-bot + принимает
команды через long-polling.
Использует прямые HTTP-запросы через aiohttp, без внешних библиотек.
"""
import os
import logging
import time
import asyncio
from datetime import datetime
from typing import Optional, Callable, Awaitable, Union

import aiohttp

logger = logging.getLogger("telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


class TelegramNotifier:
    """Отправляет уведомления в Telegram.
    Использует прямые HTTP запросы без telegram-library."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self._last_hourly: float = 0.0
        self._last_sent: dict = {}  # per-key cooldowns
        self._enabled = bool(token and chat_id)
        # Command polling state
        self._commands: dict = {}      # name -> async/sync handler(args_text)
        self._poll_offset: int = 0     # next update_id
        self._poll_running: bool = False

    async def send(
        self, text: str, parse_mode: str = "HTML",
        cooldown_key: Optional[str] = None,
        cooldown_sec: float = 0,
    ) -> bool:
        """Отправляет сообщение. cooldown_key/sec — троттлинг повторов."""
        if not self._enabled:
            return False

        if cooldown_key and cooldown_sec > 0:
            now = time.time()
            last = self._last_sent.get(cooldown_key, 0)
            if now - last < cooldown_sec:
                return False
            self._last_sent[cooldown_key] = now

        url = TELEGRAM_API.format(token=self.token, method="sendMessage")
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 200:
                        return True
                    body = await r.text()
                    logger.warning(
                        f"Telegram error {r.status}: {body[:200]}"
                    )
                    return False
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
            return False

    # ── Разовые уведомления ────────────────────────────────

    async def notify_loss(
        self,
        window_id: str,
        price: float,
        size: float,
        usdc_spent: float,
        outcome: str,
        seconds_to_expiry: Optional[float] = None,
        token_label: str = "",
    ) -> None:
        """Уведомление о проигранной ставке MakerBuy."""
        loss = round(usdc_spent, 2)
        sec_line = (f"⏱ При филле оставалось: {seconds_to_expiry:.0f}с\n"
                    if seconds_to_expiry is not None else "")
        text = (
            f"❌ <b>ПРОИГРЫШ MakerBuy</b>\n\n"
            f"📍 Окно: <code>{window_id[-20:]}</code>\n"
            f"🎯 Ставили на: {token_label or '?'}\n"
            f"💸 Цена входа: {price:.3f}\n"
            f"📦 Шаров: {size:.1f}\n"
            f"💵 Потрачено: ${usdc_spent:.2f}\n"
            f"📉 Убыток: <b>-${loss}</b>\n"
            f"{sec_line}"
            f"🎲 Итог: <b>{outcome}</b>\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(text)

    async def notify_losses_batch(self, losses: list) -> None:
        """Один сводный лог по нескольким проигрышам в окне."""
        if not losses:
            return
        total = round(sum(l.get("usdc_spent", 0) for l in losses), 2)
        window_id = losses[0].get("window_id", "?")
        outcome = losses[0].get("outcome", "?")
        lines = []
        for l in losses[:10]:
            lines.append(
                f"  • {l.get('token_label','?')} "
                f"{l.get('size',0):.1f}sh @ {l.get('price',0):.3f} "
                f"= ${l.get('usdc_spent',0):.2f}"
            )
        more = f"\n  ... и ещё {len(losses) - 10}" if len(losses) > 10 else ""
        text = (
            f"❌ <b>ПРОИГРЫШ MakerBuy</b>\n\n"
            f"📍 Окно: <code>{window_id[-20:]}</code>\n"
            f"🎲 Итог: <b>{outcome}</b>\n"
            f"📦 Филлов: {len(losses)}\n"
            f"{chr(10).join(lines)}{more}\n\n"
            f"💸 Итого убыток: <b>-${total}</b>\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(text)

    async def notify_error(
        self, error_msg: str, *, source: str = "",
        cooldown_sec: float = 60,
    ) -> None:
        """Уведомление об ошибке. Тот же source не чаще раза в минуту."""
        text = (
            f"⚠️ <b>ОШИБКА БОТА</b>\n\n"
            f"{'📍 Источник: ' + source + chr(10) if source else ''}"
            f"<code>{str(error_msg)[:400]}</code>\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(
            text,
            cooldown_key=f"err:{source}" if source else "err:global",
            cooldown_sec=cooldown_sec,
        )

    async def notify_split_failed(self, window_id: str, error: str) -> None:
        text = (
            f"⚠️ <b>Split failed</b>\n\n"
            f"Окно: <code>{window_id[-20:]}</code>\n"
            f"Ошибка: {error[:200]}\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(text)

    async def send_startup(self, enabled_modules: Optional[list] = None) -> None:
        mods = enabled_modules or []
        lines = "\n".join(f"  {m}" for m in mods) if mods else "  (none)"
        text = (
            f"🚀 <b>PM Bot запущен</b>\n\n"
            f"Модули:\n{lines}\n"
            f"🕐 {datetime.now().strftime('%d.%m %H:%M:%S')}"
        )
        await self.send(text)

    async def send_shutdown(self, reason: str = "") -> None:
        text = (
            f"🛑 <b>PM Bot остановлен</b>\n"
            f"{reason}\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
        )
        await self.send(text)

    # ── Часовой отчёт ──────────────────────────────────────

    async def send_hourly_report(self, stats: dict) -> None:
        """Дайджест. Внутренний кулдаун 60 мин чтобы случайно не задвоить."""
        now = time.time()
        if now - self._last_hourly < 3600:
            return
        self._last_hourly = now
        await self._send_report(stats)

    async def _send_report(self, stats: dict) -> None:
        winrate = stats.get("winrate_pct", 0)
        pnl = stats.get("total_pnl_today", 0)
        trades = stats.get("trades_today", 0)
        wins = stats.get("wins_today", 0)
        losses = max(0, trades - wins)
        cash = stats.get("cash_usdc", 0)
        portfolio = stats.get("portfolio_value", 0)
        winnings = stats.get("winnings_usdc", 0)
        bot_alive = stats.get("bot_alive", True)
        last_min = stats.get("last_trade_minutes_ago", 0)

        status_icon = "✅" if bot_alive else "🔴"
        pnl_icon = "📈" if pnl >= 0 else "📉"

        text = (
            f"{status_icon} <b>Отчёт · {datetime.now().strftime('%H:%M')}</b>\n\n"
            f"💼 Портфель: <b>${portfolio:.2f}</b>\n"
            f"💵 Наличные: ${cash:.2f}\n"
            f"🏆 К получению: ${winnings:.2f}\n\n"
            f"📊 Сегодня:\n"
            f"  Ставок: <b>{trades}</b> ({wins}✓ / {losses}✗)\n"
            f"  Winrate: {winrate:.1f}%\n"
            f"  {pnl_icon} P&L: <b>${pnl:+.2f}</b>\n\n"
            f"⏱ Последний филл: {last_min:.0f} мин назад"
        )

        if not bot_alive:
            text += "\n\n🔴 <b>БОТ НЕ АКТИВЕН!</b>"
        elif last_min > 30:
            text += "\n\n⚠️ Нет филлов более 30 минут"

        await self.send(text)


    # ── Команды (long-polling getUpdates) ───────────────────

    def register_command(
        self, name: str,
        handler: Callable[[str], Union[str, Awaitable[str], None]],
    ) -> None:
        """Register /command handler.
        Handler receives args_text (everything after the command).
        Can be sync or async. Return string is sent as response.
        None/empty return means no reply.
        """
        key = name.lstrip("/").lower()
        self._commands[key] = handler

    async def poll_updates_loop(self) -> None:
        """Long-poll Telegram getUpdates and dispatch commands.
        Only accepts commands from the authorized chat_id.
        Runs until cancelled."""
        if not self._enabled:
            return
        if self._poll_running:
            logger.warning("Telegram poll loop already running")
            return
        self._poll_running = True
        logger.info("Telegram poll loop started")
        try:
            while True:
                try:
                    await self._poll_once()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Telegram poll err: {e}")
                    await asyncio.sleep(5)
        finally:
            self._poll_running = False

    async def _poll_once(self) -> None:
        url = TELEGRAM_API.format(token=self.token, method="getUpdates")
        # Long-poll: Telegram holds the connection up to `timeout` seconds
        params = {"offset": self._poll_offset, "timeout": 25}
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                if r.status != 200:
                    await asyncio.sleep(2)
                    return
                data = await r.json()
        if not data or not data.get("ok"):
            return
        for upd in data.get("result", []):
            self._poll_offset = upd.get("update_id", 0) + 1
            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            chat_id = str(msg.get("chat", {}).get("id", ""))
            from_id = str(msg.get("from", {}).get("id", ""))
            text = (msg.get("text") or "").strip()
            # Authorization: only authorized chat_id can issue commands
            if chat_id != self.chat_id and from_id != self.chat_id:
                logger.warning(
                    f"Telegram unauthorized msg from chat={chat_id} "
                    f"user={from_id}: {text[:60]}"
                )
                continue
            if not text.startswith("/"):
                continue
            # Parse "/cmd rest of args"  or  "/cmd@botname rest"
            parts = text.split(maxsplit=1)
            cmd = parts[0].lstrip("/").split("@")[0].lower()
            args = parts[1] if len(parts) > 1 else ""
            handler = self._commands.get(cmd)
            if handler is None:
                await self.send(
                    f"Unknown command: /{cmd}\nTry /help"
                )
                continue
            try:
                result = handler(args)
                if asyncio.iscoroutine(result):
                    result = await result
                if result:
                    await self.send(str(result))
            except Exception as e:
                logger.warning(f"Telegram /{cmd} handler err: {e}")
                try:
                    await self.send(f"⚠️ Ошибка в /{cmd}: {e}")
                except Exception:
                    pass


def create_telegram_notifier() -> Optional[TelegramNotifier]:
    """Создаёт TelegramNotifier из env или None если выключен/не настроен."""
    enabled = os.getenv("TELEGRAM_ENABLED", "false").lower() == "true"
    if not enabled:
        return None
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        logger.warning(
            "Telegram enabled but TELEGRAM_BOT_TOKEN/CHAT_ID not set"
        )
        return None
    return TelegramNotifier(token=token, chat_id=chat_id)
