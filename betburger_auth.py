"""
BetBurger автообновление токена через /users/sign_in
------------------------------------------------------
Как работает:
  1. POST /users/sign_in  {user: {email, password}}
  2. Ответ содержит заголовок Authorization: Bearer <jwt>
     или тело {"jti": "..."}  — это и есть access_token для API
  3. Также GET /settings/user_data возвращает {"jti": "..."} при живой сессии

Использование:
  auth = BetBurgerAuth(email="...", password="...")
  token = await auth.get_token()   # возвращает свежий токен
"""

import asyncio
import logging
import time
import aiohttp

log = logging.getLogger(__name__)

LOGIN_URL    = "https://www.betburger.com/users/sign_in"
USER_DATA_URL = "https://www.betburger.com/settings/user_data"

# Время жизни токена — обновляем с запасом каждые 50 минут
TOKEN_TTL = 50 * 60


class BetBurgerAuth:
    def __init__(self, email: str, password: str):
        self.email    = email
        self.password = password
        self._token: str | None = None
        self._expires_at: float = 0
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # cookie_jar важен — сессионные куки сохраняются между запросами
            self._session = aiohttp.ClientSession(
                cookie_jar=aiohttp.CookieJar()
            )
        return self._session

    async def get_token(self) -> str:
        """Возвращает свежий токен, обновляя при необходимости."""
        if self._token and time.time() < self._expires_at:
            return self._token
        return await self._refresh()

    async def _refresh(self) -> str:
        log.info("BetBurger: обновляем токен через логин...")
        session = await self._get_session()

        # Шаг 1: логин
        try:
            async with session.post(
                LOGIN_URL,
                json={"user": {"email": self.email, "password": self.password}},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 401:
                    raise ValueError("BetBurger: неверный email или пароль")
                if resp.status not in (200, 201):
                    text = await resp.text()
                    raise ValueError(f"BetBurger login ошибка {resp.status}: {text[:200]}")

                data = await resp.json()
                log.debug("BetBurger login ответ: %s", list(data.keys()))

                # Токен может быть в теле ответа как jti
                token = data.get("jti") or data.get("access_token") or data.get("token")

                # Или в заголовке Authorization
                if not token:
                    auth_header = resp.headers.get("Authorization", "")
                    if auth_header.startswith("Bearer "):
                        token = auth_header[7:]

        except aiohttp.ClientError as e:
            raise ConnectionError(f"BetBurger login сетевая ошибка: {e}")

        # Шаг 2: если токен не в логин-ответе — берём из settings/user_data
        if not token:
            log.debug("jti не в логин-ответе, пробуем /settings/user_data...")
            try:
                async with session.get(
                    USER_DATA_URL,
                    headers={"Accept": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp2:
                    if resp2.status == 200:
                        udata = await resp2.json()
                        token = udata.get("jti")
                        log.debug("user_data keys: %s", list(udata.keys()))
            except Exception as e:
                log.warning("BetBurger user_data запрос упал: %s", e)

        if not token:
            raise ValueError("BetBurger: не удалось получить токен ни из логина, ни из user_data")

        self._token      = token
        self._expires_at = time.time() + TOKEN_TTL
        log.info("BetBurger: токен обновлён (первые 8 симв: %s...)", token[:8])
        return token

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ──────────────────────────────────────────────
# Быстрый тест
# ──────────────────────────────────────────────
async def _test():
    import os
    from dotenv import load_dotenv
    load_dotenv()

    email    = os.getenv("BETBURGER_EMAIL", "")
    password = os.getenv("BETBURGER_PASSWORD", "")

    if not email or not password:
        print("❌ Задай BETBURGER_EMAIL и BETBURGER_PASSWORD в .env")
        return

    auth = BetBurgerAuth(email, password)
    try:
        token = await auth.get_token()
        print(f"✅ Токен получен: {token[:8]}...{token[-4:]}")
        print(f"   Длина: {len(token)} символов")

        # Проверяем токен реальным запросом
        import aiohttp as ah
        filter_id = int(os.getenv("BETBURGER_FILTER_ID_VALUEBET", "665262"))
        fdata = ah.FormData()
        fdata.add_field("search_filter[]", str(filter_id))
        fdata.add_field("per_page", "10")
        async with ah.ClientSession() as s:
            async with s.post(
                "https://rest-api-pr.betburger.com/api/v1/valuebets/bot_pro_search",
                params={"access_token": token, "locale": "en"},
                data=fdata,
                timeout=ah.ClientTimeout(total=15)
            ) as r:
                raw = await r.json()
                bets = raw.get("bets", []) if isinstance(raw, dict) else raw
                poly = [b for b in bets if b.get("bookmaker_id") == 483]
                print(f"✅ API проверка: всего бетов={len(bets)}, Polymarket={len(poly)}")
    finally:
        await auth.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(_test())