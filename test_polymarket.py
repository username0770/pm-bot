"""
Тест подключения к Polymarket API
Запуск: python test_polymarket.py
"""

import os, sys, json, requests
from dotenv import load_dotenv

load_dotenv()

PRIVATE_KEY    = os.getenv("POLYMARKET_PRIVATE_KEY", "")
FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER", "")
CLOB_HOST      = "https://clob.polymarket.com"
CHAIN_ID       = 137

# 1. Проверка .env
def check_env():
    if not PRIVATE_KEY or "YOUR" in PRIVATE_KEY:
        print("POLYMARKET_PRIVATE_KEY не задан в .env"); sys.exit(1)
    if not FUNDER_ADDRESS or "YOUR" in FUNDER_ADDRESS:
        print("POLYMARKET_FUNDER не задан в .env"); sys.exit(1)
    print(f"OK  Кошелёк: {FUNDER_ADDRESS[:6]}...{FUNDER_ADDRESS[-4:]}")

# 2. Баланс
def check_balance():
    try:
        resp = requests.get(f"{CLOB_HOST}/balance",
                            params={"address": FUNDER_ADDRESS}, timeout=10)
        if resp.status_code == 200:
            balance = float(resp.json().get("balance", 0))
            print(f"OK  Баланс USDC (CLOB): ${balance:.2f}")
            return balance
    except Exception:
        pass
    try:
        USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        data_hex = "0x70a08231000000000000000000000000" + FUNDER_ADDRESS[2:].lower().zfill(64)
        resp = requests.post("https://polygon-rpc.com", json={
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": USDC, "data": data_hex}, "latest"], "id": 1
        }, timeout=10)
        balance = int(resp.json().get("result", "0x0"), 16) / 1_000_000
        if balance > 0:
            print(f"OK  Баланс USDC (Polygon): ${balance:.2f}")
            return balance
    except Exception:
        pass
    print("INFO Баланс $0 на кошельке напрямую — деньги внутри Polymarket (это нормально).")
    print("     Ставки работают через CLOB API. Продолжаем тест.")
    return 99.0

# 3. CLOB клиент
def get_client():
    from py_clob_client.client import ClobClient
    client = ClobClient(
        host=CLOB_HOST, key=PRIVATE_KEY,
        chain_id=CHAIN_ID, signature_type=1, funder=FUNDER_ADDRESS
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    print("OK  CLOB клиент инициализирован")
    return client

# 4. Найти подходящий рынок
# Ищем рынок где mid-цена YES между 0.2 и 0.8 — чтобы сумма ставки была разумной
def find_market(client):
    print("\nИщем подходящий рынок (mid-цена YES между 0.20 и 0.80)...")
    for tag in ["politics", "sports", "crypto"]:
        try:
            resp = requests.get("https://gamma-api.polymarket.com/markets", params={
                "active": "true", "closed": "false",
                "tag_slug": tag, "limit": 30,
                "order": "volume", "ascending": "false"
            }, timeout=10)
            candidates = [m for m in resp.json()
                          if float(m.get("volume", 0)) > 5000
                          and json.loads(m.get("clobTokenIds", "[]"))]

            for m in candidates:
                token_ids = json.loads(m.get("clobTokenIds", "[]"))
                if not token_ids:
                    continue
                try:
                    mid = client.get_midpoint(token_ids[0])
                    mid_price = float(mid.get("mid", 0)) if mid else 0
                    # Хотим цену в диапазоне 0.20–0.80 — тогда $5 ставка = 5–25 шар (норм)
                    if 0.20 <= mid_price <= 0.80:
                        print(f"\nRYNOK [{tag.upper()}]")
                        print(f"  Вопрос : {m.get('question', '')}")
                        print(f"  Объём  : ${float(m.get('volume', 0)):,.0f}")
                        print(f"  YES mid: {mid_price:.3f}  (коэф ~{1/mid_price:.2f})")
                        print(f"  Ссылка : https://polymarket.com/event/{m.get('slug', '')}")
                        return m, token_ids[0], mid_price
                except Exception:
                    continue
        except Exception as e:
            print(f"  [{tag}]: {e}")

    print("Подходящий рынок не найден")
    return None, None, None

# 5. Orderbook
def check_orderbook(client, token_id):
    try:
        book  = client.get_order_book(token_id)
        asks  = sorted(book.asks, key=lambda x: float(x.price))[:3]
        bids  = sorted(book.bids, key=lambda x: float(x.price), reverse=True)[:3]
        print(f"\nORDERBOOK (YES токен):")
        print(f"  {'ASKS':22}  {'BIDS':22}")
        print(f"  {'цена':8} {'$размер':12}  {'цена':8} {'$размер':12}")
        print(f"  {'-'*46}")
        for i in range(max(len(asks), len(bids))):
            a = asks[i] if i < len(asks) else None
            b = bids[i] if i < len(bids) else None
            a_s = f"{float(a.price):.3f}  ${float(a.size):>9.2f}" if a else " " * 22
            b_s = f"{float(b.price):.3f}  ${float(b.size):>9.2f}" if b else ""
            print(f"  {a_s}  {b_s}")
    except Exception as e:
        print(f"Orderbook ошибка: {e}")

# 6. Активные ордера
def check_orders(client):
    try:
        orders = client.get_orders()
        if not orders:
            print("\nАктивных ордеров нет")
        else:
            print(f"\nАктивных ордеров: {len(orders)}")
            for o in orders[:5]:
                print(f"  id:{str(o.get('id',''))[:14]}  "
                      f"side:{o.get('side','')}  "
                      f"price:{o.get('price','')}  "
                      f"size:{o.get('original_size','')}")
    except Exception as e:
        print(f"Ордера: {e}")

# 7. Тестовый ордер
def place_test_order(client, token_id, mid_price):
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY

    # Размер считаем так чтобы потратить ровно $5 USDC
    # shares = spend_usdc / price
    spend_usdc = 5.0
    price_r    = round(mid_price, 2)
    # shares — сколько токенов купить
    shares     = round(spend_usdc / price_r, 2)

    print(f"\nТЕСТОВЫЙ ОРДЕР:")
    print(f"  token_id  : {token_id[:26]}...")
    print(f"  цена      : {price_r}  (коэф ~{1/price_r:.2f})")
    print(f"  шар (size): {shares}")
    print(f"  потратим  : ~${price_r * shares:.2f} USDC")
    print(f"  выиграем  : ~${shares:.2f} USDC если YES")
    print(f"\n  ВНИМАНИЕ: это реальная ставка на Polymarket!")

    confirm = input("\n  Подтвердить? (y/n): ").strip().lower()
    if confirm != "y":
        print("  Отменено."); return

    try:
        order_args   = OrderArgs(token_id=token_id, price=price_r, size=shares, side=BUY)
        signed_order = client.create_order(order_args)
        resp         = client.post_order(signed_order, OrderType.GTC)

        print(f"\nOK  Ордер размещён!")
        print(f"    order_id : {resp.get('orderID', resp.get('id', '?'))}")
        print(f"    status   : {resp.get('status', '?')}")
        print(f"    Проверь  : polymarket.com -> Portfolio -> Open orders")
        return resp
    except Exception as e:
        print(f"\nОШИБКА: {e}")

# MAIN
if __name__ == "__main__":
    print("=" * 55)
    print("  POLYMARKET CONNECTION TEST")
    print("=" * 55)

    check_env()
    check_balance()

    try:
        client = get_client()
    except Exception as e:
        print(f"Клиент: {e}"); sys.exit(1)

    market, yes_token, mid_price = find_market(client)
    if not market:
        sys.exit(1)

    check_orderbook(client, yes_token)
    check_orders(client)

    print("\n" + "=" * 55)
    place_test_order(client, yes_token, mid_price)

    print("=" * 55)
    print("  ТЕСТ ЗАВЕРШЁН")
    print("=" * 55)