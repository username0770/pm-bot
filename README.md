# ARB BOT — BetBurger + PS3838 + Polymarket

Два независимых бота:
- **ArbBot** — вилки: PS3838 (Pinnacle) + Polymarket
- **ValueBetBot** — вэлью-беты только на Polymarket

---

## Быстрый старт (VSCode)

### 1. Установить Python 3.11+
https://www.python.org/downloads/

### 2. Открыть папку в VSCode
```
File → Open Folder → выбрать папку arb_bot
```

### 3. Открыть терминал в VSCode
```
Terminal → New Terminal  (или Ctrl+`)
```

### 4. Создать виртуальное окружение
```bash
python -m venv .venv
```

Активировать:
- Windows:  `.venv\Scripts\activate`
- Mac/Linux: `source .venv/bin/activate`

### 5. Установить зависимости
```bash
pip install -r requirements.txt
```

### 6. Создать .env файл
Скопировать `.env.example` → `.env` и заполнить:
```bash
copy .env.example .env        # Windows
cp .env.example .env          # Mac/Linux
```

Открыть `.env` в VSCode и вписать свои ключи (см. ниже).

### 7. Тест подключения Polymarket
```bash
python test_polymarket.py
```

Скрипт:
- Проверит .env
- Покажет баланс USDC
- Найдёт активный рынок
- Покажет стакан
- Предложит поставить $1 тестовый ордер

### 8. Запуск полного бота
```bash
python run.py           # оба бота
python run.py arb       # только вилки
python run.py value     # только вэлью
python run.py stats     # статистика
```

---

## Где взять ключи

### Polymarket (PRIVATE_KEY + FUNDER)
1. Зайти на polymarket.com
2. Клик на аватар → Settings
3. В левом меню: **Private Key**
4. Нажать **Start Export** → войти через Magic.link
5. Скопировать ключ → вставить в `.env` как `POLYMARKET_PRIVATE_KEY`
6. Публичный адрес кошелька (`0x...`) → `POLYMARKET_FUNDER`

⚠️ Приватный ключ = полный доступ к кошельку. Только в `.env` локально.

### BetBurger (TOKEN + FILTER_ID)
1. betburger.com → My Account → API
2. Скопировать токен → `BETBURGER_TOKEN`
3. My Account → Multifilters → создать фильтр с Polymarket + PS3838
4. ID фильтра виден в URL или в настройках → `BETBURGER_FILTER_ID`

### PS3838
Логин и пароль от аккаунта → `PS3838_USERNAME` / `PS3838_PASSWORD`

---

## Пополнить USDC на Polymarket

Polymarket работает на Polygon, нужен USDC:
1. Купить USDC на бирже (Binance, OKX и т.д.)
2. Вывести на Polygon сеть (не Ethereum!)
3. Адрес — твой `POLYMARKET_FUNDER`

Или прямо на сайте Polymarket: **Deposit** → карта/крипто.

---

## Структура файлов

```
arb_bot/
├── run.py               ← запуск
├── main.py              ← ArbBot (вилки)
├── valuebet_bot.py      ← ValueBetBot (вэлью)
├── betburger_client.py  ← BetBurger API
├── ps3838_client.py     ← PS3838 API
├── polymarket_client.py ← Polymarket CLOB
├── db.py                ← БД вилок
├── db_valuebet.py       ← БД вэлью
├── models.py            ← модели данных
├── config.py            ← конфиг из .env
├── test_polymarket.py   ← тест подключения ← НАЧАТЬ СЮДА
├── requirements.txt
├── .env.example         ← шаблон
├── .env                 ← твои ключи (создать вручную)
├── .gitignore
└── demo.html            ← визуальный симулятор
```

---

## Закрыть вэлью-ставку после события

```bash
python run.py settle <uid> won    # выиграла
python run.py settle <uid> lost   # проиграла
```

uid — первые 8 символов из лога или `python run.py stats`
