# Backlog — Ideas & Future Plans

## High Priority

### Live Score Arbitrage
Интегрировать быстрый источник live scores (WebSocket от букмекеров или ESPN).
Искать ставки с завышенными коэффициентами которые ещё не отреагировали на гол/событие.
Подключать к этим источникам ставки на итоги, которые с опозданием реагируют на ситуацию в матче.
Например: гол забит → наш источник показывает 1:0 → Polymarket ещё не обновил линию → покупаем.

### Market Making Improvements
- Adaptive spread на основе волатильности (wider spread = больше маржа, меньше fills)
- Cross-market hedging (если есть позиция на Spread -2.5, хеджировать через Moneyline)
- WebSocket fill notifications вместо polling (мгновенная реакция на fills)
- Persisted order state в БД (таблица mm_orders + sync с PM при рестарте)
- Live score integration для circuit breaker (гол = мгновенный cancel)

### Settlement Sniper (модуль near_settlement_bot)
- Мониторить маркеты где price >= 0.95 и accepting_orders = true
- Проверить через внешний API что матч закончился
- Купить за 0.95-0.99 → ждать settlement → получить $1.00
- Двойная проверка = минимум риска void/dispute

## Medium Priority

### Мульти-аккаунт
- Несколько Polymarket аккаунтов для увеличения capacity
- Round-robin или по спортам
- Общий dashboard с агрегированной статистикой

### Stale Line Detection (быстрый источник)
- Подключение к быстрым API (Pinnacle WS, Betfair Exchange)
- Сравнение implied probability PM vs sharp bookmaker
- Alert если расхождение > X%

### Position Exit Strategies
- Auto-sell позиции при определённом P&L (take profit / stop loss)
- Time-based exit: продать за N минут до начала матча
- Trailing stop: продавать если цена развернулась на X%

## Low Priority / Research

### Покупка "мёртвых" исходов за 1c
- Мониторить маркеты с ценой 0.01-0.05
- Фильтровать по спортам где камбэки чаще (теннис, баскетбол)
- Статистический анализ: реальная вероятность vs implied

### Machine Learning Edge
- Модель предсказания на основе линий БК + Polymarket
- Feature engineering: odds movement, volume, time to start
- Backtest на исторических данных

### Automated Market Discovery
- Авто-поиск маркетов с аномальными спредами
- Мониторинг новых маркетов (early liquidity advantage)
- Cross-sport correlation (NBA player props vs game outcome)

---

*Обновлять этот файл при появлении новых идей.*
*Перемещать реализованные пункты в секцию "Done" внизу.*

## Done
- [x] Valuebet bot (prematch + live)
- [x] Auto-resell mode
- [x] Market Making module (basic)
- [x] Dutching module
- [x] Line movement tracking
- [x] Fee tracking
