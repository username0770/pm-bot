# Plan: Оптимизация архитектуры Hedge Analyzer

## Проблема
- 14K+ пар генерируются (NCAA), каждая требует API запрос за ценой
- UI виснет при Scan/Analyze — слишком долго
- Нет автоматического определения knockout стадий
- League-формат (NBA regular season) не работает с текущей моделью

## Решение: 3 уровня оптимизации

### 1. Умная фильтрация ДО запроса цен (убираем 90% пар)

**В `gamma_client.py`:**
- Фильтр по `outcome_prices` из Gamma API (цена уже есть в event data!)
  - Gamma event содержит поле `outcomePrices: "[0.55, 0.45]"` — используем его
  - Отсекаем пары где турнирная цена < 3% (лонгшоты без ROI)
  - Отсекаем пары где матчевая цена > 95% или < 5% (слишком однозначные)
- Фильтр `knockout_only=True` — пропускаем league-формат
- **Определение knockout автоматически:**
  - Tennis Grand Slams, NCAA Tournament, UCL knockout stage → knockout
  - NBA/NHL regular season, Soccer leagues → league
  - Добавить `_is_knockout_format(event_title, tag)` метод
- Лимит пар на спорт: макс 200 (топ по турнирной цене)
- **Результат**: вместо 14K пар → ~200-500

### 2. Batch price fetching + кэш (ускоряем в 10x)

**В `_run_analyze_worker`:**
- Уже реализовано: `ThreadPoolExecutor(20)` + `PriceCache(ttl=60)`
- Добавить: использование `outcome_prices` из Gamma как fallback
  - Gamma цены менее точные (обновляются реже), но для первичного фильтра достаточно
- Фетчить CLOB цены только для пар прошедших Gamma-фильтр (top ~50-100)
- Параллелить liquidity checks тоже через executor

### 3. Автоматический режим + knockout detection

**Новые config параметры:**
- `HEDGE_KNOCKOUT_ONLY: true` — сканировать только knockout турниры
- `HEDGE_MIN_TOURNEY_PRICE: 0.03` — минимальная турнирная цена (3%)
- `HEDGE_MAX_PAIRS_PER_SPORT: 200` — лимит пар

**`_is_knockout_format()` логика:**
```
Knockout (is_knockout=True):
- Теннис: Grand Slam, ATP/WTA турниры (всегда knockout)
- NCAA: Tournament (March Madness)
- UFC/MMA: всегда knockout
- Soccer: UCL knockout, World Cup knockout
- NFL: Playoffs

League (is_knockout=False):
- NBA regular season (unless "Playoffs"/"Finals" в title)
- NHL regular season
- Soccer: Premier League, La Liga, etc.
- MLB regular season
```

**UI изменения:**
- Checkbox "Knockout only" (default on)
- Progress bar при analyze (уже есть через polling)
- Показывать badge [KO] или [League] рядом с opportunity

## Порядок реализации

1. `gamma_client.py`: добавить `_is_knockout_format()`, фильтр по outcome_prices, лимит пар
2. `config.py`: новые параметры
3. `dashboard_server.py` (`_run_analyze_worker`): использовать Gamma prices для pre-filter, параллелить liquidity
4. UI: checkbox knockout, badge KO/League

## Оценка результата
- Время Analyze: 60-90с → 5-10с
- Количество API запросов: 4000+ → 100-200
- Качество: только реально работающие knockout-пары
