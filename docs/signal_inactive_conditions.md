# Условия, при которых сигнал считается неактивным для live-торговли

## Основные условия (проверяются в `demo_trade_executor.place_order_for_signal`)

### 1. Сигнал закрыт (signal.status != "ACTIVE")
**Проверка:** `signal.status.upper() in ("CLOSED", "STOP_LOSS", "TAKE_PROFIT")`

**Действие:** Сигнал **полностью игнорируется**, никаких действий на бирже не выполняется.

**Статусы:**
- `CLOSED` - сигнал закрыт (достигнут TP или SL)
- `STOP_LOSS` - сигнал закрыт по стоп-лоссу
- `TAKE_PROFIT` - сигнал закрыт по тейк-профиту

---

### 2. Live-торговля отключена пользователем
**Проверка:** `not is_live_trading_enabled()`

**Действие:** Сигнал помечается `demo_status = "LIVE_DISABLED"`, ордер не ставится.

---

### 3. Bybit API не настроен
**Проверка:** `not bybit_demo_client.is_enabled()`

**Действие:** Сигнал помечается `demo_status = "NOT_CONFIGURED"`, ордер не ставится.

---

### 4. Сигнал уже обработан (финальный demo_status)
**Проверка:** `signal.demo_status.upper() not in RETRYABLE_STATUSES`

**RETRYABLE_STATUSES (могут быть повторно обработаны):**
- `WAITING_FOR_PRICE` - ожидание подхода цены
- `PRICE_DEVIATION_TOO_LARGE` - цена ушла слишком далеко (может вернуться)
- `SIGNAL_TOO_OLD` - сигнал устарел (но может быть обработан, если возраст < 30 мин)
- `INVALID_ENTRY` - некорректная цена входа (может исправиться)
- `INVALID_QUANTITY` - некорректное количество (может исправиться)
- `INVALID_MARKET_PRICE` - некорректная рыночная цена (может исправиться)
- `FAILED` - ошибка при размещении (может быть повторена)
- `CANCELLED` - ордер отменен (может быть повторен)
- `LIVE_DISABLED` - торговля отключена (может быть включена)
- `NOT_CONFIGURED` - API не настроен (может быть настроен)

**ФИНАЛЬНЫЕ статусы (НЕ обрабатываются повторно):**
- `PLACED` - ордер размещен
- `FILLED` - ордер исполнен
- `NEW` - новый ордер на бирже
- `SUBMITTING` - ордер отправляется
- `OPEN_POSITION` - позиция открыта (активная сделка)
- `ORDER_CANCELLED_PRICE_MOVED` - ордер отменен из-за отклонения цены >2%
- `PRICE_DEVIATION_TOO_LARGE` - цена ушла на >2% от уровня (финальный статус)
- `LEVEL_BROKEN` - уровень пробит против нашего направления (финальный статус)
- `SIGNAL_CLOSED_NO_ORDER` - сигнал закрыт без ордера
- `SL_TO_BREAKEVEN` - стоп-лосс установлен в безубыток через открытие новой сделки на минимальный объем

**Действие:** Если `demo_status` в финальных статусах → сигнал **игнорируется**, возвращается `status: "already_processed"`.

---

### 5. Сигнал слишком старый
**Проверка:** `signal_age_seconds > MAX_SIGNAL_AGE_SECONDS` (30 минут = 1800 секунд)

**Расчет:** `(datetime.now(timezone.utc) - signal.timestamp).total_seconds()`

**Действие:** Сигнал помечается `demo_status = "SIGNAL_TOO_OLD"`, ордер не ставится.

**Где проверяется:**
- В `place_order_for_signal` перед попыткой размещения ордера
- В `watch_waiting_signals` для всех блоков:
  - Для сигналов без `demo_status` (pending signals)
  - Для сигналов в статусе `WAITING_FOR_PRICE`
  - Для сигналов в статусе `LEVEL_BROKEN` (при перепроверке)

**ВАЖНО:** Watcher **фильтрует по возрасту на уровне SQL-запроса**, чтобы не обрабатывать недельные сигналы, даже если цена вернулась к старому уровню. Это предотвращает случайное открытие сделок по очень старым сигналам.

---

### 6. Пробитие уровня против нашего направления
**Проверка:** `check_signal_invalidated()` - цена ушла против направления сигнала на значительное расстояние

**Логика:**
- Для **LONG**: если `current_price < level_price` И отклонение >0.2% (цена ушла ниже уровня на >0.2%) → пробитие вниз
- Для **SHORT**: если `current_price > level_price` И отклонение >0.2% (цена ушла выше уровня на >0.2%) → пробитие вверх

**Порог пробития:** 0.2% (чтобы не блокировать сигналы, когда цена практически на уровне, например, отклонение 0.02%)

**Действие:** Сигнал помечается `demo_status = "LEVEL_BROKEN"`, ордер не ставится, сигнал становится **неактивным**.

**Где проверяется:**
- В `place_order_for_signal` перед попыткой размещения ордера
- В `watch_waiting_signals` для сигналов в статусе `WAITING_FOR_PRICE`

---

### 7. Отклонение цены на >2% от уровня
**Проверка:** `check_signal_invalidated()` - `abs((current_price / level_price - 1) * 100) > 2.0`

**Действие:** Сигнал помечается `demo_status = "PRICE_DEVIATION_TOO_LARGE"`, ордер не ставится, сигнал становится **неактивным**.

**Где проверяется:**
- В `place_order_for_signal` перед попыткой размещения ордера
- В `watch_waiting_signals` для сигналов в статусе `WAITING_FOR_PRICE`
- В `watch_waiting_signals` для открытых ордеров (отмена ордера, если цена ушла >2%)

**Примечание:** `PRICE_DEVIATION_TOO_LARGE` теперь **финальный статус** (удален из `RETRYABLE_STATUSES`).

---

## Условия в Watcher (`watch_waiting_signals`)

### 0. Перепроверка LEVEL_BROKEN сигналов
**Условие:** `Signal.status == "ACTIVE"` AND `Signal.demo_status == "LEVEL_BROKEN"` AND `Signal.timestamp >= cutoff_time` (где `cutoff_time = now - MAX_SIGNAL_AGE_SECONDS`)

**Действие:** Watcher **перепроверяет** сигналы, которые были помечены как `LEVEL_BROKEN`, но **только для свежих сигналов** (не старше 30 минут). Если цена вернулась в допустимый диапазон, сигнал восстанавливается в статус `WAITING_FOR_PRICE`.

**Проверки:**
1. **Возраст сигнала:** Если сигнал старше `MAX_SIGNAL_AGE_SECONDS` (30 минут) → пропускается, не перепроверяется.
2. **Проверка пробития:** Вызывается `check_signal_invalidated()` для проверки, действительно ли уровень пробит сейчас.
3. **Восстановление:** Если уровень больше не пробит → `demo_status = "WAITING_FOR_PRICE"`, сигнал становится активным снова.

**ВАЖНО:** Watcher **фильтрует по возрасту на уровне SQL-запроса**, чтобы не перепроверять недельные сигналы. Это предотвращает случайное восстановление и обработку очень старых сигналов, даже если цена вернулась к старому уровню.

---

### 1. Обработка WAITING_FOR_PRICE
**Условие:** `Signal.status == "ACTIVE"` AND `Signal.demo_status == "WAITING_FOR_PRICE"` AND `Signal.timestamp >= cutoff_time` (где `cutoff_time = now - MAX_SIGNAL_AGE_SECONDS`)

**Действие:** Watcher **продолжает проверять цену** и пытается поставить ордер, но **только для свежих сигналов** (не старше 30 минут).

**Проверки перед обработкой:**
1. **Возраст сигнала:** Если сигнал старше `MAX_SIGNAL_AGE_SECONDS` (30 минут) → `demo_status = "SIGNAL_TOO_OLD"`, сигнал становится неактивным.
2. **Пробитие уровня:** Если цена ушла против направления (LONG: цена < уровня, SHORT: цена > уровня) → `demo_status = "LEVEL_BROKEN"`, сигнал становится неактивным.
3. **Отклонение >2%:** Если цена ушла на >2% от уровня → `demo_status = "PRICE_DEVIATION_TOO_LARGE"`, сигнал становится неактивным.

**ВАЖНО:** Watcher **фильтрует по возрасту на уровне SQL-запроса** (`Signal.timestamp >= cutoff_time`), чтобы не обрабатывать недельные сигналы. Это предотвращает случайное открытие сделок по очень старым сигналам, даже если цена вернулась к старому уровню.

---

### 2. Проверка активных ордеров
**Условие:** `Signal.status == "ACTIVE"` AND `Signal.demo_order_id IS NOT NULL` AND `Signal.demo_status IN ("NEW", "OPEN", "PLACED", "SUBMITTING")`

**Действие:** Watcher проверяет отклонение цены от уровня.

**Если отклонение > 2%:** Ордер отменяется, `demo_status = "ORDER_CANCELLED_PRICE_MOVED"` → сигнал становится **неактивным**.

---

### 3. Закрытые сигналы без ордеров
**Условие:** `Signal.status != "ACTIVE"` AND `Signal.demo_order_id IS NULL` AND `Signal.demo_status IN (WAITING_FOR_PRICE, PRICE_DEVIATION_TOO_LARGE, SIGNAL_TOO_OLD, ...)`

**Действие:** Watcher помечает их `demo_status = "SIGNAL_CLOSED_NO_ORDER"` → сигнал становится **неактивным**.

---

### 4. Breakeven (установка SL в безубыток)
**Условие:** `Signal.status == "ACTIVE"` AND `Signal.demo_status == "OPEN_POSITION"` AND `Signal.demo_filled_at IS NOT NULL`

**Действие:** Watcher проверяет условия для установки SL в безубыток (40 минут + движение 0.4%). SL устанавливается через открытие новой сделки на минимальный объем (15-25 USDT) с правильными TP/SL в params.

**После установки:** `demo_status = "SL_TO_BREAKEVEN"` → сигнал **остается активным** (позиция открыта), но новые ордера не ставятся.

---

## Итоговая таблица: когда сигнал НЕ обрабатывается

| Условие | Проверяется в | Действие |
|---------|---------------|----------|
| `signal.status != "ACTIVE"` | `place_order_for_signal` | Полностью игнорируется |
| `demo_status` в финальных статусах | `place_order_for_signal` | Игнорируется (already_processed) |
| `signal_age > 30 минут` | `place_order_for_signal`, `watch_waiting_signals` | Помечается `SIGNAL_TOO_OLD` (фильтруется на уровне SQL-запроса) |
| `live_trading_enabled == False` | `place_order_for_signal` | Помечается `LIVE_DISABLED` |
| `bybit_api not configured` | `place_order_for_signal` | Помечается `NOT_CONFIGURED` |
| Есть открытая позиция или активный входной ордер по паре | `place_order_for_signal` | Помечается `POSITION_ALREADY_OPEN`, новые ордера не ставятся |
| `level_broken` (пробитие против направления) | `place_order_for_signal`, `watch_waiting_signals` | Помечается `LEVEL_BROKEN` |
| `price_deviation > 2%` (от уровня) | `place_order_for_signal`, `watch_waiting_signals` | Помечается `PRICE_DEVIATION_TOO_LARGE` |
| `price_deviation > 2%` (для открытых ордеров) | `watch_waiting_signals` | Ордер отменяется, статус `ORDER_CANCELLED_PRICE_MOVED` |
| `signal.status != "ACTIVE"` (для `WAITING_FOR_PRICE`) | `watch_waiting_signals` | Помечается `SIGNAL_CLOSED_NO_ORDER` |

---

## Важные нюансы

1. **WAITING_FOR_PRICE** обрабатывается **только если** `signal.status == "ACTIVE"`. Если сигнал закрылся, watcher переведет его в `SIGNAL_CLOSED_NO_ORDER`.

2. **OPEN_POSITION** - это **активный статус** (позиция открыта), но новые ордера не ставятся. Watcher только проверяет breakeven.

3. **PRICE_DEVIATION_TOO_LARGE** теперь **финальный статус** (удален из `RETRYABLE_STATUSES`) - если цена ушла на >2% от уровня, сигнал становится неактивным.

4. **LEVEL_BROKEN** - это **финальный статус** - если уровень пробит против нашего направления, сигнал становится неактивным.

5. **Возраст сигнала** проверяется **внутри** `place_order_for_signal`, но watcher **не фильтрует** по возрасту на уровне SQL-запроса для `WAITING_FOR_PRICE`.

6. **ORDER_CANCELLED_PRICE_MOVED** - это **финальный статус**, сигнал больше не обрабатывается, даже если цена вернется.

7. **Проверки пробития и отклонения** выполняются **перед** попыткой размещения ордера, чтобы не тратить время на обработку уже неактивных сигналов.

