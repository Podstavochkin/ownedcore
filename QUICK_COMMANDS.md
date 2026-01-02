# ⚡ Быстрые команды для управления проектом

## 🛑 Остановка проекта

```bash
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
./STOP_ALL.sh
```

Или вручную:
```bash
# Остановить API Gateway и Celery
pkill -f "services/api_gateway/main.py"
pkill -f "celery.*worker"

# Остановить Docker контейнеры
cd docker
docker-compose down
```

---

## 🚀 Запуск проекта

```bash
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
./START_ALL.sh
```

Или вручную:

### 1. Запустить Docker контейнеры
```bash
cd docker
docker-compose up -d
```

### 2. Запустить API Gateway (в отдельном терминале)
```bash
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
./START_API.sh
```

### 3. Запустить Celery Worker (в отдельном терминале)
```bash
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
./START_CELERY.sh
```

---

## 📋 Проверка статуса

```bash
# Проверить API Gateway
curl http://localhost:8000/health

# Проверить Docker контейнеры
cd docker
docker-compose ps

# Проверить активные задачи Celery
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
celery -A tasks.celery_app inspect active

# Проверить список задач через API
curl http://localhost:8000/api/tasks | python3 -m json.tool
```

---

## 🔗 Полезные ссылки

- **Dashboard**: http://localhost:8000/dashboard
- **Signals**: http://localhost:8000/signals
- **API Docs**: http://localhost:8000/docs
- **API Health**: http://localhost:8000/health
- **RabbitMQ UI**: http://localhost:15672 (guest/guest)

---

## 📝 Логи

```bash
# Логи API Gateway
tail -f /tmp/api_gateway.log

# Логи Celery Worker
tail -f /tmp/celery_worker.log

# Логи Docker контейнеров
cd docker
docker-compose logs -f
```

---

## 🔄 Перезагрузка исторических данных OHLCV

Для исправления неправильных исторических данных на графиках:

```bash
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
./RELOAD_HISTORICAL_OHLCV.sh
```

### Примеры использования:

```bash
# Перезагрузить последние 3 дня для конкретной пары и таймфрейма
./RELOAD_HISTORICAL_OHLCV.sh -p BTC/USDT -t 1h -d 3

# Перезагрузить последний день для всех пар (быстро)
./RELOAD_HISTORICAL_OHLCV.sh -d 1

# Перезагрузить с принудительным обновлением закрытых свечей (для исправления ошибок)
./RELOAD_HISTORICAL_OHLCV.sh -p AAVE/USDT -t 1h -d 3 -f

# Показать справку
./RELOAD_HISTORICAL_OHLCV.sh -h
```

**Параметры:**
- `-p, --pair` - Торговая пара (например, BTC/USDT)
- `-t, --timeframe` - Таймфрейм (1m, 5m, 15m, 1h, 4h)
- `-d, --days` - Количество дней истории (по умолчанию: 3)
- `-f, --force` - Обновить даже закрытые свечи (для исправления ошибок)

---

## 🛡️ Watchdog Celery worker

Чтобы автоматически перезапускать worker при ошибках `watch_waiting_signals`, можно периодически запускать сторожевой скрипт:

```bash
cd "/Users/andrejpodstavockin/Documents/OwnedCore- CryptoProject v0.1 04"
python3 scripts/celery_watchdog.py
```

Пример cron-задания (каждые 5 минут):

```
*/5 * * * * /usr/bin/python3 /Users/andrejpodstavockin/Documents/OwnedCore-\ CryptoProject\ v0.1\ 04/scripts/celery_watchdog.py >> /tmp/celery_watchdog.log 2>&1
```

Скрипт проверяет, что Celery worker запущен и в `/tmp/celery_worker.log` нет свежих ошибок, и при необходимости перезапускает его автоматически.

---

**Приятного отдыха! 🏖️**

