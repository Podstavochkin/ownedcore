#!/bin/bash
# Скрипт для запуска всех компонентов проекта

cd "$(dirname "$0")"
PROJECT_ROOT=$(pwd)

echo "🧾 Загрузка переменных окружения..."
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$PROJECT_ROOT/.env"
    set +a
    echo "   ✅ .env загружен"
else
    echo "   ⚠️  Файл .env не найден, будут использованы только системные переменные"
fi

echo "🚀 Запуск проекта OwnedCore..."
echo ""

# Запуск Docker контейнеров
echo "🐳 Запуск Docker контейнеров (PostgreSQL, Redis, RabbitMQ)..."
cd "$PROJECT_ROOT/docker"
docker-compose up -d 2>&1 | grep -E "(Creating|Starting|started|error)" || true

# Ждем, пока сервисы запустятся
echo "   ⏳ Ожидание запуска сервисов (5 секунд)..."
sleep 5

# Проверка статуса контейнеров
echo ""
echo "📊 Статус Docker контейнеров:"
docker-compose ps 2>/dev/null | grep -E "(CONTAINER|ownedcore)" || echo "   ⚠️  Не удалось проверить статус"

# Запуск API Gateway
echo ""
echo "📡 Запуск API Gateway..."

# Проверяем, не запущен ли уже API Gateway
if lsof -ti:8000 > /dev/null 2>&1; then
    OLD_PID=$(lsof -ti:8000 | head -1)
    echo "   ⚠️  Порт 8000 уже занят (PID: $OLD_PID). Останавливаем старый процесс..."
    kill $OLD_PID > /dev/null 2>&1
    sleep 1
fi

cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
nohup python3 services/api_gateway/main.py > /tmp/api_gateway.log 2>&1 &
API_PID=$!
sleep 3

if ps -p $API_PID > /dev/null 2>&1; then
    echo "   ✅ API Gateway запущен (PID: $API_PID)"
    echo "   📝 Логи: tail -f /tmp/api_gateway.log"
else
    echo "   ❌ Ошибка запуска API Gateway"
    echo "   📝 Проверьте логи: tail -20 /tmp/api_gateway.log"
    echo "   💡 Возможные причины:"
    echo "      - Ошибка в коде (проверьте логи выше)"
    echo "      - Порт 8000 занят другим процессом"
    echo "      - Проблемы с подключением к БД или Redis"
fi

# Запуск Celery Worker
echo ""
echo "⚙️  Запуск Celery Worker..."
cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Проверка RabbitMQ перед запуском
if ! docker exec ownedcore_rabbitmq rabbitmq-diagnostics ping &> /dev/null; then
    echo "   ⚠️  RabbitMQ не работает. Запустите: docker-compose -f docker/docker-compose.yml up -d"
else
    CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-4}
    echo "   ➤ Конкурентность Celery worker: $CELERY_CONCURRENCY"
    nohup python3 -m celery -A tasks.celery_app worker --loglevel=info --queues=analysis,signals --concurrency="$CELERY_CONCURRENCY" > /tmp/celery_worker.log 2>&1 &
    CELERY_WORKER_PID=$!
    sleep 3
    
    if ps -p $CELERY_WORKER_PID > /dev/null 2>&1 || pgrep -f "celery.*worker" > /dev/null; then
        echo "   ✅ Celery Worker запущен"
        echo "   📝 Логи: tail -f /tmp/celery_worker.log"
    else
        echo "   ⚠️  Проверьте логи Celery Worker: tail -f /tmp/celery_worker.log"
    fi
fi

# Запуск watchdog для Celery
echo ""
echo "🛡️  Запуск Celery Watchdog..."
CELERY_WATCHDOG_CMD="python3 scripts/celery_watchdog.py --loop"
nohup bash -c "$CELERY_WATCHDOG_CMD" > /tmp/celery_watchdog.log 2>&1 &
CELERY_WATCHDOG_PID=$!
echo $CELERY_WATCHDOG_PID > /tmp/celery_watchdog.pid
echo "   ✅ Watchdog запущен (PID: $CELERY_WATCHDOG_PID)"
echo "   📝 Логи: tail -f /tmp/celery_watchdog.log"

# Запуск Celery Beat (периодические задачи)
echo ""
echo "⏰ Запуск Celery Beat (периодические задачи)..."
cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Проверка RabbitMQ перед запуском
if ! docker exec ownedcore_rabbitmq rabbitmq-diagnostics ping &> /dev/null; then
    echo "   ⚠️  RabbitMQ не работает. Запустите: docker-compose -f docker/docker-compose.yml up -d"
else
    nohup python3 -m celery -A tasks.celery_app beat --loglevel=info > /tmp/celery_beat.log 2>&1 &
    CELERY_BEAT_PID=$!
    sleep 2
    
    if ps -p $CELERY_BEAT_PID > /dev/null 2>&1 || pgrep -f "celery.*beat" > /dev/null; then
        echo "   ✅ Celery Beat запущен"
        echo "   📝 Логи: tail -f /tmp/celery_beat.log"
        echo "   📋 Периодические задачи:"
        echo "      - Анализ всех пар: каждые 5 минут"
        echo "      - Обновление P&L: каждую минуту"
        echo "      - Проверка касаний уровней: каждую минуту (скальпинг)"
    else
        echo "   ⚠️  Проверьте логи Celery Beat: tail -f /tmp/celery_beat.log"
    fi
fi

# Финальная проверка
echo ""
echo "🔍 Проверка доступности сервисов..."

# Проверка API Gateway (с таймаутом 2 секунды)
if curl -s --max-time 2 --connect-timeout 2 http://localhost:8000/health > /dev/null 2>&1; then
    echo "   ✅ API Gateway: http://localhost:8000"
else
    echo "   ⚠️  API Gateway: не отвечает (возможно еще запускается)"
fi

# Проверка RabbitMQ (с таймаутом через фоновый процесс)
(docker exec ownedcore_rabbitmq rabbitmq-diagnostics ping > /dev/null 2>&1) & RABBITMQ_PID=$!
sleep 2
if kill -0 $RABBITMQ_PID > /dev/null 2>&1; then
    kill $RABBITMQ_PID > /dev/null 2>&1
    wait $RABBITMQ_PID > /dev/null 2>&1
    echo "   ⚠️  RabbitMQ: не отвечает"
else
    wait $RABBITMQ_PID > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "   ✅ RabbitMQ: http://localhost:15672"
    else
        echo "   ⚠️  RabbitMQ: не отвечает"
    fi
fi

# Проверка PostgreSQL (с таймаутом через фоновый процесс)
(docker exec ownedcore_postgres pg_isready -U postgres > /dev/null 2>&1) & POSTGRES_PID=$!
sleep 2
if kill -0 $POSTGRES_PID > /dev/null 2>&1; then
    kill $POSTGRES_PID > /dev/null 2>&1
    wait $POSTGRES_PID > /dev/null 2>&1
    echo "   ⚠️  PostgreSQL: не отвечает"
else
    wait $POSTGRES_PID > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "   ✅ PostgreSQL: localhost:5432"
    else
        echo "   ⚠️  PostgreSQL: не отвечает"
    fi
fi

# Проверка Redis (с таймаутом через фоновый процесс)
(docker exec ownedcore_redis redis-cli ping > /dev/null 2>&1) & REDIS_PID=$!
sleep 2
if kill -0 $REDIS_PID > /dev/null 2>&1; then
    kill $REDIS_PID > /dev/null 2>&1
    wait $REDIS_PID > /dev/null 2>&1
    echo "   ⚠️  Redis: не отвечает"
else
    wait $REDIS_PID > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "   ✅ Redis: localhost:6379"
    else
        echo "   ⚠️  Redis: не отвечает"
    fi
fi

echo ""
echo "✅ Проект запущен!"
echo ""
echo "📋 Полезные ссылки:"
echo "   - Dashboard: http://localhost:8000/dashboard"
echo "   - Signals: http://localhost:8000/signals"
echo "   - API Docs: http://localhost:8000/docs"
echo "   - API Health: http://localhost:8000/health"
echo "   - RabbitMQ UI: http://localhost:15672 (guest/guest)"
echo ""
echo "💡 Для остановки используйте: ./STOP_ALL.sh"

