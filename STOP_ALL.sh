#!/bin/bash
# Скрипт для остановки всех компонентов проекта

cd "$(dirname "$0")"
PROJECT_ROOT=$(pwd)

echo "🛑 Остановка проекта OwnedCore..."
echo ""

# Остановка API Gateway
echo "📡 Остановка API Gateway..."
pkill -f "services/api_gateway/main.py" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✅ API Gateway остановлен"
else
    echo "   ℹ️  API Gateway не был запущен"
fi

# Остановка Celery Worker
echo "⚙️  Остановка Celery Worker..."
pkill -f "celery.*worker" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✅ Celery Worker остановлен"
else
    echo "   ℹ️  Celery Worker не был запущен"
fi

# Остановка Celery Beat
echo "⏰ Остановка Celery Beat..."
pkill -f "celery.*beat" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✅ Celery Beat остановлен"
else
    echo "   ℹ️  Celery Beat не был запущен"
fi

# Остановка Celery Watchdog
echo "🛡️  Остановка Celery Watchdog..."
if [ -f /tmp/celery_watchdog.pid ]; then
    WATCHDOG_PID=$(cat /tmp/celery_watchdog.pid)
    if ps -p $WATCHDOG_PID > /dev/null 2>&1; then
        kill $WATCHDOG_PID 2>/dev/null
        sleep 1
    fi
    rm -f /tmp/celery_watchdog.pid
fi
pkill -f "scripts/celery_watchdog.py" 2>/dev/null
echo "   ✅ Watchdog остановлен"

# Остановка Docker контейнеров
echo "🐳 Остановка Docker контейнеров..."
cd "$PROJECT_ROOT/docker"
docker-compose down 2>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✅ Docker контейнеры остановлены"
else
    echo "   ⚠️  Ошибка при остановке Docker контейнеров"
fi

echo ""
echo "✅ Все компоненты остановлены!"
echo ""
echo "💡 Для запуска проекта используйте: ./START_ALL.sh"

