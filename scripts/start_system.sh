#!/bin/bash
# Скрипт для запуска всей системы OwnedCore

echo "🚀 Запуск системы OwnedCore MVP"
echo "================================"
echo ""

# Проверка Docker сервисов
echo "🔍 Проверка Docker сервисов..."
if ! docker-compose -f docker/docker-compose.yml ps | grep -q "Up"; then
    echo "⚠️  Docker сервисы не запущены. Запускаю..."
    docker-compose -f docker/docker-compose.yml up -d
    echo "⏳ Ожидание запуска сервисов (10 секунд)..."
    sleep 10
fi

echo "✅ Docker сервисы работают"
echo ""

# Проверка подключения
echo "🔍 Проверка подключений..."
python3 -c "
from core.database import init_database
from core.cache import init_redis
db_ok = init_database()
redis_ok = init_redis()
if db_ok and redis_ok:
    print('✅ Подключения работают')
    exit(0)
else:
    print('❌ Ошибка подключений')
    exit(1)
" 2>&1

if [ $? -ne 0 ]; then
    echo "❌ Ошибка подключений. Проверьте настройки в .env файле"
    exit 1
fi

echo ""
echo "================================"
echo "📋 Инструкции по запуску:"
echo ""
echo "ТЕРМИНАЛ 1 - API Gateway:"
echo "  python3 -m services.api_gateway.main"
echo ""
echo "ТЕРМИНАЛ 2 - Celery Worker:"
echo "  celery -A tasks.celery_app worker --loglevel=info"
echo ""
echo "После запуска откройте:"
echo "  - http://localhost:8000/health"
echo "  - http://localhost:8000/dashboard"
echo "  - http://localhost:8000/api/pairs-status"
echo ""
echo "================================"

