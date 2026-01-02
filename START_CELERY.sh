#!/bin/bash
# Скрипт для запуска Celery Worker

# Переходим в директорию скрипта (корень проекта)
cd "$(dirname "$0")"
PROJECT_ROOT=$(pwd)
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

echo "🚀 Запуск Celery Worker..."
echo "   Директория: $PROJECT_ROOT"
echo ""

# Проверка RabbitMQ
echo "🔍 Проверка RabbitMQ..."
if docker exec ownedcore_rabbitmq rabbitmq-diagnostics ping &> /dev/null; then
    echo "✅ RabbitMQ работает"
else
    echo "❌ RabbitMQ не работает. Запустите: docker-compose -f docker/docker-compose.yml up -d"
    exit 1
fi

echo ""
echo "🚀 Запуск Celery Worker..."
echo "   Очереди: analysis, signals"
echo ""

# Контролируем количество процессов, чтобы не забивать PostgreSQL соединениями
CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-4}
echo "   Конкурентность: $CELERY_CONCURRENCY"
echo ""

# Используем python3 -m celery для надежности
python3 -m celery -A tasks.celery_app worker \
    --loglevel=info \
    --queues=analysis,signals \
    --concurrency="$CELERY_CONCURRENCY"

