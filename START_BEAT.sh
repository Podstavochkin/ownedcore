#!/bin/bash
# Скрипт для запуска Celery Beat (периодические задачи)

# Переходим в директорию скрипта (корень проекта)
cd "$(dirname "$0")"
PROJECT_ROOT=$(pwd)
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

echo "⏰ Запуск Celery Beat (периодические задачи)..."
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
echo "⏰ Запуск Celery Beat..."
echo "   Периодические задачи:"
echo "   - Анализ всех пар: каждые 5 минут"
echo "   - Обновление P&L: каждую минуту"
echo "   - Проверка касаний уровней: каждую минуту (скальпинг)"
echo ""

# Используем python3 -m celery для надежности
python3 -m celery -A tasks.celery_app beat --loglevel=info

