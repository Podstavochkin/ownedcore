#!/bin/bash
# Скрипт для проверки доступности сервисов

echo "🔍 Проверка доступности сервисов"
echo "================================="
echo ""

# Проверка PostgreSQL
echo -n "PostgreSQL: "
if command -v psql &> /dev/null; then
    if psql -U postgres -h localhost -d postgres -c "SELECT 1;" &> /dev/null; then
        echo "✅ Доступен"
    else
        echo "❌ Недоступен (проверьте что сервис запущен)"
    fi
else
    echo "❌ Не установлен"
fi

# Проверка Redis
echo -n "Redis: "
if command -v redis-cli &> /dev/null; then
    if redis-cli ping &> /dev/null; then
        echo "✅ Доступен"
    else
        echo "❌ Недоступен (проверьте что сервис запущен)"
    fi
else
    echo "❌ Не установлен"
fi

# Проверка RabbitMQ
echo -n "RabbitMQ: "
if command -v rabbitmq-diagnostics &> /dev/null; then
    if rabbitmq-diagnostics ping &> /dev/null; then
        echo "✅ Доступен"
    else
        echo "❌ Недоступен (проверьте что сервис запущен)"
    fi
else
    echo "⚠️  Не установлен (опционально для начала)"
fi

# Проверка Docker
echo -n "Docker: "
if command -v docker &> /dev/null; then
    if docker ps &> /dev/null; then
        echo "✅ Доступен"
        echo ""
        echo "Запущенные контейнеры:"
        docker ps --format "table {{.Names}}\t{{.Status}}"
    else
        echo "⚠️  Установлен, но не запущен"
    fi
else
    echo "❌ Не установлен"
fi

echo ""

