#!/bin/bash
# Скрипт для установки PostgreSQL и Redis локально (без Docker)

echo "🔧 Установка локальных сервисов для OwnedCore MVP"
echo "================================================"
echo ""

# Проверка Homebrew
if ! command -v brew &> /dev/null; then
    echo "❌ Homebrew не установлен"
    echo "Установите Homebrew: https://brew.sh"
    exit 1
fi

echo "✅ Homebrew найден"
echo ""

# Установка PostgreSQL
if ! command -v psql &> /dev/null; then
    echo "📦 Установка PostgreSQL..."
    brew install postgresql@15
    brew services start postgresql@15
    echo "✅ PostgreSQL установлен и запущен"
else
    echo "✅ PostgreSQL уже установлен"
fi

# Установка Redis
if ! command -v redis-cli &> /dev/null; then
    echo "📦 Установка Redis..."
    brew install redis
    brew services start redis
    echo "✅ Redis установлен и запущен"
else
    echo "✅ Redis уже установлен"
fi

# Установка RabbitMQ (для Celery)
if ! command -v rabbitmq-server &> /dev/null; then
    echo "📦 Установка RabbitMQ..."
    brew install rabbitmq
    brew services start rabbitmq
    echo "✅ RabbitMQ установлен и запущен"
else
    echo "✅ RabbitMQ уже установлен"
fi

echo ""
echo "================================================"
echo "✅ Все сервисы установлены!"
echo ""
echo "Следующие шаги:"
echo "1. Создайте базу данных: createdb ownedcore"
echo "2. Создайте .env файл: cp .env.example .env"
echo "3. Запустите миграции: alembic upgrade head"
echo ""

