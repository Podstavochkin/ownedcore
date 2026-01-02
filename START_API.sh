#!/bin/bash
# Скрипт для запуска API Gateway

# Получаем корневую директорию проекта
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
PROJECT_ROOT=$(pwd)

echo "🚀 Запуск API Gateway..."
echo "📁 Рабочая директория: $PROJECT_ROOT"
echo ""

# Проверка подключений
cd "$PROJECT_ROOT"
python3 << 'PYEOF'
import sys
import os

# Добавляем текущую директорию в путь
current_dir = os.getcwd()
sys.path.insert(0, current_dir)

try:
    from core.database import init_database
    from core.cache import init_redis
    
    print("🔍 Проверка подключений...")
    db_ok = init_database()
    redis_ok = init_redis()
    
    if db_ok and redis_ok:
        print("✅ Все подключения работают")
        print("")
        print("🚀 Запуск API Gateway на http://localhost:8000")
        print("")
    else:
        print("❌ Ошибка подключений")
        print(f"   БД: {db_ok}")
        print(f"   Redis: {redis_ok}")
        sys.exit(1)
except Exception as e:
    print(f"❌ Ошибка импорта: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYEOF

if [ $? -ne 0 ]; then
    echo "❌ Ошибка при проверке подключений"
    exit 1
fi

# Запуск API Gateway
echo "🚀 Запуск сервера..."
cd "$PROJECT_ROOT"
python3 -m services.api_gateway.main
