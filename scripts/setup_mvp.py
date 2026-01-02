#!/usr/bin/env python3
"""
Скрипт для настройки MVP инфраструктуры
Проверяет все зависимости и создает необходимые структуры
"""

import os
import sys
import subprocess
from pathlib import Path

def check_command(command, name):
    """Проверяет наличие команды в системе"""
    try:
        result = subprocess.run(
            ['which', command],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"✅ {name} установлен: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {name} не найден")
            return False
    except:
        print(f"❌ Ошибка проверки {name}")
        return False


def check_python_package(package):
    """Проверяет наличие Python пакета"""
    try:
        __import__(package)
        print(f"✅ {package} установлен")
        return True
    except ImportError:
        print(f"❌ {package} не установлен")
        return False


def main():
    """Основная функция настройки"""
    print("=" * 60)
    print("🔧 Настройка MVP инфраструктуры OwnedCore")
    print("=" * 60)
    print()
    
    # Проверка системных зависимостей
    print("Проверка системных зависимостей:")
    print("-" * 60)
    postgres_ok = check_command('psql', 'PostgreSQL client')
    redis_ok = check_command('redis-cli', 'Redis client')
    docker_ok = check_command('docker', 'Docker')
    docker_compose_ok = check_command('docker-compose', 'Docker Compose')
    print()
    
    # Проверка Python пакетов
    print("Проверка Python пакетов:")
    print("-" * 60)
    sqlalchemy_ok = check_python_package('sqlalchemy')
    redis_py_ok = check_python_package('redis')
    celery_ok = check_python_package('celery')
    fastapi_ok = check_python_package('fastapi')
    alembic_ok = check_python_package('alembic')
    print()
    
    # Создание .env файла если его нет
    env_file = Path('.env')
    env_example = Path('.env.example')
    
    if not env_file.exists() and env_example.exists():
        print("Создание .env файла...")
        import shutil
        shutil.copy(env_example, env_file)
        print("✅ .env файл создан из .env.example")
        print("⚠️  Не забудьте изменить SECRET_KEY в .env файле!")
        print()
    
    # Итоговый отчет
    print("=" * 60)
    print("📊 Итоговый отчет:")
    print("=" * 60)
    
    all_ok = all([
        sqlalchemy_ok, redis_py_ok, celery_ok, fastapi_ok, alembic_ok
    ])
    
    if all_ok:
        print("✅ Все Python пакеты установлены")
    else:
        print("❌ Некоторые пакеты отсутствуют")
        print("   Установите их: pip install -r config/requirements.txt")
        print()
    
    if docker_ok and docker_compose_ok:
        print("✅ Docker готов к использованию")
        print("   Запустите: docker-compose -f docker/docker-compose.yml up -d")
    else:
        print("⚠️  Docker не установлен (опционально для локальной разработки)")
        print()
    
    print()
    print("Следующие шаги:")
    print("1. Установите зависимости: pip install -r config/requirements.txt")
    print("2. Запустите Docker сервисы: docker-compose -f docker/docker-compose.yml up -d")
    print("3. Создайте миграции: alembic revision --autogenerate -m 'Initial migration'")
    print("4. Примените миграции: alembic upgrade head")
    print("5. Мигрируйте данные: python scripts/migrate_json_to_db.py")
    print("6. Запустите API: python -m services.api_gateway.main")
    print("7. Запустите Celery worker: celery -A tasks.celery_app worker --loglevel=info")


if __name__ == '__main__':
    main()

