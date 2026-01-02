#!/usr/bin/env python3
"""
Скрипт для очистки старых лог-файлов
Можно запускать вручную или через Celery задачу
"""
import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from core.logging_config import cleanup_old_logs


def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Очистка старых лог-файлов')
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Количество дней для хранения логов (по умолчанию: 7)'
    )
    parser.add_argument(
        '--logs-dir',
        type=str,
        default='logs',
        help='Директория с логами (по умолчанию: logs)'
    )
    
    args = parser.parse_args()
    
    print(f"🧹 Очистка лог-файлов старше {args.days} дней из {args.logs_dir}...")
    cleanup_old_logs(logs_dir=args.logs_dir, days_to_keep=args.days)
    print("✅ Очистка завершена")


if __name__ == '__main__':
    main()

