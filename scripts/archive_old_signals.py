#!/usr/bin/env python3
"""
Скрипт для архивации всех старых сигналов перед началом новой статистики с live-торговлей.
Архивирует все сигналы, которые были созданы до текущего момента.
"""

import sys
import os
from datetime import datetime, timezone

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.database as database
from core.models import Signal
from core.cache import cache
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def archive_old_signals():
    """Архивирует все существующие сигналы"""
    if not database.init_database():
        logger.error("❌ Не удалось инициализировать базу данных")
        return False
    
    assert database.SessionLocal is not None, "SessionLocal не инициализирован"
    session = database.SessionLocal()
    try:
        # Получаем все неархивированные сигналы
        signals = session.query(Signal).filter(Signal.archived == False).all()
        
        if not signals:
            logger.info("✅ Нет сигналов для архивации")
            return True
        
        logger.info(f"📦 Найдено {len(signals)} сигналов для архивации")
        
        # Архивируем все сигналы
        now = datetime.now(timezone.utc)
        archived_count = 0
        
        for signal in signals:
            signal.archived = True
            signal.archived_at = now
            archived_count += 1
        
        session.commit()
        
        logger.info(f"✅ Успешно заархивировано {archived_count} сигналов")
        
        # Очищаем кэш сигналов
        cache.delete('signals:all')
        logger.info("🗑️  Кэш сигналов очищен")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при архивации сигналов: {e}")
        session.rollback()
        return False
    finally:
        session.close()


if __name__ == "__main__":
    print("=" * 60)
    print("📦 Архивация старых сигналов")
    print("=" * 60)
    print()
    print("Этот скрипт архивирует ВСЕ существующие сигналы.")
    print("После архивации они не будут отображаться в основной статистике.")
    print()
    
    response = input("Продолжить архивацию? (yes/no): ").strip().lower()
    
    if response not in ('yes', 'y', 'да', 'д'):
        print("❌ Архивация отменена")
        sys.exit(0)
    
    print()
    success = archive_old_signals()
    
    if success:
        print()
        print("=" * 60)
        print("✅ Архивация завершена успешно!")
        print("=" * 60)
        print()
        print("Теперь все новые сигналы будут формироваться с live-торговлей.")
        print("Старые сигналы останутся в базе данных, но не будут учитываться")
        print("в статистике и не будут отображаться в списке.")
        sys.exit(0)
    else:
        print()
        print("=" * 60)
        print("❌ Ошибка при архивации")
        print("=" * 60)
        sys.exit(1)

