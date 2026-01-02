#!/usr/bin/env python3
"""
Скрипт для очистки некорректных ценовых фигур (chart patterns) с датой 1970-01-01
или другими некорректными датами
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import database
from core.models import ChartPattern
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_invalid_patterns():
    """Удаляет паттерны с некорректными датами"""
    
    if not database.init_database() or database.SessionLocal is None:
        logger.error("❌ Не удалось инициализировать БД")
        return
    
    db = database.SessionLocal()
    
    try:
        # Минимальная валидная дата (2000-01-01)
        MIN_VALID_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)
        
        # Находим все паттерны с некорректными датами
        invalid_patterns = db.query(ChartPattern).filter(
            (ChartPattern.start_time < MIN_VALID_DATE) |
            (ChartPattern.end_time < MIN_VALID_DATE)
        ).all()
        
        if not invalid_patterns:
            logger.info("✅ Некорректных паттернов не найдено")
            return
        
        logger.info(f"🔍 Найдено {len(invalid_patterns)} паттернов с некорректными датами")
        
        # Показываем примеры
        for i, pattern in enumerate(invalid_patterns[:5]):
            logger.info(
                f"  {i+1}. ID {pattern.id}: {pattern.symbol} {pattern.timeframe} "
                f"{pattern.pattern_type} - {pattern.start_time} / {pattern.end_time}"
            )
        
        if len(invalid_patterns) > 5:
            logger.info(f"  ... и еще {len(invalid_patterns) - 5} паттернов")
        
        # Подтверждение
        print(f"\n⚠️  Будет удалено {len(invalid_patterns)} паттернов с некорректными датами")
        response = input("Продолжить? (yes/no): ")
        
        if response.lower() != 'yes':
            logger.info("❌ Отменено пользователем")
            return
        
        # Удаляем некорректные паттерны
        deleted_count = 0
        for pattern in invalid_patterns:
            try:
                db.delete(pattern)
                deleted_count += 1
            except Exception as e:
                logger.error(f"  ❌ Ошибка удаления паттерна ID {pattern.id}: {e}")
        
        db.commit()
        
        logger.info(f"✅ Удалено {deleted_count} некорректных паттернов")
        
        # Также деактивируем паттерны с датами в будущем (более чем на 1 день вперед)
        MAX_VALID_DATE = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59) + \
                        __import__('datetime').timedelta(days=1)
        
        future_patterns = db.query(ChartPattern).filter(
            (ChartPattern.start_time > MAX_VALID_DATE) |
            (ChartPattern.end_time > MAX_VALID_DATE)
        ).all()
        
        if future_patterns:
            logger.info(f"🔍 Найдено {len(future_patterns)} паттернов с датами в будущем")
            for pattern in future_patterns:
                pattern.is_active = False
            db.commit()
            logger.info(f"✅ Деактивировано {len(future_patterns)} паттернов с датами в будущем")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке паттернов: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()
        database.SessionLocal.remove()


if __name__ == '__main__':
    clean_invalid_patterns()

