#!/usr/bin/env python3
"""
Скрипт для инициализации данных в базе:
1. Добавляет торговые пары из TRADING_PAIRS
2. Запускает первый анализ
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database, SessionLocal, create_tables
from core.models import TradingPair
from core.analysis_engine import TRADING_PAIRS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_trading_pairs():
    """Инициализирует торговые пары в базе"""
    logger.info("Инициализация базы данных...")
    
    if not init_database():
        logger.error("Не удалось инициализировать БД")
        return False
    
    # Создаем таблицы если их нет
    create_tables()
    
    # Импортируем SessionLocal после инициализации
    from core.database import SessionLocal
    
    # Получаем сессию
    db = SessionLocal()
    
    try:
        logger.info(f"Добавление {len(TRADING_PAIRS)} торговых пар...")
        
        added_count = 0
        existing_count = 0
        
        for symbol in TRADING_PAIRS:
            # Проверяем существование
            existing = db.query(TradingPair).filter_by(symbol=symbol).first()
            if existing:
                existing_count += 1
                continue
            
            # Создаем новую пару
            pair = TradingPair(
                symbol=symbol,
                exchange='binance',
                enabled=True
            )
            db.add(pair)
            added_count += 1
            logger.info(f"  ✓ Добавлена пара: {symbol}")
        
        db.commit()
        
        total = db.query(TradingPair).count()
        
        logger.info("")
        logger.info(f"✅ Инициализация завершена:")
        logger.info(f"   Добавлено новых пар: {added_count}")
        logger.info(f"   Уже существовало: {existing_count}")
        logger.info(f"   Всего пар в базе: {total}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка инициализации: {e}")
        db.rollback()
        return False
    finally:
        db.close()


if __name__ == '__main__':
    success = init_trading_pairs()
    if success:
        print("\n🎯 Следующий шаг: запустите анализ через API:")
        print("   curl -X POST http://localhost:8000/api/force-analysis")
        sys.exit(0)
    else:
        print("\n❌ Ошибка инициализации данных")
        sys.exit(1)

