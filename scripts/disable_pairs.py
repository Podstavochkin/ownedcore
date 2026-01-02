#!/usr/bin/env python3
"""
Скрипт для отключения торговых пар в базе данных
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database, create_tables
from core.models import TradingPair
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def disable_pairs(symbols):
    """
    Отключает указанные торговые пары в базе данных
    
    Args:
        symbols: список символов пар для отключения (например, ['MKR/USDT', 'FTM/USDT'])
    """
    # Инициализируем базу данных
    init_database()
    create_tables()
    
    # Импортируем SessionLocal после инициализации
    from core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        disabled_count = 0
        not_found = []
        
        for symbol in symbols:
            pair = db.query(TradingPair).filter_by(symbol=symbol).first()
            if pair:
                if pair.enabled:
                    pair.enabled = False
                    disabled_count += 1
                    logger.info(f"  ✓ Отключена пара: {symbol}")
                else:
                    logger.info(f"  ⚠ Пара {symbol} уже отключена")
            else:
                not_found.append(symbol)
                logger.warning(f"  ✗ Пара {symbol} не найдена в базе данных")
        
        if disabled_count > 0:
            db.commit()
            logger.info(f"\n✅ Отключено пар: {disabled_count}")
        else:
            logger.info("\nℹ️  Не было пар для отключения")
        
        if not_found:
            logger.warning(f"\n⚠️  Не найдено пар в базе: {', '.join(not_found)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка отключения пар: {e}")
        db.rollback()
        return False
    finally:
        db.close()


if __name__ == '__main__':
    # Отключаем пары, которые недоступны на Bybit
    pairs_to_disable = ['MKR/USDT', 'FTM/USDT']
    
    logger.info(f"Отключение торговых пар: {', '.join(pairs_to_disable)}")
    logger.info("Эти пары недоступны на Bybit и будут исключены из анализа\n")
    
    success = disable_pairs(pairs_to_disable)
    
    if success:
        logger.info("\n✅ Готово! Пары отключены в базе данных.")
        logger.info("Они больше не будут анализироваться системой.")
    else:
        logger.error("\n❌ Произошла ошибка при отключении пар.")
        sys.exit(1)

