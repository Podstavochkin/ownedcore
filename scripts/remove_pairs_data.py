#!/usr/bin/env python3
"""
Скрипт для удаления всех данных (уровни, сигналы) для указанных торговых пар
и очистки кэша
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database, create_tables
from core.models import TradingPair, Level, Signal
from core.cache import cache
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_pairs_data(symbols):
    """
    Удаляет все данные (уровни, сигналы) для указанных торговых пар и очищает кэш
    
    Args:
        symbols: список символов пар для удаления (например, ['MKR/USDT', 'FTM/USDT'])
    """
    # Инициализируем базу данных
    init_database()
    create_tables()
    
    # Импортируем SessionLocal после инициализации
    from core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        levels_deleted = 0
        signals_deleted = 0
        
        for symbol in symbols:
            pair = db.query(TradingPair).filter_by(symbol=symbol).first()
            if not pair:
                logger.warning(f"  ⚠ Пара {symbol} не найдена в базе данных")
                continue
            
            # Удаляем все уровни для этой пары
            levels = db.query(Level).filter_by(pair_id=pair.id).all()
            for level in levels:
                db.delete(level)
                levels_deleted += 1
            logger.info(f"  ✓ Удалено уровней для {symbol}: {len(levels)}")
            
            # Удаляем все сигналы для этой пары
            signals = db.query(Signal).filter_by(pair_id=pair.id).all()
            for signal in signals:
                db.delete(signal)
                signals_deleted += 1
            logger.info(f"  ✓ Удалено сигналов для {symbol}: {len(signals)}")
        
        if levels_deleted > 0 or signals_deleted > 0:
            db.commit()
            logger.info(f"\n✅ Удалено уровней: {levels_deleted}, сигналов: {signals_deleted}")
        else:
            logger.info("\nℹ️  Не было данных для удаления")
        
        # Очищаем кэш
        try:
            cache.delete("potential_signals:all")
            cache.delete("signals:all")
            logger.info("✅ Кэш очищен")
        except Exception as e:
            logger.warning(f"⚠️  Ошибка очистки кэша: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка удаления данных: {e}")
        db.rollback()
        return False
    finally:
        db.close()


if __name__ == '__main__':
    # Удаляем данные для пар, которые недоступны на Bybit
    pairs_to_remove = ['MKR/USDT', 'FTM/USDT']
    
    logger.info(f"Удаление данных для торговых пар: {', '.join(pairs_to_remove)}")
    logger.info("Эти пары недоступны на Bybit\n")
    
    success = remove_pairs_data(pairs_to_remove)
    
    if success:
        logger.info("\n✅ Готово! Все данные для этих пар удалены из базы данных и кэша.")
    else:
        logger.error("\n❌ Произошла ошибка при удалении данных.")
        sys.exit(1)

