#!/usr/bin/env python3
"""
Скрипт для полной перестройки ценовых фигур:
1. Удаляет все существующие паттерны из БД
2. Запускает детекцию паттернов для всех пар и таймфреймов
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import database
from core.models import ChartPattern
from tasks.chart_patterns_tasks import detect_chart_patterns_for_pair
from core.analysis_engine import TRADING_PAIRS
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def rebuild_all_patterns(auto_confirm: bool = False):
    """Удаляет все паттерны и запускает детекцию заново"""
    
    if not database.init_database() or database.SessionLocal is None:
        logger.error("❌ Не удалось инициализировать БД")
        return
    
    db = database.SessionLocal()
    
    try:
        # Подсчитываем количество паттернов
        total_patterns = db.query(ChartPattern).count()
        
        if total_patterns == 0:
            logger.info("✅ В БД нет паттернов для удаления")
        else:
            logger.info(f"🔍 Найдено {total_patterns} паттернов в БД")
            
            # Подтверждение (только если не auto_confirm)
            if not auto_confirm:
                print(f"\n⚠️  Будет удалено {total_patterns} паттернов из БД")
                try:
                    response = input("Продолжить? (yes/no): ")
                    if response.lower() != 'yes':
                        logger.info("❌ Отменено пользователем")
                        return
                except (EOFError, KeyboardInterrupt):
                    logger.info("❌ Отменено (нет интерактивного ввода)")
                    return
            
            # Удаляем все паттерны
            deleted_count = db.query(ChartPattern).delete(synchronize_session=False)
            db.commit()
            
            logger.info(f"✅ Удалено {deleted_count} паттернов из БД")
        
        # Запускаем детекцию паттернов для всех пар и таймфреймов
        logger.info("\n🔄 Запуск детекции паттернов для всех пар и таймфреймов...")
        logger.info(f"   Пар для обработки: {len(TRADING_PAIRS)}")
        logger.info("   Таймфреймы: 15m, 1h, 4h")
        
        timeframes = ['15m', '1h', '4h']
        lookback_candles = {
            '15m': 200,
            '1h': 200,
            '4h': 200
        }
        
        total_patterns = 0
        total_pairs = 0
        errors = []
        
        for pair in TRADING_PAIRS:
            for timeframe in timeframes:
                try:
                    logger.info(f"  🔍 Обработка {pair} {timeframe}...")
                    result = detect_chart_patterns_for_pair(
                        pair,
                        timeframe,
                        lookback_candles.get(timeframe, 200)
                    )
                    
                    if result.get('success'):
                        patterns_count = result.get('patterns_found', 0)
                        total_patterns += patterns_count
                        total_pairs += 1
                        if patterns_count > 0:
                            logger.info(f"    ✅ Найдено {patterns_count} паттернов")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        errors.append(f"{pair} {timeframe}: {error_msg}")
                        logger.warning(f"    ⚠️ Ошибка: {error_msg}")
                    
                except Exception as e:
                    error_msg = f"{pair} {timeframe}: {e}"
                    errors.append(error_msg)
                    logger.error(f"    ❌ {error_msg}", exc_info=True)
                    continue
        
        logger.info(f"\n✅ Детекция завершена!")
        logger.info(f"   Обработано пар: {total_pairs}")
        logger.info(f"   Найдено паттернов: {total_patterns}")
        if errors:
            logger.warning(f"   Ошибок: {len(errors)}")
            for error in errors[:5]:
                logger.warning(f"     - {error}")
            if len(errors) > 5:
                logger.warning(f"     ... и еще {len(errors) - 5} ошибок")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при перестройке паттернов: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()
        database.SessionLocal.remove()


if __name__ == '__main__':
    import sys
    # Если передан аргумент --yes, пропускаем подтверждение
    auto_confirm = '--yes' in sys.argv or '-y' in sys.argv
    rebuild_all_patterns(auto_confirm=auto_confirm)

