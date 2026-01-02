#!/usr/bin/env python3
"""
Скрипт для очистки кэша потенциальных сигналов и проверки данных
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.cache import cache
from core.database import init_database
from core.models import Level
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_cache():
    """Очищает кэш потенциальных сигналов"""
    try:
        # Очищаем кэш потенциальных сигналов
        deleted = cache.delete('potential_signals:all')
        logger.info(f"Кэш 'potential_signals:all' {'удален' if deleted else 'не найден или не удален'}")
        
        # Очищаем все ключи с паттерном potential_signals:*
        cleared = cache.clear_pattern('potential_signals:*')
        logger.info(f"Очищено {cleared} ключей с паттерном 'potential_signals:*'")
        
        # Также очищаем кэш уровней и сигналов
        cache.delete('levels:all')
        cache.delete('signals:all')
        logger.info("Кэш 'levels:all' и 'signals:all' очищен")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка очистки кэша: {e}")
        return False

def check_levels_data():
    """Проверяет данные уровней в БД"""
    try:
        if not init_database():
            logger.error("Не удалось инициализировать базу данных")
            return False
        
        from core.database import SessionLocal
        session = SessionLocal()
        try:
            levels = session.query(Level).filter(Level.is_active == True).limit(10).all()
            logger.info(f"Проверяем {len(levels)} активных уровней...")
            
            for level in levels:
                meta = level.meta_data or {}
                metadata = meta.get('metadata', {}) or {}
                elder_screens = metadata.get('elder_screens')
                
                if elder_screens:
                    screen_2 = elder_screens.get('screen_2', {})
                    passed = screen_2.get('passed', False)
                    blocked_reason = screen_2.get('blocked_reason')
                    
                    logger.info(f"Уровень {level.pair.symbol if level.pair else 'N/A'} @ {level.price}:")
                    logger.info(f"  Screen 2 passed: {passed}")
                    logger.info(f"  Blocked reason: {blocked_reason if blocked_reason else 'ОТСУТСТВУЕТ'}")
                    
                    if not passed and not blocked_reason:
                        logger.warning(f"  ⚠️ ПРОБЛЕМА: Экран 2 не пройден, но blocked_reason отсутствует!")
                        # Пытаемся сформировать из checks
                        checks = screen_2.get('checks', {})
                        logger.info(f"  Checks доступны: {list(checks.keys())}")
                else:
                    logger.info(f"Уровень {level.pair.symbol if level.pair else 'N/A'} @ {level.price}: Elder's Screens отсутствуют")
            
            return True
        finally:
            session.close()
    except Exception as e:
        logger.error(f"Ошибка проверки данных: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    logger.info("🧹 Очистка кэша потенциальных сигналов...")
    clear_cache()
    
    logger.info("\n🔍 Проверка данных уровней в БД...")
    check_levels_data()
    
    logger.info("\n✅ Готово! Теперь обновите страницу /potential-signals")

