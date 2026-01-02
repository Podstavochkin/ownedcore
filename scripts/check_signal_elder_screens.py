#!/usr/bin/env python3
"""
Скрипт для проверки Elder's Screens для конкретного сигнала
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database, create_tables
from core.models import Signal, TradingPair
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_signal_elder_screens(pair_symbol: str, timestamp_str: str = None):
    """
    Проверяет Elder's Screens для сигнала
    
    Args:
        pair_symbol: символ пары (например, 'XLM/USDT')
        timestamp_str: строка с timestamp сигнала (опционально)
    """
    # Инициализируем базу данных
    init_database()
    create_tables()
    
    from core.database import SessionLocal
    
    db = SessionLocal()
    
    try:
        # Находим пару
        pair = db.query(TradingPair).filter_by(symbol=pair_symbol).first()
        if not pair:
            logger.error(f"Пара {pair_symbol} не найдена в базе данных")
            return
        
        # Находим сигналы для этой пары
        query = db.query(Signal).filter_by(pair_id=pair.id)
        
        if timestamp_str:
            # Парсим timestamp
            try:
                signal_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                # Ищем сигналы в пределах 1 минуты от указанного времени
                from datetime import timedelta
                time_start = signal_time - timedelta(minutes=1)
                time_end = signal_time + timedelta(minutes=1)
                query = query.filter(Signal.timestamp >= time_start, Signal.timestamp <= time_end)
            except Exception as e:
                logger.warning(f"Ошибка парсинга timestamp: {e}")
        
        signals = query.order_by(Signal.timestamp.desc()).limit(10).all()
        
        if not signals:
            logger.warning(f"Сигналы для {pair_symbol} не найдены")
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Найдено сигналов: {len(signals)}")
        logger.info(f"{'='*80}\n")
        
        for signal in signals:
            logger.info(f"\n📊 Сигнал ID: {signal.id}")
            logger.info(f"   Пара: {signal.pair.symbol if signal.pair else 'N/A'}")
            logger.info(f"   Тип: {signal.signal_type}")
            logger.info(f"   Уровень: {signal.level_price}")
            logger.info(f"   Таймфрейм: {signal.level_timeframe}")
            logger.info(f"   Время создания: {signal.timestamp}")
            logger.info(f"   Статус: {signal.status}")
            
            logger.info(f"\n   Elder's Screens:")
            logger.info(f"   - Экран 1 (4H Тренд):")
            logger.info(f"     * passed: {signal.elder_screen_1_passed}")
            logger.info(f"     * blocked_reason: {signal.elder_screen_1_blocked_reason}")
            
            logger.info(f"   - Экран 2 (1H Анализ):")
            logger.info(f"     * passed: {signal.elder_screen_2_passed}")
            logger.info(f"     * blocked_reason: {signal.elder_screen_2_blocked_reason}")
            
            logger.info(f"   - Экран 3 (15M Вход):")
            logger.info(f"     * passed: {signal.elder_screen_3_passed}")
            logger.info(f"     * blocked_reason: {signal.elder_screen_3_blocked_reason}")
            
            # Проверяем метаданные
            elder_metadata = signal.elder_screens_metadata
            if elder_metadata:
                logger.info(f"\n   Метаданные Elder's Screens:")
                screen_1 = elder_metadata.get('screen_1', {})
                screen_2 = elder_metadata.get('screen_2', {})
                
                logger.info(f"   - screen_1:")
                logger.info(f"     * passed: {screen_1.get('passed')}")
                logger.info(f"     * blocked_reason: {screen_1.get('blocked_reason')}")
                logger.info(f"     * checks: {screen_1.get('checks', {})}")
                
                logger.info(f"   - screen_2:")
                logger.info(f"     * passed: {screen_2.get('passed')}")
                logger.info(f"     * blocked_reason: {screen_2.get('blocked_reason')}")
                logger.info(f"     * checks: {screen_2.get('checks', {})}")
                
                logger.info(f"   - final_decision: {elder_metadata.get('final_decision')}")
            else:
                logger.warning(f"   ⚠️ Метаданные Elder's Screens отсутствуют!")
            
            # Проверяем meta_data
            meta_data = signal.meta_data
            if meta_data:
                elder_screens_in_meta = meta_data.get('elder_screens_metadata')
                if elder_screens_in_meta:
                    logger.info(f"\n   Elder's Screens в meta_data:")
                    logger.info(f"     {elder_screens_in_meta}")
                else:
                    logger.warning(f"   ⚠️ Elder's Screens не найдены в meta_data")
            
            logger.info(f"\n{'-'*80}\n")
        
    except Exception as e:
        logger.error(f"Ошибка проверки сигнала: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == '__main__':
    # Проверяем сигнал XLM/USDT 1H от 07.12.2025 23:30:39
    pair = 'XLM/USDT'
    timestamp = '2025-12-07T23:30:39+03:00'  # Москва время
    
    logger.info(f"Проверка Elder's Screens для сигнала {pair} от {timestamp}")
    check_signal_elder_screens(pair, timestamp)

