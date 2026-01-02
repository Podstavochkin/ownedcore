"""
Celery задачи для детекции паттернов свечного анализа
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.celery_app import celery_app
import core.database as db_module
from core.models import CandlestickPattern
from core.ohlcv_store import ohlcv_store
from core.candlestick_patterns import pattern_detector
from core.analysis_engine import TRADING_PAIRS
import logging

logger = logging.getLogger(__name__)


@celery_app.task(name='tasks.pattern_detection_tasks.detect_patterns_for_pair', queue='analysis')
def detect_patterns_for_pair(pair: str, timeframe: str, lookback_candles: int = 100):
    """
    Детектирует паттерны для указанной пары и таймфрейма
    
    Args:
        pair: Торговая пара (например, 'BTC/USDT')
        timeframe: Таймфрейм ('1m', '5m', '15m', '1h', '4h')
        lookback_candles: Количество свечей для анализа (по умолчанию 100)
    
    Returns:
        dict: Результат детекции
    """
    try:
        db_module.init_database()
        db = db_module.SessionLocal()
        
        try:
            # Получаем свечи из локального хранилища
            candles = ohlcv_store.get_ohlcv(pair, timeframe, limit=lookback_candles)
            
            if len(candles) < 3:
                logger.warning(f"⚠️ Недостаточно свечей для детекции паттернов: {pair} {timeframe} ({len(candles)} свечей)")
                return {
                    'success': False,
                    'error': 'Недостаточно свечей',
                    'pair': pair,
                    'timeframe': timeframe
                }
            
            # Детектируем паттерны
            detected_patterns = pattern_detector.detect_patterns(candles, pair, timeframe)
            
            if not detected_patterns:
                logger.debug(f"  {pair} {timeframe}: паттернов не обнаружено")
                return {
                    'success': True,
                    'patterns_found': 0,
                    'pair': pair,
                    'timeframe': timeframe
                }
            
            # Сохраняем паттерны в БД (только новые, избегаем дубликатов)
            saved_count = 0
            skipped_count = 0
            
            for pattern_data in detected_patterns:
                # Проверяем, существует ли уже такой паттерн
                existing = db.query(CandlestickPattern).filter(
                    CandlestickPattern.symbol == pair,
                    CandlestickPattern.timeframe == timeframe,
                    CandlestickPattern.pattern_type == pattern_data['pattern_type'],
                    CandlestickPattern.timestamp == pattern_data['timestamp']
                ).first()
                
                if existing:
                    # Обновляем существующий паттерн (помечаем как активный)
                    existing.is_active = True
                    existing.reliability = pattern_data['reliability']
                    existing.updated_at = datetime.now(timezone.utc)
                    skipped_count += 1
                else:
                    # Создаем новый паттерн
                    pattern = CandlestickPattern(
                        symbol=pair,
                        timeframe=timeframe,
                        pattern_type=pattern_data['pattern_type'],
                        direction=pattern_data['direction'],
                        reliability=pattern_data['reliability'],
                        candles_indices=pattern_data['candles_indices'],
                        timestamp=pattern_data['timestamp'],
                        price=pattern_data['price'],
                        pattern_zone=pattern_data.get('pattern_zone', 'neutral'),
                        is_active=True
                    )
                    db.add(pattern)
                    saved_count += 1
            
            db.commit()
            
            logger.info(f"✅ {pair} {timeframe}: обнаружено {len(detected_patterns)} паттернов, сохранено {saved_count}, обновлено {skipped_count}")
            
            return {
                'success': True,
                'patterns_found': len(detected_patterns),
                'patterns_saved': saved_count,
                'patterns_updated': skipped_count,
                'pair': pair,
                'timeframe': timeframe
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка детекции паттернов для {pair} {timeframe}: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'pair': pair,
            'timeframe': timeframe
        }


@celery_app.task(name='tasks.pattern_detection_tasks.detect_patterns_periodic', queue='analysis')
def detect_patterns_periodic():
    """
    Периодическая детекция паттернов для всех активных пар и таймфреймов
    Вызывается по расписанию:
    - Каждую минуту для 1m таймфрейма
    - Каждые 15 минут для 15m таймфрейма
    - Каждый час для 1h таймфрейма
    - Каждые 4 часа для 4h таймфрейма
    """
    try:
        logger.info(f"🔍 Начало периодической детекции паттернов для {len(TRADING_PAIRS)} пар...")
        
        # Таймфреймы и количество свечей для анализа
        timeframes_config = {
            '1m': {'lookback': 100, 'description': '1 минута'},
            '5m': {'lookback': 100, 'description': '5 минут'},
            '15m': {'lookback': 100, 'description': '15 минут'},
            '1h': {'lookback': 200, 'description': '1 час'},
            '4h': {'lookback': 200, 'description': '4 часа'}
        }
        
        total_patterns_found = 0
        total_patterns_saved = 0
        results = {}
        
        for pair in TRADING_PAIRS:
            pair_results = {}
            for timeframe, config in timeframes_config.items():
                try:
                    result = detect_patterns_for_pair(pair, timeframe, config['lookback'])
                    pair_results[timeframe] = result
                    
                    if result.get('success'):
                        total_patterns_found += result.get('patterns_found', 0)
                        total_patterns_saved += result.get('patterns_saved', 0)
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка детекции паттернов {pair} {timeframe}: {e}")
                    pair_results[timeframe] = {'success': False, 'error': str(e)}
            
            results[pair] = pair_results
        
        logger.info(f"✅ Периодическая детекция завершена: найдено {total_patterns_found} паттернов, сохранено {total_patterns_saved}")
        
        return {
            'success': True,
            'total_patterns_found': total_patterns_found,
            'total_patterns_saved': total_patterns_saved,
            'results': results
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка периодической детекции паттернов: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


@celery_app.task(name='tasks.pattern_detection_tasks.deactivate_old_patterns', queue='analysis')
def deactivate_old_patterns(days_old: int = 7):
    """
    Деактивирует старые паттерны (помечает is_active=False)
    Используется для очистки устаревших паттернов
    
    Args:
        days_old: Количество дней, после которых паттерн считается устаревшим
    """
    try:
        db_module.init_database()
        db = db_module.SessionLocal()
        
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
            
            # Находим старые активные паттерны
            old_patterns = db.query(CandlestickPattern).filter(
                CandlestickPattern.is_active == True,
                CandlestickPattern.timestamp < cutoff_date
            ).all()
            
            deactivated_count = 0
            for pattern in old_patterns:
                pattern.is_active = False
                deactivated_count += 1
            
            db.commit()
            
            logger.info(f"✅ Деактивировано {deactivated_count} устаревших паттернов (старше {days_old} дней)")
            
            return {
                'success': True,
                'deactivated_count': deactivated_count
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка деактивации старых паттернов: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }

