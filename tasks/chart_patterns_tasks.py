"""
Celery задачи для детекции ценовых фигур (Chart Patterns)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.celery_app import celery_app
import core.database as db_module
from core.models import ChartPattern
from core.ohlcv_store import ohlcv_store
from core.chart_patterns_detector import ChartPatternDetector
from core.analysis_engine import TRADING_PAIRS
import logging

logger = logging.getLogger(__name__)

# Создаем экземпляр детектора
chart_pattern_detector = ChartPatternDetector()


@celery_app.task(name='tasks.chart_patterns_tasks.detect_chart_patterns_for_pair', queue='analysis')
def detect_chart_patterns_for_pair(pair: str, timeframe: str, lookback_candles: int = 200):
    """
    Детектирует ценовые фигуры для указанной пары и таймфрейма
    
    Args:
        pair: Торговая пара (например, 'BTC/USDT')
        timeframe: Таймфрейм ('15m', '1h', '4h') - для ценовых фигур нужны более крупные таймфреймы
        lookback_candles: Количество свечей для анализа (по умолчанию 200)
    
    Returns:
        dict: Результат детекции
    """
    try:
        db_module.init_database()
        db = db_module.SessionLocal()
        
        try:
            # Получаем свечи из локального хранилища
            candles_raw = ohlcv_store.get_ohlcv(pair, timeframe, limit=lookback_candles)
            
            if len(candles_raw) < chart_pattern_detector.min_pattern_candles:
                logger.warning(
                    f"⚠️ Недостаточно свечей для детекции ценовых фигур: {pair} {timeframe} "
                    f"({len(candles_raw)} свечей, требуется минимум {chart_pattern_detector.min_pattern_candles})"
                )
                return {
                    'success': False,
                    'error': 'Недостаточно свечей',
                    'pair': pair,
                    'timeframe': timeframe,
                    'candles_count': len(candles_raw)
                }
            
            # Конвертируем формат свечей из ohlcv_store в формат для детектора
            # ohlcv_store возвращает timestamp в миллисекундах, детектор ожидает в секундах
            candles = []
            for candle in candles_raw:
                # Конвертируем timestamp из миллисекунд в секунды
                timestamp_seconds = candle['timestamp'] / 1000 if candle['timestamp'] > 1e10 else candle['timestamp']
                
                candles.append({
                    'time': timestamp_seconds,
                    'open': float(candle['open']),
                    'high': float(candle['high']),
                    'low': float(candle['low']),
                    'close': float(candle['close']),
                    'volume': float(candle.get('volume', 0))
                })
            
            # Детектируем ценовые фигуры
            detected_patterns = chart_pattern_detector.detect_all_patterns(candles, pair, timeframe)
            
            if not detected_patterns:
                logger.debug(f"  {pair} {timeframe}: ценовых фигур не обнаружено")
                return {
                    'success': True,
                    'patterns_found': 0,
                    'pair': pair,
                    'timeframe': timeframe
                }
            
            # --- ВЫБОР ОДНОЙ ЛУЧШЕЙ ФИГУРЫ-ТРЕУГОЛЬНИКА ДЛЯ ПАРЫ/ТАЙМФРЕЙМА ---
            triangle_types = {'ascending_triangle', 'descending_triangle', 'symmetrical_triangle'}
            triangles = [p for p in detected_patterns if p.get('pattern_type') in triangle_types]
            other_patterns = [p for p in detected_patterns if p.get('pattern_type') not in triangle_types]

            best_triangle = None
            best_score = -1.0
            best_end_time = None

            for p in triangles:
                reliability = float(p.get('reliability', 0.0) or 0.0)
                end_time = p.get('end_time')
                # При равной надежности берем более свежую фигуру
                score_key = (reliability, end_time or datetime.min.replace(tzinfo=timezone.utc))
                if score_key > (best_score, best_end_time or datetime.min.replace(tzinfo=timezone.utc)):
                    best_triangle = p
                    best_score = reliability
                    best_end_time = end_time

            filtered_patterns = []
            if best_triangle:
                filtered_patterns.append(best_triangle)
            filtered_patterns.extend(other_patterns)

            # Перед сохранением деактивируем все существующие треугольники для этой пары/таймфрейма
            existing_triangles = db.query(ChartPattern).filter(
                ChartPattern.symbol == pair,
                ChartPattern.timeframe == timeframe,
                ChartPattern.pattern_type.in_(list(triangle_types))
            ).all()
            for et in existing_triangles:
                et.is_active = False

            # Сохраняем фигуры в БД (только новые, избегаем дубликатов)
            saved_count = 0
            updated_count = 0
            skipped_count = 0
            
            for pattern_data in filtered_patterns:
                # Проверяем, существует ли уже такая фигура
                # Проверяем по symbol, timeframe, pattern_type и start_time
                existing = db.query(ChartPattern).filter(
                    ChartPattern.symbol == pair,
                    ChartPattern.timeframe == timeframe,
                    ChartPattern.pattern_type == pattern_data['pattern_type'],
                    ChartPattern.start_time == pattern_data['start_time']
                ).first()
                
                if existing:
                    # Обновляем существующую фигуру
                    existing.is_active = True
                    existing.reliability = pattern_data.get('reliability', 0.5)
                    existing.end_time = pattern_data['end_time']
                    existing.confirmation_time = pattern_data.get('confirmation_time')
                    existing.support_level = pattern_data.get('support_level')
                    existing.resistance_level = pattern_data.get('resistance_level')
                    existing.neckline = pattern_data.get('neckline')
                    existing.target_price = pattern_data.get('target_price')
                    existing.pattern_height = pattern_data.get('pattern_height')
                    existing.pattern_width = pattern_data.get('pattern_width')
                    existing.candles_count = pattern_data.get('candles_count')
                    existing.pattern_data = pattern_data.get('pattern_data')
                    existing.updated_at = datetime.now(timezone.utc)
                    updated_count += 1
                else:
                    # Создаем новую фигуру
                    pattern = ChartPattern(
                        symbol=pair,
                        timeframe=timeframe,
                        pattern_type=pattern_data['pattern_type'],
                        pattern_category=pattern_data['pattern_category'],
                        direction=pattern_data['direction'],
                        reliability=pattern_data.get('reliability', 0.5),
                        start_time=pattern_data['start_time'],
                        end_time=pattern_data['end_time'],
                        confirmation_time=pattern_data.get('confirmation_time'),
                        support_level=pattern_data.get('support_level'),
                        resistance_level=pattern_data.get('resistance_level'),
                        neckline=pattern_data.get('neckline'),
                        target_price=pattern_data.get('target_price'),
                        pattern_height=pattern_data.get('pattern_height'),
                        pattern_width=pattern_data.get('pattern_width'),
                        volume_confirmation=pattern_data.get('volume_confirmation', False),
                        is_active=True,
                        is_confirmed=pattern_data.get('is_confirmed', False),
                        candles_count=pattern_data.get('candles_count'),
                        pattern_data=pattern_data.get('pattern_data')
                    )
                    db.add(pattern)
                    saved_count += 1
            
            db.commit()
            
            logger.info(
                f"✅ {pair} {timeframe}: обнаружено {len(detected_patterns)} фигур "
                f"(сохранено: {saved_count}, обновлено: {updated_count})"
            )
            
            return {
                'success': True,
                'patterns_found': len(detected_patterns),
                'saved': saved_count,
                'updated': updated_count,
                'pair': pair,
                'timeframe': timeframe
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"❌ Ошибка детекции ценовых фигур для {pair} {timeframe}: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'pair': pair,
                'timeframe': timeframe
            }
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в detect_chart_patterns_for_pair для {pair} {timeframe}: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'pair': pair,
            'timeframe': timeframe
        }


@celery_app.task(name='tasks.chart_patterns_tasks.detect_chart_patterns_periodic', queue='analysis')
def detect_chart_patterns_periodic():
    """
    Периодическая задача для детекции ценовых фигур для всех пар и таймфреймов
    
    Для ценовых фигур используем более крупные таймфреймы:
    - 15m, 1h, 4h (ценовые фигуры формируются на большем количестве свечей)
    """
    try:
        db_module.init_database()
        
        # Таймфреймы для детекции ценовых фигур (более крупные, чем для свечных паттернов)
        timeframes = ['15m', '1h', '4h']
        
        # Количество свечей для анализа (больше для ценовых фигур)
        lookback_candles = {
            '15m': 200,  # ~2 дня
            '1h': 200,   # ~8 дней
            '4h': 200    # ~33 дня
        }
        
        total_patterns = 0
        total_pairs = 0
        
        for pair in TRADING_PAIRS:
            for timeframe in timeframes:
                try:
                    result = detect_chart_patterns_for_pair(
                        pair,
                        timeframe,
                        lookback_candles.get(timeframe, 200)
                    )
                    
                    if result.get('success'):
                        total_patterns += result.get('patterns_found', 0)
                        total_pairs += 1
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка детекции фигур для {pair} {timeframe}: {e}", exc_info=True)
                    continue
        
        logger.info(
            f"✅ Периодическая детекция ценовых фигур завершена: "
            f"обработано {total_pairs} пар, обнаружено {total_patterns} фигур"
        )
        
        return {
            'success': True,
            'patterns_found': total_patterns,
            'pairs_processed': total_pairs
        }
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в detect_chart_patterns_periodic: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


@celery_app.task(name='tasks.chart_patterns_tasks.deactivate_old_chart_patterns', queue='analysis')
def deactivate_old_chart_patterns(days_old: int = 30, hard_delete_after_days: int = 180):
    """
    Деактивирует старые ценовые фигуры (старше указанного количества дней)
    
    Args:
        days_old: Количество дней, после которых фигура считается устаревшей (по умолчанию 30)
    """
    try:
        db_module.init_database()
        db = db_module.SessionLocal()
        
        try:
            now = datetime.now(timezone.utc)
            cutoff_date = now - timedelta(days=days_old)
            hard_delete_cutoff = now - timedelta(days=hard_delete_after_days)
            
            # 1) Деактивируем фигуры, которые закончились более days_old дней назад
            updated = db.query(ChartPattern).filter(
                ChartPattern.is_active == True,
                ChartPattern.end_time < cutoff_date
            ).update({
                'is_active': False,
                'updated_at': now
            })
            
            # 2) Жёстко удаляем совсем устаревшие фигуры (по умолчанию старше 180 дней)
            deleted = db.query(ChartPattern).filter(
                ChartPattern.end_time < hard_delete_cutoff
            ).delete(synchronize_session=False)
            
            # 3) Удаляем паттерны с некорректными датами (1970 год или раньше)
            # Минимальная валидная дата (2000-01-01)
            MIN_VALID_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)
            invalid_deleted = db.query(ChartPattern).filter(
                (ChartPattern.start_time < MIN_VALID_DATE) |
                (ChartPattern.end_time < MIN_VALID_DATE)
            ).delete(synchronize_session=False)
            
            # 4) Деактивируем паттерны с датами в далеком будущем (более чем на 1 день)
            MAX_VALID_DATE = now + timedelta(days=1)
            future_deactivated = db.query(ChartPattern).filter(
                ChartPattern.is_active == True,
                ((ChartPattern.start_time > MAX_VALID_DATE) |
                 (ChartPattern.end_time > MAX_VALID_DATE))
            ).update({
                'is_active': False,
                'updated_at': now
            })
            
            db.commit()
            
            total_deleted = deleted + invalid_deleted
            if total_deleted > 0 or invalid_deleted > 0 or future_deactivated > 0:
                logger.info(
                    f"✅ Деактивировано {updated} старых ценовых фигур (старше {days_old} дней), "
                    f"жёстко удалено: {total_deleted} фигур (из них {invalid_deleted} с некорректными датами), "
                    f"деактивировано {future_deactivated} паттернов с датами в будущем"
                )
            else:
                logger.info(
                    f"✅ Деактивировано {updated} старых ценовых фигур (старше {days_old} дней)"
                )
            
            return {
                'success': True,
                'deactivated': updated,
                'deleted': deleted + invalid_deleted,
                'invalid_deleted': invalid_deleted,
                'future_deactivated': future_deactivated
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"❌ Ошибка деактивации старых фигур: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в deactivate_old_chart_patterns: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }

