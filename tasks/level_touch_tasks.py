"""
Celery задачи для проверки касаний уровней (для скальпинга - каждую минуту)
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.celery_app import celery_app
from core.database import SessionLocal, init_database
from core.models import Level, TradingPair, Signal
from core.cache import cache, init_redis
from core.signal_manager import signal_manager
from core.analysis_engine import analysis_engine
from sqlalchemy import func
import logging
import asyncio

logger = logging.getLogger(__name__)


@celery_app.task(name='tasks.level_touch_tasks.check_level_touches', queue='signals')
def check_level_touches():
    """
    Проверяет уровни из Elder's Screen (ES) и генерирует сигналы.
    
    КРИТИЧНО: Сигналы генерируются ТОЛЬКО из ES (ready_for_signal = True).
    Все сигналы должны пройти проверки Elder's Triple Screen System.
    
    Адаптивная частота проверки в зависимости от расстояния до уровня:
    - <1%: каждые 30 секунд
    - 1-2.5%: каждую минуту
    - 2.5-5%: каждые 5 минут
    - >5%: каждые 10 минут
    """
    try:
        logger.info("Проверка касаний уровней (скальпинг)...")
        
        if not init_database():
            return None
        
        init_redis()
        
        from core.database import SessionLocal
        
        session = SessionLocal()
        
        try:
            # Получаем все активные уровни из БД
            active_levels_db = session.query(Level).filter(
                Level.is_active == True
            ).all()
            
            if not active_levels_db:
                logger.info("Нет активных уровней для проверки")
                return {'status': 'success', 'checked': 0, 'signals_generated': 0}
            
            # Группируем уровни по парам
            levels_by_pair = {}
            for level in active_levels_db:
                if level.pair:
                    pair_symbol = level.pair.symbol
                    if pair_symbol not in levels_by_pair:
                        levels_by_pair[pair_symbol] = []
                    levels_by_pair[pair_symbol].append(level)
            
            signals_generated = 0
            checked_count = 0
            
            # Создаем event loop для асинхронных операций
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            for pair_symbol, levels in levels_by_pair.items():
                try:
                    # Получаем текущую цену (последняя свеча 15m)
                    candles = loop.run_until_complete(
                        analysis_engine.fetch_ohlcv(pair_symbol, '15m', 10)
                    )
                    
                    if not candles:
                        continue
                    
                    current_price = candles[-1]['close']
                    
                    # Получаем тренд для проверки условий сигнала
                    candles_1h = loop.run_until_complete(
                        analysis_engine.fetch_ohlcv(pair_symbol, '1h', 50)
                    )
                    
                    if not candles_1h:
                        continue
                    
                    trend_1h = analysis_engine.determine_trend_1h(candles_1h)
                    
                    # Проверяем каждый уровень на касание с адаптивной частотой
                    for level in levels:
                        checked_count += 1
                        
                        # Инициализируем should_generate для каждого уровня
                        should_generate = False
                        
                        # КРИТИЧЕСКАЯ ПРОВЕРКА: проверяем, был ли уже АКТУАЛЬНЫЙ сигнал для этого уровня
                        # Используем ту же логику, что и в signal_manager: только актуальные сигналы (не старше 30 минут)
                        level_price = level.price
                        if level_price > 0:
                            from datetime import timedelta, timezone as tz
                            price_tolerance = level_price * 0.001  # 0.1%
                            MAX_SIGNAL_AGE_SECONDS = 30 * 60
                            cutoff_time = datetime.now(tz.utc) - timedelta(seconds=MAX_SIGNAL_AGE_SECONDS)
                            
                            existing_signal = session.query(Signal).filter(
                                Signal.pair_id == level.pair_id,
                                Signal.timestamp >= cutoff_time,  # Только актуальные сигналы
                                Signal.status == 'ACTIVE',  # Только активные сигналы
                                func.abs(Signal.level_price - level_price) < price_tolerance
                            ).order_by(Signal.timestamp.desc()).first()
                            
                            if existing_signal:
                                signal_age = (datetime.now(tz.utc) - existing_signal.timestamp.replace(tzinfo=tz.utc)).total_seconds()
                                logger.info(f"⚠️ Актуальный сигнал для уровня {level_price} уже существует (ID: {existing_signal.id}, создан: {existing_signal.timestamp}, возраст: {signal_age/60:.1f} мин, статус: {existing_signal.status}). Пропускаем.")
                                continue  # Актуальный сигнал уже существует
                        
                        # Проверяем касание уровня
                        level_type = level.level_type
                        signal_type = 'LONG' if level_type == 'support' else 'SHORT'
                        
                        if level.price == 0:
                            continue
                        price_diff = abs(current_price - level.price) / level.price
                        price_diff_pct = price_diff * 100
                        
                        # ОПТИМИЗАЦИЯ: Проверяем готовые уровни (прошли Elder's Screens) при приближении
                        meta = level.meta_data or {}
                        metadata = meta.get('metadata', {}) or {}
                        elder_screens_passed = metadata.get('elder_screens_passed', False)
                        ready_for_signal = elder_screens_passed and price_diff_pct <= 0.7  # В пределах 0.7% для готовых уровней (оптимизировано 31.12.2024: должно быть меньше порога "цена ушла")
                        
                        # ДОПОЛНИТЕЛЬНОЕ ЛОГИРОВАНИЕ для отладки
                        if price_diff_pct <= 1.0:  # Логируем только близкие уровни
                            logger.info(f"🔍 [{pair_symbol}] Уровень @ {level.price:.6f}: расстояние={price_diff_pct:.2f}%, ES={elder_screens_passed}, ready={ready_for_signal}")
                        
                        # КРИТИЧНО: Если уровень готов для сигнала (ready_for_signal), 
                        # мы НЕ должны пропускать его из-за last_check_time!
                        # Готовые уровни должны проверяться ВСЕГДА, независимо от времени последней проверки
                        if ready_for_signal:
                            # Готовый уровень из ES - Elder's Screens пройдены, генерируем сигнал
                            if not elder_screens_passed:
                                logger.warning(f"⚠️ Уровень {pair_symbol} @ {level.price} помечен как готовый (ES), но Elder's Screens не пройдены. Пропускаем.")
                                continue
                            
                            logger.info(f"🎯 ES: Готовый уровень {pair_symbol} @ {level.price} (расстояние: {price_diff_pct:.2f}%), Elder's Screens пройдены → генерируем сигнал")
                            should_generate = True
                        else:
                            # Для НЕ готовых уровней применяем адаптивную частоту проверки
                            # <1%: каждые 30 секунд, 1-2.5%: каждую минуту, 2.5-5%: каждые 5 минут, >5%: каждые 10 минут
                            last_check_time_str = meta.get('last_check_time')
                            now = datetime.now()
                            
                            # Определяем интервал проверки в зависимости от расстояния
                            if price_diff_pct < 1.0:
                                check_interval_seconds = 30  # Каждые 30 секунд
                            elif price_diff_pct < 2.5:
                                check_interval_seconds = 60  # Каждую минуту
                            elif price_diff_pct < 5.0:
                                check_interval_seconds = 300  # Каждые 5 минут
                            else:
                                check_interval_seconds = 600  # Каждые 10 минут
                            
                            # Проверяем, нужно ли проверять этот уровень сейчас
                            if last_check_time_str:
                                try:
                                    last_check_time = datetime.fromisoformat(last_check_time_str.replace('Z', '+00:00'))
                                    time_since_check = (now - last_check_time.replace(tzinfo=None)).total_seconds()
                                    if time_since_check < check_interval_seconds:
                                        # Уровень проверялся недавно, пропускаем
                                        continue
                                except Exception as e:
                                    logger.debug(f"Ошибка парсинга last_check_time для уровня {level_price}: {e}")
                            
                            touch_tolerance = analysis_engine.level_settings["live_touch_tolerance"]
                            is_touching = price_diff <= touch_tolerance
                            
                            if is_touching:
                                # Обычное касание БЕЗ прохождения ES - НЕ генерируем сигнал
                                # Все сигналы должны проходить через Elder's Screen
                                logger.debug(f"⏸️ Уровень {pair_symbol} @ {level.price} касается, но не прошел ES (ready_for_signal=False). Пропускаем.")
                                continue
                            else:
                                # Уровень не касается и не готов - пропускаем
                                continue
                        
                        if should_generate:
                                # ========== ПРИМЕНЕНИЕ ФИЛЬТРОВ ==========
                                # Этап 1-5: Применяем все фильтры перед генерацией сигнала
                                meta = level.meta_data or {}
                                timeframe = meta.get('timeframe', '15m')
                                score = meta.get('level_score') or meta.get('score') or 0
                                test_count = level.test_count or 0
                                
                                # Получаем активный треугольник для этой пары и таймфрейма
                                triangle = analysis_engine.get_active_triangle_for_pair(pair_symbol, timeframe)
                                
                                # Создаем словарь уровня для проверки фильтров
                                level_dict = {
                                    'score': score,
                                    'timeframe': timeframe,
                                    'test_count': test_count
                                }
                                
                                should_block, block_reason = analysis_engine.should_block_signal_by_filters(
                                    level=level_dict,
                                    trend_1h=trend_1h,
                                    timeframe=timeframe,
                                    price_distance_pct=price_diff_pct,
                                    test_count=test_count,
                                    signal_type=signal_type,
                                    triangle=triangle
                                )
                                
                                if should_block:
                                    logger.info(f"🚫 [{pair_symbol}] БЛОКИРОВКА сигнала: {block_reason}")
                                    continue
                                
                                # Этап 2-3: Проверка приоритета (опционально, для логирования)
                                priority = analysis_engine.calculate_signal_priority(trend_1h, score, timeframe)
                                if priority < -3:
                                    logger.warning(f"⚠️ [{pair_symbol}] Низкий приоритет сигнала ({priority}), но не блокируем")
                                
                                # Проверяем, нет ли уже АКТУАЛЬНОГО активного/открытого сигнала на этом уровне (защита от дублей)
                                # Используем ту же логику, что и в signal_manager: только актуальные сигналы (не старше 30 минут)
                                from datetime import timedelta, timezone as tz
                                price_tolerance = max(level.price * 0.001, 0.0001)
                                MAX_SIGNAL_AGE_SECONDS = 30 * 60
                                cutoff_time = datetime.now(tz.utc) - timedelta(seconds=MAX_SIGNAL_AGE_SECONDS)
                                
                                duplicate_signal = session.query(Signal).filter(
                                    Signal.pair_id == level.pair_id,
                                    Signal.signal_type == signal_type,
                                    Signal.status.in_(['ACTIVE', 'OPEN']),
                                    Signal.timestamp >= cutoff_time,  # Только актуальные сигналы
                                    func.abs(Signal.level_price - level.price) <= price_tolerance
                                ).order_by(Signal.timestamp.desc()).first()
                                
                                if duplicate_signal:
                                    signal_age = (datetime.now(tz.utc) - duplicate_signal.timestamp.replace(tzinfo=tz.utc)).total_seconds()
                                    logger.info(
                                        f"⏸ Актуальный сигнал {signal_type} для {pair_symbol} @ {level.price} уже существует "
                                        f"(ID: {duplicate_signal.id}, статус: {duplicate_signal.status}, возраст: {signal_age/60:.1f} мин). Пропускаем."
                                    )
                                    continue
                                
                                historical = meta.get('historical_touches', level.test_count or 1)
                                live_tests = meta.get('live_test_count')
                                if live_tests is None:
                                    live_tests = max((level.test_count or historical) - historical, 0)
                                live_tests += 1
                                meta['historical_touches'] = historical
                                meta['live_test_count'] = live_tests
                                meta['last_check_time'] = datetime.now().isoformat()  # Сохраняем время последней проверки
                                level.meta_data = meta
                                level.test_count = historical + live_tests
                                level.last_touch = datetime.now()
                                session.commit()  # Сохраняем обновление метаданных
                                
                                # Генерируем сигнал
                                stop_loss_percent = 0.005  # 0.5%
                                if signal_type == 'LONG':
                                    stop_loss = level.price * (1 - stop_loss_percent)
                                else:
                                    stop_loss = level.price * (1 + stop_loss_percent)
                                
                                distance = meta.get('distance_percent')
                                
                                # КРИТИЧНО: Извлекаем данные Elder's Screens из метаданных уровня
                                metadata = meta.get('metadata', {}) or {}
                                elder_screens_data = metadata.get('elder_screens', {})
                                
                                # Обрабатываем случаи, когда elder_screens отсутствует
                                if not elder_screens_data:
                                    logger.warning(f"⚠️ Elder's Screens metadata отсутствует для уровня {pair_symbol} @ {level.price}")
                                    elder_screens_data = {
                                        'screen_1': {'passed': False, 'blocked_reason': 'Elder\'s Screens не были проверены при генерации сигнала'},
                                        'screen_2': {'passed': False, 'blocked_reason': 'Elder\'s Screens не были проверены при генерации сигнала'},
                                        'final_decision': 'NOT_CHECKED'
                                    }
                                
                                screen_1 = elder_screens_data.get('screen_1', {})
                                screen_2 = elder_screens_data.get('screen_2', {})
                                
                                # Обеспечиваем, что passed всегда bool, а не None
                                screen_1_passed = screen_1.get('passed')
                                if screen_1_passed is None:
                                    screen_1_passed = False
                                    if not screen_1.get('blocked_reason'):
                                        screen_1['blocked_reason'] = 'Экран 1 не был проверен'
                                
                                screen_2_passed = screen_2.get('passed')
                                if screen_2_passed is None:
                                    screen_2_passed = False
                                    if not screen_2.get('blocked_reason'):
                                        screen_2['blocked_reason'] = 'Экран 2 не был проверен'
                                
                                # Обновляем elder_screens_data с правильными значениями
                                elder_screens_data['screen_1'] = screen_1
                                elder_screens_data['screen_2'] = screen_2

                                signal = Signal(
                                    pair_id=level.pair_id,
                                    signal_type=signal_type,
                                    level_price=level.price,
                                    entry_price=level.price,
                                    current_price=current_price,
                                    stop_loss=stop_loss,
                                    trend_1h=trend_1h,
                                    level_type=level_type,
                                    test_count=level.test_count or 1,
                                    status='ACTIVE',
                                    level_timeframe=timeframe,
                                    historical_touches=historical,
                                    live_test_count=live_tests,
                                    level_score=score,
                                    distance_percent=distance,
                                    timestamp=datetime.now(),
                                    meta_data=level.meta_data or {},
                                    # Elder's Triple Screen System
                                    elder_screen_1_passed=screen_1_passed,
                                    elder_screen_1_blocked_reason=screen_1.get('blocked_reason'),
                                    elder_screen_2_passed=screen_2_passed,
                                    elder_screen_2_blocked_reason=screen_2.get('blocked_reason'),
                                    elder_screen_3_passed=None,  # Пока не используется
                                    elder_screen_3_blocked_reason=None,
                                    elder_screens_metadata=elder_screens_data
                                )
                                
                                session.add(signal)
                                session.flush()  # Получаем ID сигнала
                                
                                # Обновляем уровень (помечаем как использованный в meta_data)
                                if level.meta_data is None:
                                    level.meta_data = {}
                                level.meta_data['signal_generated'] = True
                                level.meta_data['signal_timestamp'] = datetime.now().isoformat()
                                
                                # КРИТИЧНО: коммитим сигнал в БД ДО планирования Celery задачи
                                # чтобы избежать race condition (задача может начать выполняться до commit)
                                session.commit()
                                
                                signals_generated += 1
                                logger.info(f"Сигнал {signal_type} сгенерирован для {pair_symbol} @ {level.price} (ID: {signal.id})")
                                
                                # Планируем live-торговлю для этого сигнала (после commit!)
                                try:
                                    from tasks.demo_trading_tasks import place_demo_order_for_signal
                                    from core.config import settings
                                    if settings.DEMO_AUTO_TRADING_ENABLED:
                                        task = place_demo_order_for_signal.delay(signal.id)
                                        logger.info(f"✅ Ордер запланирован в Celery для live-торговли: signal_id={signal.id}, task_id={task.id}")
                                    else:
                                        logger.debug(f"⏸️  Авто-торговля отключена, пропускаем signal_id={signal.id}")
                                except Exception as demo_err:
                                    logger.warning(f"⚠️  Не удалось запланировать ордер для сигнала {signal.id}: {demo_err}")
                        
                        # Сохраняем время последней проверки даже если сигнал не был сгенерирован
                        if level.meta_data is None:
                            level.meta_data = {}
                        level.meta_data['last_check_time'] = datetime.now().isoformat()
                        session.commit()  # Сохраняем обновление метаданных
                
                except Exception as e:
                    logger.error(f"Ошибка проверки уровней для {pair_symbol}: {e}")
                    continue
            
            loop.close()
            session.commit()
            
            # Очищаем кэш сигналов
            cache.delete('signals:all')
            
            logger.info(f"Проверка завершена: проверено {checked_count} уровней, сгенерировано {signals_generated} сигналов")
            
            return {
                'status': 'success',
                'checked': checked_count,
                'signals_generated': signals_generated,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка проверки касаний: {e}")
            import traceback
            traceback.print_exc()
            session.rollback()
            return {'status': 'error', 'error': str(e)}
        
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Критическая ошибка в задаче проверки касаний: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'error': str(e)}

