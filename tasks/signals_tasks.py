"""
Celery задачи для обработки сигналов
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.celery_app import celery_app
import core.database as database
from core.database import init_database
from core.models import Signal, TradingPair
from core.cache import cache, init_redis
from core.signal_manager import signal_manager
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)


def _update_signals_pnl_internal():
    """Обновляет P&L для всех активных сигналов (общая реализация)."""
    try:
        logger.info("Обновление P&L для сигналов...")
        
        if not init_database():
            return {'status': 'error', 'error': 'database is not initialized'}
        
        init_redis()
        if database.SessionLocal is None:
            logger.error("Session factory is not initialized")
            return {'status': 'error', 'error': 'DB session factory unavailable'}
        session = database.SessionLocal()
        loop = None
        
        try:
            from core.analysis_engine import analysis_engine
            import asyncio
            from datetime import timezone
            
            PROFIT_THRESHOLD_PERCENT = 1.5
            STOP_LOSS_THRESHOLD_PERCENT = 0.5
            
            active_signals = session.query(Signal).filter(
                Signal.status.in_(['ACTIVE', 'OPEN'])
            ).all()
            
            logger.info(f"Найдено активных сигналов: {len(active_signals)}")
            
            updated_count = 0
            closed_count = 0
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            for signal in active_signals:
                try:
                    if not signal.pair:
                        logger.debug(f"⚠️ Сигнал {signal.id}: пропущен - нет связи с парой")
                        continue
                    
                    pair_symbol = signal.pair.symbol
                    entry_price = signal.entry_price or signal.level_price
                    if not entry_price:
                        logger.debug(f"⚠️ Сигнал {signal.id} ({pair_symbol}): пропущен - нет entry_price и level_price")
                        continue
                    
                    candles = loop.run_until_complete(
                        analysis_engine.fetch_ohlcv(pair_symbol, '15m', 300)
                    )
                    if not candles:
                        logger.debug(f"⚠️ Сигнал {signal.id} ({pair_symbol}): пропущен - не удалось получить свечи")
                        continue
                    
                    current_price = candles[-1]['close']
                    signal.current_price = current_price
                    
                    signal_time = signal.timestamp
                    if signal_time.tzinfo is None:
                        signal_time = signal_time.replace(tzinfo=timezone.utc)
                    
                    # Фильтруем свечи после времени входа
                    sorted_candles = []
                    first_candle_time = None
                    last_candle_time = None
                    for c in candles:
                        candle_time = datetime.fromtimestamp(c['timestamp'] / 1000, tz=timezone.utc)
                        if first_candle_time is None:
                            first_candle_time = candle_time
                        last_candle_time = candle_time
                        if candle_time >= signal_time:
                            sorted_candles.append(c)
                    
                    if not sorted_candles:
                        # Если нет свечей после входа, проверяем почему
                        if candles and len(candles) > 0:
                            time_diff_minutes = None
                            if last_candle_time and signal_time:
                                time_diff_minutes = (signal_time - last_candle_time).total_seconds() / 60
                            
                            # Если последняя свеча раньше времени входа - это нормально для новых сигналов
                            # НО мы НЕ должны использовать эти свечи для проверки TP/SL!
                            if last_candle_time and last_candle_time < signal_time:
                                logger.warning(
                                    "⏳ Сигнал %s (%s %s): нет свечей после времени входа %s. "
                                    "Последняя свеча: %s (на %s минут раньше входа). "
                                    "Пропускаем проверку TP/SL до появления новых свечей.",
                                    signal.id,
                                    pair_symbol,
                                    signal.signal_type,
                                    signal_time.isoformat(),
                                    last_candle_time.isoformat() if last_candle_time else 'N/A',
                                    f"{abs(time_diff_minutes):.1f}" if time_diff_minutes is not None else 'N/A'
                                )
                                # НЕ используем старые свечи для проверки TP/SL - это критическая ошибка!
                                # Пропускаем проверку по текущей свече, используем только закрытые свечи (которые пусты)
                                sorted_candles = []  # Оставляем пустым, чтобы не проверять TP/SL по старым свечам
                            else:
                                # Если последняя свеча после входа, но почему-то не попала в sorted_candles
                                # (возможно, проблема с фильтрацией) - используем все свечи, но с проверкой
                                logger.warning(
                                    "⚠️ Сигнал %s (%s %s): нет свечей в sorted_candles, но последняя свеча после входа. "
                                    "Используем все свечи с дополнительной проверкой времени.",
                                    signal.id,
                                    pair_symbol,
                                    signal.signal_type
                                )
                                sorted_candles = candles
                        else:
                            logger.warning(
                                "⏳ Сигнал %s (%s %s): нет свечей вообще. Пропускаем.",
                                signal.id,
                                pair_symbol,
                                signal.signal_type
                            )
                            continue
                    
                    logger.debug(
                        f"✅ Сигнал {signal.id} ({pair_symbol} {signal.signal_type}): "
                        f"всего свечей {len(candles)}, после входа {len(sorted_candles)}"
                    )
                    
                    profit_threshold = entry_price * (PROFIT_THRESHOLD_PERCENT / 100)
                    stop_loss_threshold = entry_price * (STOP_LOSS_THRESHOLD_PERCENT / 100)
                    
                    max_favorable = signal.max_profit or 0.0
                    max_adverse = signal.max_drawdown or 0.0
                    
                    # Логируем начальные данные для диагностики
                    logger.debug(
                        f"🔍 Сигнал {signal.id} ({pair_symbol} {signal.signal_type}): "
                        f"entry={entry_price:.6f}, current={current_price:.6f}, "
                        f"TP_threshold={profit_threshold:.6f}, SL_threshold={stop_loss_threshold:.6f}"
                    )
                    
                    # КРИТИЧЕСКАЯ ПРОВЕРКА: для сигналов с live-торговлей (demo_order_id не None)
                    # НЕ устанавливаем exit_price из свечей - только из реальных данных с биржи!
                    if signal.demo_order_id is not None:
                        logger.debug(
                            f"⏭️  Сигнал {signal.id} ({pair_symbol} {signal.signal_type}): "
                            f"live-торговля активна (demo_order_id={signal.demo_order_id}), "
                            f"пропускаем теоретический расчет exit_price. Используем только реальные данные с биржи."
                        )
                        continue  # Пропускаем теоретический расчет для live-торговли
                    
                    # КРИТИЧЕСКАЯ ПРОВЕРКА: проверяем текущую цену (последняя свеча может быть незакрытой)
                    # Это позволяет фиксировать TP/SL в реальном времени, не дожидаясь закрытия свечи
                    # ВАЖНО: используем sorted_candles (свечи ПОСЛЕ входа), а не все candles!
                    if sorted_candles and signal.result_fixed is None:
                        last_candle = sorted_candles[-1]  # Используем последнюю свечу ПОСЛЕ входа
                        last_candle_time = datetime.fromtimestamp(last_candle['timestamp'] / 1000, tz=timezone.utc)
                        
                        # КРИТИЧЕСКАЯ ПРОВЕРКА: убеждаемся, что свеча действительно после входа
                        if last_candle_time < signal_time:
                            logger.warning(
                                f"⚠️ Сигнал {signal.id} ({pair_symbol} {signal.signal_type}): последняя свеча {last_candle_time.isoformat()} "
                                f"раньше времени входа {signal_time.isoformat()}. Пропускаем проверку по текущей свече, "
                                f"используем только закрытые свечи из sorted_candles."
                            )
                            # Не используем эту свечу для проверки TP/SL
                        else:
                            # Свеча после входа - можно использовать для проверки
                            current_high = last_candle.get('high', last_candle['close'])
                            current_low = last_candle.get('low', last_candle['close'])
                            current_close = last_candle['close']
                            
                            if signal.signal_type == 'LONG':
                                # Для LONG: проверяем low для SL, high для TP
                                current_adverse_move = entry_price - current_low
                                current_favorable_move = current_high - entry_price
                                exit_price_tp_current = current_high
                                exit_price_sl_current = current_low
                            else:  # SHORT
                                # Для SHORT: проверяем high для SL, low для TP
                                current_adverse_move = current_high - entry_price
                                current_favorable_move = entry_price - current_low
                                exit_price_tp_current = current_low
                                exit_price_sl_current = current_high
                            
                            # Обновляем максимальные значения с учетом текущей свечи
                            max_favorable = max(max_favorable, current_favorable_move)
                            max_adverse = max(max_adverse, current_adverse_move)
                            
                            # Дополнительная проверка для SHORT: используем current_close для более точного определения SL
                            if signal.signal_type == 'SHORT':
                                close_adverse_move = current_close - entry_price
                                if close_adverse_move >= stop_loss_threshold:
                                    logger.info(
                                        f"🔴 SHORT SL сработал по current_close: сигнал {signal.id} ({pair_symbol}), "
                                        f"entry={entry_price:.6f}, close={current_close:.6f}, "
                                        f"move={close_adverse_move:.6f}, threshold={stop_loss_threshold:.6f}, "
                                        f"время свечи={last_candle_time.isoformat()}, время входа={signal_time.isoformat()}"
                                    )
                                    fix_time = datetime.now(timezone.utc)
                                    signal.result_fixed = -0.5
                                    signal.result_fixed_at = fix_time
                                    signal.exit_price = current_close
                                    signal.exit_timestamp = fix_time
                                    signal.exit_reason = 'STOP_LOSS'
                                    signal.status = 'CLOSED'
                                    signal.max_profit = max_favorable
                                    signal.max_drawdown = max_adverse
                                    signal.updated_at = fix_time
                                    updated_count += 1
                                    closed_count += 1
                                    continue
                            
                            # Логируем движения для диагностики
                            logger.debug(
                                f"  Движения (свеча после входа): favorable={current_favorable_move:.6f} (нужно >= {profit_threshold:.6f}), "
                                f"adverse={current_adverse_move:.6f} (нужно >= {stop_loss_threshold:.6f}), "
                                f"время свечи={last_candle_time.isoformat()}"
                            )
                            
                            # Проверяем срабатывание TP/SL на текущей цене
                            if current_favorable_move >= profit_threshold:
                                logger.info(
                                    f"✅ TP сработал по current_high/low: сигнал {signal.id} ({pair_symbol} {signal.signal_type}), "
                                    f"entry={entry_price:.6f}, exit={exit_price_tp_current:.6f}, "
                                    f"move={current_favorable_move:.6f}, threshold={profit_threshold:.6f}, "
                                    f"время свечи={last_candle_time.isoformat()}, время входа={signal_time.isoformat()}"
                                )
                                fix_time = datetime.now(timezone.utc)
                                signal.result_fixed = 1.5
                                signal.result_fixed_at = fix_time
                                signal.exit_price = exit_price_tp_current
                                signal.exit_timestamp = fix_time
                                signal.exit_reason = 'TAKE_PROFIT'
                                signal.status = 'CLOSED'
                                signal.max_profit = max_favorable
                                signal.max_drawdown = max_adverse
                                signal.updated_at = fix_time
                                updated_count += 1
                                closed_count += 1
                                continue  # Переходим к следующему сигналу
                            elif current_adverse_move >= stop_loss_threshold:
                                logger.info(
                                    f"🔴 SL сработал по current_high/low: сигнал {signal.id} ({pair_symbol} {signal.signal_type}), "
                                    f"entry={entry_price:.6f}, exit={exit_price_sl_current:.6f}, "
                                    f"move={current_adverse_move:.6f}, threshold={stop_loss_threshold:.6f}, "
                                    f"время свечи={last_candle_time.isoformat()}, время входа={signal_time.isoformat()}"
                                )
                                fix_time = datetime.now(timezone.utc)
                                signal.result_fixed = -0.5
                                signal.result_fixed_at = fix_time
                                signal.exit_price = exit_price_sl_current
                                signal.exit_timestamp = fix_time
                                signal.exit_reason = 'STOP_LOSS'
                                signal.status = 'CLOSED'
                                signal.max_profit = max_favorable
                                signal.max_drawdown = max_adverse
                                signal.updated_at = fix_time
                                updated_count += 1
                                closed_count += 1
                                continue  # Переходим к следующему сигналу
                    
                    # Проверяем закрытые свечи (для истории и обновления max значений)
                    for candle in sorted_candles:
                        candle_time = datetime.fromtimestamp(candle['timestamp'] / 1000, tz=timezone.utc)
                        if signal.signal_type == 'LONG':
                            favorable_move = candle['high'] - entry_price
                            adverse_move = entry_price - candle['low']
                            exit_price_tp = candle['high']
                            exit_price_sl = candle['low']
                        else:
                            favorable_move = entry_price - candle['low']
                            adverse_move = candle['high'] - entry_price
                            exit_price_tp = candle['low']
                            exit_price_sl = candle['high']
                        
                        max_favorable = max(max_favorable, favorable_move)
                        max_adverse = max(max_adverse, adverse_move)
                        
                        if signal.result_fixed is None:
                            if favorable_move >= profit_threshold:
                                logger.info(
                                    f"✅ TP сработал по закрытой свече: сигнал {signal.id} ({pair_symbol} {signal.signal_type}), "
                                    f"entry={entry_price:.6f}, exit={exit_price_tp:.6f}, "
                                    f"move={favorable_move:.6f}, threshold={profit_threshold:.6f}"
                                )
                                fix_time = datetime.now(timezone.utc)
                                signal.result_fixed = 1.5
                                signal.result_fixed_at = fix_time
                                signal.exit_price = exit_price_tp
                                signal.exit_timestamp = fix_time
                                signal.exit_reason = 'TAKE_PROFIT'
                                signal.status = 'CLOSED'
                                closed_count += 1
                                break
                            if adverse_move >= stop_loss_threshold:
                                logger.info(
                                    f"🔴 SL сработал по закрытой свече: сигнал {signal.id} ({pair_symbol} {signal.signal_type}), "
                                    f"entry={entry_price:.6f}, exit={exit_price_sl:.6f}, "
                                    f"move={adverse_move:.6f}, threshold={stop_loss_threshold:.6f}"
                                )
                                fix_time = datetime.now(timezone.utc)
                                signal.result_fixed = -0.5
                                signal.result_fixed_at = fix_time
                                signal.exit_price = exit_price_sl
                                signal.exit_timestamp = fix_time
                                signal.exit_reason = 'STOP_LOSS'
                                signal.status = 'CLOSED'
                                closed_count += 1
                                break
                    
                    signal.max_profit = max_favorable
                    signal.max_drawdown = max_adverse
                    
                    if signal.result_fixed is not None:
                        pnl_percent = signal.result_fixed
                        signal.pnl_percent = pnl_percent
                        signal.pnl = entry_price * (pnl_percent / 100)
                    else:
                        if signal.signal_type == 'LONG':
                            pnl = current_price - entry_price
                            pnl_percent = (pnl / entry_price) * 100
                        else:
                            pnl = entry_price - current_price
                            pnl_percent = (pnl / entry_price) * 100
                        signal.pnl = pnl
                        signal.pnl_percent = pnl_percent
                        signal.status = 'ACTIVE'
                        signal.exit_price = None
                        signal.exit_timestamp = None
                        signal.exit_reason = None
                    
                    signal.updated_at = datetime.now(timezone.utc)
                    updated_count += 1
                    
                    # Логируем, если сделка все еще активна, но близка к порогам
                    if signal.result_fixed is None:
                        current_pnl_pct = signal.pnl_percent if signal.pnl_percent else 0.0
                        if abs(current_pnl_pct) >= 0.4:  # Близко к порогам (0.4% из 0.5% или 1.4% из 1.5%)
                            logger.debug(
                                f"⚠️ Сигнал {signal.id} ({pair_symbol} {signal.signal_type}) близок к порогу: "
                                f"P&L={current_pnl_pct:.2f}%, entry={entry_price:.6f}, current={current_price:.6f}, "
                                f"max_favorable={max_favorable:.6f}, max_adverse={max_adverse:.6f}"
                            )
                
                except Exception as signal_error:
                    logger.error(f"❌ Ошибка обработки сигнала {signal.id}: {signal_error}", exc_info=True)
                    continue
            
            session.commit()
            cache.delete('signals:all')
            cache.set('signals:last_update', datetime.now().isoformat(), ttl=3600)
            
            logger.info(
                f"✅ Обновление P&L завершено: обработано {updated_count} сигналов, "
                f"закрыто {closed_count} сделок"
            )
            return {
                'status': 'success',
                'updated_count': updated_count,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Ошибка обновления P&L: {e}")
            import traceback
            traceback.print_exc()
            session.rollback()
            return {'status': 'error', 'error': str(e)}
        
        finally:
            session.close()
            if loop:
                try:
                    loop.close()
                except Exception:
                    pass
        
    except Exception as e:
        logger.error(f"Критическая ошибка в задаче обновления сигналов: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name='tasks.signals_tasks.update_signals_pnl', queue='signals')
def update_signals_pnl():
    """Celery-задача для обновления P&L."""
    return _update_signals_pnl_internal()


def update_signals_pnl_sync():
    """Синхронный запуск обновления P&L (fallback без Celery)."""
    return _update_signals_pnl_internal()


@celery_app.task(name='tasks.signals_tasks.process_new_signal', queue='signals')
def process_new_signal(signal_data: dict):
    """Обрабатывает новый сигнал"""
    try:
        logger.info(f"Обработка нового сигнала: {signal_data.get('pair')}")
        
        if not init_database():
            return None
        
        if database.SessionLocal is None:
            logger.error("Session factory is not initialized")
            return {'status': 'error', 'error': 'DB session factory unavailable'}
        session = database.SessionLocal()
        
        try:
            pair_symbol = signal_data.get('pair')
            if not pair_symbol:
                return {'status': 'error', 'error': 'Pair not specified'}
            
            pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
            if not pair:
                return {'status': 'error', 'error': f'Pair {pair_symbol} not found'}
            
            # КРИТИЧЕСКАЯ ПРОВЕРКА: проверяем, не существует ли уже сигнал для этого уровня
            level_price = float(signal_data.get('level_price', 0))
            if level_price > 0:
                # Используем строгую толерантность 0.1% для проверки дубликатов
                price_tolerance = level_price * 0.001  # 0.1%
                
                existing_signal = session.query(Signal).filter(
                    Signal.pair_id == pair.id,
                    func.abs(Signal.level_price - level_price) < price_tolerance
                ).order_by(Signal.timestamp.desc()).first()
                
                if existing_signal:
                    logger.warning(f"⚠️ Сигнал для уровня {level_price} уже существует (ID: {existing_signal.id}, создан: {existing_signal.timestamp}). Пропускаем создание дубликата.")
                    return {
                        'status': 'duplicate',
                        'message': f'Signal already exists for level {level_price}',
                        'existing_signal_id': existing_signal.id
                    }
            
            timestamp_value = signal_data.get('timestamp')
            if isinstance(timestamp_value, str):
                try:
                    timestamp_value = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
                except Exception:
                    timestamp_value = datetime.now()
            else:
                timestamp_value = timestamp_value or datetime.now()

            exit_timestamp = signal_data.get('exit_timestamp')
            if isinstance(exit_timestamp, str):
                try:
                    exit_timestamp = datetime.fromisoformat(exit_timestamp.replace('Z', '+00:00'))
                except Exception:
                    exit_timestamp = None

            # Создаем сигнал только если дубликата нет
            signal = Signal(
                pair_id=pair.id,
                signal_type=signal_data.get('signal_type', 'LONG'),
                level_price=level_price,
                entry_price=signal_data.get('entry_price'),
                current_price=signal_data.get('current_price'),
                stop_loss=signal_data.get('stop_loss'),
                timestamp=timestamp_value,
                trend_1h=signal_data.get('1h_trend'),
                level_type=signal_data.get('level_type'),
                test_count=signal_data.get('test_count', 1),
                status=signal_data.get('status', 'ACTIVE'),
                level_timeframe=signal_data.get('timeframe'),
                historical_touches=signal_data.get('historical_touches'),
                live_test_count=signal_data.get('live_test_count'),
                level_score=signal_data.get('level_score') or signal_data.get('score'),
                distance_percent=signal_data.get('distance_percent'),
                exit_price=signal_data.get('exit_price'),
                exit_timestamp=exit_timestamp,
                exit_reason=signal_data.get('exit_reason'),
                notes=signal_data.get('notes'),
                meta_data=signal_data
            )
            
            session.add(signal)
            session.commit()
            
            # Очищаем кэш сигналов
            cache.delete('signals:all')
            cache.delete(f'signals:pair:{pair_symbol}')
            
            logger.info(f"Сигнал создан: ID {signal.id}")
            
            # Планируем live-торговлю для этого сигнала
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
            
            return {
                'status': 'success',
                'signal_id': signal.id,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки сигнала: {e}")
            session.rollback()
            return {'status': 'error', 'error': str(e)}
        
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Критическая ошибка в задаче обработки сигнала: {e}")
        return {'status': 'error', 'error': str(e)}

