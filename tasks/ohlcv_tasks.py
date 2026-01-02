"""
Celery задачи для обновления свечных данных (OHLCV) в локальном хранилище
"""

from tasks.celery_app import celery_app
from core.ohlcv_store import ohlcv_store
from core.analysis_engine import TRADING_PAIRS
import logging

logger = logging.getLogger(__name__)


@celery_app.task(name='tasks.ohlcv_tasks.update_current_candles', queue='analysis')
def update_current_candles():
    """
    Обновляет текущие свечи для всех активных пар и таймфреймов
    Вызывается периодически каждую минуту
    """
    try:
        # Таймфреймы для обновления
        timeframes = ['1m', '5m', '15m', '1h', '4h']
        
        logger.info(f"🔄 Обновление текущих свечей для {len(TRADING_PAIRS)} пар...")
        
        # Обновляем свечи
        results = ohlcv_store.update_current_candles(TRADING_PAIRS, timeframes)
        
        # Подсчитываем статистику
        total_updated = 0
        for pair, pair_results in results.items():
            pair_total = sum(pair_results.values())
            total_updated += pair_total
            if pair_total > 0:
                logger.debug(f"  {pair}: обновлено {pair_total} свечей")
        
        logger.info(f"✅ Обновление завершено: {total_updated} свечей обновлено")
        
        return {
            'success': True,
            'total_updated': total_updated,
            'results': results
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка обновления свечей: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


@celery_app.task(name='tasks.ohlcv_tasks.fill_historical_candles', queue='analysis')
def fill_historical_candles(pair: str = None, timeframe: str = None, days: int = 30):
    """
    Заполняет исторические данные свечей для указанной пары и таймфрейма
    Используется для первоначальной загрузки данных
    
    Args:
        pair: Торговая пара (если None - для всех пар)
        timeframe: Таймфрейм (если None - для всех таймфреймов)
        days: Количество дней истории для загрузки
    """
    try:
        pairs_to_process = [pair] if pair else TRADING_PAIRS
        timeframes_to_process = [timeframe] if timeframe else ['1m', '5m', '15m', '1h', '4h']
        
        logger.info(f"📥 Загрузка исторических данных: {len(pairs_to_process)} пар, {len(timeframes_to_process)} таймфреймов, {days} дней")
        
        total_loaded = 0
        
        for pair_symbol in pairs_to_process:
            for tf in timeframes_to_process:
                try:
                    # Определяем лимит свечей в зависимости от таймфрейма и дней
                    candles_per_day = {
                        '1m': 1440,   # 24 * 60
                        '5m': 288,    # 24 * 12
                        '15m': 96,    # 24 * 4
                        '1h': 24,     # 24
                        '4h': 6       # 24 / 4
                    }
                    limit = candles_per_day.get(tf, 100) * days
                    
                    # Запрашиваем данные (ohlcv_store автоматически сохранит в БД)
                    candles = ohlcv_store.get_ohlcv(pair_symbol, tf, limit=limit)
                    
                    if candles:
                        total_loaded += len(candles)
                        logger.info(f"  ✅ {pair_symbol} {tf}: загружено {len(candles)} свечей")
                    else:
                        logger.warning(f"  ⚠️ {pair_symbol} {tf}: данные не получены")
                        
                except Exception as e:
                    logger.error(f"  ❌ Ошибка загрузки {pair_symbol} {tf}: {e}")
                    continue
        
        logger.info(f"✅ Загрузка исторических данных завершена: {total_loaded} свечей")
        
        return {
            'success': True,
            'total_loaded': total_loaded
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки исторических данных: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


@celery_app.task(name='tasks.ohlcv_tasks.check_and_fill_gaps', queue='analysis')
def check_and_fill_gaps():
    """
    Проверяет и заполняет пропуски в данных свечей
    Вызывается периодически каждые 6 часов
    """
    try:
        # Основные таймфреймы для анализа
        timeframes = ['15m', '1h', '4h']
        
        logger.info(f"🔍 Проверка пропусков в данных для {len(TRADING_PAIRS)} пар...")
        
        # Проверяем и заполняем пропуски (максимум 24 часа назад)
        results = ohlcv_store.check_and_fill_gaps(TRADING_PAIRS, timeframes, max_gap_hours=24)
        
        # Подсчитываем статистику
        total_filled = sum(results.values())
        
        if total_filled > 0:
            logger.info(f"✅ Заполнено пропусков: {total_filled} свечей")
        else:
            logger.debug("✅ Пропусков не обнаружено")
        
        return {
            'success': True,
            'total_filled': total_filled,
            'results': results
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки пропусков: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


@celery_app.task(name='tasks.ohlcv_tasks.ensure_historical_data', queue='analysis')
def ensure_historical_data():
    """
    Обеспечивает наличие исторических данных для всех пар
    Проверяет наличие данных за последние 7 дней и загружает недостающие
    Вызывается периодически каждые 12 часов
    """
    try:
        # Основные таймфреймы для анализа
        timeframes = ['15m', '1h', '4h']
        days = 7  # 7 дней истории
        
        logger.info(f"📊 Проверка исторических данных для {len(TRADING_PAIRS)} пар (минимум {days} дней)...")
        
        total_loaded = 0
        
        for pair in TRADING_PAIRS:
            for timeframe in timeframes:
                try:
                    loaded = ohlcv_store.ensure_historical_data(pair, timeframe, days=days)
                    total_loaded += loaded
                    
                    if loaded > 0:
                        logger.info(f"  ✅ {pair} {timeframe}: загружено/дополнено {loaded} свечей")
                    
                    # Небольшая задержка, чтобы не перегружать API
                    import time
                    time.sleep(0.3)
                    
                except Exception as e:
                    logger.warning(f"  ⚠️ Ошибка для {pair} {timeframe}: {e}")
                    continue
        
        if total_loaded > 0:
            logger.info(f"✅ Обеспечение исторических данных завершено: {total_loaded} свечей загружено/дополнено")
        else:
            logger.debug("✅ Все данные актуальны")
        
        return {
            'success': True,
            'total_loaded': total_loaded
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка обеспечения исторических данных: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


@celery_app.task(name='tasks.ohlcv_tasks.reload_historical_data', queue='analysis', bind=True)
def reload_historical_data(self, pair: str = None, timeframe: str = None, days: int = 3, force_update_closed: bool = False):
    """
    Перезагружает исторические данные с биржи для исправления неправильных данных в БД
    
    ВАЖНО: Эта функция используется для исправления ошибок в исторических данных.
    По умолчанию обновляет только незакрытые свечи. Если force_update_closed=True,
    то обновляет ВСЕ свечи, включая закрытые (для исправления ошибок).
    
    Варианты вызова:
    1. Ручной вызов через API при обнаружении проблемы
    2. Автоматически при обнаружении расхождений (TODO: реализовать проверку)
    3. По расписанию (редко, например раз в неделю для последних 3 дней)
    4. При первом запуске после долгого простоя системы
    
    Args:
        pair: Торговая пара (если None - для всех пар)
        timeframe: Таймфрейм (если None - для всех таймфреймов)
        days: Количество дней истории для перезагрузки (по умолчанию 3 дня)
        force_update_closed: Если True, обновляет даже закрытые свечи
    """
    try:
        pairs_to_process = [pair] if pair else TRADING_PAIRS
        # Для полной истории используем все таймфреймы, иначе только основные
        if not timeframe and days >= 30:
            timeframes_to_process = ['15m', '1h', '4h']  # Для полной истории только основные таймфреймы
        else:
            timeframes_to_process = [timeframe] if timeframe else ['15m', '1h', '4h']
        
        total_operations = len(pairs_to_process) * len(timeframes_to_process)
        estimated_time = total_operations * 10  # Примерно 10 секунд на пару+таймфрейм
        
        logger.info(f"🔄 Перезагрузка исторических данных: {len(pairs_to_process)} пар, {len(timeframes_to_process)} таймфреймов, {days} дней")
        logger.info(f"   Всего операций: {total_operations}, примерное время: {estimated_time // 60} минут")
        if force_update_closed:
            logger.warning("⚠️ ВНИМАНИЕ: force_update_closed=True - будут обновлены даже закрытые свечи!")
        
        total_results = {
            'updated': 0,
            'created': 0,
            'skipped': 0,
            'errors': []
        }
        
        current_operation = 0
        for pair_symbol in pairs_to_process:
            for tf in timeframes_to_process:
                current_operation += 1
                try:
                    # Обновляем прогресс
                    progress_percent = int((current_operation / total_operations) * 100)
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current': current_operation,
                            'total': total_operations,
                            'percent': progress_percent,
                            'current_pair': pair_symbol,
                            'current_timeframe': tf,
                            'status': f'Обработка {pair_symbol} {tf} ({current_operation}/{total_operations})'
                        }
                    )
                    
                    result = ohlcv_store.reload_historical_data_from_exchange(
                        pair_symbol,
                        tf,
                        days=days,
                        force_update_closed=force_update_closed
                    )
                    
                    if 'error' in result:
                        error_msg = f"{pair_symbol} {tf}: {result['error']}"
                        logger.error(f"  ❌ {error_msg}")
                        total_results['errors'].append(error_msg)
                    else:
                        total_results['updated'] += result.get('updated', 0)
                        total_results['created'] += result.get('created', 0)
                        total_results['skipped'] += result.get('skipped', 0)
                        total_results['errors'].extend(result.get('errors', []))
                        
                        if result.get('updated', 0) > 0 or result.get('created', 0) > 0:
                            logger.info(f"  ✅ {pair_symbol} {tf}: обновлено {result.get('updated', 0)}, создано {result.get('created', 0)}")
                    
                    # Небольшая задержка, чтобы не перегружать API
                    import time
                    time.sleep(0.5)
                    
                except Exception as e:
                    error_msg = f"{pair_symbol} {tf}: {e}"
                    logger.error(f"  ❌ {error_msg}")
                    total_results['errors'].append(error_msg)
                    continue
        
        logger.info(f"✅ Перезагрузка завершена: обновлено {total_results['updated']}, создано {total_results['created']}, пропущено {total_results['skipped']}")
        if total_results['errors']:
            logger.warning(f"⚠️ Ошибок: {len(total_results['errors'])}")
        
        return {
            'success': True,
            'results': total_results
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка перезагрузки исторических данных: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }

