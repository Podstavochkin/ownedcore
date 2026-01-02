"""
CryptoProject v0.01 - Многопоточный AJAX сервер
Автор: CryptoProject v0.01
Описание: Многопоточная версия сервера с разделением на независимые потоки:
- Thread 1: Веб-сервер (обработка HTTP запросов)
- Thread 2: Анализ данных (получение данных и поиск уровней)
- Thread 3: Расчет сигналов (обновление P&L и статистики)
- Thread 4: Кэширование (управление общим кэшем)
"""

import asyncio
import aiohttp
from aiohttp import web
import json
from datetime import datetime, timedelta, timezone
import logging
import os
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from analysis_engine import analysis_engine, TRADING_PAIRS
from signal_manager import signal_manager, SignalManager

# Создаем директорию для логов перед настройкой логирования
os.makedirs("logs", exist_ok=True)

# Настройка логирования с ротацией
from core.logging_config import setup_server_logging
setup_server_logging()
logger = logging.getLogger(__name__)

# Глобальные переменные для кэширования (потокобезопасные)
class ThreadSafeCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._analysis_cache = {}
        self._last_analysis_time = None
        self._signals_cache = []
        self._levels_cache = {}
        self._last_signals_update = None
        
    def update_analysis_cache(self, data):
        with self._lock:
            self._analysis_cache = data
            self._last_analysis_time = datetime.now()
            
    def get_analysis_cache(self):
        with self._lock:
            return self._analysis_cache.copy(), self._last_analysis_time
            
    def update_signals_cache(self, data):
        with self._lock:
            self._signals_cache = data
            self._last_signals_update = datetime.now()
            
    def get_signals_cache(self):
        with self._lock:
            return self._signals_cache.copy(), self._last_signals_update
            
    def update_levels_cache(self, data):
        with self._lock:
            self._levels_cache = data
            
    def get_levels_cache(self):
        with self._lock:
            return self._levels_cache.copy()

# Создаем глобальный потокобезопасный кэш
cache = ThreadSafeCache()

# Очереди для межпоточного взаимодействия
analysis_queue = queue.Queue()
signals_queue = queue.Queue()
levels_queue = queue.Queue()

# Флаги состояния потоков
analysis_running = False
signals_running = False

# ============================================================================
# THREAD 1: ВЕБ-СЕРВЕР (обработка HTTP запросов)
# ============================================================================

async def get_pairs_status(request):
    """Статус всех торговых пар"""
    try:
        analysis_data, last_update = cache.get_analysis_cache()
        levels_data = cache.get_levels_cache()
        
        if analysis_data:
            # Добавляем активные уровни к данным анализа
            analysis_data['active_levels'] = levels_data
            return web.json_response(analysis_data)
        else:
            return web.json_response({
                'timestamp': datetime.now().isoformat(),
                'pairs_analyzed': 0,
                'total_signals': 0,
                'results': {},
                'active_levels': levels_data,
                'message': 'Анализ еще не запущен'
            })
    except Exception as e:
        logger.error(f"Ошибка получения статуса пар: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def get_signals(request):
    """Получение всех сигналов с статистикой"""
    try:
        signals_data, last_update = cache.get_signals_cache()
        if signals_data:
            # Добавляем поле success если его нет
            if 'success' not in signals_data:
                signals_data['success'] = True
            return web.json_response(signals_data)
        else:
            # Если кэш пуст, загружаем напрямую
            all_signals = signal_manager.load_recent_signals(limit=1000)
            
            # Подготавливаем базовую статистику
            now = datetime.now()
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            week_ago = today - timedelta(days=7)
            month_ago = today - timedelta(days=30)
            
            summary = {
                'total_count': len(all_signals),
                'long_count': 0,
                'short_count': 0,
                'today_count': 0,
                'week_count': 0,
                'month_count': 0,
                'profit_count': 0,
                'loss_count': 0,
                'in_progress_count': 0,
                'today_result': 0.0,
                'week_result': 0.0,
                'month_result': 0.0
            }
            
            # Подсчитываем статистику
            for signal in all_signals:
                # Подсчет по типу сигнала
                signal_type = (signal.get('signal_type') or signal.get('type') or '').upper()
                if signal_type == 'LONG':
                    summary['long_count'] += 1
                elif signal_type == 'SHORT':
                    summary['short_count'] += 1
                
                # Подсчет по времени
                try:
                    signal_time = datetime.fromisoformat(signal.get('timestamp', '').replace('Z', '+00:00'))
                    if signal_time >= today:
                        summary['today_count'] += 1
                    if signal_time >= week_ago:
                        summary['week_count'] += 1
                    if signal_time >= month_ago:
                        summary['month_count'] += 1
                except:
                    pass
                
                # Подсчет по результату
                result = signal.get('calculated_result', 0)
                if result > 0:
                    summary['profit_count'] += 1
                elif result < 0:
                    summary['loss_count'] += 1
                else:
                    summary['in_progress_count'] += 1
            
            return web.json_response({
                'success': True,
                'signals': all_signals,
                'summary': summary
            })
    except Exception as e:
        logger.error(f"Ошибка получения сигналов: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def get_levels(request):
    """Получение активных уровней"""
    try:
        levels_data = cache.get_levels_cache()
        return web.json_response(levels_data)
    except Exception as e:
        logger.error(f"Ошибка получения уровней: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def get_chart_data(request):
    """Получение данных для графиков"""
    try:
        pair = request.query.get('pair')
        timeframe = request.query.get('timeframe', '15m')
        
        if not pair:
            return web.json_response({'error': 'Не указана пара'})
        
        # Получаем данные напрямую из движка анализа
        candles = await analysis_engine.fetch_ohlcv(pair, timeframe, 200)
        
        if not candles:
            return web.json_response({'error': 'Нет данных для пары'})
        
        # Конвертируем DataFrame в список словарей для JSON
        if hasattr(candles, 'to_dict'):
            # Если это pandas DataFrame
            candles_list = candles.to_dict('records')
        elif isinstance(candles, list):
            # Если это уже список
            candles_list = candles
        else:
            # Если это что-то другое, пробуем конвертировать
            candles_list = list(candles)
        
        # Получаем активные уровни для этой пары
        levels_data = cache.get_levels_cache()
        pair_levels = levels_data.get(pair, [])
        
        return web.json_response({
            'success': True,
            'pair': pair,
            'timeframe': timeframe,
            'candles': candles_list,
            'levels': pair_levels,
            'count': len(candles_list)
        })
    except Exception as e:
        logger.error(f"Ошибка получения данных графика: {e}")
        return web.json_response({'success': False, 'error': str(e)})

async def get_pair_details(request):
    """Получение детальной информации по паре"""
    try:
        pair = request.match_info.get('pair', 'BTC/USDT')
        analysis_data, _ = cache.get_analysis_cache()
        
        if analysis_data and 'results' in analysis_data:
            pair_data = analysis_data['results'].get(pair, {})
            return web.json_response(pair_data)
        else:
            return web.json_response({'error': 'Данные не найдены'}, status=404)
    except Exception as e:
        logger.error(f"Ошибка получения деталей пары: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def get_analysis_status(request):
    """Статус анализа"""
    try:
        return web.json_response({
            'analysis_running': analysis_running,
            'signals_running': signals_running,
            'last_analysis': cache.get_analysis_cache()[1].isoformat() if cache.get_analysis_cache()[1] else None,
            'last_signals_update': cache.get_signals_cache()[1].isoformat() if cache.get_signals_cache()[1] else None
        })
    except Exception as e:
        logger.error(f"Ошибка получения статуса: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def force_analysis(request):
    """Принудительный запуск анализа"""
    try:
        global analysis_running
        if not analysis_running:
            analysis_queue.put('FORCE_ANALYSIS')
            return web.json_response({'message': 'Анализ запущен'})
        else:
            return web.json_response({'message': 'Анализ уже выполняется'})
    except Exception as e:
        logger.error(f"Ошибка принудительного анализа: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def serve_dashboard(request):
    """Сервирование главной страницы"""
    return web.FileResponse('../web/dashboard.html')

async def serve_signals(request):
    """Сервирование страницы сигналов"""
    return web.FileResponse('../web/signals.html')

def create_app():
    """Создание веб-приложения"""
    app = web.Application()
    
    # API эндпоинты
    app.router.add_get('/api/pairs-status', get_pairs_status)
    app.router.add_get('/api/signals', get_signals)
    app.router.add_get('/api/levels', get_levels)
    app.router.add_get('/api/chart-data', get_chart_data)
    app.router.add_get('/api/pair/{pair}', get_pair_details)
    app.router.add_get('/api/analysis-status', get_analysis_status)
    app.router.add_post('/api/force-analysis', force_analysis)
    
    # Статические файлы
    app.router.add_get('/', serve_dashboard)
    app.router.add_get('/dashboard', serve_dashboard)
    app.router.add_get('/signals', serve_signals)
    
    return app

# ============================================================================
# THREAD 2: АНАЛИЗ ДАННЫХ (получение данных и поиск уровней)
# ============================================================================

def analysis_worker():
    """Рабочий поток для анализа данных"""
    global analysis_running
    
    logger.info("Поток анализа данных запущен")
    
    while True:
        try:
            # Проверяем очередь на наличие команд
            try:
                command = analysis_queue.get_nowait()
                if command == 'STOP':
                    logger.info("Получена команда остановки анализа")
                    break
            except queue.Empty:
                pass
            
            if analysis_running:
                time.sleep(10)  # Ждем если анализ уже запущен
                continue
            
            analysis_running = True
            logger.info("Начинаем анализ всех торговых пар...")
            
            # Создаем новый event loop для этого потока
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Анализируем все пары
                results = loop.run_until_complete(analysis_engine.analyze_all_pairs())
                
                # Обновляем кэш анализа
                cache.update_analysis_cache(results)
                
                # Обновляем кэш уровней после анализа
                try:
                    levels_data = signal_manager.load_active_levels()
                    cache.update_levels_cache(levels_data)
                    logger.info(f"Кэш уровней обновлен: {len(levels_data)} пар с уровнями")
                except Exception as e:
                    logger.error(f"Ошибка обновления кэша уровней: {e}")
                
                logger.info(f"Анализ завершен. Обработано пар: {results['pairs_analyzed']}, Сигналов: {results['total_signals']}")
                
            except Exception as e:
                logger.error(f"Ошибка в анализе данных: {e}")
            finally:
                loop.close()
            
            analysis_running = False
            
            # Ждем 1 минуту до следующего анализа
            time.sleep(60)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в потоке анализа: {e}")
            analysis_running = False
            time.sleep(30)

# ============================================================================
# THREAD 3: РАСЧЕТ СИГНАЛОВ (обновление P&L и статистики)
# ============================================================================

async def analyze_signal_price_movement(signal_data):
    """Анализирует движение цены для сигнала и возвращает проценты движения."""
    try:
        pair = signal_data.get('pair')
        entry_price = signal_data.get('entry_price', 0)
        signal_type = (signal_data.get('signal_type') or '').upper()
        timestamp = signal_data.get('timestamp')
        
        if not pair or not entry_price or not timestamp:
            return 0, 0, 0
        
        try:
            entry_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except Exception:
            entry_time = datetime.now()
        
        candles = await analysis_engine.fetch_ohlcv(pair, '15m', 300)
        
        if not candles:
            return 0, 0, 0
        
        entry_index = -1
        for i, candle in enumerate(candles):
            candle_time = datetime.fromtimestamp(candle['timestamp'] / 1000)
            if candle_time >= entry_time:
                entry_index = i
                break
        
        if entry_index == -1:
            return 0, 0, 0
        
        max_favorable_move = 0.0
        max_adverse_move = 0.0
        
        for candle in candles[entry_index:]:
            high_price = candle.get('high', candle.get('close'))
            low_price = candle.get('low', candle.get('close'))
            
            if signal_type == 'LONG':
                favorable = max(0.0, ((high_price - entry_price) / entry_price) * 100)
                adverse = max(0.0, ((entry_price - low_price) / entry_price) * 100)
                max_favorable_move = max(max_favorable_move, favorable)
                max_adverse_move = max(max_adverse_move, adverse)
                
                if max_adverse_move >= 0.5:
                    return -0.5, max_favorable_move, max_adverse_move
                if max_favorable_move >= 1.5:
                    return 1.5, max_favorable_move, max_adverse_move
            
            elif signal_type == 'SHORT':
                favorable = max(0.0, ((entry_price - low_price) / entry_price) * 100)
                adverse = max(0.0, ((high_price - entry_price) / entry_price) * 100)
                max_favorable_move = max(max_favorable_move, favorable)
                max_adverse_move = max(max_adverse_move, adverse)
                
                if max_adverse_move >= 0.5:
                    return -0.5, max_favorable_move, max_adverse_move
                if max_favorable_move >= 1.5:
                    return 1.5, max_favorable_move, max_adverse_move
        
        return 0, max_favorable_move, max_adverse_move
    except Exception as e:
        logger.error(f"Ошибка анализа движения цены: {e}")
        return 0, 0, 0

def signals_worker():
    """Рабочий поток для расчета сигналов"""
    global signals_running
    
    logger.info("Поток расчета сигналов запущен")
    
    while True:
        try:
            # Проверяем очередь на наличие команд
            try:
                command = signals_queue.get_nowait()
                if command == 'STOP':
                    logger.info("Получена команда остановки расчета сигналов")
                    break
            except queue.Empty:
                pass
            
            if signals_running:
                time.sleep(10)  # Ждем если расчет уже запущен
                continue
            
            signals_running = True
            logger.info("Начинаем расчет результатов сигналов...")
            
            # Создаем новый event loop для этого потока
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Загружаем все сигналы
                all_signals = signal_manager.load_recent_signals(limit=10000)
                
                if not all_signals:
                    logger.info("Нет сигналов для расчета результатов")
                    continue
                
                logger.info(f"Найдено {len(all_signals)} сигналов для расчета результатов")
                
                # Рассчитываем результаты для каждого сигнала
                updated_signals = []
                for signal in all_signals:
                    # Анализируем движение цены по свечам от точки входа
                    result, max_favorable, max_adverse = loop.run_until_complete(
                        analyze_signal_price_movement(signal)
                    )
                    signal['calculated_result'] = result
                    signal['max_favorable_move'] = round(max_favorable, 4)
                    signal['max_adverse_move'] = round(max_adverse, 4)
                    signal_type = (signal.get('signal_type') or '').upper()
                    entry_price = signal.get('entry_price') or signal.get('level_price')
                    signal['result'] = None
                    if result > 0:
                        signal['status'] = 'CLOSED'
                        signal['result'] = 'profitable'
                        signal['exit_reason'] = 'TAKE_PROFIT'
                        if entry_price:
                            exit_multiplier = 0.985 if signal_type == 'SHORT' else 1.015
                            signal['exit_price'] = round(entry_price * exit_multiplier, 6)
                        signal['exit_timestamp'] = datetime.now(timezone.utc).isoformat()
                    elif result < 0:
                        signal['status'] = 'CLOSED'
                        signal['result'] = 'losing'
                        signal['exit_reason'] = 'STOP_LOSS'
                        if entry_price:
                            exit_multiplier = 1.005 if signal_type == 'SHORT' else 0.995
                            signal['exit_price'] = round(entry_price * exit_multiplier, 6)
                        signal['exit_timestamp'] = datetime.now(timezone.utc).isoformat()
                    else:
                        signal['status'] = signal.get('status') or 'OPEN'
                        signal['exit_reason'] = None
                        signal['exit_price'] = None
                        signal['exit_timestamp'] = None
                    updated_signals.append(signal)
                
                # Сохраняем обновленные сигналы
                if updated_signals:
                    signal_manager.save_signals_batch(updated_signals)
                    logger.info(f"Обновлено {len(updated_signals)} сигналов с результатами")
                
                # Подготавливаем данные для кэша
                now = datetime.now()
                today = now.replace(hour=0, minute=0, second=0, microsecond=0)
                week_ago = today - timedelta(days=7)
                month_ago = today - timedelta(days=30)
                
                summary = {
                    'total_count': len(updated_signals),
                    'long_count': 0,
                    'short_count': 0,
                    'today_count': 0,
                    'week_count': 0,
                    'month_count': 0,
                    'profit_count': 0,
                    'loss_count': 0,
                    'in_progress_count': 0,
                    'today_result': 0.0,
                    'week_result': 0.0,
                    'month_result': 0.0
                }
                
                for signal in updated_signals:
                    # Подсчет по типу сигнала
                    signal_type = (signal.get('signal_type') or signal.get('type') or '').upper()
                    if signal_type == 'LONG':
                        summary['long_count'] += 1
                    elif signal_type == 'SHORT':
                        summary['short_count'] += 1
                    
                    # Подсчет по времени
                    signal_time = datetime.fromisoformat(signal.get('timestamp', '').replace('Z', '+00:00'))
                    if signal_time >= today:
                        summary['today_count'] += 1
                    if signal_time >= week_ago:
                        summary['week_count'] += 1
                    if signal_time >= month_ago:
                        summary['month_count'] += 1
                    
                    # Подсчет по результату
                    result = signal.get('calculated_result', 0)
                    if result > 0:
                        summary['profit_count'] += 1
                        if signal_time >= today:
                            summary['today_result'] += result
                        if signal_time >= week_ago:
                            summary['week_result'] += result
                        if signal_time >= month_ago:
                            summary['month_result'] += result
                    elif result < 0:
                        summary['loss_count'] += 1
                        if signal_time >= today:
                            summary['today_result'] += result
                        if signal_time >= week_ago:
                            summary['week_result'] += result
                        if signal_time >= month_ago:
                            summary['month_result'] += result
                    else:
                        summary['in_progress_count'] += 1
                
                # Обновляем кэш сигналов
                cache.update_signals_cache({
                    'success': True,
                    'signals': updated_signals,
                    'summary': summary
                })
                
                logger.info("Расчет результатов сигналов завершен")
                
            except Exception as e:
                logger.error(f"Ошибка при расчете результатов сигналов: {e}")
            finally:
                loop.close()
            
            signals_running = False
            
            # Ждем 5 минут до следующего расчета
            time.sleep(300)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в потоке сигналов: {e}")
            signals_running = False
            time.sleep(30)

# ============================================================================
# THREAD 4: КЭШИРОВАНИЕ (управление общим кэшем)
# ============================================================================

def cache_worker():
    """Рабочий поток для управления кэшем"""
    logger.info("Поток кэширования запущен")
    
    while True:
        try:
            # Проверяем очередь на наличие команд
            try:
                command = levels_queue.get_nowait()
                if command == 'STOP':
                    logger.info("Получена команда остановки кэширования")
                    break
            except queue.Empty:
                pass
            
            # Обновляем кэш уровней
            try:
                levels_data = signal_manager.load_active_levels()
                cache.update_levels_cache(levels_data)
                total_levels = sum(len(levels) for levels in levels_data.values())
                logger.info(f"Кэш уровней обновлен: {len(levels_data)} пар, {total_levels} уровней")
            except Exception as e:
                logger.error(f"Ошибка обновления кэша уровней: {e}")
            
            # Ждем 30 секунд до следующего обновления
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в потоке кэширования: {e}")
            time.sleep(30)

# ============================================================================
# ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА
# ============================================================================

def find_free_port(start_port=8080, max_attempts=10):
    """Находит свободный порт"""
    import socket
    
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return port
        except OSError:
            continue
    return start_port

async def main():
    """Основная функция запуска сервера"""
    try:
        # Находим свободный порт
        port = find_free_port(8080)
        logger.info(f"Запуск многопоточного сервера на порту {port}")
        
        # Создаем приложение
        app = create_app()
        
        # Запускаем рабочие потоки
        analysis_thread = threading.Thread(target=analysis_worker, daemon=True)
        signals_thread = threading.Thread(target=signals_worker, daemon=True)
        cache_thread = threading.Thread(target=cache_worker, daemon=True)
        
        analysis_thread.start()
        signals_thread.start()
        cache_thread.start()
        
        logger.info("Все рабочие потоки запущены")
        
        # Запускаем веб-сервер
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', port)
        await site.start()
        
        logger.info(f"Многопоточный сервер запущен на http://localhost:{port}")
        logger.info(f"Дашборд: http://localhost:{port}/dashboard")
        logger.info(f"Сигналы: http://localhost:{port}/signals")
        logger.info("Архитектура потоков:")
        logger.info("- Thread 1: Веб-сервер (HTTP запросы)")
        logger.info("- Thread 2: Анализ данных (поиск уровней)")
        logger.info("- Thread 3: Расчет сигналов (P&L)")
        logger.info("- Thread 4: Кэширование (управление данными)")
        
        # Ждем завершения
        await asyncio.Future()  # Бесконечное ожидание
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена пользователем")
        # Отправляем команды остановки потокам
        analysis_queue.put('STOP')
        signals_queue.put('STOP')
        levels_queue.put('STOP')
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}") 