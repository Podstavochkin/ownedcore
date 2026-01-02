"""
CryptoProject v0.01 - AJAX сервер для веб-дашборда
Автор: CryptoProject v0.01
Описание: Современный AJAX-сервер для получения данных по криптовалютам с биржи Binance
и обслуживания веб-дашборда с профессиональными графиками Lightweight Charts.
Поддерживает множественные торговые пары, технический анализ и обновление в реальном времени.
"""

import asyncio
import aiohttp
from aiohttp import web
import json
from datetime import datetime, timedelta
import logging
import os
from analysis_engine import analysis_engine, TRADING_PAIRS
from signal_manager import signal_manager, SignalManager

# Создаем директорию для логов перед настройкой логирования
os.makedirs("logs", exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/server.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Глобальные переменные для кэширования
analysis_cache = {}
last_analysis_time = None
analysis_running = False

async def main_analysis_loop():
    """Основной цикл анализа всех торговых пар"""
    global analysis_cache, last_analysis_time, analysis_running
    
    while True:
        try:
            if analysis_running:
                await asyncio.sleep(10)  # Ждем если анализ уже запущен
                continue
            
            analysis_running = True
            logger.info("Начинаем анализ всех торговых пар...")
            
            # Анализируем все пары
            results = await analysis_engine.analyze_all_pairs()
            
            # Обновляем кэш
            analysis_cache = results
            last_analysis_time = datetime.now()
            
            logger.info(f"Анализ завершен. Обработано пар: {results['pairs_analyzed']}, Сигналов: {results['total_signals']}")
            
            analysis_running = False
            
            # Ждем 1 минуту до следующего анализа
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка в основном цикле анализа: {e}")
            analysis_running = False
            await asyncio.sleep(30)

# API эндпоинты
async def get_pairs_status(request):
    """Статус всех торговых пар"""
    try:
        # Загружаем активные уровни
        active_levels = signal_manager.load_active_levels()
        
        if analysis_cache:
            # Добавляем активные уровни к данным анализа
            response_data = analysis_cache.copy()
            response_data['active_levels'] = active_levels
            return web.json_response(response_data)
        else:
            return web.json_response({
                'timestamp': datetime.now().isoformat(),
                'pairs_analyzed': 0,
                'total_signals': 0,
                'results': {},
                'active_levels': active_levels,
                'message': 'Анализ еще не запущен'
            })
    except Exception as e:
        logger.error(f"Ошибка получения статуса пар: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def get_signals(request):
    """Получение всех сигналов с статистикой"""
    try:
        # Загружаем все сигналы
        all_signals = signal_manager.load_recent_signals(limit=1000)
        
        # Получаем текущие цены для расчета результатов
        current_prices = {}
        try:
            # Получаем текущие цены для всех пар из кэша анализа
            if analysis_cache and 'results' in analysis_cache:
                for pair, data in analysis_cache['results'].items():
                    if 'current_price' in data:
                        current_prices[pair] = data['current_price']
        except Exception as e:
            logger.warning(f"Не удалось получить текущие цены: {e}")
        
        # Подсчитываем статистику в реальном времени
        now = datetime.now()
        # Делаем все даты naive (без временной зоны)
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
        
        for signal in all_signals:
            # Подсчет по типу сигнала
            signal_type = (signal.get('signal_type') or signal.get('type') or '').upper()
            if signal_type == 'LONG':
                summary['long_count'] += 1
            elif signal_type == 'SHORT':
                summary['short_count'] += 1
            
            # Анализируем движение цены по свечам от точки входа
            result, max_favorable, max_adverse = await analyze_signal_price_movement(signal)
            signal['calculated_result'] = result
            signal['max_favorable_move'] = max_favorable
            signal['max_adverse_move'] = max_adverse
            
            # Добавляем текущую цену для отображения
            pair = signal.get('pair', '')
            current_price = current_prices.get(pair, 0)
            signal['current_price'] = current_price
            
            # Подсчет результатов
            if result == 1.5:
                summary['profit_count'] += 1
            elif result == -0.5:
                summary['loss_count'] += 1
            else:
                summary['in_progress_count'] += 1
            
            # Подсчет по периодам и результатам
            try:
                signal_time_str = signal.get('timestamp', '')
                if signal_time_str:
                    # Парсим время и делаем его naive (без временной зоны)
                    if signal_time_str.endswith('Z'):
                        signal_time_str = signal_time_str[:-1]  # Убираем 'Z'
                    
                    signal_time = datetime.fromisoformat(signal_time_str)
                    
                    # Если время имеет временную зону, конвертируем в naive
                    if signal_time.tzinfo is not None:
                        signal_time = signal_time.replace(tzinfo=None)
                    
                    if signal_time >= today:
                        summary['today_count'] += 1
                        if result == 1.5:
                            summary['today_result'] += 1.5
                        elif result == -0.5:
                            summary['today_result'] -= 0.5
                    if signal_time >= week_ago:
                        summary['week_count'] += 1
                        if result == 1.5:
                            summary['week_result'] += 1.5
                        elif result == -0.5:
                            summary['week_result'] -= 0.5
                    if signal_time >= month_ago:
                        summary['month_count'] += 1
                        if result == 1.5:
                            summary['month_result'] += 1.5
                        elif result == -0.5:
                            summary['month_result'] -= 0.5
            except Exception as e:
                logger.warning(f"Ошибка парсинга времени сигнала: {e}, timestamp: {signal.get('timestamp')}")
                continue
        
        # Добавляем отладочную информацию
        logger.info(f"Статистика сигналов: {summary}")
        
        return web.json_response({
            'success': True,
            'signals': all_signals,
            'summary': summary
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения сигналов: {e}")
        return web.json_response({
            'success': False,
            'error': str(e),
            'signals': [],
            'summary': {}
        })

async def get_levels(request):
    """Получение активных уровней"""
    try:
        levels_data = signal_manager.load_active_levels()
        
        return web.json_response({
            'success': True,
            'active_levels': levels_data,  # Возвращаем данные напрямую, без .get('active_levels', {})
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения уровней: {e}")
        return web.json_response({
            'success': False,
            'error': str(e),
            'active_levels': {}
        })

async def get_chart_data(request):
    """Получает исторические данные для графика"""
    try:
        pair = request.query.get('pair')
        timeframe = request.query.get('timeframe', '15m')
        
        if not pair:
            return web.json_response({'error': 'Не указана пара'})
        
        # Получаем данные через analysis_engine
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
        
        return web.json_response({
            'success': True,
            'pair': pair,
            'timeframe': timeframe,
            'candles': candles_list,
            'count': len(candles_list)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения данных графика: {e}")
        return web.json_response({'success': False, 'error': str(e)})

async def get_pair_details(request):
    """Детальная информация по конкретной паре"""
    try:
        pair = request.match_info['pair']
        
        if pair not in TRADING_PAIRS:
            return web.json_response({'error': 'Неизвестная пара'}, status=400)
        
        # Получаем данные пары из кэша
        pair_data = analysis_cache.get('results', {}).get(pair, {})
        
        # Получаем активные уровни для пары
        active_levels = signal_manager.load_active_levels()
        pair_levels = active_levels.get(pair, [])
        
        # Получаем последние сигналы для пары
        all_signals = signal_manager.load_recent_signals(limit=1000)
        pair_signals = [s for s in all_signals if s.get('pair') == pair][:10]
        
        return web.json_response({
            'pair': pair,
            'status': pair_data.get('status', 'unknown'),
            'trend_1h': pair_data.get('trend_1h', 'unknown'),
            'current_price': pair_data.get('current_price'),
            'active_levels': pair_levels,
            'recent_signals': pair_signals,
            'last_update': pair_data.get('last_update'),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Ошибка получения деталей пары: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def get_analysis_status(request):
    """Статус системы анализа"""
    try:
        return web.json_response({
            'analysis_running': analysis_running,
            'last_analysis_time': last_analysis_time.isoformat() if last_analysis_time else None,
            'pairs_total': len(TRADING_PAIRS),
            'pairs_analyzed': analysis_cache.get('pairs_analyzed', 0) if analysis_cache else 0,
            'total_signals': analysis_cache.get('total_signals', 0) if analysis_cache else 0,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Ошибка получения статуса анализа: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def force_analysis(request):
    """Принудительный запуск анализа"""
    try:
        global analysis_running
        
        if analysis_running:
            return web.json_response({
                'message': 'Анализ уже запущен',
                'timestamp': datetime.now().isoformat()
            })
        
        # Запускаем анализ в фоне
        asyncio.create_task(analysis_engine.analyze_all_pairs())
        
        return web.json_response({
            'message': 'Анализ запущен',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Ошибка принудительного анализа: {e}")
        return web.json_response({'error': str(e)}, status=500)

# Статические файлы
async def serve_dashboard(request):
    """Отдает главную страницу дашборда"""
    try:
        with open('dashboard.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text="Dashboard not found", status=404)

# Создание приложения
def create_app():
    app = web.Application()
    
    # Маршруты для страниц
    app.router.add_get('/', lambda r: web.FileResponse('dashboard.html'))
    app.router.add_get('/dashboard', lambda r: web.FileResponse('dashboard.html'))
    app.router.add_get('/signals', lambda r: web.FileResponse('signals.html'))
    
    # API маршруты
    app.router.add_get('/api/pairs-status', get_pairs_status)
    app.router.add_get('/api/levels', get_levels)
    app.router.add_get('/api/signals', get_signals)
    app.router.add_get('/api/chart-data', get_chart_data)
    app.router.add_get('/api/pair/{pair}', get_pair_details)
    app.router.add_get('/api/analysis-status', get_analysis_status)
    app.router.add_post('/api/force-analysis', force_analysis)
    
    return app

def find_free_port(start_port=8080, max_attempts=10):
    """Находит свободный порт начиная с start_port"""
    import socket
    
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                s.close()
                return port
        except OSError:
            continue
    
    # Если не нашли свободный порт, возвращаем 8080 и надеемся на лучшее
    logger.warning(f"Не удалось найти свободный порт в диапазоне {start_port}-{start_port + max_attempts}, используем {start_port}")
    return start_port

async def analyze_signal_price_movement(signal_data):
    """
    Анализирует движение цены по свечам от точки входа до текущего момента
    Возвращает: (result, max_favorable_move, max_adverse_move)
    result: +1.5% если достигнута цель, -0.5% если сработал SL, 0 если в процессе
    """
    try:
        entry_price = signal_data.get('entry_price', 0)
        signal_type = signal_data.get('signal_type', 'UNKNOWN')
        pair = signal_data.get('pair', '')
        entry_timestamp = signal_data.get('timestamp', '')
        
        if entry_price <= 0 or not pair or not entry_timestamp:
            return 0, 0, 0
        
        # Парсим время входа
        try:
            if entry_timestamp.endswith('Z'):
                entry_timestamp = entry_timestamp[:-1]
            entry_time = datetime.fromisoformat(entry_timestamp)
        except Exception as e:
            logger.error(f"Ошибка парсинга времени входа: {e}")
            return 0, 0, 0
        
        # Получаем исторические данные с момента входа
        candles = await analysis_engine.fetch_ohlcv(pair, '15m', 500)  # Берем больше свечей для анализа
        
        if not candles:
            logger.warning(f"Нет исторических данных для {pair}")
            return 0, 0, 0
        
        # Фильтруем свечи, которые идут после времени входа
        entry_timestamp_ms = int(entry_time.timestamp() * 1000)
        relevant_candles = []
        
        for candle in candles:
            if candle['timestamp'] >= entry_timestamp_ms:
                relevant_candles.append(candle)
        
        if not relevant_candles:
            logger.warning(f"Нет свечей после времени входа для {pair}")
            return 0, 0, 0
        
        # Анализируем движение цены по каждой свече
        max_favorable_move = 0
        max_adverse_move = 0
        
        for candle in relevant_candles:
            # Используем high и low свечи для определения максимального движения
            high_price = candle['high']
            low_price = candle['low']
            
            # Вычисляем процентные изменения
            high_change_percent = ((high_price - entry_price) / entry_price) * 100
            low_change_percent = ((low_price - entry_price) / entry_price) * 100
            
            if signal_type == 'LONG':
                # Для LONG: положительное изменение = прибыль, отрицательное = убыток
                # Обновляем максимальное благоприятное движение (high свечи)
                if high_change_percent > max_favorable_move:
                    max_favorable_move = high_change_percent
                
                # Обновляем максимальное неблагоприятное движение (low свечи)
                if low_change_percent < max_adverse_move:
                    max_adverse_move = low_change_percent
                
                # Проверяем условия выхода
                if max_adverse_move <= -0.5:
                    return -0.5, max_favorable_move, max_adverse_move  # Сработал SL
                elif max_favorable_move >= 1.5:
                    return 1.5, max_favorable_move, max_adverse_move   # Достигнута цель
                    
            elif signal_type == 'SHORT':
                # Для SHORT: отрицательное изменение = прибыль, положительное = убыток
                # Обновляем максимальное благоприятное движение (low свечи)
                if low_change_percent < max_favorable_move:
                    max_favorable_move = low_change_percent
                
                # Обновляем максимальное неблагоприятное движение (high свечи)
                if high_change_percent > max_adverse_move:
                    max_adverse_move = high_change_percent
                
                # Проверяем условия выхода
                if max_adverse_move >= 0.5:
                    return -0.5, max_favorable_move, max_adverse_move  # Сработал SL
                elif max_favorable_move <= -1.5:
                    return 1.5, max_favorable_move, max_adverse_move   # Достигнута цель
        
        # Если не достигнуты условия выхода, возвращаем текущие максимальные движения
        return 0, max_favorable_move, max_adverse_move
        
    except Exception as e:
        logger.error(f"Ошибка анализа движения цены сигнала: {e}")
        return 0, 0, 0

def calculate_signal_result(signal_data, current_price):
    """
    Рассчитывает результат сигнала на основе максимального движения цены
    Возвращает: +1.5% если достигнута цель, -0.5% если сработал SL, 0 если в процессе
    """
    try:
        entry_price = signal_data.get('entry_price', 0)
        signal_type = signal_data.get('signal_type', 'UNKNOWN')
        
        if entry_price <= 0 or current_price <= 0:
            return 0, 0, 0
        
        # Вычисляем текущее процентное изменение от цены входа
        current_change_percent = ((current_price - entry_price) / entry_price) * 100
        
        # Получаем максимальное движение (если есть)
        max_favorable_move = signal_data.get('max_favorable_move', 0)
        max_adverse_move = signal_data.get('max_adverse_move', 0)
        
        if signal_type == 'LONG':
            # Для LONG: положительное изменение = прибыль, отрицательное = убыток
            # Обновляем максимальные движения
            if current_change_percent > max_favorable_move:
                max_favorable_move = current_change_percent
            if current_change_percent < max_adverse_move:
                max_adverse_move = current_change_percent
            
            # Определяем результат на основе максимального движения
            if max_adverse_move <= -0.5:
                return -0.5, max_favorable_move, max_adverse_move  # Сработал SL
            elif max_favorable_move >= 1.5:
                return 1.5, max_favorable_move, max_adverse_move   # Достигнута цель
            else:
                return 0, max_favorable_move, max_adverse_move     # В процессе
                
        elif signal_type == 'SHORT':
            # Для SHORT: отрицательное изменение = прибыль, положительное = убыток
            # Обновляем максимальные движения
            if current_change_percent < max_favorable_move:
                max_favorable_move = current_change_percent
            if current_change_percent > max_adverse_move:
                max_adverse_move = current_change_percent
            
            # Определяем результат на основе максимального движения
            if max_adverse_move >= 0.5:
                return -0.5, max_favorable_move, max_adverse_move  # Сработал SL
            elif max_favorable_move <= -1.5:
                return 1.5, max_favorable_move, max_adverse_move   # Достигнута цель
            else:
                return 0, max_favorable_move, max_adverse_move     # В процессе
        
        return 0, 0, 0
    except Exception as e:
        logger.error(f"Ошибка расчета результата сигнала: {e}")
        return 0, 0, 0

async def calculate_all_signal_results():
    """Единоразовый расчет результатов для всех существующих сигналов"""
    try:
        logger.info("Начинаем единоразовый расчет результатов сигналов...")
        
        # Загружаем все сигналы
        all_signals = signal_manager.load_recent_signals(limit=10000)
        
        if not all_signals:
            logger.info("Нет сигналов для расчета результатов")
            return
        
        logger.info(f"Найдено {len(all_signals)} сигналов для расчета результатов")
        
        # Рассчитываем результаты для каждого сигнала
        updated_signals = []
        for signal in all_signals:
            # Анализируем движение цены по свечам от точки входа
            result, max_favorable, max_adverse = await analyze_signal_price_movement(signal)
            signal['calculated_result'] = result
            signal['max_favorable_move'] = max_favorable
            signal['max_adverse_move'] = max_adverse
            updated_signals.append(signal)
        
        # Сохраняем обновленные сигналы
        if updated_signals:
            signal_manager.save_signals_batch(updated_signals)
            logger.info(f"Обновлено {len(updated_signals)} сигналов с результатами")
        
        logger.info("Расчет результатов сигналов завершен")
        
    except Exception as e:
        logger.error(f"Ошибка при расчете результатов сигналов: {e}")

async def main():
    """Основная функция запуска сервера"""
    try:
        # Находим свободный порт
        port = find_free_port(8080)
        logger.info(f"Запуск сервера на порту {port}")
        
        # Создаем приложение
        app = create_app()
        
        # Запускаем единоразовый расчет результатов сигналов
        asyncio.create_task(calculate_all_signal_results())
        
        # Запускаем основной цикл анализа в фоне
        asyncio.create_task(main_analysis_loop())
        
        # Запускаем сервер
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', port)
        await site.start()
        
        logger.info(f"Сервер запущен на http://localhost:{port}")
        logger.info(f"Дашборд: http://localhost:{port}/dashboard")
        logger.info(f"Сигналы: http://localhost:{port}/signals")
        
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
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")