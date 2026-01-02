import asyncio
import json
import time
import logging
from aiohttp import web
import ccxt
import threading
import pandas as pd
from technical_analysis import TechnicalAnalyzer
from royal_levels_strategy import RoyalLevelsStrategy

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleAjaxDataServer:
    def __init__(self, default_symbols=None, update_interval=5):
        if default_symbols is None:
            default_symbols = ['BTC/USDT', 'SOL/USDT']
        self.symbols = default_symbols
        self.update_interval = update_interval
        self.data_cache = {symbol: {'5m': [], '15m': [], '1h': [], '4h': []} for symbol in self.symbols}
        self.last_update = {symbol: {} for symbol in self.symbols}
        self.request_stats = {'total_requests': 0, 'cache_hits': 0, 'api_calls': 0}
        self.running = False
        self.exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
            }
        })
        logging.basicConfig(level=logging.INFO, format='INFO:%(name)s:%(message)s')
        self.logger = logging.getLogger(__name__)
        self.app = web.Application()
        self.setup_routes()
        logger.info(f"API endpoints настроены: /api/data, /api/analysis, /api/status, /api/stats")

    def setup_routes(self):
        """Настройка маршрутов"""
        self.app.router.add_get('/', self.dashboard_handler)
        self.app.router.add_get('/dashboard', self.dashboard_handler)
        self.app.router.add_get('/api/data', self.data_handler)
        self.app.router.add_get('/api/analysis', self.analysis_handler)
        logger.info("API endpoints настроены: /api/data, /api/analysis, /api/status, /api/stats")
        self.app.router.add_get('/api/status', self.status_handler)
        self.app.router.add_get('/api/stats', self.stats_handler)
        # self.app.router.add_get('/api/test', self.test_handler)  # Удалено, т.к. test_handler не реализован
    
    async def dashboard_handler(self, request):
        """Главная страница с дашбордом"""
        try:
            with open('dashboard.html', 'r', encoding='utf-8') as f:
                content = f.read()
            return web.Response(text=content, content_type='text/html')
        except FileNotFoundError:
            return web.Response(text="Файл dashboard.html не найден!", content_type='text/html')
    
    def get_symbol(self, request):
        symbol = request.rel_url.query.get('symbol')
        if symbol and symbol in self.symbols:
            return symbol
        return self.symbols[0]

    async def data_handler(self, request):
        """API endpoint для получения данных"""
        self.request_stats['total_requests'] += 1
        symbol = self.get_symbol(request)
        await self.update_data(symbol)
        data_to_return = {tf: val for tf, val in self.data_cache[symbol].items()}
        return web.json_response({
            'status': 'success',
            'data': data_to_return,
            'timestamp': time.time(),
            'symbol': symbol
        })
    
    async def analysis_handler(self, request):
        """API endpoint для получения технического анализа"""
        try:
            # Получаем данные для анализа
            symbol = self.get_symbol(request)
            await self.update_data(symbol)
            
            # Анализируем каждый таймфрейм
            analysis_results = {}
            for tf in ['5m', '15m', '1h', '4h']:
                if self.data_cache[symbol][tf]:
                    # Конвертируем в DataFrame
                    df = pd.DataFrame(self.data_cache[symbol][tf])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    
                    # Упрощенный технический анализ для тестирования
                    try:
                        # Простой анализ тренда
                        if len(df) >= 2:
                            current_price = df['close'].iloc[-1]
                            prev_price = df['close'].iloc[-2]
                            price_change = ((current_price - prev_price) / prev_price) * 100
                            
                            # Простой RSI
                            gains = df['close'].diff().where(df['close'].diff() > 0, 0).rolling(14).mean()
                            losses = -df['close'].diff().where(df['close'].diff() < 0, 0).rolling(14).mean()
                            rs = gains / losses
                            rsi = 100 - (100 / (1 + rs))
                            current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
                            
                            tech_analysis = {
                                'trend': 'ВОСХОДЯЩИЙ' if price_change > 0 else 'НИСХОДЯЩИЙ',
                                'strength': abs(price_change),
                                'signal': 'ПОКУПКА' if current_rsi < 30 else 'ПРОДАЖА' if current_rsi > 70 else 'НЕТ СИГНАЛА',
                                'confidence': min(abs(price_change) * 10, 100),
                                'score': price_change,
                                'indicators': {
                                    'rsi': float(current_rsi),
                                    'macd': 0.0,
                                    'adx': 25.0,
                                    'volume_ratio': 1.0
                                }
                            }
                        else:
                            tech_analysis = {
                                'trend': 'НЕДОСТАТОЧНО ДАННЫХ',
                                'strength': 0,
                                'signal': 'НЕТ СИГНАЛА',
                                'confidence': 0,
                                'score': 0,
                                'indicators': {'rsi': 50, 'macd': 0, 'adx': 25, 'volume_ratio': 1}
                            }
                    except Exception as e:
                        logger.error(f"Ошибка технического анализа {tf}: {e}")
                        tech_analysis = {
                            'trend': 'ОШИБКА',
                            'strength': 0,
                            'signal': 'ОШИБКА',
                            'confidence': 0,
                            'score': 0,
                            'indicators': {'rsi': 50, 'macd': 0, 'adx': 25, 'volume_ratio': 1}
                        }
                    
                    # Упрощенные королевские уровни (только для 1h)
                    royal_analysis = None
                    if tf == '1h':
                        try:
                            # Простой анализ уровней
                            high_levels = df['high'].rolling(20).max().tail(10)
                            low_levels = df['low'].rolling(20).min().tail(10)
                            current_price = df['close'].iloc[-1]
                            
                            # Находим ближайшие уровни
                            resistance_levels = high_levels[high_levels > current_price].unique()
                            support_levels = low_levels[low_levels < current_price].unique()
                            
                            royal_analysis = {
                                'levels_count': len(resistance_levels) + len(support_levels),
                                'active_signals': 0,
                                'recommendation': 'АНАЛИЗ ВЫПОЛНЕН',
                                'trend': 'АНАЛИЗ',
                                'confidence': 50
                            }
                        except Exception as e:
                            logger.error(f"Ошибка анализа королевских уровней: {e}")
                            royal_analysis = {
                                'levels_count': 0,
                                'active_signals': 0,
                                'recommendation': 'ОШИБКА АНАЛИЗА',
                                'trend': 'ОШИБКА',
                                'confidence': 0
                            }
                    
                    analysis_results[tf] = {
                        'technical': tech_analysis,
                        'royal_levels': royal_analysis,
                        'current_price': df['close'].iloc[-1] if not df.empty else None,
                        'price_change_24h': self.calculate_price_change(df),
                        'volume_24h': df['volume'].sum() if not df.empty else 0
                    }
            
            return web.json_response({
                'status': 'success',
                'analysis': analysis_results,
                'timestamp': time.time(),
                'symbol': symbol
            })
        except Exception as e:
            logger.error(f"Ошибка анализа: {e}")
            return web.json_response({
                'status': 'error',
                'message': str(e)
            }, status=500)
    
    async def status_handler(self, request):
        """API endpoint для проверки статуса"""
        return web.json_response({
            'status': 'running',
            'symbol': self.get_symbol(request),
            'update_interval': self.update_interval,
            'last_update': self.last_update
        })
    
    async def stats_handler(self, request):
        """API endpoint для статистики"""
        return web.json_response({
            'total_requests': self.request_stats['total_requests'],
            'cache_hits': self.request_stats['cache_hits'],
            'api_calls': self.request_stats['api_calls'],
            'cache_hit_rate': f"{self.request_stats['cache_hits'] / max(self.request_stats['total_requests'], 1) * 100:.1f}%"
        })
    
    def get_candles(self, symbol, interval, limit=500):
        """Получение массива последних свечей с Binance"""
        try:
            logger.info(f"Запрос данных для {symbol} {interval}")
            
            # Проверяем доступные рынки
            markets = self.exchange.load_markets()
            
            if symbol not in markets:
                logger.error(f"Символ {symbol} не найден в доступных рынках")
                # Попробуем альтернативные варианты
                alternatives = [symbol.replace('.P', '/USDT:USDT'), symbol.replace('.P', 'USDT'), symbol.replace('.P', '/USDT')]
                for alt in alternatives:
                    if alt in markets:
                        logger.info(f"Найден альтернативный символ: {alt}")
                        symbol = alt
                        break
                else:
                    logger.error("Не удалось найти подходящий символ")
                    return []
            
            klines = self.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=interval,
                limit=limit
            )
            
            if not klines:
                logger.error(f"Нет данных для {symbol} {interval}")
                return []
                
            logger.info(f"Получено {len(klines)} свечей для {symbol} {interval}")
            
            return [
                {
                    'timestamp': k[0],
                    'open': float(k[1]),
                    'high': float(k[2]),
                    'low': float(k[3]),
                    'close': float(k[4]),
                    'volume': float(k[5])
                } for k in klines
            ]
        except Exception as e:
            logger.error(f"Ошибка получения данных для {symbol} {interval}: {e}")
            return []
    
    def should_update_data(self, symbol, interval):
        """Проверка необходимости обновления данных"""
        if interval not in self.last_update[symbol]:
            return True
        
        # Проверяем, прошло ли достаточно времени
        time_since_update = time.time() - self.last_update[symbol][interval]
        
        # Оптимизированные интервалы обновления для разных таймфреймов
        update_intervals = {'5m': 10, '15m': 30, '1h': 120, '4h': 600}
        
        return time_since_update >= update_intervals.get(interval, 10)
    
    async def update_data(self, symbol):
        """Обновление данных для всех таймфреймов"""
        intervals = ['5m', '15m', '1h', '4h']
        updated_count = 0
        
        for interval in intervals:
            if self.should_update_data(symbol, interval) or not self.data_cache[symbol][interval]:
                candles = self.get_candles(symbol, interval, limit=500)
                if candles:
                    # Кладём в кэш именно список свечей, а не одну свечу!
                    self.data_cache[symbol][interval] = candles
                    self.last_update[symbol][interval] = time.time()
                    logger.info(f"Обновлены данные {symbol} {interval}: {len(candles)} свечей, последняя ${candles[-1]['close']:.2f}")
                    updated_count += 1
        
        if updated_count > 0:
            self.request_stats['api_calls'] += 1
        else:
            self.request_stats['cache_hits'] += 1
    
    def calculate_price_change(self, df):
        """Расчет изменения цены за 24 часа"""
        if df is None or df.empty or len(df) < 2:
            return 0
        
        current_price = df['close'].iloc[-1]
        
        # Находим цену 24 часа назад (примерно 288 свечей для 5m)
        lookback = min(288, len(df) - 1)
        if lookback > 0:
            old_price = df['close'].iloc[-lookback]
            change_percent = ((current_price - old_price) / old_price) * 100
            return round(change_percent, 2)
        
        return 0
    
    async def start_server(self, host='localhost', port=8080):
        """Запуск AJAX сервера"""
        self.running = True
        
        # Запускаем цикл обновления данных в отдельной задаче
        update_task = asyncio.create_task(self.data_update_loop())
        
        # Запускаем aiohttp сервер
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"Простой AJAX сервер запущен на http://{host}:{port}")
        logger.info(f"Dashboard: http://{host}:{port}/")
        logger.info(f"Интервалы обновления: 5m(10с), 15m(30с), 1h(2мин), 4h(10мин)")
        logger.info(f"API endpoints:")
        logger.info(f"  - GET /api/data - получение данных")
        logger.info(f"  - GET /api/analysis - технический анализ")
        logger.info(f"  - GET /api/status - статус сервера")
        logger.info(f"  - GET /api/stats - статистика запросов")
        
        try:
            await asyncio.Future()  # Бесконечное ожидание
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки...")
        finally:
            self.running = False
            update_task.cancel()
            await runner.cleanup()
            logger.info("Сервер остановлен")
    
    async def data_update_loop(self):
        """Основной цикл обновления данных"""
        logger.info("Запуск цикла обновления данных...")
        
        while self.running:
            for symbol in self.symbols:
                try:
                    await self.update_data(symbol)
                except Exception as e:
                    logger.error(f"Ошибка обновления данных для {symbol}: {e}")
                await asyncio.sleep(self.update_interval)

def start_simple_ajax_server():
    """Функция для запуска простого сервера в отдельном потоке"""
    server = SimpleAjaxDataServer()
    
    def run_server():
        asyncio.run(server.start_server())
    
    # Запускаем сервер в отдельном потоке
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    return server

if __name__ == "__main__":
    # Тестовый запуск сервера
    server = start_simple_ajax_server()
    
    try:
        # Держим основной поток живым
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Сервер остановлен") 