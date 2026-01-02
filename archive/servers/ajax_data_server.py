import asyncio
import json
import time
import logging
from aiohttp import web
import ccxt
import threading

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AjaxDataServer:
    def __init__(self, symbol='BTCUSDT', update_interval=5):
        self.symbol = symbol
        self.update_interval = update_interval
        self.running = False
        self.data_cache = {
            '5m': [],
            '15m': [],
            '1h': [],
            '4h': []
        }
        self.last_update = {}
        
        # Инициализация биржи
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        
        # Создание aiohttp приложения
        self.app = web.Application()
        self.setup_routes()
    
    def setup_routes(self):
        """Настройка маршрутов"""
        self.app.router.add_get('/', self.dashboard_handler)
        self.app.router.add_get('/dashboard', self.dashboard_handler)
        self.app.router.add_get('/api/data', self.data_handler)
        self.app.router.add_get('/api/status', self.status_handler)
    
    async def dashboard_handler(self, request):
        """Главная страница с дашбордом"""
        try:
            with open('dashboard.html', 'r', encoding='utf-8') as f:
                content = f.read()
            return web.Response(text=content, content_type='text/html')
        except FileNotFoundError:
            return web.Response(text="Файл dashboard.html не найден!", content_type='text/html')
    
    async def data_handler(self, request):
        """API endpoint для получения данных"""
        try:
            # Получаем текущие данные
            await self.update_data()

            # Диагностика: выводим тип и пример содержимого кэша
            for tf, val in self.data_cache.items():
                print(f"[DEBUG] {tf}: type={type(val)}, example={str(val)[:200]}")

            # Гарантируем, что возвращаем именно массивы, а не DataFrame/словарь
            data_to_return = {}
            for tf, val in self.data_cache.items():
                if hasattr(val, 'to_dict'):
                    # DataFrame
                    data_to_return[tf] = val.to_dict(orient='records')
                elif isinstance(val, dict):
                    # dict (словарь) — преобразуем в массив значений
                    data_to_return[tf] = list(val.values())
                else:
                    # уже массив
                    data_to_return[tf] = val

            return web.json_response({
                'status': 'success',
                'data': data_to_return,
                'timestamp': time.time()
            })
        except Exception as e:
            logger.error(f"Ошибка получения данных: {e}")
            return web.json_response({
                'status': 'error',
                'message': str(e)
            }, status=500)
    
    async def status_handler(self, request):
        """API endpoint для проверки статуса"""
        return web.json_response({
            'status': 'running',
            'symbol': self.symbol,
            'update_interval': self.update_interval,
            'last_update': self.last_update
        })
    
    def get_candles(self, interval, limit=500):
        """Получение массива последних свечей с Binance"""
        try:
            klines = self.exchange.fetch_ohlcv(
                symbol=self.symbol,
                timeframe=interval,
                limit=limit
            )
            if not klines:
                return []
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
            logger.error(f"Ошибка получения данных: {e}")
            return []
    
    def should_update_data(self, interval):
        """Проверка необходимости обновления данных"""
        if interval not in self.last_update:
            return True
        
        # Проверяем, прошло ли достаточно времени
        time_since_update = time.time() - self.last_update[interval]
        
        # Интервалы обновления для разных таймфреймов
        update_intervals = {
            '5m': 5,    # 5 секунд
            '15m': 15,  # 15 секунд
            '1h': 60,   # 1 минута
            '4h': 300   # 5 минут
        }
        
        return time_since_update >= update_intervals.get(interval, 5)
    
    async def update_data(self):
        """Обновление данных для всех таймфреймов"""
        intervals = ['5m', '15m', '1h', '4h']
        for interval in intervals:
            if self.should_update_data(interval) or not self.data_cache[interval]:
                candles = self.get_candles(interval, limit=500)
                if candles:
                    # Кладём в кэш именно список свечей, а не одну свечу!
                    self.data_cache[interval] = candles
                    self.last_update[interval] = time.time()
                    logger.info(f"Обновлены данные {interval}: {len(candles)} свечей, последняя ${candles[-1]['close']:.2f}")
    
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
        
        logger.info(f"AJAX сервер запущен на http://{host}:{port}")
        logger.info(f"Dashboard: http://{host}:{port}/")
        logger.info(f"API endpoints:")
        logger.info(f"  - GET /api/data - получение данных")
        logger.info(f"  - GET /api/status - статус сервера")
        
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
            try:
                await self.update_data()
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Ошибка в цикле обновления: {e}")
                await asyncio.sleep(self.update_interval)

def start_ajax_server():
    """Функция для запуска сервера в отдельном потоке"""
    server = AjaxDataServer()
    
    def run_server():
        asyncio.run(server.start_server())
    
    # Запускаем сервер в отдельном потоке
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    return server

if __name__ == "__main__":
    # Тестовый запуск сервера
    server = start_ajax_server()
    
    try:
        # Держим основной поток живым
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Сервер остановлен") 