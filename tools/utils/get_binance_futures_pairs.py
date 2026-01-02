import ccxt
import pandas as pd
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_binance_futures_pairs():
    """Получает список всех торговых пар с Binance Futures"""
    try:
        # Инициализация Binance Futures
        exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'options': {
                'defaultType': 'future',  # Важно! Используем futures
            }
        })
        
        logger.info("Подключение к Binance Futures...")
        
        # Получаем все рынки
        markets = exchange.load_markets()
        
        # Фильтруем только активные фьючерсные пары
        futures_pairs = []
        
        for symbol, market in markets.items():
            if (market['type'] == 'future' and 
                market['active'] and 
                market['quote'] == 'USDT' and  # Только USDT пары
                not market['spot']):  # Исключаем спот
                
                futures_pairs.append({
                    'symbol': symbol,
                    'base': market['base'],
                    'quote': market['quote'],
                    'type': market['type'],
                    'active': market['active'],
                    'spot': market['spot'],
                    'future': market['future'],
                    'linear': market.get('linear', False),
                    'inverse': market.get('inverse', False),
                    'contractSize': market.get('contractSize', 1),
                    'expiry': market.get('expiry'),
                    'strike': market.get('strike'),
                    'optionType': market.get('optionType'),
                    'settleType': market.get('settleType'),
                    'status': market.get('status', 'active')
                })
        
        logger.info(f"Найдено {len(futures_pairs)} активных фьючерсных пар")
        
        # Создаем DataFrame
        df = pd.DataFrame(futures_pairs)
        
        # Сортируем по символу
        df = df.sort_values('symbol')
        
        # Сохраняем в CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"binance_futures_pairs_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        
        logger.info(f"Список сохранен в файл: {filename}")
        
        # Выводим первые 20 пар для проверки
        logger.info("Первые 20 торговых пар:")
        for i, row in df.head(20).iterrows():
            logger.info(f"  {row['symbol']} ({row['base']}/{row['quote']})")
        
        # Выводим статистику
        logger.info(f"\nСтатистика:")
        logger.info(f"  Всего пар: {len(df)}")
        logger.info(f"  USDT пары: {len(df[df['quote'] == 'USDT'])}")
        logger.info(f"  BUSD пары: {len(df[df['quote'] == 'BUSD'])}")
        logger.info(f"  BTC пары: {len(df[df['quote'] == 'BTC'])}")
        
        return df, filename
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных: {e}")
        return None, None

def test_symbol_format():
    """Тестирует различные форматы символов"""
    try:
        exchange = ccxt.binance({
            'options': {
                'defaultType': 'future',
            }
        })
        
        test_symbols = [
            'BTCUSDT',
            'BTC/USDT',
            'BTCUSDT.P',
            'BTCUSDT-PERP',
            'BTCUSDT:USDT',
            'SOLUSDT',
            'SOL/USDT',
            'SOLUSDT.P',
            'SOLUSDT-PERP',
            'SOLUSDT:USDT'
        ]
        
        logger.info("Тестирование форматов символов:")
        
        for symbol in test_symbols:
            try:
                ticker = exchange.fetch_ticker(symbol)
                logger.info(f"  ✓ {symbol}: {ticker['last']}")
            except Exception as e:
                logger.info(f"  ✗ {symbol}: {str(e)[:50]}...")
                
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")

if __name__ == "__main__":
    logger.info("=== Получение списка торговых пар Binance Futures ===")
    
    # Получаем список пар
    df, filename = get_binance_futures_pairs()
    
    if df is not None:
        logger.info(f"\n=== Тестирование форматов символов ===")
        test_symbol_format()
        
        logger.info(f"\n=== Рекомендуемые символы для дашборда ===")
        # Рекомендуем популярные пары
        popular_pairs = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
        
        for pair in popular_pairs:
            if pair in df['symbol'].values:
                logger.info(f"  ✓ {pair} - доступен")
            else:
                logger.info(f"  ✗ {pair} - недоступен")
    
    logger.info("\n=== Завершено ===") 