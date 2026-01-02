import ccxt
import pandas as pd
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_all_binance_pairs():
    """Получает все доступные пары с Binance"""
    try:
        # Инициализация Binance (спот)
        spot_exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
        })
        
        # Инициализация Binance Futures
        futures_exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'options': {
                'defaultType': 'future',
            }
        })
        
        logger.info("Подключение к Binance...")
        
        # Получаем спот рынки
        spot_markets = spot_exchange.load_markets()
        
        # Получаем фьючерс рынки
        futures_markets = futures_exchange.load_markets()
        
        logger.info(f"Спот рынков: {len(spot_markets)}")
        logger.info(f"Фьючерс рынков: {len(futures_markets)}")
        
        # Анализируем доступные пары
        all_pairs = []
        
        # Проверяем спот пары
        for symbol, market in spot_markets.items():
            if (market['active'] and 
                market['quote'] in ['USDT', 'BUSD', 'BTC'] and
                market['type'] == 'spot'):
                
                # Проверяем, есть ли эта пара на фьючерсах
                futures_available = symbol in futures_markets
                
                all_pairs.append({
                    'symbol': symbol,
                    'base': market['base'],
                    'quote': market['quote'],
                    'type': 'spot',
                    'futures_available': futures_available,
                    'active': market['active'],
                    'spot': True,
                    'future': futures_available
                })
        
        # Добавляем фьючерс пары, которых нет в споте
        for symbol, market in futures_markets.items():
            if (market['active'] and 
                market['quote'] in ['USDT', 'BUSD', 'BTC'] and
                market['type'] == 'future' and
                symbol not in [p['symbol'] for p in all_pairs]):
                
                all_pairs.append({
                    'symbol': symbol,
                    'base': market['base'],
                    'quote': market['quote'],
                    'type': 'future',
                    'futures_available': True,
                    'active': market['active'],
                    'spot': False,
                    'future': True
                })
        
        logger.info(f"Всего уникальных пар: {len(all_pairs)}")
        
        # Создаем DataFrame
        df = pd.DataFrame(all_pairs)
        
        # Сортируем по символу
        df = df.sort_values('symbol')
        
        # Сохраняем в CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"binance_all_pairs_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        
        logger.info(f"Список сохранен в файл: {filename}")
        
        # Статистика
        usdt_pairs = df[df['quote'] == 'USDT']
        futures_usdt = usdt_pairs[usdt_pairs['futures_available'] == True]
        
        logger.info(f"\nСтатистика:")
        logger.info(f"  Всего пар: {len(df)}")
        logger.info(f"  USDT пар: {len(usdt_pairs)}")
        logger.info(f"  USDT фьючерсов: {len(futures_usdt)}")
        logger.info(f"  BUSD пар: {len(df[df['quote'] == 'BUSD'])}")
        logger.info(f"  BTC пар: {len(df[df['quote'] == 'BTC'])}")
        
        return df, filename
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных: {e}")
        return None, None

def test_futures_symbols():
    """Тестирует символы на фьючерсах"""
    try:
        exchange = ccxt.binance({
            'options': {
                'defaultType': 'future',
            }
        })
        
        # Популярные пары для тестирования
        test_symbols = [
            'BTCUSDT',
            'ETHUSDT', 
            'SOLUSDT',
            'BNBUSDT',
            'ADAUSDT',
            'XRPUSDT',
            'DOTUSDT',
            'LINKUSDT',
            'MATICUSDT',
            'AVAXUSDT',
            'LTCUSDT',
            'BCHUSDT',
            'EOSUSDT',
            'TRXUSDT',
            'FILUSDT'
        ]
        
        logger.info("Тестирование символов на фьючерсах:")
        
        working_symbols = []
        
        for symbol in test_symbols:
            try:
                ticker = exchange.fetch_ticker(symbol)
                logger.info(f"  ✓ {symbol}: ${ticker['last']:.2f}")
                working_symbols.append(symbol)
            except Exception as e:
                logger.info(f"  ✗ {symbol}: {str(e)[:50]}...")
        
        logger.info(f"\nРаботающие символы ({len(working_symbols)}): {', '.join(working_symbols)}")
        return working_symbols
                
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        return []

def get_top_volume_futures():
    """Получает топ пар по объему на фьючерсах"""
    try:
        exchange = ccxt.binance({
            'options': {
                'defaultType': 'future',
            }
        })
        
        # Получаем тикеры для всех пар
        tickers = exchange.fetch_tickers()
        
        # Фильтруем только USDT пары с объемом
        volume_data = []
        
        for symbol, ticker in tickers.items():
            if (symbol.endswith('USDT') and 
                ticker.get('quoteVolume') and 
                ticker['quoteVolume'] > 1000000):  # Объем > 1M USDT
                
                volume_data.append({
                    'symbol': symbol,
                    'price': ticker['last'],
                    'volume_24h': ticker['quoteVolume'],
                    'change_24h': ticker.get('percentage', 0),
                    'high_24h': ticker.get('high'),
                    'low_24h': ticker.get('low')
                })
        
        # Сортируем по объему
        volume_data.sort(key=lambda x: x['volume_24h'], reverse=True)
        
        logger.info(f"\nТоп-30 пар по объему торгов на фьючерсах:")
        for i, data in enumerate(volume_data[:30], 1):
            logger.info(f"  {i:2d}. {data['symbol']}: ${data['price']:.2f} | Объем: ${data['volume_24h']:,.0f}")
        
        return volume_data
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных объема: {e}")
        return []

def create_recommended_list():
    """Создает рекомендуемый список пар для дашборда"""
    try:
        exchange = ccxt.binance({
            'options': {
                'defaultType': 'future',
            }
        })
        
        # Популярные пары с высоким объемом
        popular_pairs = [
            'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'ADAUSDT',
            'XRPUSDT', 'DOTUSDT', 'LINKUSDT', 'MATICUSDT', 'AVAXUSDT',
            'LTCUSDT', 'BCHUSDT', 'EOSUSDT', 'TRXUSDT', 'FILUSDT',
            'NEARUSDT', 'ATOMUSDT', 'FTMUSDT', 'ALGOUSDT', 'VETUSDT'
        ]
        
        recommended = []
        
        logger.info("Проверка рекомендуемых пар:")
        
        for symbol in popular_pairs:
            try:
                ticker = exchange.fetch_ticker(symbol)
                recommended.append({
                    'symbol': symbol,
                    'price': ticker['last'],
                    'volume_24h': ticker.get('quoteVolume', 0),
                    'change_24h': ticker.get('percentage', 0),
                    'status': 'available'
                })
                logger.info(f"  ✓ {symbol}: ${ticker['last']:.2f}")
            except Exception as e:
                logger.info(f"  ✗ {symbol}: недоступен")
                recommended.append({
                    'symbol': symbol,
                    'price': 0,
                    'volume_24h': 0,
                    'change_24h': 0,
                    'status': 'unavailable'
                })
        
        # Создаем DataFrame и сохраняем
        df = pd.DataFrame(recommended)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"recommended_pairs_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        
        logger.info(f"\nРекомендуемые пары сохранены в: {filename}")
        
        # Выводим только доступные
        available = df[df['status'] == 'available']
        logger.info(f"Доступных пар: {len(available)}")
        logger.info("Символы для дашборда:")
        for _, row in available.iterrows():
            logger.info(f"  {row['symbol']}")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании списка: {e}")
        return None

if __name__ == "__main__":
    logger.info("=== Анализ торговых пар Binance ===")
    
    # Получаем все пары
    df, filename = get_all_binance_pairs()
    
    if df is not None:
        logger.info(f"\n=== Тестирование фьючерсов ===")
        working_symbols = test_futures_symbols()
        
        logger.info(f"\n=== Топ по объему ===")
        volume_data = get_top_volume_futures()
        
        logger.info(f"\n=== Рекомендуемые пары ===")
        recommended_df = create_recommended_list()
    
    logger.info("\n=== Завершено ===") 