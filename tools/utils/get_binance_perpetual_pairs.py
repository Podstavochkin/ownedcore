import ccxt
import pandas as pd
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_binance_perpetual_pairs():
    """Получает список бессрочных контрактов с Binance Futures"""
    try:
        # Инициализация Binance Futures
        exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'options': {
                'defaultType': 'future',
            }
        })
        
        logger.info("Подключение к Binance Futures...")
        
        # Получаем все рынки
        markets = exchange.load_markets()
        
        # Фильтруем только бессрочные контракты
        perpetual_pairs = []
        
        for symbol, market in markets.items():
            # Бессрочные контракты не имеют expiry
            if (market['type'] == 'future' and 
                market['active'] and 
                market['quote'] == 'USDT' and
                not market['spot'] and
                market.get('expiry') is None and  # Нет даты экспирации
                market.get('strike') is None):  # Не опционы
                
                perpetual_pairs.append({
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
        
        logger.info(f"Найдено {len(perpetual_pairs)} бессрочных контрактов")
        
        # Создаем DataFrame
        df = pd.DataFrame(perpetual_pairs)
        
        # Сортируем по символу
        df = df.sort_values('symbol')
        
        # Сохраняем в CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"binance_perpetual_pairs_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        
        logger.info(f"Список сохранен в файл: {filename}")
        
        # Выводим первые 30 пар для проверки
        logger.info("Первые 30 бессрочных контрактов:")
        for i, row in df.head(30).iterrows():
            logger.info(f"  {row['symbol']} ({row['base']}/{row['quote']})")
        
        # Выводим статистику
        logger.info(f"\nСтатистика:")
        logger.info(f"  Всего бессрочных контрактов: {len(df)}")
        logger.info(f"  USDT пары: {len(df[df['quote'] == 'USDT'])}")
        logger.info(f"  BUSD пары: {len(df[df['quote'] == 'BUSD'])}")
        logger.info(f"  BTC пары: {len(df[df['quote'] == 'BTC'])}")
        
        return df, filename
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных: {e}")
        return None, None

def test_perpetual_symbols():
    """Тестирует бессрочные контракты"""
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
            'AVAXUSDT'
        ]
        
        logger.info("Тестирование бессрочных контрактов:")
        
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

def get_24h_volume_data():
    """Получает данные об объеме торгов за 24 часа"""
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
        
        logger.info(f"\nТоп-20 пар по объему торгов:")
        for i, data in enumerate(volume_data[:20], 1):
            logger.info(f"  {i:2d}. {data['symbol']}: ${data['price']:.2f} | Объем: ${data['volume_24h']:,.0f}")
        
        return volume_data
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных объема: {e}")
        return []

if __name__ == "__main__":
    logger.info("=== Получение бессрочных контрактов Binance Futures ===")
    
    # Получаем список бессрочных контрактов
    df, filename = get_binance_perpetual_pairs()
    
    if df is not None:
        logger.info(f"\n=== Тестирование символов ===")
        working_symbols = test_perpetual_symbols()
        
        logger.info(f"\n=== Данные об объеме торгов ===")
        volume_data = get_24h_volume_data()
        
        # Сохраняем топ-50 пар по объему
        if volume_data:
            top_pairs_df = pd.DataFrame(volume_data[:50])
            top_filename = f"top_volume_pairs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            top_pairs_df.to_csv(top_filename, index=False, encoding='utf-8')
            logger.info(f"Топ-50 пар по объему сохранены в: {top_filename}")
    
    logger.info("\n=== Завершено ===") 