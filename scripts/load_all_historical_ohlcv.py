#!/usr/bin/env python3
"""
Автоматическая загрузка исторических данных свечей для всех пар
Использование: python3 scripts/load_all_historical_ohlcv.py [days]
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.ohlcv_store import ohlcv_store
from core.analysis_engine import TRADING_PAIRS
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Загружает исторические данные свечей для всех пар"""
    init_database()
    
    # Параметры из аргументов командной строки
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 7  # По умолчанию 7 дней
    
    print("=" * 80)
    print("АВТОМАТИЧЕСКАЯ ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ СВЕЧЕЙ")
    print("=" * 80)
    print()
    print(f"Пары: {len(TRADING_PAIRS)}")
    print(f"Таймфреймы: 15m, 1h, 4h (основные для анализа)")
    print(f"Дней истории: {days}")
    print()
    
    # Фокусируемся на основных таймфреймах для анализа (15m, 1h, 4h)
    # 1m и 5m загружаются автоматически фоновым процессом
    timeframes_to_load = ['15m', '1h', '4h']
    
    print(f"Будет загружено: {len(TRADING_PAIRS)} пар × {len(timeframes_to_load)} таймфреймов")
    print()
    print("Начинаем загрузку...")
    print()
    
    total_loaded = 0
    total_errors = 0
    start_time = time.time()
    
    for idx, pair_symbol in enumerate(TRADING_PAIRS, 1):
        print(f"[{idx}/{len(TRADING_PAIRS)}] {pair_symbol}")
        
        for tf in timeframes_to_load:
            try:
                # Определяем лимит свечей
                candles_per_day = {
                    '15m': 96,   # 24 * 4
                    '1h': 24,    # 24
                    '4h': 6      # 24 / 4
                }
                limit = candles_per_day.get(tf, 100) * days
                
                # Загружаем данные (ohlcv_store автоматически сохранит в БД)
                candles = ohlcv_store.get_ohlcv(pair_symbol, tf, limit=limit)
                
                if candles:
                    total_loaded += len(candles)
                    print(f"  ✅ {tf:5} : {len(candles):5} свечей")
                else:
                    print(f"  ⚠️ {tf:5} : данные не получены")
                    total_errors += 1
                
                # Задержка, чтобы не перегружать API биржи (Binance лимит: 2400 req/min)
                # Для 28 пар × 3 таймфрейма = 84 запроса, нужно минимум 2.1 секунды между запросами
                time.sleep(2.5)
                    
            except Exception as e:
                print(f"  ❌ {tf:5} : ошибка - {e}")
                total_errors += 1
                continue
        
        print()
    
    elapsed_time = time.time() - start_time
    
    print("=" * 80)
    print(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА")
    print(f"Всего загружено свечей: {total_loaded}")
    print(f"Ошибок: {total_errors}")
    print(f"Время выполнения: {elapsed_time:.1f} секунд")
    print("=" * 80)


if __name__ == '__main__':
    main()

