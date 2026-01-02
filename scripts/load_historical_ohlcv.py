#!/usr/bin/env python3
"""
Скрипт для первоначальной загрузки исторических данных свечей в локальное хранилище
Использование: python3 scripts/load_historical_ohlcv.py [pair] [timeframe] [days]
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.ohlcv_store import ohlcv_store
from core.analysis_engine import TRADING_PAIRS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Загружает исторические данные свечей"""
    init_database()
    
    # Параметры из аргументов командной строки
    pair = sys.argv[1] if len(sys.argv) > 1 else None
    timeframe = sys.argv[2] if len(sys.argv) > 2 else None
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    
    print("=" * 80)
    print("ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ СВЕЧЕЙ")
    print("=" * 80)
    print()
    
    if pair:
        print(f"Пара: {pair}")
    else:
        print(f"Все пары: {len(TRADING_PAIRS)}")
    
    if timeframe:
        print(f"Таймфрейм: {timeframe}")
    else:
        print("Все таймфреймы: 1m, 5m, 15m, 1h, 4h")
    
    print(f"Дней истории: {days}")
    print()
    
    # Определяем пары и таймфреймы для загрузки
    pairs_to_load = [pair] if pair else TRADING_PAIRS
    timeframes_to_load = [timeframe] if timeframe else ['1m', '5m', '15m', '1h', '4h']
    
    print(f"Будет загружено: {len(pairs_to_load)} пар × {len(timeframes_to_load)} таймфреймов")
    print()
    
    # Подтверждение
    if not pair:
        response = input("Продолжить загрузку для всех пар? (yes/no): ")
        if response.lower() != 'yes':
            print("Отменено")
            return
    
    total_loaded = 0
    
    for pair_symbol in pairs_to_load:
        for tf in timeframes_to_load:
            try:
                print(f"📥 Загрузка {pair_symbol} {tf}...")
                
                # Определяем лимит свечей
                candles_per_day = {
                    '1m': 1440,
                    '5m': 288,
                    '15m': 96,
                    '1h': 24,
                    '4h': 6
                }
                limit = candles_per_day.get(tf, 100) * days
                
                # Загружаем данные (ohlcv_store автоматически сохранит в БД)
                candles = ohlcv_store.get_ohlcv(pair_symbol, tf, limit=limit)
                
                if candles:
                    total_loaded += len(candles)
                    print(f"  ✅ Загружено {len(candles)} свечей")
                else:
                    print(f"  ⚠️ Данные не получены")
                    
            except Exception as e:
                print(f"  ❌ Ошибка: {e}")
                continue
    
    print()
    print("=" * 80)
    print(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА")
    print(f"Всего загружено свечей: {total_loaded}")
    print("=" * 80)


if __name__ == '__main__':
    main()

