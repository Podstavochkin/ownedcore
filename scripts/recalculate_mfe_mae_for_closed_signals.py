#!/usr/bin/env python3
"""
Скрипт для пересчета MFE/MAE и порогов прибыли для закрытых позиций.
Использует исторические данные свечей для точного расчета максимальных движений.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
import json

import core.database as database
from core.models import Signal, TradingPair
from core.trading.bybit_demo_client import bybit_demo_client
import ccxt
import time

def calculate_mfe_mae_from_candles(signal: Signal, candles: list) -> dict:
    """
    Вычисляет MFE/MAE и пороги прибыли на основе исторических свечей.
    Учитывает ТОЧНОЕ время входа и выхода - анализирует только движение между этими точками.
    
    Args:
        signal: Объект Signal
        candles: Список свечей [(timestamp, open, high, low, close, volume), ...]
    
    Returns:
        dict с ключами: max_favorable_move_pct, max_adverse_move_pct,
        first_touch_0_5_pct_ts, first_touch_1_0_pct_ts, first_touch_1_5_pct_ts
    """
    if not signal.entry_price or signal.entry_price <= 0:
        return {}
    
    if not signal.demo_filled_at:
        return {}
    
    entry_price = float(signal.entry_price)
    entry_time = signal.demo_filled_at
    exit_time = signal.exit_timestamp
    
    if not exit_time:
        # Если нет времени выхода, используем текущее время или последнюю свечу
        if candles:
            last_candle_time = candles[-1][0] if isinstance(candles[-1][0], datetime) else datetime.fromtimestamp(candles[-1][0] / 1000, tz=timezone.utc)
            exit_time = last_candle_time
        else:
            exit_time = datetime.now(timezone.utc)
    
    # Фильтруем свечи, которые пересекаются с периодом [entry_time, exit_time]
    relevant_candles = []
    for candle in candles:
        candle_time = candle[0] if isinstance(candle[0], datetime) else datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
        # Определяем длительность свечи (зависит от таймфрейма, но для 1m это 60 секунд)
        # Будем считать, что свеча длится до следующей свечи
        candle_end = candle_time + timedelta(minutes=1)  # Для 1m таймфрейма
        
        # Берем свечу, если она пересекается с периодом [entry_time, exit_time]
        if candle_time <= exit_time and candle_end >= entry_time:
            relevant_candles.append(candle)
    
    if not relevant_candles:
        return {}
    
    # Инициализируем значения
    # MFE начинаем с 0 (или с первого значения, если оно отрицательное)
    # MAE начинаем с 0 (или с первого значения, если оно положительное)
    max_favorable = None
    max_adverse = None
    first_touch_0_5 = None
    first_touch_1_0 = None
    first_touch_1_5 = None
    
    # Проходим по всем свечам в периоде [entry_time, exit_time]
    for candle in relevant_candles:
        candle_time = candle[0] if isinstance(candle[0], datetime) else datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
        candle_end = candle_time + timedelta(minutes=1)
        high = float(candle[2])
        low = float(candle[3])
        close = float(candle[4])
        
        # Вычисляем PnL для разных точек свечи
        if signal.signal_type == "LONG":
            # Для LONG: прибыль = рост цены
            close_pnl = ((close - entry_price) / entry_price) * 100.0
        else:  # SHORT
            # Для SHORT: прибыль = падение цены
            close_pnl = ((entry_price - close) / entry_price) * 100.0
        
        # Обновляем MFE (максимальная прибыль) - используем close для консервативности
        # MFE - это максимальный плюс, который был достигнут между входом и выходом
        if max_favorable is None or close_pnl > max_favorable:
            max_favorable = close_pnl
        
        # Обновляем MAE (максимальный убыток) - используем close для консервативности
        # MAE - это максимальный минус, который был достигнут между входом и выходом
        if max_adverse is None or close_pnl < max_adverse:
            max_adverse = close_pnl
        
        # Проверяем пороги прибыли (используем close для определения момента достижения)
        # Только если свеча полностью в периоде [entry_time, exit_time]
        if candle_time >= entry_time and candle_end <= exit_time:
            if close_pnl >= 0.5 and first_touch_0_5 is None:
                first_touch_0_5 = candle_time.isoformat()
            if close_pnl >= 1.0 and first_touch_1_0 is None:
                first_touch_1_0 = candle_time.isoformat()
            if close_pnl >= 1.5 and first_touch_1_5 is None:
                first_touch_1_5 = candle_time.isoformat()
    
    # Если не было ни одного значения, возвращаем пустой результат
    if max_favorable is None or max_adverse is None:
        return {}
    
    # ВАЖНО: Учитываем цену выхода для финального MAE
    # MAE должен всегда отражать максимальный убыток, включая цену выхода
    if signal.exit_price and signal.exit_price > 0:
        exit_price = float(signal.exit_price)
        if signal.signal_type == "LONG":
            exit_pnl = ((exit_price - entry_price) / entry_price) * 100.0
        else:  # SHORT
            exit_pnl = ((entry_price - exit_price) / entry_price) * 100.0
        
        # MAE - это минимальное значение PnL между входом и выходом
        # Всегда учитываем цену выхода
        if max_adverse is None or exit_pnl < max_adverse:
            max_adverse = exit_pnl
        
        # ОСОБЫЙ СЛУЧАЙ: Если сделка закрылась очень быстро (менее 1 минуты) и в убытке,
        # то нужно проверить, была ли цена реально в плюсе между входом и выходом
        # Для таких быстрых сделок close свечи может не отражать реальное движение
        # (close свечи - это цена в конце свечи, а не в момент выхода)
        duration_seconds = (exit_time - entry_time).total_seconds()
        if duration_seconds < 60 and exit_pnl < 0:
            # Консервативный подход: если сделка закрылась быстро в минусе,
            # считаем, что цена не успела зафиксироваться в плюсе, MFE = 0
            # Это правильно, так как мы не можем точно знать, была ли цена в плюсе
            # между входом и выходом на 1m таймфрейме
            max_favorable = 0.0
    
    result = {
        "max_favorable_move_pct": round(max_favorable, 3),
        "max_adverse_move_pct": round(max_adverse, 3),
    }
    
    if first_touch_0_5:
        result["first_touch_0_5_pct_ts"] = first_touch_0_5
    if first_touch_1_0:
        result["first_touch_1_0_pct_ts"] = first_touch_1_0
    if first_touch_1_5:
        result["first_touch_1_5_pct_ts"] = first_touch_1_5
    
    return result


def main():
    """Основная функция скрипта"""
    print("🔄 Пересчет MFE/MAE для закрытых позиций...")
    
    assert database.init_database() and database.SessionLocal is not None
    session = database.SessionLocal()
    
    # Инициализируем exchange для получения исторических данных
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'spot',
        }
    })
    
    try:
        # Находим все закрытые сигналы с ордерами, у которых нет MFE/MAE данных
        closed_signals = (
            session.query(Signal)
            .join(TradingPair)
            .filter(
                Signal.status == "CLOSED",
                Signal.demo_order_id.isnot(None),
                Signal.entry_price.isnot(None),
                Signal.demo_filled_at.isnot(None),
            )
            .order_by(Signal.timestamp.desc())
            .all()
        )
        
        print(f"Найдено {len(closed_signals)} закрытых сигналов с ордерами")
        
        updated_count = 0
        skipped_count = 0
        
        for signal in closed_signals:
            if not signal.pair:
                skipped_count += 1
                continue
            
            # Пересчитываем все сигналы заново с новой логикой (по close)
            # Удаляем старые данные MFE/MAE для пересчета
            meta = signal.meta_data or {}
            # Пропускаем только если данные уже пересчитаны сегодня (опционально)
            # Для полного пересчета - убираем эту проверку
            
            try:
                # Получаем исторические свечи
                symbol = signal.pair.symbol
                # Используем 1m таймфрейм для точности расчета между точками входа и выхода
                timeframe = "1m"
                
                # Определяем период: от входа до выхода (или до текущего момента, если выхода нет)
                start_time = signal.demo_filled_at
                end_time = signal.exit_timestamp or datetime.now(timezone.utc)
                
                # Добавляем небольшой запас до и после для получения полных свечей
                start_ts = int((start_time - timedelta(minutes=5)).timestamp() * 1000)
                end_ts = int((end_time + timedelta(minutes=5)).timestamp() * 1000)
                
                print(f"\n📊 Обработка сигнала ID={signal.id} ({symbol} {timeframe})")
                print(f"   Вход: {signal.demo_filled_at}, Выход: {signal.exit_timestamp}")
                
                # Загружаем свечи через ccxt
                all_candles = []
                current_ts = start_ts
                
                while current_ts < end_ts:
                    try:
                        batch = exchange.fetch_ohlcv(symbol, timeframe, since=current_ts, limit=1000)
                        if not batch:
                            break
                        all_candles.extend(batch)
                        current_ts = batch[-1][0] + 1
                        time.sleep(0.1)  # Rate limit
                    except Exception as e:
                        print(f"   ⚠️  Ошибка загрузки свечей: {e}")
                        break
                
                # Конвертируем формат свечей: [timestamp_ms, open, high, low, close, volume] -> (datetime, open, high, low, close, volume)
                candles = []
                for candle in all_candles:
                    candle_time = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
                    candles.append((candle_time, candle[1], candle[2], candle[3], candle[4], candle[5]))
                
                if not candles:
                    print(f"   ⚠️  Не удалось загрузить свечи для {symbol}")
                    skipped_count += 1
                    continue
                
                # Вычисляем MFE/MAE
                mfe_mae_data = calculate_mfe_mae_from_candles(signal, candles)
                
                if not mfe_mae_data:
                    print(f"   ⚠️  Не удалось вычислить MFE/MAE")
                    skipped_count += 1
                    continue
                
                # Обновляем meta_data
                if not signal.meta_data:
                    signal.meta_data = {}
                
                signal.meta_data.update(mfe_mae_data)
                # SQLAlchemy не отслеживает изменения в JSON полях автоматически
                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(signal, "meta_data")
                session.flush()
                
                print(f"   ✅ Обновлено:")
                print(f"      MFE: {mfe_mae_data.get('max_favorable_move_pct', 0):.3f}%")
                print(f"      MAE: {mfe_mae_data.get('max_adverse_move_pct', 0):.3f}%")
                if mfe_mae_data.get('first_touch_0_5_pct_ts'):
                    print(f"      Порог +0.5%: {mfe_mae_data['first_touch_0_5_pct_ts']}")
                if mfe_mae_data.get('first_touch_1_0_pct_ts'):
                    print(f"      Порог +1.0%: {mfe_mae_data['first_touch_1_0_pct_ts']}")
                if mfe_mae_data.get('first_touch_1_5_pct_ts'):
                    print(f"      Порог +1.5%: {mfe_mae_data['first_touch_1_5_pct_ts']}")
                
                updated_count += 1
                
            except Exception as err:
                print(f"   ❌ Ошибка обработки сигнала ID={signal.id}: {err}")
                skipped_count += 1
                continue
        
        session.commit()
        
        print(f"\n✅ Готово!")
        print(f"   Обновлено: {updated_count}")
        print(f"   Пропущено: {skipped_count}")
        
    except Exception as err:
        session.rollback()
        print(f"❌ Критическая ошибка: {err}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


if __name__ == "__main__":
    main()

