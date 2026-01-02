#!/usr/bin/env python3
"""
Скрипт для анализа корректности зафиксированных уровней для торговой пары
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database
from core.models import TradingPair, Level
from core.analysis_engine import analysis_engine
import asyncio

async def analyze_pair_levels(pair_symbol: str):
    """Анализирует корректность уровней для пары"""
    
    print(f"\n{'='*60}")
    print(f"АНАЛИЗ УРОВНЕЙ ДЛЯ {pair_symbol}")
    print(f"{'='*60}\n")
    
    # Инициализируем БД
    if not init_database():
        print("❌ Не удалось инициализировать БД")
        return
    
    # Импортируем SessionLocal после инициализации
    from core.database import SessionLocal
    session = SessionLocal()
    
    try:
        # Получаем пару
        pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
        if not pair:
            print(f"❌ Пара {pair_symbol} не найдена в БД")
            return
        
        # Получаем все уровни (и активные, и неактивные)
        all_levels = session.query(Level).filter_by(pair_id=pair.id).all()
        active_levels = [l for l in all_levels if l.is_active]
        inactive_levels = [l for l in all_levels if not l.is_active]
        
        print(f"📊 СТАТИСТИКА:")
        print(f"   Всего уровней в БД: {len(all_levels)}")
        print(f"   Активных: {len(active_levels)}")
        print(f"   Неактивных: {len(inactive_levels)}")
        print()
        
        # Получаем текущие данные
        print("🔍 Получение данных с биржи...")
        candles_15m = await analysis_engine.fetch_ohlcv(pair_symbol, '15m', 200)
        candles_1h = await analysis_engine.fetch_ohlcv(pair_symbol, '1h', 200)
        
        if not candles_15m or not candles_1h:
            print("❌ Не удалось получить данные с биржи")
            return
        
        current_price = candles_15m[-1]['close']
        trend_1h = analysis_engine.determine_trend_1h(candles_1h)
        
        print(f"   Текущая цена: ${current_price:.4f}")
        print(f"   Тренд 1H: {trend_1h}")
        print()
        
        # Анализируем каждый активный уровень
        if active_levels:
            print(f"📈 АНАЛИЗ {len(active_levels)} АКТИВНЫХ УРОВНЕЙ:\n")
            
            for i, level in enumerate(active_levels, 1):
                level_price = level.price
                level_type = level.level_type
                created_at = level.created_at
                
                # Проверяем возраст уровня
                age_hours = (datetime.now(timezone.utc) - created_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                
                # Проверяем пробой
                is_broken = False
                break_reason = ""
                
                if level_type == 'support':
                    # Для поддержки - проверяем, не упала ли цена ниже уровня
                    price_diff = (level_price - current_price) / level_price
                    if price_diff > 0.003:  # 0.3% ниже
                        is_broken = True
                        break_reason = f"Цена ниже уровня на {price_diff*100:.2f}%"
                    
                    # Проверяем исторический пробой
                    for candle in candles_15m[-20:]:
                        if candle['low'] < level_price * 0.997 or candle['close'] < level_price * 0.997:
                            is_broken = True
                            break_reason = "Пробит в истории (последние 20 свечей)"
                            break
                else:  # resistance
                    # Для сопротивления - проверяем, не поднялась ли цена выше уровня
                    price_diff = (current_price - level_price) / level_price
                    if price_diff > 0.003:  # 0.3% выше
                        is_broken = True
                        break_reason = f"Цена выше уровня на {price_diff*100:.2f}%"
                    
                    # Проверяем исторический пробой
                    for candle in candles_15m[-20:]:
                        if candle['high'] > level_price * 1.003 or candle['close'] > level_price * 1.003:
                            is_broken = True
                            break_reason = "Пробит в истории (последние 20 свечей)"
                            break
                
                # Проверяем расстояние от текущей цены
                price_distance = abs(current_price - level_price) / level_price * 100
                
                # Формируем вывод
                status_icon = "❌" if (is_broken or age_hours > 48) else "✅"
                status_text = "НЕКОРРЕКТЕН" if (is_broken or age_hours > 48) else "КОРРЕКТЕН"
                
                print(f"{status_icon} Уровень #{i}: {level_type.upper()} @ ${level_price:.4f}")
                print(f"   Статус: {status_text}")
                print(f"   Возраст: {age_hours:.1f} часов")
                print(f"   Расстояние от цены: {price_distance:.2f}%")
                print(f"   Test count: {level.test_count}")
                
                if is_broken:
                    print(f"   ⚠️  ПРОБОЙ: {break_reason}")
                elif age_hours > 48:
                    print(f"   ⚠️  СЛИШКОМ СТАРЫЙ: {age_hours:.1f} часов > 48 часов")
                else:
                    print(f"   ✅ Уровень валиден")
                
                print()
        else:
            print("ℹ️  Активных уровней не найдено\n")
        
        # Проверяем, есть ли сигналы на этом уровне
        from core.models import Signal
        signals_on_levels = session.query(Signal).filter_by(pair_id=pair.id).all()
        
        if signals_on_levels:
            print(f"📊 СИГНАЛЫ НА УРОВНЯХ ({len(signals_on_levels)}):\n")
            for signal in signals_on_levels[:5]:  # Показываем последние 5
                level_match = None
                for level in active_levels:
                    if abs(level.price - signal.level_price) / signal.level_price < 0.005:
                        level_match = level
                        break
                
                match_status = "✅ Найден активный уровень" if level_match else "❌ Уровень неактивен или удален"
                print(f"   Сигнал {signal.signal_type} @ ${signal.level_price:.4f} - {match_status}")
        
        print(f"\n{'='*60}")
        print("РЕКОМЕНДАЦИИ:")
        print(f"{'='*60}\n")
        
        # Подсчитываем некорректные уровни
        incorrect_levels = []
        for level in active_levels:
            age_hours = (datetime.now(timezone.utc) - level.created_at.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            
            is_broken = False
            # Проверяем текущий пробой
            if level.level_type == 'support':
                if current_price < level.price * 0.997:
                    is_broken = True
            else:  # resistance
                if current_price > level.price * 1.003:
                    is_broken = True
            
            # Проверяем исторический пробой
            if not is_broken:
                for candle in candles_15m[-20:]:
                    if level.level_type == 'support':
                        if candle['low'] < level.price * 0.997 or candle['close'] < level.price * 0.997:
                            is_broken = True
                            break
                    else:  # resistance
                        if candle['high'] > level.price * 1.003 or candle['close'] > level.price * 1.003:
                            is_broken = True
                            break
            
            if is_broken or age_hours > 48:
                incorrect_levels.append(level)
        
        if incorrect_levels:
            print(f"⚠️  Найдено {len(incorrect_levels)} некорректных активных уровней:")
            for level in incorrect_levels:
                print(f"   - {level.level_type} @ ${level.price:.4f} (ID: {level.id})")
            
            # Жестко удаляем из БД, чтобы не хранить мертвые уровни
            print(f"\n🔧 Удаляем некорректные уровни из БД...")
            for level in incorrect_levels:
                try:
                    session.delete(level)
                    session.commit()
                    print(f"   ✅ Удален: {level.level_type} @ ${level.price:.4f} (ID: {level.id})")
                except Exception as e:
                    session.rollback()
                    print(f"   ❌ Ошибка удаления уровня ID {level.id}: {e}")
            
            print(f"\n💡 Рекомендуется также запустить полную очистку:")
            print(f"   curl -X POST http://localhost:8000/api/force-analysis")
        else:
            print("✅ Все активные уровни корректны!")
        
        print()
        
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


if __name__ == '__main__':
    pair = sys.argv[1] if len(sys.argv) > 1 else 'LINK/USDT'
    asyncio.run(analyze_pair_levels(pair))

