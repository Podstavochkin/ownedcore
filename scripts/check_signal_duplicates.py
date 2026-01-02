#!/usr/bin/env python3
"""
Проверка логики дубликатов сигналов: анализ проблемы со старыми сигналами
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, Level, TradingPair
from datetime import datetime, timedelta, timezone
from sqlalchemy import func

def check_signal_duplicates():
    """Проверяет логику дубликатов сигналов"""
    init_database()
    from core.database import SessionLocal
    db = SessionLocal()
    try:
        print("=" * 80)
        print("АНАЛИЗ ПРОБЛЕМЫ С ДУБЛИКАТАМИ СИГНАЛОВ")
        print("=" * 80)
        print()
        
        # Находим уровни с несколькими сигналами
        print("📊 УРОВНИ С НЕСКОЛЬКИМИ СИГНАЛАМИ:")
        print("-" * 80)
        
        # Группируем сигналы по паре и уровню (с допуском 0.1%)
        signals = db.query(Signal).join(TradingPair).filter(
            TradingPair.enabled == True
        ).order_by(Signal.timestamp.desc()).all()
        
        # Группируем по паре и уровню
        levels_with_signals = {}
        for signal in signals:
            if not signal.pair or not signal.level_price:
                continue
            
            pair_symbol = signal.pair.symbol
            level_price = signal.level_price
            price_tolerance = level_price * 0.001  # 0.1%
            
            # Ищем существующую группу
            found_group = False
            for (p, lp), sig_list in levels_with_signals.items():
                if p == pair_symbol and abs(lp - level_price) < price_tolerance:
                    sig_list.append(signal)
                    found_group = True
                    break
            
            if not found_group:
                levels_with_signals[(pair_symbol, level_price)] = [signal]
        
        # Находим уровни с несколькими сигналами
        duplicate_levels = {k: v for k, v in levels_with_signals.items() if len(v) > 1}
        
        print(f"Найдено уровней с несколькими сигналами: {len(duplicate_levels)}")
        print()
        
        # Показываем примеры
        max_age_seconds = 30 * 60  # 30 минут
        now = datetime.now(timezone.utc)
        
        problematic_count = 0
        for (pair_symbol, level_price), sig_list in list(duplicate_levels.items())[:10]:
            sig_list.sort(key=lambda s: s.timestamp, reverse=True)
            newest = sig_list[0]
            oldest = sig_list[-1]
            
            newest_age = (now - newest.timestamp.replace(tzinfo=timezone.utc)).total_seconds()
            oldest_age = (now - oldest.timestamp.replace(tzinfo=timezone.utc)).total_seconds()
            
            # Проверяем, может ли старый сигнал блокировать новый
            is_problematic = (
                oldest_age > max_age_seconds and  # Старый сигнал устарел
                newest_age < max_age_seconds and  # Новый сигнал свежий
                oldest.status == 'CLOSED'  # Старый сигнал закрыт
            )
            
            if is_problematic:
                problematic_count += 1
                print(f"⚠️ ПРОБЛЕМА: {pair_symbol} @ ${level_price:.4f}")
                print(f"   Старый сигнал: ID {oldest.id}, создан {oldest.timestamp}, возраст {oldest_age/60:.1f} мин, статус {oldest.status}")
                print(f"   Новый сигнал: ID {newest.id}, создан {newest.timestamp}, возраст {newest_age/60:.1f} мин, статус {newest.status}")
                print()
        
        if problematic_count > 0:
            print(f"❌ Найдено {problematic_count} потенциально проблемных случаев")
        else:
            print("✅ Проблемных случаев не найдено")
        
        print()
        print("=" * 80)
        print("РЕКОМЕНДАЦИИ:")
        print("=" * 80)
        print("1. В save_signal: разрешать создание нового сигнала, если старый:")
        print("   - Старше MAX_SIGNAL_AGE_SECONDS (30 минут)")
        print("   - Имеет статус CLOSED, SIGNAL_TOO_OLD, или другой финальный")
        print()
        print("2. В get_potential_signals: показывать только актуальные сигналы:")
        print("   - Не старше MAX_SIGNAL_AGE_SECONDS")
        print("   - Или только ACTIVE статус")
        print()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    check_signal_duplicates()

