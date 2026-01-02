#!/usr/bin/env python3
"""
Анализ нереализованных сигналов (цена ушла, уровень пробит и т.д.)
Показывает, сколько потенциальных сделок было потеряно
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, TradingPair
from sqlalchemy.orm import joinedload

def analyze_unrealized_signals(days: int = 2):
    """Анализирует нереализованные сигналы за последние N дней"""
    
    if not init_database():
        print("❌ Не удалось инициализировать базу данных")
        return
    
    from core.database import SessionLocal
    db = SessionLocal()
    
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        print("=" * 80)
        print(f"📊 АНАЛИЗ НЕРЕАЛИЗОВАННЫХ СИГНАЛОВ (за последние {days} дней)")
        print("=" * 80)
        print()
        
        # Получаем все сигналы за период
        signals = (
            db.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(Signal.timestamp >= cutoff_date)
            .order_by(Signal.timestamp.desc())
            .all()
        )
        
        print(f"📈 Всего сигналов за {days} дней: {len(signals)}")
        print()
        
        # Группируем по demo_status
        by_status = defaultdict(list)
        for signal in signals:
            status = signal.demo_status or "NONE"
            by_status[status].append(signal)
        
        # Группируем по status (ACTIVE, CLOSED, etc.)
        by_main_status = defaultdict(list)
        for signal in signals:
            status = signal.status or "NONE"
            by_main_status[status].append(signal)
        
        print("📊 РАСПРЕДЕЛЕНИЕ ПО demo_status:")
        print()
        
        # Реализованные сделки
        realized = [s for s in signals if s.demo_order_id and s.entry_price and s.exit_price]
        print(f"✅ Реализованные сделки (с ордером и результатом): {len(realized)}")
        
        # Нереализованные сигналы
        unrealized = [s for s in signals if not (s.demo_order_id and s.entry_price and s.exit_price)]
        print(f"❌ Нереализованные сигналы: {len(unrealized)}")
        print()
        
        # Детальный анализ по статусам
        print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ НЕРЕАЛИЗОВАННЫХ СИГНАЛОВ:")
        print()
        
        # Статусы, которые означают "цена ушла" или "не реализовано"
        price_deviation_statuses = [
            'PRICE_DEVIATION_TOO_LARGE',
            'ORDER_CANCELLED_PRICE_MOVED',
            'LEVEL_BROKEN',
            'SIGNAL_TOO_OLD',
            'WAITING_FOR_PRICE',
            'SIGNAL_CLOSED_NO_ORDER'
        ]
        
        for status in sorted(by_status.keys()):
            status_signals = by_status[status]
            count = len(status_signals)
            
            # Определяем тип статуса
            if status in price_deviation_statuses:
                icon = "❌"
                category = "Нереализовано"
            elif status in ['PLACED', 'FILLED', 'OPEN_POSITION']:
                icon = "⏳"
                category = "В процессе"
            elif status == 'CLOSED':
                icon = "✅"
                category = "Закрыто"
            else:
                icon = "❓"
                category = "Другое"
            
            print(f"{icon} {status}: {count} сигналов ({category})")
            
            # Показываем примеры для нереализованных
            if status in price_deviation_statuses and count > 0:
                example = status_signals[0]
                pair = example.pair.symbol if example.pair else "N/A"
                timestamp = example.timestamp.strftime('%Y-%m-%d %H:%M')
                print(f"   Пример: {pair} {example.signal_type} @ {example.level_price:.4f} ({timestamp})")
        
        print()
        print("=" * 80)
        print("📊 СТАТИСТИКА ПО demo_status:")
        print("=" * 80)
        print()
        
        total_unrealized = 0
        for status in price_deviation_statuses:
            count = len(by_status.get(status, []))
            if count > 0:
                total_unrealized += count
                pct = (count / len(signals)) * 100 if signals else 0
                print(f"   {status}: {count} ({pct:.1f}%)")
        
        print()
        print(f"📉 Всего нереализованных сигналов: {total_unrealized}")
        print(f"📈 Всего реализованных сделок: {len(realized)}")
        
        if len(signals) > 0:
            realized_pct = (len(realized) / len(signals)) * 100
            unrealized_pct = (total_unrealized / len(signals)) * 100
            print(f"   Реализовано: {realized_pct:.1f}%")
            print(f"   Нереализовано: {unrealized_pct:.1f}%")
        
        print()
        print("=" * 80)
        print("💡 ВЫВОДЫ:")
        print("=" * 80)
        print()
        
        if total_unrealized > len(realized) * 2:
            print("⚠️  КРИТИЧНО: Слишком много нереализованных сигналов!")
            print(f"   Нереализовано: {total_unrealized}, Реализовано: {len(realized)}")
            print("   → Возможно, фильтры слишком строгие или цена часто уходит от уровней")
        elif total_unrealized > len(realized):
            print("⚠️  Много нереализованных сигналов")
            print(f"   Нереализовано: {total_unrealized}, Реализовано: {len(realized)}")
            print("   → Это нормально, но стоит проверить настройки")
        else:
            print("✅ Соотношение нереализованных и реализованных сигналов в норме")
        
        # Анализ по причинам нереализации
        print()
        print("=" * 80)
        print("🔍 ПРИЧИНЫ НЕРЕАЛИЗАЦИИ:")
        print("=" * 80)
        print()
        
        price_deviation_count = len(by_status.get('PRICE_DEVIATION_TOO_LARGE', []))
        level_broken_count = len(by_status.get('LEVEL_BROKEN', []))
        too_old_count = len(by_status.get('SIGNAL_TOO_OLD', []))
        waiting_count = len(by_status.get('WAITING_FOR_PRICE', []))
        
        if price_deviation_count > 0:
            print(f"❌ PRICE_DEVIATION_TOO_LARGE: {price_deviation_count}")
            print("   → Цена ушла слишком далеко от уровня (>2%)")
            print("   → Сигналы НЕ анализируются в comprehensive_trade_analysis.py")
        
        if level_broken_count > 0:
            print(f"❌ LEVEL_BROKEN: {level_broken_count}")
            print("   → Уровень пробит против направления сигнала")
            print("   → Сигналы НЕ анализируются в comprehensive_trade_analysis.py")
        
        if too_old_count > 0:
            print(f"⏰ SIGNAL_TOO_OLD: {too_old_count}")
            print("   → Сигнал слишком старый (>30 минут)")
            print("   → Сигналы НЕ анализируются в comprehensive_trade_analysis.py")
        
        if waiting_count > 0:
            print(f"⏳ WAITING_FOR_PRICE: {waiting_count}")
            print("   → Сигнал ждет подхода цены к уровню")
            print("   → Может быть реализован позже")
        
        print()
        print("=" * 80)
        
    finally:
        db.close()
        SessionLocal.remove()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Анализ нереализованных сигналов')
    parser.add_argument('--days', type=int, default=2, help='Количество дней для анализа (по умолчанию 2)')
    args = parser.parse_args()
    
    analyze_unrealized_signals(days=args.days)

