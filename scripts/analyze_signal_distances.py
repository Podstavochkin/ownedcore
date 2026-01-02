#!/usr/bin/env python3
"""
Анализ расстояний при генерации сигналов и при статусе "цена ушла"
Подбор оптимальных параметров
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import statistics

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, TradingPair, SignalLiveLog
from sqlalchemy.orm import joinedload
from sqlalchemy import func

def analyze_signal_distances(days: int = 7):
    """Анализирует расстояния при генерации сигналов и при статусе 'цена ушла'"""
    
    if not init_database():
        print("❌ Не удалось инициализировать базу данных")
        return
    
    from core.database import SessionLocal
    db = SessionLocal()
    
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        print("=" * 80)
        print(f"📊 АНАЛИЗ РАССТОЯНИЙ ПРИ ГЕНЕРАЦИИ СИГНАЛОВ И СТАТУСЕ 'ЦЕНА УШЛА'")
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
        
        # Анализируем расстояния при генерации сигналов
        print("=" * 80)
        print("1. РАССТОЯНИЕ ПРИ ГЕНЕРАЦИИ СИГНАЛОВ")
        print("=" * 80)
        print()
        
        generation_distances = []
        for signal in signals:
            if signal.distance_percent is not None:
                generation_distances.append(signal.distance_percent)
        
        if generation_distances:
            print(f"📊 Найдено сигналов с distance_percent: {len(generation_distances)}")
            print(f"   Минимум: {min(generation_distances):.3f}%")
            print(f"   Максимум: {max(generation_distances):.3f}%")
            print(f"   Среднее: {statistics.mean(generation_distances):.3f}%")
            print(f"   Медиана: {statistics.median(generation_distances):.3f}%")
            if len(generation_distances) > 1:
                print(f"   Стандартное отклонение: {statistics.stdev(generation_distances):.3f}%")
            
            # Распределение по диапазонам
            print()
            print("📊 Распределение по диапазонам:")
            ranges = [
                (0, 0.3, "0-0.3%"),
                (0.3, 0.5, "0.3-0.5%"),
                (0.5, 0.7, "0.5-0.7%"),
                (0.7, 1.0, "0.7-1.0%"),
                (1.0, 1.5, "1.0-1.5%"),
                (1.5, 2.0, "1.5-2.0%"),
                (2.0, float('inf'), ">2.0%")
            ]
            
            for min_val, max_val, label in ranges:
                count = sum(1 for d in generation_distances if min_val <= d < max_val)
                pct = (count / len(generation_distances)) * 100 if generation_distances else 0
                print(f"   {label}: {count} ({pct:.1f}%)")
        else:
            print("⚠️  Нет данных о distance_percent в сигналах")
        
        print()
        
        # Анализируем сигналы со статусом "цена ушла"
        print("=" * 80)
        print("2. РАССТОЯНИЕ ПРИ СТАТУСЕ 'ЦЕНА УШЛА' (PRICE_DEVIATION_TOO_LARGE)")
        print("=" * 80)
        print()
        
        price_deviation_signals = [
            s for s in signals 
            if s.demo_status == 'PRICE_DEVIATION_TOO_LARGE'
        ]
        
        print(f"📊 Сигналов со статусом PRICE_DEVIATION_TOO_LARGE: {len(price_deviation_signals)}")
        print()
        
        if price_deviation_signals:
            # Получаем логи для этих сигналов, чтобы найти расстояние при установке статуса
            signal_ids = [s.id for s in price_deviation_signals]
            logs = (
                db.query(SignalLiveLog)
                .filter(
                    SignalLiveLog.signal_id.in_(signal_ids),
                    SignalLiveLog.status == 'PRICE_DEVIATION_TOO_LARGE'
                )
                .order_by(SignalLiveLog.created_at.asc())
                .all()
            )
            
            # Извлекаем расстояния из логов
            deviation_distances = []
            for log in logs:
                if log.details:
                    details = log.details if isinstance(log.details, dict) else {}
                    # Пробуем разные поля
                    deviation = (
                        details.get('deviation_pct') or
                        details.get('price_deviation_pct') or
                        details.get('deviation') or
                        None
                    )
                    if deviation is not None:
                        deviation_distances.append(float(deviation))
            
            # Если нет в логах, вычисляем из distance_percent сигнала
            if not deviation_distances:
                print("   ⚠️  Не найдено расстояний в логах, используем distance_percent сигналов")
                for signal in price_deviation_signals:
                    if signal.distance_percent is not None:
                        deviation_distances.append(signal.distance_percent)
            
            if deviation_distances:
                print(f"📊 Найдено расстояний: {len(deviation_distances)}")
                print(f"   Минимум: {min(deviation_distances):.3f}%")
                print(f"   Максимум: {max(deviation_distances):.3f}%")
                print(f"   Среднее: {statistics.mean(deviation_distances):.3f}%")
                print(f"   Медиана: {statistics.median(deviation_distances):.3f}%")
                if len(deviation_distances) > 1:
                    print(f"   Стандартное отклонение: {statistics.stdev(deviation_distances):.3f}%")
                
                # Распределение
                print()
                print("📊 Распределение по диапазонам:")
                for min_val, max_val, label in ranges:
                    count = sum(1 for d in deviation_distances if min_val <= d < max_val)
                    pct = (count / len(deviation_distances)) * 100 if deviation_distances else 0
                    print(f"   {label}: {count} ({pct:.1f}%)")
            else:
                print("   ⚠️  Не удалось определить расстояния")
        else:
            print("   ℹ️  Нет сигналов со статусом PRICE_DEVIATION_TOO_LARGE")
        
        print()
        
        # Сравнение: расстояние при генерации vs расстояние при "цена ушла"
        print("=" * 80)
        print("3. СРАВНЕНИЕ: ГЕНЕРАЦИЯ vs 'ЦЕНА УШЛА'")
        print("=" * 80)
        print()
        
        # Текущие настройки
        from core.config import settings
        from core.trading.demo_trade_executor import DemoTradeExecutor
        
        print("📋 ТЕКУЩИЕ НАСТРОЙКИ:")
        print(f"   SIGNAL_FILTER_MAX_DISTANCE_PCT: {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%")
        print(f"   ready_for_signal расстояние: ≤ 1.0%")
        print(f"   MAX_DEVIATION_PCT (check_signal_invalidated): 2.0%")
        print(f"   TOO_FAR_MULTIPLIER: {DemoTradeExecutor.TOO_FAR_MULTIPLIER}")
        print(f"   BASE_MAX_DEVIATION_PCT: {DemoTradeExecutor.BASE_MAX_DEVIATION_PCT}%")
        print(f"   MAX_DEVIATION_PCT (adaptive): {DemoTradeExecutor.MAX_DEVIATION_PCT}%")
        print()
        
        # Вычисляем фактический порог "цена ушла"
        # В place_order_for_signal: allowed_deviation_pct * TOO_FAR_MULTIPLIER
        # allowed_deviation_pct = BASE_MAX_DEVIATION_PCT (0.3%) или адаптивное (до 1.0%)
        # TOO_FAR_MULTIPLIER = 3.0
        # Итого: 0.3% * 3.0 = 0.9% (минимум) или 1.0% * 3.0 = 3.0% (максимум)
        
        min_too_far = DemoTradeExecutor.BASE_MAX_DEVIATION_PCT * DemoTradeExecutor.TOO_FAR_MULTIPLIER
        max_too_far = DemoTradeExecutor.MAX_DEVIATION_PCT * DemoTradeExecutor.TOO_FAR_MULTIPLIER
        
        print("📊 ФАКТИЧЕСКИЕ ПОРОГИ 'ЦЕНА УШЛА':")
        print(f"   Минимальный (BASE): {min_too_far:.3f}% (0.3% * 3.0)")
        print(f"   Максимальный (adaptive): {max_too_far:.3f}% (1.0% * 3.0)")
        print(f"   В check_signal_invalidated: 2.0%")
        print()
        
        # Проблема: если сигнал генерируется при 1.0%, а порог "цена ушла" = 0.9%, то сразу попадет в статус
        if settings.SIGNAL_FILTER_MAX_DISTANCE_PCT >= min_too_far:
            print("⚠️  ПРОБЛЕМА ОБНАРУЖЕНА:")
            print(f"   Сигналы генерируются при расстоянии ≤ {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%")
            print(f"   Но порог 'цена ушла' = {min_too_far:.3f}% (BASE)")
            print(f"   → Сигналы, сгенерированные при {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%, могут сразу попасть в статус 'цена ушла'!")
        
        print()
        print("=" * 80)
        print("💡 РЕКОМЕНДАЦИИ")
        print("=" * 80)
        print()
        
        # Анализируем реальные данные
        if generation_distances and deviation_distances:
            avg_generation = statistics.mean(generation_distances)
            avg_deviation = statistics.mean(deviation_distances)
            median_generation = statistics.median(generation_distances)
            median_deviation = statistics.median(deviation_distances)
            
            print("📊 АНАЛИЗ РЕАЛЬНЫХ ДАННЫХ:")
            print(f"   Среднее расстояние при генерации: {avg_generation:.3f}%")
            print(f"   Среднее расстояние при 'цена ушла': {avg_deviation:.3f}%")
            print(f"   Медиана при генерации: {median_generation:.3f}%")
            print(f"   Медиана при 'цена ушла': {median_deviation:.3f}%")
            print()
            
            # Рекомендации
            print("💡 РЕКОМЕНДУЕМЫЕ НАСТРОЙКИ:")
            print()
            
            # 1. Максимальное расстояние для генерации сигнала
            # Должно быть меньше порога "цена ушла"
            recommended_max_generation = min(median_deviation * 0.7, 0.8)  # 70% от медианы "цена ушла" или 0.8%
            print(f"1. SIGNAL_FILTER_MAX_DISTANCE_PCT:")
            print(f"   Текущее: {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%")
            print(f"   Рекомендуемое: {recommended_max_generation:.2f}%")
            print(f"   Обоснование: Должно быть меньше порога 'цена ушла' ({min_too_far:.2f}%)")
            print()
            
            # 2. ready_for_signal расстояние
            recommended_ready_distance = min(recommended_max_generation, 0.7)
            print(f"2. ready_for_signal расстояние:")
            print(f"   Текущее: ≤ 1.0%")
            print(f"   Рекомендуемое: ≤ {recommended_ready_distance:.2f}%")
            print(f"   Обоснование: Соответствует SIGNAL_FILTER_MAX_DISTANCE_PCT")
            print()
            
            # 3. Порог "цена ушла" в place_order_for_signal
            # Должен быть больше максимального расстояния генерации
            recommended_too_far_base = recommended_max_generation * 1.5  # В 1.5 раза больше
            print(f"3. TOO_FAR порог (BASE_MAX_DEVIATION_PCT * TOO_FAR_MULTIPLIER):")
            print(f"   Текущее: {min_too_far:.3f}% (0.3% * 3.0)")
            print(f"   Рекомендуемое: {recommended_too_far_base:.3f}%")
            print(f"   → BASE_MAX_DEVIATION_PCT: {recommended_too_far_base / DemoTradeExecutor.TOO_FAR_MULTIPLIER:.3f}%")
            print(f"   Обоснование: Должен быть больше максимального расстояния генерации")
            print()
            
            # 4. Порог в check_signal_invalidated
            recommended_max_deviation = recommended_too_far_base * 1.2  # Еще больше для финальной проверки
            print(f"4. MAX_DEVIATION_PCT (check_signal_invalidated):")
            print(f"   Текущее: 2.0%")
            print(f"   Рекомендуемое: {recommended_max_deviation:.2f}%")
            print(f"   Обоснование: Финальная проверка, должна быть больше всех остальных")
            print()
        
        else:
            print("⚠️  Недостаточно данных для точных рекомендаций")
            print()
            print("💡 ОБЩИЕ РЕКОМЕНДАЦИИ:")
            print()
            print("1. SIGNAL_FILTER_MAX_DISTANCE_PCT: 0.7-0.8%")
            print("   → Уменьшить с 1.0% до 0.7-0.8%")
            print()
            print("2. ready_for_signal расстояние: ≤ 0.7%")
            print("   → Уменьшить с 1.0% до 0.7%")
            print()
            print("3. BASE_MAX_DEVIATION_PCT: 0.4-0.5%")
            print("   → Увеличить с 0.3% до 0.4-0.5%")
            print("   → Тогда TOO_FAR = 0.4% * 3.0 = 1.2% или 0.5% * 3.0 = 1.5%")
            print()
            print("4. MAX_DEVIATION_PCT (check_signal_invalidated): 2.0-2.5%")
            print("   → Оставить 2.0% или увеличить до 2.5%")
            print()
        
        print("=" * 80)
        
    finally:
        db.close()
        SessionLocal.remove()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Анализ расстояний при генерации сигналов')
    parser.add_argument('--days', type=int, default=7, help='Количество дней для анализа (по умолчанию 7)')
    args = parser.parse_args()
    
    analyze_signal_distances(days=args.days)

