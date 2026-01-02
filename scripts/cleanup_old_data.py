#!/usr/bin/env python3
"""
Скрипт для очистки старых данных:
1. Удаление старых сделок (signals) до 10.12.2024
2. Анализ и очистка старых уровней
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, Level, SignalLiveLog, TradingPair
from sqlalchemy import and_, func
from sqlalchemy.orm import joinedload


def delete_old_signals(cutoff_date: datetime) -> Tuple[int, int]:
    """
    Удаляет старые сделки (signals) до указанной даты
    Возвращает (количество удаленных сигналов, количество удаленных логов)
    """
    from core.database import SessionLocal
    session = SessionLocal()
    
    try:
        # Находим все сигналы до cutoff_date
        old_signals = (
            session.query(Signal)
            .filter(Signal.timestamp < cutoff_date)
            .all()
        )
        
        signal_ids = [s.id for s in old_signals]
        signals_count = len(signal_ids)
        
        if signals_count == 0:
            print("   ℹ️  Нет старых сигналов для удаления")
            return 0, 0
        
        # Удаляем логи сигналов (каскадное удаление должно сработать, но удалим явно)
        logs_deleted = 0
        if signal_ids:
            logs = session.query(SignalLiveLog).filter(
                SignalLiveLog.signal_id.in_(signal_ids)
            ).all()
            logs_deleted = len(logs)
            for log in logs:
                session.delete(log)
        
        # Удаляем сигналы
        for signal in old_signals:
            session.delete(signal)
        
        session.commit()
        print(f"   ✅ Удалено сигналов: {signals_count}")
        print(f"   ✅ Удалено логов: {logs_deleted}")
        
        return signals_count, logs_deleted
        
    except Exception as e:
        session.rollback()
        print(f"   ❌ Ошибка удаления сигналов: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0
    finally:
        session.close()


def analyze_levels_age():
    """Анализирует возраст уровней в системе"""
    from core.database import SessionLocal
    session = SessionLocal()
    
    try:
        # Получаем все активные уровни
        active_levels = (
            session.query(Level)
            .options(joinedload(Level.pair))
            .filter(Level.is_active == True)
            .all()
        )
        
        print(f"\n📊 Анализ активных уровней:")
        print(f"   Всего активных уровней: {len(active_levels)}")
        
        if not active_levels:
            return
        
        now = datetime.now(timezone.utc)
        
        # Группируем по возрасту
        age_buckets = {
            '< 1 дня': [],
            '1-3 дня': [],
            '3-7 дней': [],
            '7-14 дней': [],
            '14-30 дней': [],
            '> 30 дней': []
        }
        
        # Группируем по парам
        levels_by_pair = {}
        
        for level in active_levels:
            pair_symbol = level.pair.symbol if level.pair else "N/A"
            if pair_symbol not in levels_by_pair:
                levels_by_pair[pair_symbol] = []
            levels_by_pair[pair_symbol].append(level)
            
            # Определяем возраст
            created_at = level.created_at or level.first_touch
            if not created_at:
                age_buckets['> 30 дней'].append(level)
                continue
            
            age_delta = now - created_at.replace(tzinfo=timezone.utc) if created_at.tzinfo is None else now - created_at
            age_hours = age_delta.total_seconds() / 3600
            age_days = age_delta.days
            
            if age_hours < 24:
                age_buckets['< 1 дня'].append(level)
            elif age_days < 3:
                age_buckets['1-3 дня'].append(level)
            elif age_days < 7:
                age_buckets['3-7 дней'].append(level)
            elif age_days < 14:
                age_buckets['7-14 дней'].append(level)
            elif age_days < 30:
                age_buckets['14-30 дней'].append(level)
            else:
                age_buckets['> 30 дней'].append(level)
        
        print(f"\n   Распределение по возрасту:")
        for bucket, levels in age_buckets.items():
            if levels:
                pct = (len(levels) / len(active_levels) * 100) if active_levels else 0
                print(f"      {bucket}: {len(levels)} ({pct:.1f}%)")
        
        # Анализ по парам (топ-10 с наибольшим количеством уровней)
        print(f"\n   Топ-10 пар по количеству уровней:")
        sorted_pairs = sorted(levels_by_pair.items(), key=lambda x: len(x[1]), reverse=True)[:10]
        for pair_symbol, pair_levels in sorted_pairs:
            old_levels = [l for l in pair_levels if l.created_at and (now - l.created_at.replace(tzinfo=timezone.utc) if l.created_at.tzinfo is None else now - l.created_at).days > 7]
            print(f"      {pair_symbol}: {len(pair_levels)} уровней (старше 7 дней: {len(old_levels)})")
        
        return age_buckets, levels_by_pair
        
    except Exception as e:
        print(f"   ❌ Ошибка анализа уровней: {e}")
        import traceback
        traceback.print_exc()
        return None, None
    finally:
        session.close()


def cleanup_old_levels(max_age_days: int = 7, dry_run: bool = True) -> Dict[str, int]:
    """
    Удаляет старые уровни
    max_age_days: максимальный возраст уровня в днях
    dry_run: если True, только показывает что будет удалено, не удаляет
    """
    from core.database import SessionLocal
    session = SessionLocal()
    
    try:
        now = datetime.now(timezone.utc)
        cutoff_date = now - timedelta(days=max_age_days)
        
        # Находим старые активные уровни
        old_levels = (
            session.query(Level)
            .options(joinedload(Level.pair))
            .filter(
                and_(
                    Level.is_active == True,
                    Level.created_at < cutoff_date
                )
            )
            .all()
        )
        
        if not old_levels:
            print(f"   ℹ️  Нет уровней старше {max_age_days} дней")
            return {'deleted': 0, 'by_pair': {}}
        
        # Группируем по парам
        levels_by_pair = {}
        for level in old_levels:
            pair_symbol = level.pair.symbol if level.pair else "N/A"
            if pair_symbol not in levels_by_pair:
                levels_by_pair[pair_symbol] = []
            levels_by_pair[pair_symbol].append(level)
        
        print(f"\n   Найдено уровней старше {max_age_days} дней: {len(old_levels)}")
        print(f"   Распределение по парам:")
        for pair_symbol, pair_levels in sorted(levels_by_pair.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"      {pair_symbol}: {len(pair_levels)} уровней")
        
        if dry_run:
            print(f"\n   ⚠️  DRY RUN: уровни НЕ будут удалены")
            print(f"   Для реального удаления запустите с dry_run=False")
            return {'deleted': 0, 'by_pair': {pair: len(levels) for pair, levels in levels_by_pair.items()}}
        
        # Удаляем уровни
        deleted_count = 0
        for level in old_levels:
            session.delete(level)
            deleted_count += 1
        
        session.commit()
        print(f"\n   ✅ Удалено уровней: {deleted_count}")
        
        return {
            'deleted': deleted_count,
            'by_pair': {pair: len(levels) for pair, levels in levels_by_pair.items()}
        }
        
    except Exception as e:
        session.rollback()
        print(f"   ❌ Ошибка удаления уровней: {e}")
        import traceback
        traceback.print_exc()
        return {'deleted': 0, 'by_pair': {}}
    finally:
        session.close()


def cleanup_levels_far_from_price(max_distance_pct: float = 5.0, dry_run: bool = True):
    """
    Удаляет уровни, которые слишком далеко от текущей цены
    max_distance_pct: максимальное расстояние от текущей цены в процентах
    """
    from core.analysis_engine import AnalysisEngine
    from core.database import SessionLocal
    import asyncio
    
    session = SessionLocal()
    analysis_engine = AnalysisEngine()
    
    try:
        # Получаем все активные уровни
        active_levels = (
            session.query(Level)
            .options(joinedload(Level.pair))
            .filter(Level.is_active == True)
            .all()
        )
        
        if not active_levels:
            print(f"   ℹ️  Нет активных уровней")
            return {'deleted': 0}
        
        # Группируем по парам
        levels_by_pair = {}
        for level in active_levels:
            pair_symbol = level.pair.symbol if level.pair else "N/A"
            if pair_symbol not in levels_by_pair:
                levels_by_pair[pair_symbol] = []
            levels_by_pair[pair_symbol].append(level)
        
        levels_to_delete = []
        
        # Для каждой пары получаем текущую цену
        for pair_symbol, pair_levels in levels_by_pair.items():
            try:
                # Получаем текущую цену
                candles = asyncio.run(analysis_engine.fetch_ohlcv(pair_symbol, '1h', 1))
                if not candles:
                    continue
                
                current_price = candles[-1]['close']
                
                # Проверяем расстояние каждого уровня
                for level in pair_levels:
                    distance_pct = abs(level.price - current_price) / current_price * 100
                    if distance_pct > max_distance_pct:
                        levels_to_delete.append((level, pair_symbol, distance_pct))
                        
            except Exception as e:
                print(f"   ⚠️  Ошибка проверки пары {pair_symbol}: {e}")
                continue
        
        if not levels_to_delete:
            print(f"   ℹ️  Нет уровней дальше {max_distance_pct}% от текущей цены")
            return {'deleted': 0}
        
        print(f"\n   Найдено уровней дальше {max_distance_pct}% от цены: {len(levels_to_delete)}")
        
        # Группируем по парам для отчета
        by_pair = {}
        for level, pair, distance in levels_to_delete:
            if pair not in by_pair:
                by_pair[pair] = []
            by_pair[pair].append((level, distance))
        
        print(f"   Распределение по парам:")
        for pair, levels_list in sorted(by_pair.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"      {pair}: {len(levels_list)} уровней")
        
        if dry_run:
            print(f"\n   ⚠️  DRY RUN: уровни НЕ будут удалены")
            print(f"   Для реального удаления запустите с dry_run=False")
            return {'deleted': 0, 'found': len(levels_to_delete)}
        
        # Удаляем уровни
        deleted_count = 0
        for level, pair, distance in levels_to_delete:
            session.delete(level)
            deleted_count += 1
        
        session.commit()
        print(f"\n   ✅ Удалено уровней: {deleted_count}")
        
        return {'deleted': deleted_count, 'found': len(levels_to_delete)}
        
    except Exception as e:
        session.rollback()
        print(f"   ❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return {'deleted': 0}
    finally:
        session.close()


def main():
    """Основная функция"""
    print("=" * 100)
    print("ОЧИСТКА СТАРЫХ ДАННЫХ")
    print("=" * 100)
    print()
    
    if not init_database():
        print("❌ Ошибка инициализации базы данных")
        return
    
    # Дата отсечки: 10.12.2024 00:00:00 МСК
    moscow_tz = timezone(timedelta(hours=3))
    cutoff_date_moscow = datetime(2024, 12, 10, 0, 0, 0, tzinfo=moscow_tz)
    cutoff_date_utc = cutoff_date_moscow.astimezone(timezone.utc)
    
    print(f"📅 Дата отсечки: {cutoff_date_moscow.strftime('%d.%m.%Y %H:%M:%S')} МСК")
    print()
    
    # 1. Удаление старых сигналов
    print("=" * 100)
    print("1. УДАЛЕНИЕ СТАРЫХ СИГНАЛОВ (до 10.12.2024)")
    print("=" * 100)
    print()
    
    signals_deleted, logs_deleted = delete_old_signals(cutoff_date_utc)
    
    # 2. Анализ уровней
    print()
    print("=" * 100)
    print("2. АНАЛИЗ УРОВНЕЙ")
    print("=" * 100)
    
    age_buckets, levels_by_pair = analyze_levels_age()
    
    # 3. Очистка старых уровней
    print()
    print("=" * 100)
    print("3. ОЧИСТКА СТАРЫХ УРОВНЕЙ (старше 7 дней)")
    print("=" * 100)
    print()
    print("⚠️  ВНИМАНИЕ: Это DRY RUN (пробный запуск)")
    print("   Для реального удаления измените dry_run=False в коде")
    print()
    
    cleanup_old_levels(max_age_days=7, dry_run=True)
    
    # 4. Очистка уровней далеко от цены
    print()
    print("=" * 100)
    print("4. ОЧИСТКА УРОВНЕЙ ДАЛЕКО ОТ ТЕКУЩЕЙ ЦЕНЫ (> 5%)")
    print("=" * 100)
    print()
    print("⚠️  ВНИМАНИЕ: Это DRY RUN (пробный запуск)")
    print("   Для реального удаления измените dry_run=False в коде")
    print()
    
    cleanup_levels_far_from_price(max_distance_pct=5.0, dry_run=True)
    
    print()
    print("=" * 100)
    print("✅ АНАЛИЗ ЗАВЕРШЕН")
    print("=" * 100)
    print()
    print("💡 Для реального удаления:")
    print("   1. Измените dry_run=False в функциях cleanup_old_levels и cleanup_levels_far_from_price")
    print("   2. Запустите скрипт снова")
    print()


if __name__ == "__main__":
    main()

