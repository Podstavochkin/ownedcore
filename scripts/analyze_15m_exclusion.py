#!/usr/bin/env python3
"""
Анализ эффекта от исключения сделок на 15M таймфрейме
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, TradingPair
from sqlalchemy import and_
from sqlalchemy.orm import joinedload

# Комиссия Bybit USDT perpetual: 0.035% вход + 0.035% выход
COMMISSION_PCT = 0.07


def calc_pnl_pct(signal: Signal) -> float:
    """Считает фактический PnL в процентах для сигнала с учетом комиссий"""
    if not signal.entry_price or not signal.exit_price:
        return 0.0

    entry = float(signal.entry_price)
    exit_ = float(signal.exit_price)
    if entry <= 0 or exit_ <= 0:
        return 0.0

    if signal.signal_type == "LONG":
        move_pct = (exit_ - entry) / entry * 100.0
    else:  # SHORT
        move_pct = (entry - exit_) / entry * 100.0

    net_pct = move_pct - COMMISSION_PCT
    return round(net_pct, 4)


def analyze_15m_exclusion():
    """Анализирует эффект от исключения 15M сделок"""
    
    print("=" * 100)
    print("АНАЛИЗ ЭФФЕКТА ОТ ИСКЛЮЧЕНИЯ 15M СДЕЛОК")
    print("=" * 100)
    print()
    
    # Инициализация БД
    if not init_database():
        print("❌ Ошибка инициализации базы данных")
        return
    
    from core.database import SessionLocal
    session = SessionLocal()
    
    try:
        # Дата начала анализа: 10.12.2024 00:00:00 МСК
        moscow_tz = timezone(timedelta(hours=3))
        start_date_moscow = datetime(2024, 12, 10, 0, 0, 0, tzinfo=moscow_tz)
        start_date_utc = start_date_moscow.astimezone(timezone.utc)
        end_date_utc = datetime.now(timezone.utc)
        
        print(f"📅 Период анализа:")
        print(f"   Начало: {start_date_moscow.strftime('%d.%m.%Y %H:%M:%S')} МСК")
        print(f"   Конец:  {end_date_utc.astimezone(moscow_tz).strftime('%d.%m.%Y %H:%M:%S')} МСК")
        print()
        
        # Загружаем все закрытые сделки с реальными ордерами
        closed_trades = (
            session.query(Signal)
            .join(TradingPair)
            .filter(
                and_(
                    Signal.timestamp >= start_date_utc,
                    Signal.status == 'CLOSED',
                    Signal.demo_order_id.isnot(None),
                    Signal.entry_price.isnot(None),
                    Signal.exit_price.isnot(None),
                    Signal.entry_price > 0,
                    Signal.exit_price > 0
                )
            )
            .options(joinedload(Signal.pair))
            .order_by(Signal.timestamp.asc())
            .all()
        )
        
        # Фильтруем только сделки с реальным результатом
        closed_trades = [t for t in closed_trades if calc_pnl_pct(t) != 0.0]
        
        total_trades = len(closed_trades)
        print(f"📊 Всего закрытых сделок: {total_trades}")
        print()
        
        # Группируем по таймфреймам
        trades_by_tf = {
            '15m': [],
            '1h': [],
            '4h': [],
            'other': []
        }
        
        for trade in closed_trades:
            tf = trade.level_timeframe or "N/A"
            if tf == '15m':
                trades_by_tf['15m'].append(trade)
            elif tf == '1h':
                trades_by_tf['1h'].append(trade)
            elif tf == '4h':
                trades_by_tf['4h'].append(trade)
            else:
                trades_by_tf['other'].append(trade)
        
        # Текущая ситуация
        print("=" * 100)
        print("📊 ТЕКУЩАЯ СИТУАЦИЯ (ВСЕ ТАЙМФРЕЙМЫ)")
        print("=" * 100)
        print()
        
        wins = [t for t in closed_trades if calc_pnl_pct(t) > 0]
        losses = [t for t in closed_trades if calc_pnl_pct(t) < 0]
        total_pnl = sum(calc_pnl_pct(t) for t in closed_trades)
        total_win_pnl = sum(calc_pnl_pct(t) for t in wins)
        total_loss_pnl = sum(abs(calc_pnl_pct(t)) for t in losses)
        
        winrate = (len(wins) / total_trades * 100.0) if total_trades else 0.0
        avg_win = (total_win_pnl / len(wins)) if wins else 0.0
        avg_loss = (total_loss_pnl / len(losses)) if losses else 0.0
        profit_factor = (total_win_pnl / total_loss_pnl) if total_loss_pnl > 0 else float('inf')
        avg_pnl = total_pnl / total_trades if total_trades else 0.0
        
        print(f"Всего сделок: {total_trades}")
        print(f"Прибыльных: {len(wins)} ({len(wins)/total_trades*100:.1f}%)")
        print(f"Убыточных: {len(losses)} ({len(losses)/total_trades*100:.1f}%)")
        print(f"Winrate: {winrate:.2f}%")
        print(f"Общий P&L: {total_pnl:+.2f}%")
        print(f"Средний P&L: {avg_pnl:+.2f}%")
        print(f"Средняя прибыль: {avg_win:+.2f}%")
        print(f"Средний убыток: -{avg_loss:.2f}%")
        print(f"Profit Factor: {profit_factor:.2f}")
        print()
        
        # Распределение по таймфреймам
        print("=" * 100)
        print("📊 РАСПРЕДЕЛЕНИЕ СДЕЛОК ПО ТАЙМФРЕЙМАМ")
        print("=" * 100)
        print()
        
        for tf in ['15m', '1h', '4h', 'other']:
            trades = trades_by_tf[tf]
            if trades:
                pct = (len(trades) / total_trades * 100) if total_trades else 0
                tf_pnl = sum(calc_pnl_pct(t) for t in trades)
                tf_wins = [t for t in trades if calc_pnl_pct(t) > 0]
                tf_wr = (len(tf_wins) / len(trades) * 100) if trades else 0
                print(f"{tf.upper():<10} {len(trades):<6} сделок ({pct:>5.1f}%) | Winrate: {tf_wr:>5.1f}% | P&L: {tf_pnl:>+7.2f}%")
        print()
        
        # Сценарий БЕЗ 15M
        print("=" * 100)
        print("📊 СЦЕНАРИЙ БЕЗ 15M СДЕЛОК")
        print("=" * 100)
        print()
        
        trades_without_15m = trades_by_tf['1h'] + trades_by_tf['4h'] + trades_by_tf['other']
        trades_15m = trades_by_tf['15m']
        
        print(f"Исключаем: {len(trades_15m)} сделок на 15M ({len(trades_15m)/total_trades*100:.1f}% от всех)")
        print(f"Остается: {len(trades_without_15m)} сделок ({len(trades_without_15m)/total_trades*100:.1f}% от всех)")
        print()
        
        if trades_without_15m:
            wins_no_15m = [t for t in trades_without_15m if calc_pnl_pct(t) > 0]
            losses_no_15m = [t for t in trades_without_15m if calc_pnl_pct(t) < 0]
            total_pnl_no_15m = sum(calc_pnl_pct(t) for t in trades_without_15m)
            total_win_pnl_no_15m = sum(calc_pnl_pct(t) for t in wins_no_15m)
            total_loss_pnl_no_15m = sum(abs(calc_pnl_pct(t)) for t in losses_no_15m)
            
            winrate_no_15m = (len(wins_no_15m) / len(trades_without_15m) * 100.0) if trades_without_15m else 0.0
            avg_win_no_15m = (total_win_pnl_no_15m / len(wins_no_15m)) if wins_no_15m else 0.0
            avg_loss_no_15m = (total_loss_pnl_no_15m / len(losses_no_15m)) if losses_no_15m else 0.0
            profit_factor_no_15m = (total_win_pnl_no_15m / total_loss_pnl_no_15m) if total_loss_pnl_no_15m > 0 else float('inf')
            avg_pnl_no_15m = total_pnl_no_15m / len(trades_without_15m) if trades_without_15m else 0.0
            
            print(f"Всего сделок: {len(trades_without_15m)}")
            print(f"Прибыльных: {len(wins_no_15m)} ({len(wins_no_15m)/len(trades_without_15m)*100:.1f}%)")
            print(f"Убыточных: {len(losses_no_15m)} ({len(losses_no_15m)/len(trades_without_15m)*100:.1f}%)")
            print(f"Winrate: {winrate_no_15m:.2f}%")
            print(f"Общий P&L: {total_pnl_no_15m:+.2f}%")
            print(f"Средний P&L: {avg_pnl_no_15m:+.2f}%")
            print(f"Средняя прибыль: {avg_win_no_15m:+.2f}%")
            print(f"Средний убыток: -{avg_loss_no_15m:.2f}%")
            print(f"Profit Factor: {profit_factor_no_15m:.2f}")
            print()
            
            # Сравнение
            print("=" * 100)
            print("📊 СРАВНЕНИЕ: С 15M vs БЕЗ 15M")
            print("=" * 100)
            print()
            
            print(f"{'Метрика':<25} {'С 15M':<20} {'БЕЗ 15M':<20} {'Изменение':<20}")
            print("-" * 100)
            
            wr_diff = winrate_no_15m - winrate
            pnl_diff = total_pnl_no_15m - total_pnl
            pf_diff = profit_factor_no_15m - profit_factor
            trades_diff = len(trades_without_15m) - total_trades
            
            print(f"{'Всего сделок':<25} {total_trades:<20} {len(trades_without_15m):<20} {trades_diff:+d} ({trades_diff/total_trades*100:+.1f}%)")
            print(f"{'Winrate':<25} {winrate:>6.2f}%{'':<13} {winrate_no_15m:>6.2f}%{'':<13} {wr_diff:>+6.2f}%")
            print(f"{'Общий P&L':<25} {total_pnl:>+7.2f}%{'':<12} {total_pnl_no_15m:>+7.2f}%{'':<12} {pnl_diff:>+7.2f}%")
            print(f"{'Средний P&L':<25} {avg_pnl:>+7.2f}%{'':<12} {avg_pnl_no_15m:>+7.2f}%{'':<12} {avg_pnl_no_15m - avg_pnl:>+7.2f}%")
            print(f"{'Profit Factor':<25} {profit_factor:>6.2f}{'':<13} {profit_factor_no_15m:>6.2f}{'':<13} {pf_diff:>+6.2f}")
            print()
            
            # Выводы
            print("=" * 100)
            print("💡 ВЫВОДЫ И РЕКОМЕНДАЦИИ")
            print("=" * 100)
            print()
            
            if winrate_no_15m > winrate and profit_factor_no_15m > profit_factor and total_pnl_no_15m > total_pnl:
                print("✅ ИСКЛЮЧЕНИЕ 15M СДЕЛОК ДАСТ ПОЛОЖИТЕЛЬНЫЙ ЭФФЕКТ:")
                print()
                print(f"   📈 Winrate улучшится на {wr_diff:+.2f}% ({winrate:.2f}% → {winrate_no_15m:.2f}%)")
                print(f"   💰 Общий P&L улучшится на {pnl_diff:+.2f}% ({total_pnl:+.2f}% → {total_pnl_no_15m:+.2f}%)")
                print(f"   📉 Profit Factor улучшится на {pf_diff:+.2f} ({profit_factor:.2f} → {profit_factor_no_15m:.2f})")
                print(f"   📊 Количество сделок уменьшится на {abs(trades_diff)} ({total_trades} → {len(trades_without_15m)})")
                print()
                print("   ⚠️  ВНИМАНИЕ: Количество сделок уменьшится на {:.1f}%".format(abs(trades_diff)/total_trades*100))
                print("      Это может привести к меньшему количеству торговых возможностей.")
                print()
                print("   💡 РЕКОМЕНДАЦИЯ: Можно исключить 15M сделки, но:")
                print("      - Убедитесь, что 1H и 4H таймфреймы генерируют достаточно сигналов")
                print("      - Мониторьте количество сигналов после внедрения")
                print("      - Рассмотрите возможность частичного исключения (только худшие комбинации на 15M)")
            else:
                print("⚠️  ИСКЛЮЧЕНИЕ 15M СДЕЛОК НЕ ДАСТ ОДНОЗНАЧНОГО УЛУЧШЕНИЯ:")
                print()
                if winrate_no_15m < winrate:
                    print(f"   ❌ Winrate ухудшится на {abs(wr_diff):.2f}%")
                if profit_factor_no_15m < profit_factor:
                    print(f"   ❌ Profit Factor ухудшится на {abs(pf_diff):.2f}")
                if total_pnl_no_15m < total_pnl:
                    print(f"   ❌ Общий P&L ухудшится на {abs(pnl_diff):.2f}%")
                print()
                print("   💡 РЕКОМЕНДАЦИЯ: Вместо полного исключения 15M:")
                print("      - Применить более строгие фильтры для 15M (уже реализовано: min_score=35)")
                print("      - Исключить только худшие комбинации на 15M")
                print("      - Приоритизировать 1H и 4H сигналы")
            
            print()
            
            # Альтернатива: только лучшие 15M сделки
            print("=" * 100)
            print("📊 АЛЬТЕРНАТИВА: ТОЛЬКО ЛУЧШИЕ 15M СДЕЛКИ (level_score >= 35)")
            print("=" * 100)
            print()
            
            good_15m_trades = [t for t in trades_15m if (t.level_score or 0) >= 35]
            bad_15m_trades = [t for t in trades_15m if (t.level_score or 0) < 35]
            
            if good_15m_trades:
                good_15m_wins = [t for t in good_15m_trades if calc_pnl_pct(t) > 0]
                good_15m_pnl = sum(calc_pnl_pct(t) for t in good_15m_trades)
                good_15m_wr = (len(good_15m_wins) / len(good_15m_trades) * 100) if good_15m_trades else 0
                
                print(f"15M сделки с level_score >= 35:")
                print(f"   Количество: {len(good_15m_trades)} ({len(good_15m_trades)/len(trades_15m)*100:.1f}% от всех 15M)")
                print(f"   Winrate: {good_15m_wr:.1f}%")
                print(f"   P&L: {good_15m_pnl:+.2f}%")
                print()
            
            if bad_15m_trades:
                bad_15m_wins = [t for t in bad_15m_trades if calc_pnl_pct(t) > 0]
                bad_15m_pnl = sum(calc_pnl_pct(t) for t in bad_15m_trades)
                bad_15m_wr = (len(bad_15m_wins) / len(bad_15m_trades) * 100) if bad_15m_trades else 0
                
                print(f"15M сделки с level_score < 35 (исключаем):")
                print(f"   Количество: {len(bad_15m_trades)} ({len(bad_15m_trades)/len(trades_15m)*100:.1f}% от всех 15M)")
                print(f"   Winrate: {bad_15m_wr:.1f}%")
                print(f"   P&L: {bad_15m_pnl:+.2f}%")
                print()
            
            # Сценарий с фильтрацией 15M
            if good_15m_trades:
                trades_filtered_15m = trades_without_15m + good_15m_trades
                
                filtered_wins = [t for t in trades_filtered_15m if calc_pnl_pct(t) > 0]
                filtered_pnl = sum(calc_pnl_pct(t) for t in trades_filtered_15m)
                filtered_wr = (len(filtered_wins) / len(trades_filtered_15m) * 100) if trades_filtered_15m else 0
                filtered_win_pnl = sum(calc_pnl_pct(t) for t in filtered_wins)
                filtered_loss_pnl = sum(abs(calc_pnl_pct(t)) for t in trades_filtered_15m if calc_pnl_pct(t) < 0)
                filtered_pf = (filtered_win_pnl / filtered_loss_pnl) if filtered_loss_pnl > 0 else float('inf')
                
                print(f"Сценарий с фильтрацией 15M (только level_score >= 35):")
                print(f"   Всего сделок: {len(trades_filtered_15m)}")
                print(f"   Winrate: {filtered_wr:.2f}%")
                print(f"   Общий P&L: {filtered_pnl:+.2f}%")
                print(f"   Profit Factor: {filtered_pf:.2f}")
                print()
        
        print("=" * 100)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    analyze_15m_exclusion()

