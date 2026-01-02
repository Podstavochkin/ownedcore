#!/usr/bin/env python3
"""
Детальный анализ успешности сделок по таймфреймам (15m, 1h, 4h)
На основе данных с 10.12.2024
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, TradingPair, SignalLiveLog
from sqlalchemy import and_, or_, func
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


def get_level_score_bucket(score: Optional[float]) -> str:
    """Группирует level_score в бакеты"""
    if score is None:
        return "N/A"
    if score < 30:
        return "0-30"
    elif score < 40:
        return "30-40"
    elif score < 50:
        return "40-50"
    elif score < 60:
        return "50-60"
    elif score < 70:
        return "60-70"
    else:
        return "70+"


def analyze_timeframe_performance():
    """Детальный анализ успешности сделок по таймфреймам"""
    
    print("=" * 100)
    print("АНАЛИЗ УСПЕШНОСТИ СДЕЛОК ПО ТАЙМФРЕЙМАМ")
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
        
        print(f"📊 Всего закрытых сделок с реальными ордерами: {len(closed_trades)}")
        print()
        
        if len(closed_trades) == 0:
            print("❌ Нет закрытых сделок за указанный период")
            return
        
        # Группируем по таймфреймам
        trades_by_tf = defaultdict(list)
        for trade in closed_trades:
            tf = trade.level_timeframe or "N/A"
            trades_by_tf[tf].append(trade)
        
        # Анализируем каждый таймфрейм
        for timeframe in ['15m', '1h', '4h']:
            if timeframe not in trades_by_tf:
                continue
            
            trades = trades_by_tf[timeframe]
            print("=" * 100)
            print(f"📊 АНАЛИЗ ТАЙМФРЕЙМА: {timeframe.upper()}")
            print("=" * 100)
            print()
            
            # Общая статистика
            wins = [t for t in trades if calc_pnl_pct(t) > 0]
            losses = [t for t in trades if calc_pnl_pct(t) < 0]
            
            total_pnl = sum(calc_pnl_pct(t) for t in trades)
            total_win_pnl = sum(calc_pnl_pct(t) for t in wins)
            total_loss_pnl = sum(abs(calc_pnl_pct(t)) for t in losses)
            
            winrate = (len(wins) / len(trades) * 100.0) if trades else 0.0
            avg_win = (total_win_pnl / len(wins)) if wins else 0.0
            avg_loss = (total_loss_pnl / len(losses)) if losses else 0.0
            profit_factor = (total_win_pnl / total_loss_pnl) if total_loss_pnl > 0 else float('inf')
            avg_pnl = total_pnl / len(trades) if trades else 0.0
            
            print(f"📈 Общая статистика:")
            print(f"   Всего сделок: {len(trades)}")
            print(f"   Прибыльных: {len(wins)} ({len(wins)/len(trades)*100:.1f}%)")
            print(f"   Убыточных: {len(losses)} ({len(losses)/len(trades)*100:.1f}%)")
            print(f"   Winrate: {winrate:.2f}%")
            print(f"   Общий P&L: {total_pnl:+.2f}%")
            print(f"   Средний P&L: {avg_pnl:+.2f}%")
            print(f"   Средняя прибыль: {avg_win:+.2f}%")
            print(f"   Средний убыток: -{avg_loss:.2f}%")
            print(f"   Profit Factor: {profit_factor:.2f}")
            print()
            
            # Анализ по типу сигнала
            print(f"📊 По типу сигнала:")
            long_trades = [t for t in trades if t.signal_type == 'LONG']
            short_trades = [t for t in trades if t.signal_type == 'SHORT']
            
            if long_trades:
                long_wins = [t for t in long_trades if calc_pnl_pct(t) > 0]
                long_pnl = sum(calc_pnl_pct(t) for t in long_trades)
                long_wr = (len(long_wins) / len(long_trades) * 100) if long_trades else 0
                print(f"   LONG: {len(long_trades)} сделок, Winrate: {long_wr:.1f}%, P&L: {long_pnl:+.2f}%")
            
            if short_trades:
                short_wins = [t for t in short_trades if calc_pnl_pct(t) > 0]
                short_pnl = sum(calc_pnl_pct(t) for t in short_trades)
                short_wr = (len(short_wins) / len(short_trades) * 100) if short_trades else 0
                print(f"   SHORT: {len(short_trades)} сделок, Winrate: {short_wr:.1f}%, P&L: {short_pnl:+.2f}%")
            print()
            
            # Анализ по level_score
            print(f"📊 По level_score:")
            score_buckets = defaultdict(list)
            for trade in trades:
                bucket = get_level_score_bucket(trade.level_score)
                score_buckets[bucket].append(trade)
            
            for bucket in sorted(score_buckets.keys()):
                bucket_trades = score_buckets[bucket]
                if len(bucket_trades) >= 3:  # Минимум 3 сделки
                    bucket_wins = [t for t in bucket_trades if calc_pnl_pct(t) > 0]
                    bucket_pnl = sum(calc_pnl_pct(t) for t in bucket_trades)
                    bucket_wr = (len(bucket_wins) / len(bucket_trades) * 100) if bucket_trades else 0
                    bucket_avg = bucket_pnl / len(bucket_trades) if bucket_trades else 0
                    print(f"   Score {bucket}: {len(bucket_trades)} сделок, Winrate: {bucket_wr:.1f}%, Средний P&L: {bucket_avg:+.2f}%")
            print()
            
            # Анализ по тренду
            print(f"📊 По тренду (trend_1h):")
            trend_buckets = defaultdict(list)
            for trade in trades:
                trend = trade.trend_1h or "N/A"
                trend_buckets[trend].append(trade)
            
            for trend in sorted(trend_buckets.keys(), key=lambda x: len(trend_buckets[x]), reverse=True):
                trend_trades = trend_buckets[trend]
                if len(trend_trades) >= 3:
                    trend_wins = [t for t in trend_trades if calc_pnl_pct(t) > 0]
                    trend_pnl = sum(calc_pnl_pct(t) for t in trend_trades)
                    trend_wr = (len(trend_wins) / len(trend_trades) * 100) if trend_trades else 0
                    trend_avg = trend_pnl / len(trend_trades) if trend_trades else 0
                    print(f"   {trend}: {len(trend_trades)} сделок, Winrate: {trend_wr:.1f}%, Средний P&L: {trend_avg:+.2f}%")
            print()
            
            # Анализ по типу уровня
            print(f"📊 По типу уровня:")
            support_trades = [t for t in trades if t.level_type == 'support']
            resistance_trades = [t for t in trades if t.level_type == 'resistance']
            
            if support_trades:
                sup_wins = [t for t in support_trades if calc_pnl_pct(t) > 0]
                sup_pnl = sum(calc_pnl_pct(t) for t in support_trades)
                sup_wr = (len(sup_wins) / len(support_trades) * 100) if support_trades else 0
                print(f"   Support: {len(support_trades)} сделок, Winrate: {sup_wr:.1f}%, P&L: {sup_pnl:+.2f}%")
            
            if resistance_trades:
                res_wins = [t for t in resistance_trades if calc_pnl_pct(t) > 0]
                res_pnl = sum(calc_pnl_pct(t) for t in resistance_trades)
                res_wr = (len(res_wins) / len(resistance_trades) * 100) if resistance_trades else 0
                print(f"   Resistance: {len(resistance_trades)} сделок, Winrate: {res_wr:.1f}%, P&L: {res_pnl:+.2f}%")
            print()
            
            # Комбинации: тренд + level_score
            print(f"📊 Комбинации: Тренд + Level Score (топ-10 по количеству сделок):")
            combos = defaultdict(list)
            for trade in trades:
                trend = trade.trend_1h or "N/A"
                score_bucket = get_level_score_bucket(trade.level_score)
                key = f"{trend}_{score_bucket}"
                combos[key].append(trade)
            
            for combo, combo_trades in sorted(combos.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
                if len(combo_trades) >= 3:
                    combo_wins = [t for t in combo_trades if calc_pnl_pct(t) > 0]
                    combo_pnl = sum(calc_pnl_pct(t) for t in combo_trades)
                    combo_wr = (len(combo_wins) / len(combo_trades) * 100) if combo_trades else 0
                    combo_avg = combo_pnl / len(combo_trades) if combo_trades else 0
                    print(f"   {combo}: {len(combo_trades)} сделок, Winrate: {combo_wr:.1f}%, Средний P&L: {combo_avg:+.2f}%")
            print()
            
            # Топ-5 лучших и худших сделок
            sorted_trades = sorted(trades, key=lambda t: calc_pnl_pct(t), reverse=True)
            print(f"📊 Топ-5 лучших сделок:")
            for i, trade in enumerate(sorted_trades[:5], 1):
                pnl = calc_pnl_pct(trade)
                print(f"   {i}. ID={trade.id} {trade.signal_type}, P&L: {pnl:+.2f}%, Score: {trade.level_score}, Trend: {trade.trend_1h}")
            print()
            
            print(f"📊 Топ-5 худших сделок:")
            for i, trade in enumerate(sorted_trades[-5:], 1):
                pnl = calc_pnl_pct(trade)
                print(f"   {i}. ID={trade.id} {trade.signal_type}, P&L: {pnl:+.2f}%, Score: {trade.level_score}, Trend: {trade.trend_1h}")
            print()
            
            # Распределение P&L
            print(f"📊 Распределение результатов:")
            pnl_ranges = {
                "> +2%": 0,
                "+1% to +2%": 0,
                "+0.5% to +1%": 0,
                "0% to +0.5%": 0,
                "-0.5% to 0%": 0,
                "-1% to -0.5%": 0,
                "< -1%": 0
            }
            
            for trade in trades:
                pnl = calc_pnl_pct(trade)
                if pnl > 2.0:
                    pnl_ranges["> +2%"] += 1
                elif pnl > 1.0:
                    pnl_ranges["+1% to +2%"] += 1
                elif pnl > 0.5:
                    pnl_ranges["+0.5% to +1%"] += 1
                elif pnl > 0:
                    pnl_ranges["0% to +0.5%"] += 1
                elif pnl > -0.5:
                    pnl_ranges["-0.5% to 0%"] += 1
                elif pnl > -1.0:
                    pnl_ranges["-1% to -0.5%"] += 1
                else:
                    pnl_ranges["< -1%"] += 1
            
            for range_name, count in pnl_ranges.items():
                if count > 0:
                    pct = (count / len(trades) * 100) if trades else 0
                    print(f"   {range_name}: {count} сделок ({pct:.1f}%)")
            print()
        
        # Сравнительная таблица
        print("=" * 100)
        print("📊 СРАВНИТЕЛЬНАЯ ТАБЛИЦА ТАЙМФРЕЙМОВ")
        print("=" * 100)
        print()
        print(f"{'Таймфрейм':<10} {'Сделок':<10} {'Winrate':<12} {'Ср. P&L':<12} {'Profit Factor':<15} {'Общий P&L':<12}")
        print("-" * 100)
        
        for timeframe in ['15m', '1h', '4h']:
            if timeframe not in trades_by_tf:
                continue
            
            trades = trades_by_tf[timeframe]
            wins = [t for t in trades if calc_pnl_pct(t) > 0]
            total_pnl = sum(calc_pnl_pct(t) for t in trades)
            total_win_pnl = sum(calc_pnl_pct(t) for t in wins)
            total_loss_pnl = sum(abs(calc_pnl_pct(t)) for t in trades if calc_pnl_pct(t) < 0)
            
            winrate = (len(wins) / len(trades) * 100.0) if trades else 0.0
            avg_pnl = total_pnl / len(trades) if trades else 0.0
            profit_factor = (total_win_pnl / total_loss_pnl) if total_loss_pnl > 0 else float('inf')
            
            print(f"{timeframe:<10} {len(trades):<10} {winrate:>6.1f}%{'':<5} {avg_pnl:>+6.2f}%{'':<5} {profit_factor:>6.2f}{'':<8} {total_pnl:>+6.2f}%")
        
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
    analyze_timeframe_performance()

