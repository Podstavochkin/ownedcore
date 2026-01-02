#!/usr/bin/env python3
"""
Анализ статистики торговли за сегодня
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Signal, TradingPair
from sqlalchemy import func, and_
from datetime import datetime, timezone, timedelta
import asyncio

def analyze_today_statistics():
    """Анализирует статистику торговли за сегодня"""
    init_database()
    from core.database import SessionLocal
    db = SessionLocal()
    try:
        print("=" * 80)
        print("АНАЛИЗ СТАТИСТИКИ ЗА СЕГОДНЯ")
        print("=" * 80)
        print()
        
        # Определяем начало и конец дня (Московское время)
        now_moscow = datetime.now(timezone(timedelta(hours=3)))  # UTC+3
        today_start = now_moscow.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start + timedelta(days=1)
        
        # Конвертируем в UTC для БД
        today_start_utc = today_start.astimezone(timezone.utc)
        today_end_utc = today_end.astimezone(timezone.utc)
        
        print(f"📅 Период анализа:")
        print(f"   Начало: {today_start.strftime('%d.%m.%Y %H:%M:%S')} МСК")
        print(f"   Конец: {today_end.strftime('%d.%m.%Y %H:%M:%S')} МСК")
        print()
        
        # Получаем все сигналы за сегодня
        signals_today = db.query(Signal).join(TradingPair).filter(
            and_(
                Signal.timestamp >= today_start_utc,
                Signal.timestamp < today_end_utc,
                Signal.archived == False
            )
        ).order_by(Signal.timestamp.asc()).all()
        
        print(f"📊 ВСЕГО СИГНАЛОВ ЗА СЕГОДНЯ: {len(signals_today)}")
        print()
        
        if len(signals_today) == 0:
            print("❌ Нет сигналов за сегодня")
            return
        
        # Анализ по типам сигналов
        long_signals = [s for s in signals_today if s.signal_type == 'LONG']
        short_signals = [s for s in signals_today if s.signal_type == 'SHORT']
        
        print(f"📈 LONG сигналов: {len(long_signals)}")
        print(f"📉 SHORT сигналов: {len(short_signals)}")
        print()
        
        # Анализ по статусам
        active_signals = [s for s in signals_today if s.status == 'ACTIVE']
        closed_signals = [s for s in signals_today if s.status == 'CLOSED']
        open_signals = [s for s in signals_today if s.status == 'OPEN']
        
        print(f"🟢 ACTIVE: {len(active_signals)}")
        print(f"🔴 CLOSED: {len(closed_signals)}")
        print(f"🟡 OPEN: {len(open_signals)}")
        print()
        
        # КРИТИЧНО: Анализируем только РЕАЛЬНЫЕ сделки (с demo_order_id)
        # Теоретические расчеты (result_fixed) не отражают реальность!
        real_closed = [s for s in closed_signals if s.demo_order_id is not None and s.entry_price and s.exit_price]
        
        print("=" * 80)
        print("АНАЛИЗ РЕАЛЬНЫХ ЗАКРЫТЫХ СДЕЛОК")
        print("=" * 80)
        print()
        print(f"⚠️ ВАЖНО: Анализ только РЕАЛЬНЫХ сделок (с demo_order_id)")
        print(f"   Теоретические расчеты (result_fixed) не учитываются!")
        print()
        print(f"📊 Реальных закрытых сделок: {len(real_closed)}")
        
        if len(real_closed) == 0:
            print("❌ Нет реальных закрытых сделок за сегодня")
            print("   Все сигналы имеют demo_order_id=None (не были открыты на бирже)")
            return
        
        # Расчет реального P&L на основе entry_price и exit_price
        profitable = []
        losing = []
        total_pnl_percent = 0.0
        commission = 0.07  # 0.035% вход + 0.035% выход
        
        for signal in real_closed:
            entry = float(signal.entry_price)
            exit_price = float(signal.exit_price)
            
            # Рассчитываем реальный P&L
            if signal.signal_type == 'LONG':
                move_pct = ((exit_price - entry) / entry) * 100.0
            else:  # SHORT
                move_pct = ((entry - exit_price) / entry) * 100.0
            
            # Учитываем комиссию
            net_pct = move_pct - commission
            
            total_pnl_percent += net_pct
            
            if net_pct > 0:
                profitable.append(signal)
            else:
                losing.append(signal)
        
        print(f"✅ Прибыльных: {len(profitable)} ({len(profitable)/len(real_closed)*100:.1f}%)")
        print(f"❌ Убыточных: {len(losing)} ({len(losing)/len(real_closed)*100:.1f}%)")
        print()
        
        print(f"💰 ОБЩИЙ РЕАЛЬНЫЙ РЕЗУЛЬТАТ:")
        print(f"   P&L в %: {total_pnl_percent:+.2f}%")
        print(f"   (с учетом комиссии {commission}%)")
        print()
        
        # Анализ по парам
        print("=" * 80)
        print("АНАЛИЗ ПО ПАРАМ")
        print("=" * 80)
        print()
        
        pairs_stats = {}
        for signal in real_closed:
            pair_symbol = signal.pair.symbol if signal.pair else "UNKNOWN"
            if pair_symbol not in pairs_stats:
                pairs_stats[pair_symbol] = {
                    'total': 0,
                    'profitable': 0,
                    'losing': 0,
                    'pnl_percent': 0.0
                }
            pairs_stats[pair_symbol]['total'] += 1
            
            # Рассчитываем реальный P&L
            entry = float(signal.entry_price)
            exit_price = float(signal.exit_price)
            if signal.signal_type == 'LONG':
                move_pct = ((exit_price - entry) / entry) * 100.0
            else:  # SHORT
                move_pct = ((entry - exit_price) / entry) * 100.0
            net_pct = move_pct - commission
            
            if net_pct > 0:
                pairs_stats[pair_symbol]['profitable'] += 1
            else:
                pairs_stats[pair_symbol]['losing'] += 1
            pairs_stats[pair_symbol]['pnl_percent'] += net_pct
        
        # Сортируем по количеству сделок
        sorted_pairs = sorted(pairs_stats.items(), key=lambda x: x[1]['total'], reverse=True)
        
        for pair_symbol, stats in sorted_pairs[:10]:  # Топ 10 пар
            winrate = (stats['profitable'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"📊 {pair_symbol}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['profitable']}, Убыточных: {stats['losing']}")
            print(f"   Winrate: {winrate:.1f}%, P&L: {stats['pnl_percent']:+.2f}%")
            print()
        
        # Детальный анализ всех реальных закрытых сделок
        print("=" * 80)
        print("ДЕТАЛЬНЫЙ АНАЛИЗ ВСЕХ РЕАЛЬНЫХ ЗАКРЫТЫХ СДЕЛОК")
        print("=" * 80)
        print()
        
        for signal in real_closed:
            pair_symbol = signal.pair.symbol if signal.pair else "UNKNOWN"
            timestamp_moscow = signal.timestamp.astimezone(timezone(timedelta(hours=3)))
            
            # Рассчитываем реальный P&L
            entry = float(signal.entry_price)
            exit_price = float(signal.exit_price)
            if signal.signal_type == 'LONG':
                move_pct = ((exit_price - entry) / entry) * 100.0
            else:  # SHORT
                move_pct = ((entry - exit_price) / entry) * 100.0
            net_pct = move_pct - commission
            
            result_emoji = "✅" if net_pct > 0 else "❌"
            
            print(f"{result_emoji} {pair_symbol} {signal.signal_type}")
            print(f"   Время: {timestamp_moscow.strftime('%H:%M:%S')} МСК")
            print(f"   Entry: {entry:.6f}, Exit: {exit_price:.6f}")
            print(f"   Движение: {move_pct:+.2f}%, Комиссия: -{commission:.2f}%")
            print(f"   Реальный P&L: {net_pct:+.2f}%")
            if signal.max_profit:
                print(f"   Макс. прибыль: {signal.max_profit:+.2f}%")
            if signal.max_drawdown:
                print(f"   Макс. просадка: {signal.max_drawdown:+.2f}%")
            print()
        
        # Анализ активных сигналов
        if active_signals:
            print("=" * 80)
            print("АКТИВНЫЕ СИГНАЛЫ")
            print("=" * 80)
            print()
            
            for signal in active_signals[:10]:  # Первые 10
                pair_symbol = signal.pair.symbol if signal.pair else "UNKNOWN"
                timestamp_moscow = signal.timestamp.astimezone(timezone(timedelta(hours=3)))
                print(f"⏳ {pair_symbol} {signal.signal_type} @ {signal.level_price:.6f}")
                print(f"   Время создания: {timestamp_moscow.strftime('%H:%M:%S')} МСК")
                if signal.demo_status:
                    print(f"   Статус: {signal.demo_status}")
                print()
        
        # Вердикт
        print("=" * 80)
        print("ВЕРДИКТ")
        print("=" * 80)
        print()
        
        if len(real_closed) == 0:
            print("⚠️ Нет реальных закрытых сделок за сегодня")
            print(f"   Активных сигналов: {len(active_signals)}")
            print(f"   Открытых позиций: {len(open_signals)}")
        else:
            winrate = len(profitable) / len(real_closed) * 100
            
            if winrate >= 60 and total_pnl_percent > 0:
                verdict = "✅ ОТЛИЧНЫЙ ДЕНЬ"
                verdict_desc = f"Высокий winrate ({winrate:.1f}%) и положительный P&L ({total_pnl_percent:+.2f}%)"
            elif winrate >= 50 and total_pnl_percent > 0:
                verdict = "✅ ХОРОШИЙ ДЕНЬ"
                verdict_desc = f"Положительный winrate ({winrate:.1f}%) и прибыль ({total_pnl_percent:+.2f}%)"
            elif winrate >= 50 and total_pnl_percent < 0:
                verdict = "⚠️ НЕЙТРАЛЬНЫЙ ДЕНЬ"
                verdict_desc = f"Winrate хороший ({winrate:.1f}%), но общий P&L отрицательный ({total_pnl_percent:+.2f}%)"
            elif winrate < 50 and total_pnl_percent > 0:
                verdict = "⚠️ СЛОЖНЫЙ ДЕНЬ"
                verdict_desc = f"Низкий winrate ({winrate:.1f}%), но прибыль есть ({total_pnl_percent:+.2f}%)"
            else:
                verdict = "❌ ПЛОХОЙ ДЕНЬ"
                verdict_desc = f"Низкий winrate ({winrate:.1f}%) и убыток ({total_pnl_percent:+.2f}%)"
            
            print(f"{verdict}")
            print(f"   {verdict_desc}")
            print()
            print(f"📊 Статистика (ТОЛЬКО РЕАЛЬНЫЕ СДЕЛКИ):")
            print(f"   Всего реальных сделок: {len(real_closed)}")
            print(f"   Прибыльных: {len(profitable)} ({len(profitable)/len(real_closed)*100:.1f}%)")
            print(f"   Убыточных: {len(losing)} ({len(losing)/len(real_closed)*100:.1f}%)")
            print(f"   Реальный P&L: {total_pnl_percent:+.2f}%")
            print()
            print(f"⚠️ ВАЖНО: Это РЕАЛЬНЫЕ данные с биржи, не теоретические расчеты!")
        
        print()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    analyze_today_statistics()

