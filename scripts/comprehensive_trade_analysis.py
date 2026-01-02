#!/usr/bin/env python3
"""
Комплексный анализ торговых сделок с 10.12.2024
Детальный анализ по всем фильтрам и логам для выявления причин низкого винрейта
"""

import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import json

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


def analyze_by_filter(trades: List[Signal], filter_name: str, filter_func) -> Dict:
    """Анализирует сделки по заданному фильтру"""
    results = defaultdict(lambda: {
        'total': 0,
        'wins': 0,
        'losses': 0,
        'total_pnl': 0.0,
        'total_win_pnl': 0.0,
        'total_loss_pnl': 0.0,
        'trades': []
    })
    
    for trade in trades:
        filter_value = filter_func(trade)
        if filter_value is None:
            filter_value = "N/A"
        elif isinstance(filter_value, bool):
            filter_value = "True" if filter_value else "False"
        else:
            filter_value = str(filter_value)
        
        pnl = calc_pnl_pct(trade)
        results[filter_value]['total'] += 1
        results[filter_value]['total_pnl'] += pnl
        results[filter_value]['trades'].append({
            'id': trade.id,
            'pnl': pnl,
            'pair': trade.pair.symbol if trade.pair else "N/A"
        })
        
        if pnl > 0:
            results[filter_value]['wins'] += 1
            results[filter_value]['total_win_pnl'] += pnl
        elif pnl < 0:
            results[filter_value]['losses'] += 1
            results[filter_value]['total_loss_pnl'] += abs(pnl)
    
    # Вычисляем метрики
    for key in results:
        stats = results[key]
        if stats['total'] > 0:
            stats['winrate'] = (stats['wins'] / stats['total']) * 100.0
            stats['avg_pnl'] = stats['total_pnl'] / stats['total']
            stats['avg_win'] = stats['total_win_pnl'] / stats['wins'] if stats['wins'] > 0 else 0.0
            stats['avg_loss'] = stats['total_loss_pnl'] / stats['losses'] if stats['losses'] > 0 else 0.0
            stats['profit_factor'] = (stats['total_win_pnl'] / stats['total_loss_pnl']) if stats['total_loss_pnl'] > 0 else float('inf')
        else:
            stats['winrate'] = 0.0
            stats['avg_pnl'] = 0.0
            stats['avg_win'] = 0.0
            stats['avg_loss'] = 0.0
            stats['profit_factor'] = 0.0
    
    return dict(results)


def analyze_logs_for_trade(signal: Signal) -> Dict:
    """Анализирует логи по конкретной сделке"""
    logs = signal.live_logs if hasattr(signal, 'live_logs') else []
    
    analysis = {
        'total_logs': len(logs),
        'event_types': defaultdict(int),
        'statuses': defaultdict(int),
        'errors': [],
        'order_events': [],
        'status_changes': [],
        'key_messages': []
    }
    
    for log in logs:
        if log.event_type:
            analysis['event_types'][log.event_type] += 1
        if log.status:
            analysis['statuses'][log.status] += 1
        
        # Собираем ошибки
        if 'error' in log.message.lower() or 'fail' in log.message.lower():
            analysis['errors'].append({
                'time': log.created_at.isoformat() if log.created_at else None,
                'message': log.message,
                'details': log.details
            })
        
        # Собираем события по ордерам
        if 'order' in log.message.lower() or (log.event_type and 'order' in log.event_type.lower()):
            analysis['order_events'].append({
                'time': log.created_at.isoformat() if log.created_at else None,
                'message': log.message,
                'event_type': log.event_type,
                'status': log.status
            })
        
        # Собираем изменения статусов
        if log.status and log.status != 'NONE':
            analysis['status_changes'].append({
                'time': log.created_at.isoformat() if log.created_at else None,
                'status': log.status,
                'message': log.message
            })
        
        # Важные сообщения
        if any(keyword in log.message.lower() for keyword in ['filled', 'executed', 'closed', 'stop', 'take profit', 'cancel']):
            analysis['key_messages'].append({
                'time': log.created_at.isoformat() if log.created_at else None,
                'message': log.message,
                'event_type': log.event_type,
                'status': log.status
            })
    
    return analysis


def comprehensive_analysis():
    """Проводит комплексный анализ всех сделок с 10.12.2024"""
    
    print("=" * 100)
    print("КОМПЛЕКСНЫЙ АНАЛИЗ ТОРГОВЫХ СДЕЛОК")
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
        
        # Загружаем все закрытые сделки с реальными ордерами и реальным результатом
        # Фильтруем только сделки, где есть entry_price и exit_price (реально реализованные сделки)
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
                    # Убеждаемся, что есть реальный результат (не нулевой)
                    Signal.entry_price > 0,
                    Signal.exit_price > 0
                )
            )
            .options(joinedload(Signal.pair))
            .order_by(Signal.timestamp.asc())
            .all()
        )
        
        # Дополнительная фильтрация: убираем сделки без реального результата
        closed_trades = [
            t for t in closed_trades 
            if calc_pnl_pct(t) != 0.0  # Только сделки с реальным результатом (не безубыточные)
        ]
        
        # Загружаем логи для всех сделок
        trade_ids = [t.id for t in closed_trades]
        if trade_ids:
            logs = (
                session.query(SignalLiveLog)
                .filter(SignalLiveLog.signal_id.in_(trade_ids))
                .order_by(SignalLiveLog.created_at.asc())
                .all()
            )
            # Группируем логи по signal_id
            logs_by_signal = defaultdict(list)
            for log in logs:
                logs_by_signal[log.signal_id].append(log)
            
            # Присваиваем логи к сигналам
            for trade in closed_trades:
                trade.live_logs = logs_by_signal.get(trade.id, [])
        else:
            # Если нет сделок, инициализируем пустые логи
            for trade in closed_trades:
                trade.live_logs = []
        
        print(f"📊 Всего закрытых сделок с реальными ордерами: {len(closed_trades)}")
        print()
        
        if len(closed_trades) == 0:
            print("❌ Нет закрытых сделок за указанный период")
            return
        
        # Общая статистика
        print("=" * 100)
        print("1. ОБЩАЯ СТАТИСТИКА")
        print("=" * 100)
        print()
        
        wins = [t for t in closed_trades if calc_pnl_pct(t) > 0]
        losses = [t for t in closed_trades if calc_pnl_pct(t) < 0]
        breakeven = [t for t in closed_trades if calc_pnl_pct(t) == 0]
        
        total_pnl = sum(calc_pnl_pct(t) for t in closed_trades)
        total_win_pnl = sum(calc_pnl_pct(t) for t in wins)
        total_loss_pnl = sum(abs(calc_pnl_pct(t)) for t in losses)
        
        winrate = (len(wins) / len(closed_trades) * 100.0) if closed_trades else 0.0
        avg_win = (total_win_pnl / len(wins)) if wins else 0.0
        avg_loss = (total_loss_pnl / len(losses)) if losses else 0.0
        profit_factor = (total_win_pnl / total_loss_pnl) if total_loss_pnl > 0 else float('inf')
        avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
        
        print(f"✅ Прибыльных сделок: {len(wins)} ({len(wins)/len(closed_trades)*100:.1f}%)")
        print(f"❌ Убыточных сделок:  {len(losses)} ({len(losses)/len(closed_trades)*100:.1f}%)")
        print(f"⚪ Безубыточных:      {len(breakeven)} ({len(breakeven)/len(closed_trades)*100:.1f}%)")
        print()
        print(f"📈 Winrate:           {winrate:.2f}%")
        print(f"💰 Общий P&L:         {total_pnl:+.2f}%")
        print(f"📊 Средний P&L:       {avg_pnl:+.2f}%")
        print(f"✅ Средняя прибыль:   {avg_win:+.2f}%")
        print(f"❌ Средний убыток:    -{avg_loss:.2f}%")
        print(f"📉 Profit Factor:     {profit_factor:.2f}")
        print()
        
        # Анализ по фильтрам
        print("=" * 100)
        print("2. АНАЛИЗ ПО ТИПУ СИГНАЛА (LONG vs SHORT)")
        print("=" * 100)
        print()
        
        by_type = analyze_by_filter(closed_trades, "signal_type", lambda t: t.signal_type)
        for signal_type, stats in sorted(by_type.items()):
            print(f"📊 {signal_type}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
            print(f"   Winrate: {stats['winrate']:.2f}%")
            print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
            print(f"   Profit Factor: {stats['profit_factor']:.2f}")
            print()
        
        # Анализ по таймфреймам
        print("=" * 100)
        print("3. АНАЛИЗ ПО ТАЙМФРЕЙМАМ")
        print("=" * 100)
        print()
        
        by_timeframe = analyze_by_filter(closed_trades, "timeframe", lambda t: t.level_timeframe)
        for tf, stats in sorted(by_timeframe.items(), key=lambda x: x[1]['total'], reverse=True):
            print(f"📊 {tf}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
            print(f"   Winrate: {stats['winrate']:.2f}%")
            print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
            print(f"   Profit Factor: {stats['profit_factor']:.2f}")
            print()
        
        # Анализ по level_score
        print("=" * 100)
        print("4. АНАЛИЗ ПО LEVEL_SCORE (качество уровня)")
        print("=" * 100)
        print()
        
        by_score = analyze_by_filter(closed_trades, "level_score", lambda t: get_level_score_bucket(t.level_score))
        for score_bucket, stats in sorted(by_score.items()):
            print(f"📊 Score {score_bucket}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
            print(f"   Winrate: {stats['winrate']:.2f}%")
            print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
            print(f"   Profit Factor: {stats['profit_factor']:.2f}")
            if stats['total'] > 0:
                # Примеры сделок
                example_trades = sorted(stats['trades'], key=lambda x: x['pnl'], reverse=True)[:3]
                print(f"   Примеры: {', '.join([f'ID={t['id']} ({t['pnl']:+.2f}%)' for t in example_trades])}")
            print()
        
        # Анализ по тренду
        print("=" * 100)
        print("5. АНАЛИЗ ПО ТРЕНДУ (trend_1h)")
        print("=" * 100)
        print()
        
        by_trend = analyze_by_filter(closed_trades, "trend", lambda t: t.trend_1h)
        for trend, stats in sorted(by_trend.items(), key=lambda x: x[1]['total'], reverse=True):
            print(f"📊 {trend}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
            print(f"   Winrate: {stats['winrate']:.2f}%")
            print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
            print(f"   Profit Factor: {stats['profit_factor']:.2f}")
            print()
        
        # Анализ по типу уровня
        print("=" * 100)
        print("6. АНАЛИЗ ПО ТИПУ УРОВНЯ (support vs resistance)")
        print("=" * 100)
        print()
        
        by_level_type = analyze_by_filter(closed_trades, "level_type", lambda t: t.level_type)
        for level_type, stats in sorted(by_level_type.items()):
            print(f"📊 {level_type}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
            print(f"   Winrate: {stats['winrate']:.2f}%")
            print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
            print(f"   Profit Factor: {stats['profit_factor']:.2f}")
            print()
        
        # Анализ по Elder Screens
        print("=" * 100)
        print("7. АНАЛИЗ ПО ELDER SCREENS")
        print("=" * 100)
        print()
        
        by_elder_screen1 = analyze_by_filter(closed_trades, "elder_screen_1", lambda t: t.elder_screen_1_passed)
        print("📊 Elder Screen 1 (4H тренд):")
        for passed, stats in sorted(by_elder_screen1.items()):
            print(f"   Прошел ({passed}): Сделок: {stats['total']}, Winrate: {stats['winrate']:.2f}%, P&L: {stats['avg_pnl']:+.2f}%")
        print()
        
        by_elder_screen2 = analyze_by_filter(closed_trades, "elder_screen_2", lambda t: t.elder_screen_2_passed)
        print("📊 Elder Screen 2 (1H анализ):")
        for passed, stats in sorted(by_elder_screen2.items()):
            print(f"   Прошел ({passed}): Сделок: {stats['total']}, Winrate: {stats['winrate']:.2f}%, P&L: {stats['avg_pnl']:+.2f}%")
        print()
        
        # Анализ по test_count
        print("=" * 100)
        print("8. АНАЛИЗ ПО КОЛИЧЕСТВУ ТЕСТОВ УРОВНЯ")
        print("=" * 100)
        print()
        
        by_test_count = analyze_by_filter(closed_trades, "test_count", lambda t: str(t.test_count) if t.test_count else "N/A")
        for test_count, stats in sorted(by_test_count.items()):
            print(f"📊 Тестов: {test_count}:")
            print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
            print(f"   Winrate: {stats['winrate']:.2f}%")
            print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
            print()
        
        # Анализ по distance_percent (расстояние до уровня)
        print("=" * 100)
        print("9. АНАЛИЗ ПО РАССТОЯНИЮ ДО УРОВНЯ (distance_percent)")
        print("=" * 100)
        print()
        
        def get_distance_bucket(distance: Optional[float]) -> str:
            """Группирует distance_percent в бакеты"""
            if distance is None:
                return "N/A"
            if distance < 0.1:
                return "0-0.1%"
            elif distance < 0.3:
                return "0.1-0.3%"
            elif distance < 0.5:
                return "0.3-0.5%"
            elif distance < 0.7:
                return "0.5-0.7%"
            elif distance < 1.0:
                return "0.7-1.0%"
            else:
                return "1.0%+"
        
        by_distance = analyze_by_filter(closed_trades, "distance", lambda t: get_distance_bucket(t.distance_percent))
        for distance_bucket, stats in sorted(by_distance.items()):
            if stats['total'] >= 3:  # Минимум 3 сделки для статистики
                print(f"📊 {distance_bucket}:")
                print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
                print(f"   Winrate: {stats['winrate']:.2f}%")
                print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
                print(f"   Profit Factor: {stats['profit_factor']:.2f}")
                print()
        
        # Анализ по historical_touches
        print("=" * 100)
        print("10. АНАЛИЗ ПО ИСТОРИЧЕСКИМ КАСАНИЯМ УРОВНЯ (historical_touches)")
        print("=" * 100)
        print()
        
        def get_touches_bucket(touches: Optional[int]) -> str:
            """Группирует historical_touches в бакеты"""
            if touches is None:
                return "N/A"
            if touches < 3:
                return "0-2"
            elif touches < 5:
                return "3-4"
            elif touches < 10:
                return "5-9"
            elif touches < 20:
                return "10-19"
            else:
                return "20+"
        
        by_touches = analyze_by_filter(closed_trades, "touches", lambda t: get_touches_bucket(t.historical_touches))
        for touches_bucket, stats in sorted(by_touches.items()):
            if stats['total'] >= 3:
                print(f"📊 Касаний: {touches_bucket}:")
                print(f"   Сделок: {stats['total']}, Прибыльных: {stats['wins']}, Убыточных: {stats['losses']}")
                print(f"   Winrate: {stats['winrate']:.2f}%")
                print(f"   Средний P&L: {stats['avg_pnl']:+.2f}%")
                print(f"   Profit Factor: {stats['profit_factor']:.2f}")
                print()
        
        # Детальный анализ логов по убыточным сделкам
        print("=" * 100)
        print("11. ДЕТАЛЬНЫЙ АНАЛИЗ ЛОГОВ ПО УБЫТОЧНЫМ СДЕЛКАМ")
        print("=" * 100)
        print()
        
        # Сортируем убыточные сделки по размеру убытка
        worst_losses = sorted(losses, key=lambda t: calc_pnl_pct(t))[:20]  # Топ-20 худших
        
        print(f"🔍 Анализ топ-20 худших убыточных сделок:")
        print()
        
        for i, trade in enumerate(worst_losses, 1):
            pnl = calc_pnl_pct(trade)
            log_analysis = analyze_logs_for_trade(trade)
            
            print(f"{i}. Signal ID: {trade.id} | {trade.signal_type}")
            print(f"   P&L: {pnl:+.2f}%")
            print(f"   Entry: {trade.entry_price}, Exit: {trade.exit_price}")
            print(f"   Level Score: {trade.level_score}, Timeframe: {trade.level_timeframe}")
            print(f"   Trend: {trade.trend_1h}, Level Type: {trade.level_type}")
            print(f"   Distance: {trade.distance_percent}%, Historical Touches: {trade.historical_touches}")
            print(f"   Test Count: {trade.test_count}")
            print(f"   Логов: {log_analysis['total_logs']}")
            
            if log_analysis['errors']:
                print(f"   ⚠️ Ошибки ({len(log_analysis['errors'])}):")
                for err in log_analysis['errors'][:3]:  # Показываем первые 3
                    print(f"      - {err['message'][:100]}")
            
            if log_analysis['key_messages']:
                print(f"   📝 Ключевые события:")
                for msg in log_analysis['key_messages'][:5]:  # Показываем первые 5
                    print(f"      - {msg['message'][:100]}")
            
            print()
        
        # Анализ корреляций
        print("=" * 100)
        print("12. КОРРЕЛЯЦИОННЫЙ АНАЛИЗ")
        print("=" * 100)
        print()
        
        # Корреляция level_score и результата
        print("📊 Корреляция level_score и результата:")
        score_ranges = {
            "0-30": [],
            "30-40": [],
            "40-50": [],
            "50-60": [],
            "60-70": [],
            "70+": []
        }
        
        for trade in closed_trades:
            bucket = get_level_score_bucket(trade.level_score)
            if bucket in score_ranges:
                score_ranges[bucket].append(calc_pnl_pct(trade))
        
        for bucket, pnls in score_ranges.items():
            if pnls:
                avg_pnl = sum(pnls) / len(pnls)
                winrate = len([p for p in pnls if p > 0]) / len(pnls) * 100
                print(f"   {bucket}: {len(pnls)} сделок, Winrate: {winrate:.1f}%, Средний P&L: {avg_pnl:+.2f}%")
        print()
        
        # Анализ комбинаций фильтров
        print("=" * 100)
        print("13. АНАЛИЗ КОМБИНАЦИЙ ФИЛЬТРОВ")
        print("=" * 100)
        print()
        
        # Комбинация: тип сигнала + level_score
        print("📊 Комбинация: Тип сигнала + Level Score:")
        combinations = defaultdict(lambda: {'trades': [], 'wins': 0, 'losses': 0})
        
        for trade in closed_trades:
            key = f"{trade.signal_type}_{get_level_score_bucket(trade.level_score)}"
            combinations[key]['trades'].append(trade)
            pnl = calc_pnl_pct(trade)
            if pnl > 0:
                combinations[key]['wins'] += 1
            elif pnl < 0:
                combinations[key]['losses'] += 1
        
        for combo, data in sorted(combinations.items(), key=lambda x: len(x[1]['trades']), reverse=True):
            if len(data['trades']) >= 3:  # Минимум 3 сделки для статистики
                total = len(data['trades'])
                winrate = (data['wins'] / total * 100) if total > 0 else 0
                avg_pnl = sum(calc_pnl_pct(t) for t in data['trades']) / total
                print(f"   {combo}: {total} сделок, Winrate: {winrate:.1f}%, Средний P&L: {avg_pnl:+.2f}%")
        print()
        
        # Комбинация: тренд + level_score
        print("📊 Комбинация: Тренд + Level Score:")
        trend_score_combos = defaultdict(lambda: {'trades': [], 'wins': 0, 'losses': 0})
        
        for trade in closed_trades:
            trend = trade.trend_1h or "N/A"
            score_bucket = get_level_score_bucket(trade.level_score)
            key = f"{trend}_{score_bucket}"
            trend_score_combos[key]['trades'].append(trade)
            pnl = calc_pnl_pct(trade)
            if pnl > 0:
                trend_score_combos[key]['wins'] += 1
            elif pnl < 0:
                trend_score_combos[key]['losses'] += 1
        
        for combo, data in sorted(trend_score_combos.items(), key=lambda x: len(x[1]['trades']), reverse=True):
            if len(data['trades']) >= 3:
                total = len(data['trades'])
                winrate = (data['wins'] / total * 100) if total > 0 else 0
                avg_pnl = sum(calc_pnl_pct(t) for t in data['trades']) / total
                print(f"   {combo}: {total} сделок, Winrate: {winrate:.1f}%, Средний P&L: {avg_pnl:+.2f}%")
        print()
        
        # Комбинация: таймфрейм + тренд
        print("📊 Комбинация: Таймфрейм + Тренд:")
        tf_trend_combos = defaultdict(lambda: {'trades': [], 'wins': 0, 'losses': 0})
        
        for trade in closed_trades:
            tf = trade.level_timeframe or "N/A"
            trend = trade.trend_1h or "N/A"
            key = f"{tf}_{trend}"
            tf_trend_combos[key]['trades'].append(trade)
            pnl = calc_pnl_pct(trade)
            if pnl > 0:
                tf_trend_combos[key]['wins'] += 1
            elif pnl < 0:
                tf_trend_combos[key]['losses'] += 1
        
        for combo, data in sorted(tf_trend_combos.items(), key=lambda x: len(x[1]['trades']), reverse=True):
            if len(data['trades']) >= 3:
                total = len(data['trades'])
                winrate = (data['wins'] / total * 100) if total > 0 else 0
                avg_pnl = sum(calc_pnl_pct(t) for t in data['trades']) / total
                print(f"   {combo}: {total} сделок, Winrate: {winrate:.1f}%, Средний P&L: {avg_pnl:+.2f}%")
        print()
        
        # Выводы и рекомендации
        print("=" * 100)
        print("14. ВЫВОДЫ И РЕКОМЕНДАЦИИ")
        print("=" * 100)
        print()
        
        print("🔍 Ключевые находки:")
        print()
        
        # Находим лучшие и худшие фильтры (только технические показатели)
        all_filters = {
            'Тип сигнала': by_type,
            'Таймфрейм': by_timeframe,
            'Level Score': by_score,
            'Тренд': by_trend,
            'Тип уровня': by_level_type
        }
        
        best_filters = []
        worst_filters = []
        
        for filter_name, filter_data in all_filters.items():
            for key, stats in filter_data.items():
                if stats['total'] >= 5:  # Минимум 5 сделок
                    if stats['winrate'] >= 60 and stats['avg_pnl'] > 0:
                        best_filters.append((filter_name, key, stats))
                    elif stats['winrate'] < 40 and stats['avg_pnl'] < -0.5:
                        worst_filters.append((filter_name, key, stats))
        
        if best_filters:
            print("✅ ЛУЧШИЕ ФИЛЬТРЫ (Winrate >= 60%, P&L > 0):")
            for filter_name, key, stats in sorted(best_filters, key=lambda x: x[2]['winrate'], reverse=True)[:10]:
                print(f"   {filter_name}: {key} - Winrate: {stats['winrate']:.1f}%, P&L: {stats['avg_pnl']:+.2f}%, Сделок: {stats['total']}")
            print()
        
        if worst_filters:
            print("❌ ХУДШИЕ ФИЛЬТРЫ (Winrate < 40%, P&L < -0.5%):")
            for filter_name, key, stats in sorted(worst_filters, key=lambda x: x[2]['winrate'])[:10]:
                print(f"   {filter_name}: {key} - Winrate: {stats['winrate']:.1f}%, P&L: {stats['avg_pnl']:+.2f}%, Сделок: {stats['total']}")
            print()
        
        # Рекомендации
        print("💡 РЕКОМЕНДАЦИИ:")
        print()
        
        if winrate < 50:
            print(f"   ⚠️ Общий winrate ({winrate:.1f}%) ниже 50% - требуется улучшение фильтрации сигналов")
        
        if profit_factor < 1.0:
            print(f"   ⚠️ Profit Factor ({profit_factor:.2f}) < 1.0 - система убыточна")
        
        if avg_loss > avg_win * 1.5:
            print(f"   ⚠️ Средний убыток ({avg_loss:.2f}%) значительно больше средней прибыли ({avg_win:.2f}%)")
            print(f"      Рекомендуется пересмотреть управление рисками (stop-loss)")
        
        # Анализ level_score
        if by_score:
            best_score_bucket = max(by_score.items(), key=lambda x: x[1]['winrate'] if x[1]['total'] >= 5 else 0)
            worst_score_bucket = min(by_score.items(), key=lambda x: x[1]['winrate'] if x[1]['total'] >= 5 else 100)
            
            if best_score_bucket[1]['total'] >= 5:
                print(f"   ✅ Лучшие результаты при level_score в диапазоне {best_score_bucket[0]} (Winrate: {best_score_bucket[1]['winrate']:.1f}%)")
            
            if worst_score_bucket[1]['total'] >= 5:
                print(f"   ❌ Худшие результаты при level_score в диапазоне {worst_score_bucket[0]} (Winrate: {worst_score_bucket[1]['winrate']:.1f}%)")
                print(f"      Рекомендуется установить минимальный порог level_score для фильтрации сигналов")
        
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
    comprehensive_analysis()

