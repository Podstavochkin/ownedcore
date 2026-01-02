#!/usr/bin/env python3
"""
Диагностический скрипт для анализа причин блокировки уровней Elder's Triple Screen System.
Показывает статистику по каждому условию блокировки.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.database as database
from core.models import Level, TradingPair
from core.analysis_engine import analysis_engine
from sqlalchemy.orm import joinedload
import asyncio
from collections import defaultdict

async def diagnose_elder_screens():
    """Анализирует причины блокировки уровней"""
    if not database.init_database():
        print("❌ Не удалось инициализировать БД")
        return
    
    session = database.SessionLocal()
    try:
        # Получаем все активные уровни
        levels = session.query(Level).options(
            joinedload(Level.pair)
        ).filter(Level.is_active == True).all()
        
        print(f"\n📊 Всего активных уровней: {len(levels)}\n")
        
        if not levels:
            print("Нет активных уровней для анализа")
            return
        
        # Статистика блокировок
        stats = {
            'total': len(levels),
            'passed': 0,
            'blocked_screen_1': 0,
            'blocked_screen_2': 0,
            'not_checked': 0,
            'screen_1_reasons': defaultdict(int),
            'screen_2_reasons': defaultdict(int),
            'btc_trend_distribution': defaultdict(int),
            'pair_trend_distribution': defaultdict(int),
            'rsi_distribution': defaultdict(int),
            'macd_blocked': 0,
            'price_approach_blocked': 0,
        }
        
        # Получаем текущую цену BTC для контекста
        try:
            btc_candles_4h = await analysis_engine.fetch_ohlcv('BTC/USDT', '4h', 50)
            if btc_candles_4h:
                btc_trend = await analysis_engine.get_btc_market_trend_4h()
                print(f"📈 Текущий BTC тренд (4H): {btc_trend}\n")
                stats['btc_trend_distribution']['current'] = btc_trend
        except Exception as e:
            print(f"⚠️ Не удалось получить BTC тренд: {e}\n")
        
        # Анализируем каждый уровень
        for i, level in enumerate(levels):
            if not level.pair:
                continue
            
            pair_symbol = level.pair.symbol
            meta = level.meta_data or {}
            metadata = meta.get('metadata', {}) or {}
            elder_screens = metadata.get('elder_screens', {})
            
            if not elder_screens:
                stats['not_checked'] += 1
                continue
            
            final_decision = elder_screens.get('final_decision', 'UNKNOWN')
            
            if final_decision == 'PASSED':
                stats['passed'] += 1
            elif final_decision == 'BLOCKED_SCREEN_1':
                stats['blocked_screen_1'] += 1
                screen_1 = elder_screens.get('screen_1', {})
                blocked_reason = screen_1.get('blocked_reason', 'Неизвестная причина')
                stats['screen_1_reasons'][blocked_reason] += 1
                
                # Анализируем детали Screen 1
                checks = screen_1.get('checks', {})
                btc_trend = checks.get('btc_trend', 'UNKNOWN')
                stats['btc_trend_distribution'][btc_trend] += 1
                
                pair_trend_data = checks.get('pair_trend', {})
                if isinstance(pair_trend_data, dict):
                    pair_trend = pair_trend_data.get('trend', 'UNKNOWN')
                    stats['pair_trend_distribution'][pair_trend] += 1
                elif isinstance(pair_trend_data, str):
                    stats['pair_trend_distribution'][pair_trend_data] += 1
                    
            elif final_decision == 'BLOCKED_SCREEN_2':
                stats['blocked_screen_2'] += 1
                screen_2 = elder_screens.get('screen_2', {})
                blocked_reason = screen_2.get('blocked_reason', 'Неизвестная причина')
                stats['screen_2_reasons'][blocked_reason] += 1
                
                # Анализируем детали Screen 2
                checks = screen_2.get('checks', {})
                
                # RSI
                rsi_check = checks.get('rsi', {})
                if rsi_check.get('blocked'):
                    rsi_value = rsi_check.get('value', 0)
                    stats['rsi_distribution'][f"blocked_{rsi_value:.0f}"] += 1
                
                # MACD
                macd_check = checks.get('macd', {})
                if macd_check.get('blocked'):
                    stats['macd_blocked'] += 1
                
                # Price approach
                approach_check = checks.get('price_approach', {})
                if checks.get('approach_blocked'):
                    stats['price_approach_blocked'] += 1
        
        # Выводим статистику
        print("=" * 80)
        print("📊 СТАТИСТИКА БЛОКИРОВКИ УРОВНЕЙ")
        print("=" * 80)
        print(f"\nВсего уровней: {stats['total']}")
        print(f"✅ Прошли все экраны: {stats['passed']} ({stats['passed']/stats['total']*100:.1f}%)")
        print(f"❌ Заблокированы Экран 1: {stats['blocked_screen_1']} ({stats['blocked_screen_1']/stats['total']*100:.1f}%)")
        print(f"❌ Заблокированы Экран 2: {stats['blocked_screen_2']} ({stats['blocked_screen_2']/stats['total']*100:.1f}%)")
        print(f"⚠️ Не проверены: {stats['not_checked']} ({stats['not_checked']/stats['total']*100:.1f}%)")
        
        if stats['blocked_screen_1'] > 0:
            print("\n" + "=" * 80)
            print("🔍 ПРИЧИНЫ БЛОКИРОВКИ ЭКРАН 1:")
            print("=" * 80)
            for reason, count in sorted(stats['screen_1_reasons'].items(), key=lambda x: x[1], reverse=True):
                print(f"  • {reason}: {count} уровней ({count/stats['blocked_screen_1']*100:.1f}%)")
            
            print("\n📈 Распределение BTC трендов (для заблокированных):")
            btc_items = [(k, v) for k, v in stats['btc_trend_distribution'].items() if k != 'current' and isinstance(v, int)]
            for trend, count in sorted(btc_items, key=lambda x: x[1], reverse=True):
                print(f"  • {trend}: {count} уровней")
            
            print("\n📊 Распределение трендов пар (для заблокированных):")
            pair_items = [(k, v) for k, v in stats['pair_trend_distribution'].items() if isinstance(v, int)]
            for trend, count in sorted(pair_items, key=lambda x: x[1], reverse=True):
                print(f"  • {trend}: {count} уровней")
        
        if stats['blocked_screen_2'] > 0:
            print("\n" + "=" * 80)
            print("🔍 ПРИЧИНЫ БЛОКИРОВКИ ЭКРАН 2:")
            print("=" * 80)
            for reason, count in sorted(stats['screen_2_reasons'].items(), key=lambda x: x[1], reverse=True):
                print(f"  • {reason}: {count} уровней ({count/stats['blocked_screen_2']*100:.1f}%)")
            
            print(f"\n📊 Детали блокировок Экран 2:")
            print(f"  • RSI блокировка: {sum(1 for k in stats['rsi_distribution'] if k.startswith('blocked_'))} уровней")
            print(f"  • MACD блокировка: {stats['macd_blocked']} уровней")
            print(f"  • Направление подхода: {stats['price_approach_blocked']} уровней")
            
            if stats['rsi_distribution']:
                print("\n📈 Распределение заблокированных RSI:")
                for rsi_key, count in sorted(stats['rsi_distribution'].items(), key=lambda x: x[1], reverse=True):
                    print(f"  • {rsi_key}: {count} уровней")
        
        print("\n" + "=" * 80)
        print("💡 РЕКОМЕНДАЦИИ:")
        print("=" * 80)
        
        if stats['blocked_screen_1'] > stats['total'] * 0.5:
            print("⚠️ Более 50% уровней блокируются Экран 1!")
            print("   Возможные причины:")
            print("   1. Слишком строгие условия для BTC тренда")
            print("   2. Слишком строгие условия для тренда пары")
            print("   3. Большинство уровней имеют level_score < 60")
            print("   Рекомендация: Рассмотреть смягчение условий или снижение порога level_score")
        
        if stats['blocked_screen_2'] > stats['total'] * 0.3:
            print("⚠️ Более 30% уровней блокируются Экран 2!")
            print("   Возможные причины:")
            print("   1. Слишком строгие условия для RSI (75/25)")
            print("   2. Слишком строгие условия для MACD")
            print("   3. Проблемы с проверкой направления подхода")
            print("   Рекомендация: Рассмотреть смягчение условий осцилляторов")
        
        if stats['passed'] < stats['total'] * 0.05:
            print("⚠️ Менее 5% уровней проходят все экраны!")
            print("   Это указывает на слишком строгие условия фильтрации.")
            print("   Рекомендация: Пересмотреть все условия блокировки")
        
        print("\n")
        
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(diagnose_elder_screens())

