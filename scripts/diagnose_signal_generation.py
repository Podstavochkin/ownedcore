#!/usr/bin/env python3
"""
Диагностика генерации сигналов
Проверяет, почему сигналы не генерируются
"""

import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime, timedelta, timezone
from core.database import init_database, SessionLocal
from core.models import Level, TradingPair, Signal
from core.analysis_engine import analysis_engine
from core.config import settings
from sqlalchemy import func
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_timeframe_min_score(timeframe: str) -> float:
    """Получает минимальный score для таймфрейма"""
    if timeframe == '15m':
        return settings.SIGNAL_FILTER_15M_MIN_SCORE
    elif timeframe == '1h':
        return settings.SIGNAL_FILTER_1H_MIN_SCORE
    elif timeframe == '4h':
        return settings.SIGNAL_FILTER_4H_MIN_SCORE
    return settings.SIGNAL_FILTER_MIN_LEVEL_SCORE


def diagnose_signal_generation():
    """Диагностика генерации сигналов"""
    
    if not init_database():
        print("❌ Не удалось инициализировать базу данных")
        return
    
    from core.database import SessionLocal
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("🔍 ДИАГНОСТИКА ГЕНЕРАЦИИ СИГНАЛОВ")
        print("=" * 80)
        print()
        
        # 1. Проверяем текущие настройки фильтров
        print("📋 ТЕКУЩИЕ НАСТРОЙКИ ФИЛЬТРОВ:")
        print(f"   SIGNAL_FILTER_MIN_LEVEL_SCORE: {settings.SIGNAL_FILTER_MIN_LEVEL_SCORE}")
        print(f"   SIGNAL_FILTER_15M_MIN_SCORE: {settings.SIGNAL_FILTER_15M_MIN_SCORE}")
        print(f"   SIGNAL_FILTER_1H_MIN_SCORE: {settings.SIGNAL_FILTER_1H_MIN_SCORE}")
        print(f"   SIGNAL_FILTER_4H_MIN_SCORE: {settings.SIGNAL_FILTER_4H_MIN_SCORE}")
        print(f"   SIGNAL_FILTER_BLOCK_SIDEWAYS: {settings.SIGNAL_FILTER_BLOCK_SIDEWAYS}")
        print(f"   SIGNAL_FILTER_MAX_DISTANCE_PCT: {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%")
        print(f"   SIGNAL_FILTER_MAX_TEST_COUNT: {settings.SIGNAL_FILTER_MAX_TEST_COUNT}")
        print()
        
        # 2. Получаем все активные уровни
        from sqlalchemy.orm import joinedload
        levels = db.query(Level).options(joinedload(Level.pair)).filter(Level.is_active == True).all()
        print(f"📊 АКТИВНЫЕ УРОВНИ: {len(levels)}")
        print()
        
        if len(levels) == 0:
            print("⚠️  Нет активных уровней! Это может быть причиной отсутствия сигналов.")
            return
        
        # 3. Анализируем уровни по таймфреймам
        levels_by_tf = {}
        for level in levels:
            meta = level.meta_data or {}
            timeframe = meta.get('timeframe', '15m')
            if timeframe not in levels_by_tf:
                levels_by_tf[timeframe] = []
            levels_by_tf[timeframe].append(level)
        
        print("📈 РАСПРЕДЕЛЕНИЕ УРОВНЕЙ ПО ТАЙМФРЕЙМАМ:")
        for tf, tf_levels in levels_by_tf.items():
            print(f"   {tf}: {len(tf_levels)} уровней")
        print()
        
        # 4. Проверяем уровни на соответствие фильтрам
        print("🔍 АНАЛИЗ УРОВНЕЙ ПО ФИЛЬТРАМ:")
        print()
        
        blocked_by_score = {tf: 0 for tf in ['15m', '1h', '4h']}
        blocked_by_distance = 0
        blocked_by_test_count = 0
        blocked_by_sideways = 0
        passed_filters = 0
        
        # Получаем текущие цены для пар
        from core.ohlcv_store import ohlcv_store
        
        for level in levels:
            meta = level.meta_data or {}
            timeframe = meta.get('timeframe', '15m')
            score = meta.get('level_score') or meta.get('score') or 0
            test_count = level.test_count or 0
            
            # Получаем текущую цену
            pair = level.pair
            if not pair:
                continue
            
            try:
                candles = ohlcv_store.get_ohlcv(pair.symbol, '1h', 1)
                if not candles or len(candles) == 0:
                    continue
                current_price = candles[-1]['close']
            except:
                continue
            
            # Проверяем расстояние
            price_diff = abs(current_price - level.price) / current_price * 100
            price_diff_pct = price_diff
            
            # Получаем тренд (упрощенно)
            try:
                candles_1h = ohlcv_store.get_ohlcv(pair.symbol, '1h', 50)
                if candles_1h and len(candles_1h) >= 20:
                    trend_1h = analysis_engine.get_pair_trend_1h(candles_1h).get('trend', 'UNKNOWN')
                else:
                    trend_1h = 'UNKNOWN'
            except:
                trend_1h = 'UNKNOWN'
            
            # Проверяем фильтры
            min_score = get_timeframe_min_score(timeframe)
            level_dict = {'score': score, 'timeframe': timeframe, 'test_count': test_count}
            
            should_block, reason = analysis_engine.should_block_signal_by_filters(
                level=level_dict,
                trend_1h=trend_1h,
                timeframe=timeframe,
                price_distance_pct=price_diff_pct,
                test_count=test_count
            )
            
            if should_block:
                if 'level_score' in reason:
                    if timeframe in blocked_by_score:
                        blocked_by_score[timeframe] += 1
                elif 'расстояние' in reason:
                    blocked_by_distance += 1
                elif 'тестов' in reason:
                    blocked_by_test_count += 1
                elif 'боковой' in reason:
                    blocked_by_sideways += 1
            else:
                passed_filters += 1
        
        print(f"   ✅ Прошли фильтры: {passed_filters}")
        print(f"   ❌ Заблокированы по score:")
        for tf, count in blocked_by_score.items():
            print(f"      {tf}: {count}")
        print(f"   ❌ Заблокированы по расстоянию (> {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%): {blocked_by_distance}")
        print(f"   ❌ Заблокированы по test_count (> {settings.SIGNAL_FILTER_MAX_TEST_COUNT}): {blocked_by_test_count}")
        print(f"   ❌ Заблокированы по боковому тренду: {blocked_by_sideways}")
        print()
        
        # 5. Проверяем последние сигналы
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=2)
        recent_signals = db.query(Signal).filter(
            Signal.timestamp >= cutoff_time
        ).order_by(Signal.timestamp.desc()).limit(10).all()
        
        print(f"📊 ПОСЛЕДНИЕ СИГНАЛЫ (за 2 дня): {len(recent_signals)}")
        if recent_signals:
            for signal in recent_signals[:5]:
                print(f"   {signal.timestamp.strftime('%Y-%m-%d %H:%M')} - {signal.pair.symbol if signal.pair else 'N/A'} - {signal.signal_type} @ {signal.level_price}")
        else:
            print("   ⚠️  Нет сигналов за последние 2 дня!")
        print()
        
        # 6. Рекомендации
        print("💡 РЕКОМЕНДАЦИИ:")
        if passed_filters == 0:
            print("   ⚠️  КРИТИЧНО: Ни один уровень не проходит фильтры!")
            print(f"   → Рассмотрите снижение SIGNAL_FILTER_MAX_DISTANCE_PCT с {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}% до 1.0%")
            print(f"   → Рассмотрите снижение минимальных score:")
            print(f"      - 15m: {settings.SIGNAL_FILTER_15M_MIN_SCORE} → 30.0")
            print(f"      - 1h: {settings.SIGNAL_FILTER_1H_MIN_SCORE} → 25.0")
            print(f"      - 4h: {settings.SIGNAL_FILTER_4H_MIN_SCORE} → 25.0")
        elif passed_filters < 5:
            print(f"   ⚠️  Только {passed_filters} уровней проходят фильтры - это очень мало")
            print(f"   → Рассмотрите ослабление фильтров")
        else:
            print(f"   ✅ {passed_filters} уровней проходят фильтры - это нормально")
        
        if blocked_by_distance > len(levels) * 0.5:
            print(f"   ⚠️  {blocked_by_distance} уровней заблокированы по расстоянию (> {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%)")
            print(f"   → SIGNAL_FILTER_MAX_DISTANCE_PCT={settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}% слишком строгий")
            print(f"   → Рекомендуется увеличить до 1.0%")
        
        print()
        print("=" * 80)
        
    finally:
        db.close()
        SessionLocal.remove()


if __name__ == "__main__":
    diagnose_signal_generation()

