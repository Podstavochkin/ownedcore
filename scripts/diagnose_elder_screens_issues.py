#!/usr/bin/env python3
"""
Диагностический скрипт для проверки проблем с Elder's Screens:
1. Почему RSI и MACD равны 0
2. Почему так мало готовых сигналов из 350 уровней
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import init_database
from core.models import Level, TradingPair
from core.analysis_engine import analysis_engine
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def diagnose_issues():
    """Диагностика проблем с Elder's Screens"""
    try:
        if not init_database():
            logger.error("Не удалось инициализировать базу данных")
            return
        
        from core.database import SessionLocal
        session = SessionLocal()
        try:
            # Получаем все активные уровни
            levels = session.query(Level).filter(Level.is_active == True).limit(20).all()
            logger.info(f"Проверяем {len(levels)} активных уровней...")
            
            ready_count = 0
            blocked_screen1_count = 0
            blocked_screen2_count = 0
            rsi_zero_count = 0
            macd_zero_count = 0
            no_oscillators_count = 0
            
            for level in levels:
                pair = level.pair.symbol if level.pair else 'N/A'
                meta = level.meta_data or {}
                metadata = meta.get('metadata', {}) or {}
                elder_screens = metadata.get('elder_screens')
                
                if not elder_screens:
                    logger.info(f"Уровень {pair} @ {level.price}: Elder's Screens отсутствуют")
                    continue
                
                screen_1 = elder_screens.get('screen_1', {})
                screen_2 = elder_screens.get('screen_2', {})
                final_decision = elder_screens.get('final_decision', 'NOT_CHECKED')
                
                # Проверяем готовность
                if final_decision == 'PASSED':
                    ready_count += 1
                elif final_decision == 'BLOCKED_SCREEN_1':
                    blocked_screen1_count += 1
                elif final_decision == 'BLOCKED_SCREEN_2':
                    blocked_screen2_count += 1
                
                # Проверяем RSI и MACD
                checks_2 = screen_2.get('checks', {})
                oscillators = checks_2.get('oscillators', {})
                rsi_check = checks_2.get('rsi', {})
                macd_check = checks_2.get('macd', {})
                
                if oscillators.get('error'):
                    no_oscillators_count += 1
                    logger.warning(f"Уровень {pair} @ {level.price}: Ошибка расчета осцилляторов: {oscillators.get('error')}")
                else:
                    rsi = rsi_check.get('value') if rsi_check else oscillators.get('rsi')
                    macd = macd_check.get('macd') if macd_check else oscillators.get('macd')
                    
                    if rsi is not None:
                        if rsi == 0.0:
                            rsi_zero_count += 1
                            logger.warning(f"Уровень {pair} @ {level.price}: RSI = 0.00 (подозрительно!)")
                        else:
                            logger.info(f"Уровень {pair} @ {level.price}: RSI = {rsi:.2f}")
                    else:
                        logger.warning(f"Уровень {pair} @ {level.price}: RSI = None")
                    
                    if macd is not None:
                        if macd == 0.0:
                            macd_zero_count += 1
                            logger.warning(f"Уровень {pair} @ {level.price}: MACD = 0.0000 (подозрительно!)")
                        else:
                            logger.info(f"Уровень {pair} @ {level.price}: MACD = {macd:.4f}")
                    else:
                        logger.warning(f"Уровень {pair} @ {level.price}: MACD = None")
                
                # Проверяем, почему уровень не готов
                if final_decision != 'PASSED':
                    logger.info(f"Уровень {pair} @ {level.price}: {final_decision}")
                    if not screen_1.get('passed'):
                        logger.info(f"  Экран 1: {screen_1.get('blocked_reason', 'N/A')}")
                    if not screen_2.get('passed'):
                        logger.info(f"  Экран 2: {screen_2.get('blocked_reason', 'N/A')}")
            
            # Итоговая статистика
            logger.info("\n" + "="*60)
            logger.info("ИТОГОВАЯ СТАТИСТИКА:")
            logger.info(f"  Всего проверено уровней: {len(levels)}")
            logger.info(f"  Готовых к сигналу: {ready_count}")
            logger.info(f"  Заблокировано Экран 1: {blocked_screen1_count}")
            logger.info(f"  Заблокировано Экран 2: {blocked_screen2_count}")
            logger.info(f"  RSI = 0.00: {rsi_zero_count}")
            logger.info(f"  MACD = 0.0000: {macd_zero_count}")
            logger.info(f"  Ошибки расчета осцилляторов: {no_oscillators_count}")
            logger.info("="*60)
            
            # Тестируем расчет осцилляторов на реальных данных
            logger.info("\nТестируем расчет осцилляторов на реальных данных...")
            test_pairs = ['FTM/USDT', 'MKR/USDT', 'BTC/USDT']
            for pair_symbol in test_pairs:
                try:
                    candles_1h = await analysis_engine.fetch_ohlcv(pair_symbol, '1h', 50)
                    if candles_1h and len(candles_1h) >= 26:
                        oscillators = analysis_engine.calculate_oscillators(candles_1h)
                        logger.info(f"{pair_symbol}: RSI={oscillators.get('rsi')}, MACD={oscillators.get('macd')}, Signal={oscillators.get('macd_signal')}")
                        if oscillators.get('error'):
                            logger.error(f"{pair_symbol}: Ошибка расчета: {oscillators.get('error')}")
                    else:
                        logger.warning(f"{pair_symbol}: Недостаточно свечей 1H ({len(candles_1h) if candles_1h else 0})")
                except Exception as e:
                    logger.error(f"{pair_symbol}: Ошибка: {e}")
            
        finally:
            session.close()
    except Exception as e:
        logger.error(f"Ошибка диагностики: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(diagnose_issues())

