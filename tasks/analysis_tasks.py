"""
Celery задачи для анализа данных
"""

import sys
import os
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.celery_app import celery_app
from core.database import SessionLocal, init_database
from core.models import TradingPair, AnalysisData
from core.cache import cache, init_redis
from core.analysis_engine import analysis_engine, TRADING_PAIRS
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


@celery_app.task(name='tasks.analysis_tasks.analyze_all_pairs', queue='analysis')
def analyze_all_pairs():
    """Анализирует все торговые пары в фоне"""
    try:
        logger.info("Начинаем фоновый анализ всех торговых пар...")
        
        # Инициализируем БД если нужно
        if not init_database():
            logger.error("Не удалось инициализировать БД")
            return None
        
        # Инициализируем Redis
        init_redis()
        
        # Импортируем SessionLocal после инициализации
        from core.database import SessionLocal
        
        # Создаем сессию БД
        session = SessionLocal()
        
        try:
            # Запускаем анализ через существующий движок
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            results = loop.run_until_complete(analysis_engine.analyze_all_pairs())
            
            # Сохраняем результаты в кэш
            cache.set('analysis:all_pairs', results, ttl=600)  # 10 минут
            
            # Сохраняем результаты в БД
            for pair_symbol, pair_data in results.get('results', {}).items():
                pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
                if not pair:
                    continue
                
                # Создаем запись анализа для 1H таймфрейма
                analysis = AnalysisData(
                    pair_id=pair.id,
                    timeframe='1h',
                    current_price=pair_data.get('current_price'),
                    trend=pair_data.get('trend_1h'),
                    price_change_24h=pair_data.get('price_change_24h'),
                    volume_24h=pair_data.get('volume_24h'),
                    analyzed_at=datetime.now()
                )
                session.add(analysis)
            
            session.commit()
            
            logger.info(f"Анализ завершен: {results.get('pairs_analyzed', 0)} пар")
            
            return {
                'status': 'success',
                'pairs_analyzed': results.get('pairs_analyzed', 0),
                'total_signals': results.get('total_signals', 0),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа: {e}")
            session.rollback()
            return {'status': 'error', 'error': str(e)}
        
        finally:
            session.close()
            loop.close()
            
    except Exception as e:
        logger.error(f"Критическая ошибка в задаче анализа: {e}")
        return {'status': 'error', 'error': str(e)}


@celery_app.task(name='tasks.analysis_tasks.analyze_pair', queue='analysis')
def analyze_pair(pair_symbol: str):
    """Анализирует одну торговую пару"""
    try:
        logger.info(f"Анализ пары: {pair_symbol}")
        
        if not init_database():
            return None
        
        # Импортируем SessionLocal после инициализации
        from core.database import SessionLocal
        
        session = SessionLocal()
        
        try:
            pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
            if not pair:
                return {'status': 'error', 'error': f'Pair {pair_symbol} not found'}
            
            # Анализируем через движок
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(analysis_engine.analyze_pair(pair_symbol))
            
            # Кэшируем результат
            cache.set(f'analysis:pair:{pair_symbol}', result, ttl=300)  # 5 минут
            
            return {
                'status': 'success',
                'pair': pair_symbol,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }
            
        finally:
            session.close()
            loop.close()
            
    except Exception as e:
        logger.error(f"Ошибка анализа пары {pair_symbol}: {e}")
        return {'status': 'error', 'error': str(e)}

