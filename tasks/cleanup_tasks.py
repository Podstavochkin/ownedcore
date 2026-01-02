"""
Периодические задачи для очистки старых данных
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.celery_app import celery_app
from core.analysis_engine import analysis_engine
import logging

logger = logging.getLogger(__name__)


@celery_app.task(name='tasks.cleanup_tasks.cleanup_outdated_levels_periodic', queue='analysis')
def cleanup_outdated_levels_periodic():
    """
    Периодическая задача для очистки неактуальных уровней.
    Запускается автоматически через Celery Beat.
    """
    try:
        import asyncio
        logger.info("🧹 Запуск периодической очистки неактуальных уровней...")
        result = asyncio.run(analysis_engine.cleanup_outdated_levels())
        
        if result.get('status') == 'success':
            removed = result.get('removed_count', 0)
            logger.info(f"✅ Очистка завершена: удалено {removed} неактуальных уровней")
        else:
            logger.warning(f"⚠️ Очистка завершена с предупреждениями: {result.get('message', 'Unknown error')}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке уровней: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': str(e)
        }


@celery_app.task(name='tasks.cleanup_tasks.cleanup_old_signals_periodic', queue='analysis')
def cleanup_old_signals_periodic():
    """
    Периодическая задача для удаления старых закрытых сигналов.
    Удаляет сигналы старше 30 дней.
    """
    try:
        from core.database import SessionLocal, init_database
        from core.models import Signal, SignalLiveLog
        
        if not init_database():
            logger.error("❌ БД не инициализирована")
            return {'status': 'error', 'message': 'БД не инициализирована'}
        
        session = SessionLocal()
        
        try:
            # Удаляем закрытые сигналы старше 30 дней
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
            
            old_signals = (
                session.query(Signal)
                .filter(
                    Signal.status == 'CLOSED',
                    Signal.timestamp < cutoff_date
                )
                .all()
            )
            
            signal_ids = [s.id for s in old_signals]
            signals_count = len(signal_ids)
            
            if signals_count == 0:
                logger.info("ℹ️ Нет старых сигналов для удаления")
                return {'status': 'success', 'removed_count': 0}
            
            # Удаляем логи
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
            logger.info(f"✅ Удалено старых сигналов: {signals_count}, логов: {logs_deleted}")
            
            return {
                'status': 'success',
                'removed_signals': signals_count,
                'removed_logs': logs_deleted
            }
            
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Ошибка удаления старых сигналов: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в cleanup_old_signals_periodic: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}

