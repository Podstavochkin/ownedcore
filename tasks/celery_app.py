"""
Celery приложение для фоновых задач
"""

from celery import Celery
from core.config import settings
import logging

logger = logging.getLogger(__name__)

# Создаем Celery приложение
celery_app = Celery(
    'ownedcore',
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        'tasks.analysis_tasks',
        'tasks.signals_tasks',
        'tasks.level_touch_tasks',
        'tasks.demo_trading_tasks',
        'tasks.ohlcv_tasks',
        'tasks.chart_patterns_tasks',
        'tasks.cleanup_tasks',
    ]
)

# Конфигурация Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 минут
    task_soft_time_limit=25 * 60,  # 25 минут
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    # Результаты задач хранятся 24 часа
    result_expires=3600 * 24,
    # Периодические задачи (Celery Beat) - автоматический анализ
    beat_schedule={
        'analyze-all-pairs-periodic': {
            'task': 'tasks.analysis_tasks.analyze_all_pairs',
            'schedule': 300.0,  # Каждые 5 минут (300 секунд)
        },
        'update-signals-pnl-periodic': {
            'task': 'tasks.signals_tasks.update_signals_pnl',
            'schedule': 30.0,  # Каждые 30 секунд для более оперативного обновления
        },
        'check-level-touches-periodic': {
            'task': 'tasks.level_touch_tasks.check_level_touches',
            'schedule': 30.0,  # Каждые 30 секунд - для проверки ближайших уровней (<1%)
        },
        'watch-waiting-signals-periodic': {
            'task': 'tasks.demo_trading_tasks.watch_waiting_signals',
            'schedule': 30.0,  # Каждые 30 секунд проверяем сигналы в WAITING_FOR_PRICE
        },
        'update-current-candles-periodic': {
            'task': 'tasks.ohlcv_tasks.update_current_candles',
            'schedule': 60.0,  # Каждую минуту обновляем текущие свечи
        },
        'check-and-fill-gaps-periodic': {
            'task': 'tasks.ohlcv_tasks.check_and_fill_gaps',
            'schedule': 21600.0,  # Каждые 6 часов проверяем и заполняем пропуски
        },
        'ensure-historical-data-periodic': {
            'task': 'tasks.ohlcv_tasks.ensure_historical_data',
            'schedule': 43200.0,  # Каждые 12 часов проверяем наличие исторических данных
        },
        'detect-chart-patterns-periodic': {
            'task': 'tasks.chart_patterns_tasks.detect_chart_patterns_periodic',
            'schedule': 900.0,  # Каждые 15 минут детектируем ценовые фигуры
        },
        'deactivate-old-chart-patterns-periodic': {
            'task': 'tasks.chart_patterns_tasks.deactivate_old_chart_patterns',
            'schedule': 86400.0,  # Раз в сутки деактивируем старые фигуры (старше 30 дней)
        },
        'cleanup-outdated-levels-periodic': {
            'task': 'tasks.cleanup_tasks.cleanup_outdated_levels_periodic',
            'schedule': 3600.0,  # Каждый час очищаем неактуальные уровни
        },
        'cleanup-old-signals-periodic': {
            'task': 'tasks.cleanup_tasks.cleanup_old_signals_periodic',
            'schedule': 86400.0,  # Раз в сутки удаляем старые закрытые сигналы (старше 30 дней)
        },
        'cleanup-old-logs-periodic': {
            'task': 'tasks.cleanup_tasks.cleanup_old_logs_periodic',
            'schedule': 86400.0,  # Раз в сутки удаляем старые лог-файлы (старше 7 дней)
        },
    },
)

# Оптимизация для производительности
celery_app.conf.task_routes = {
    'tasks.analysis_tasks.*': {'queue': 'analysis'},
    'tasks.signals_tasks.*': {'queue': 'signals'},
    'tasks.level_touch_tasks.*': {'queue': 'signals'},
    'tasks.demo_trading_tasks.*': {'queue': 'signals'},
    'tasks.ohlcv_tasks.*': {'queue': 'analysis'},
    'tasks.chart_patterns_tasks.*': {'queue': 'analysis'},
    'tasks.cleanup_tasks.*': {'queue': 'analysis'},
}

# Логирование
celery_app.conf.worker_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
celery_app.conf.worker_task_log_format = '[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s'

if __name__ == '__main__':
    celery_app.start()

