"""
Конфигурация приложения
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    try:
        load_dotenv(ENV_PATH, override=False)
    except PermissionError as exc:
        logger.warning("Не удалось прочитать .env (%s): %s", ENV_PATH, exc)
    except Exception as exc:
        logger.warning("Ошибка загрузки .env (%s): %s", ENV_PATH, exc)


def _load_demo_settings() -> Dict[str, Any]:
    """Загружает настройки live-торговли из config/demo_trading_settings.json."""
    defaults: Dict[str, Any] = {
        "auto_trading_enabled": True,
        "order_size_usdt": 1000,
        "order_type": "market",
        "take_profit_percent": 1.5,
        "stop_loss_percent": 0.5,
        "symbol_suffix": ":USDT",
        "quantity_precision": 3,
        "time_in_force": "GTC",
        "leverage": 2,
        "position_idx": None,
        "api_base_url": None,
        "demo_header_enabled": False,
        "market_type": "contract",
        "market_entry_threshold_pct": 0.0,
    }

    settings_path = Path(__file__).resolve().parent.parent / "config" / "demo_trading_settings.json"
    if not settings_path.exists():
        return defaults

    try:
        with open(settings_path, "r", encoding="utf-8") as file:
            loaded = json.load(file)
            if isinstance(loaded, dict):
                defaults.update({k: loaded[k] for k in defaults.keys() if k in loaded})
    except Exception as exc:
        logger.warning("Не удалось загрузить demo_trading_settings.json: %s", exc)
    return defaults


DEMO_SETTINGS = _load_demo_settings()


class Settings(BaseSettings):
    """Настройки приложения"""
    
    # База данных
    DB_USER: str = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD', 'postgres')
    DB_HOST: str = os.getenv('DB_HOST', 'localhost')
    DB_PORT: str = os.getenv('DB_PORT', '5432')
    DB_NAME: str = os.getenv('DB_NAME', 'ownedcore')
    
    # Redis
    REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_PASSWORD: Optional[str] = os.getenv('REDIS_PASSWORD', None)
    REDIS_DB: int = int(os.getenv('REDIS_DB', '0'))
    
    # Celery
    CELERY_BROKER_URL: str = os.getenv('CELERY_BROKER_URL', 'amqp://guest:guest@localhost:5672//')
    CELERY_RESULT_BACKEND: str = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    
    # API
    API_HOST: str = os.getenv('API_HOST', '0.0.0.0')
    API_PORT: int = int(os.getenv('API_PORT', '8000'))
    API_DEBUG: bool = os.getenv('API_DEBUG', 'False').lower() == 'true'
    
    # Безопасность
    SECRET_KEY: str = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    ALGORITHM: str = 'HS256'
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Binance API
    BINANCE_API_KEY: Optional[str] = os.getenv('BINANCE_API_KEY', None)
    BINANCE_API_SECRET: Optional[str] = os.getenv('BINANCE_API_SECRET', None)
    
    # Bybit API (для демо и реальной торговли)
    BYBIT_API_KEY: Optional[str] = os.getenv('BYBIT_API_KEY', None)
    BYBIT_API_SECRET: Optional[str] = os.getenv('BYBIT_API_SECRET', None)
    BYBIT_DEMO: bool = os.getenv('BYBIT_DEMO', 'false').lower() == 'true'
    
    # Настройки live-торговли (из config/demo_trading_settings.json)
    DEMO_AUTO_TRADING_ENABLED: bool = bool(DEMO_SETTINGS.get('auto_trading_enabled', True))
    DEMO_ORDER_SIZE_USDT: float = float(DEMO_SETTINGS.get('order_size_usdt', 50))
    DEMO_ORDER_TYPE: str = str(DEMO_SETTINGS.get('order_type', 'market')).lower()
    DEMO_TAKE_PROFIT_PERCENT: float = float(DEMO_SETTINGS.get('take_profit_percent', 1.5))
    DEMO_STOP_LOSS_PERCENT: float = float(DEMO_SETTINGS.get('stop_loss_percent', 0.5))
    DEMO_SYMBOL_SUFFIX: str = str(DEMO_SETTINGS.get('symbol_suffix', ':USDT'))
    DEMO_QUANTITY_PRECISION: int = int(DEMO_SETTINGS.get('quantity_precision', 3))
    DEMO_TIME_IN_FORCE: Optional[str] = DEMO_SETTINGS.get('time_in_force', 'GTC')
    
    DEMO_LEVERAGE: Optional[float] = (
        float(DEMO_SETTINGS.get('leverage')) if DEMO_SETTINGS.get('leverage') is not None else None
    )
    DEMO_POSITION_IDX: Optional[int] = (
        int(DEMO_SETTINGS.get('position_idx'))
        if isinstance(DEMO_SETTINGS.get('position_idx'), (int, float)) and DEMO_SETTINGS.get('position_idx') is not None
        else None
    )
    DEMO_BYBIT_API_BASE_URL: Optional[str] = DEMO_SETTINGS.get('api_base_url')
    DEMO_BYBIT_DEMO_HEADER: bool = bool(DEMO_SETTINGS.get('demo_header_enabled', False))
    DEMO_MARKET_TYPE: str = str(DEMO_SETTINGS.get('market_type', 'contract'))
    DEMO_MARKET_ENTRY_THRESHOLD_PCT: float = float(DEMO_SETTINGS.get('market_entry_threshold_pct', 0.0))
    DEMO_ORDER_CANCEL_DEVIATION_PCT: float = float(DEMO_SETTINGS.get('order_cancel_deviation_pct', 1.5))
    
    # Кэширование
    CACHE_TTL: int = int(os.getenv('CACHE_TTL', '600'))  # 10 минут
    
    # Логирование
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Настройки фильтрации сигналов (на основе анализа от 10.12.2024)
    # ОСЛАБЛЕНО 31.12.2024: слишком строгие фильтры блокировали все сигналы
    SIGNAL_FILTER_MIN_LEVEL_SCORE: float = float(os.getenv('SIGNAL_FILTER_MIN_LEVEL_SCORE', '25.0'))  # Было 30.0
    SIGNAL_FILTER_BLOCK_SIDEWAYS: bool = os.getenv('SIGNAL_FILTER_BLOCK_SIDEWAYS', 'true').lower() == 'true'
    SIGNAL_FILTER_MAX_TEST_COUNT: int = int(os.getenv('SIGNAL_FILTER_MAX_TEST_COUNT', '20'))
    SIGNAL_FILTER_MAX_DISTANCE_PCT: float = float(os.getenv('SIGNAL_FILTER_MAX_DISTANCE_PCT', '0.7'))  # Оптимизировано 31.12.2024: должно быть меньше порога "цена ушла"
    SIGNAL_FILTER_15M_MIN_SCORE: float = float(os.getenv('SIGNAL_FILTER_15M_MIN_SCORE', '30.0'))  # Было 35.0
    SIGNAL_FILTER_1H_MIN_SCORE: float = float(os.getenv('SIGNAL_FILTER_1H_MIN_SCORE', '25.0'))  # Было 30.0
    SIGNAL_FILTER_4H_MIN_SCORE: float = float(os.getenv('SIGNAL_FILTER_4H_MIN_SCORE', '25.0'))  # Было 30.0
    SIGNAL_FILTER_ENABLE_PRIORITY: bool = os.getenv('SIGNAL_FILTER_ENABLE_PRIORITY', 'true').lower() == 'true'
    
# Глобальный экземпляр настроек
settings = Settings()


def get_timeframe_min_score(timeframe: str) -> float:
    """Получает минимальный score для таймфрейма"""
    if timeframe == '15m':
        return settings.SIGNAL_FILTER_15M_MIN_SCORE
    elif timeframe == '1h':
        return settings.SIGNAL_FILTER_1H_MIN_SCORE
    elif timeframe == '4h':
        return settings.SIGNAL_FILTER_4H_MIN_SCORE
    return settings.SIGNAL_FILTER_MIN_LEVEL_SCORE

