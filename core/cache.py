"""
Модуль для работы с Redis кэшем
"""

import json
import redis
from typing import Optional, Any
from datetime import timedelta
import logging
from core.config import settings

logger = logging.getLogger(__name__)

# Глобальное подключение к Redis
redis_client: Optional[redis.Redis] = None


def init_redis():
    """Инициализирует подключение к Redis"""
    global redis_client
    
    try:
        redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            db=settings.REDIS_DB,
            decode_responses=True,  # Автоматическая декодировка строк
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        
        # Проверка подключения
        redis_client.ping()
        logger.info(f"Redis подключен: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        redis_client = None
        return False


def get_redis() -> Optional[redis.Redis]:
    """Получает клиент Redis"""
    if redis_client is None:
        init_redis()
    return redis_client


class Cache:
    """Класс для работы с кэшем"""
    
    @staticmethod
    def get(key: str) -> Optional[Any]:
        """Получает значение из кэша"""
        try:
            client = get_redis()
            if client is None:
                return None
            
            value = client.get(key)
            if value is None:
                return None
            
            # Пытаемся распарсить JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            logger.error(f"Ошибка получения из кэша {key}: {e}")
            return None
    
    @staticmethod
    def set(key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Устанавливает значение в кэш"""
        try:
            client = get_redis()
            if client is None:
                return False
            
            # Сериализуем в JSON если это не строка
            if not isinstance(value, str):
                value = json.dumps(value, default=str)
            
            ttl = ttl or settings.CACHE_TTL
            client.setex(key, ttl, value)
            return True
            
        except Exception as e:
            logger.error(f"Ошибка установки в кэш {key}: {e}")
            return False
    
    @staticmethod
    def delete(key: str) -> bool:
        """Удаляет ключ из кэша"""
        try:
            client = get_redis()
            if client is None:
                return False
            
            client.delete(key)
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления из кэша {key}: {e}")
            return False
    
    @staticmethod
    def exists(key: str) -> bool:
        """Проверяет существование ключа"""
        try:
            client = get_redis()
            if client is None:
                return False
            
            return client.exists(key) > 0
            
        except Exception as e:
            logger.error(f"Ошибка проверки ключа {key}: {e}")
            return False
    
    @staticmethod
    def clear_pattern(pattern: str) -> int:
        """Удаляет все ключи по паттерну"""
        try:
            client = get_redis()
            if client is None:
                return 0
            
            keys = client.keys(pattern)
            if keys:
                return client.delete(*keys)
            return 0
            
        except Exception as e:
            logger.error(f"Ошибка очистки по паттерну {pattern}: {e}")
            return 0
    
    @staticmethod
    def get_or_set(key: str, callback, ttl: Optional[int] = None):
        """Получает значение из кэша или устанавливает через callback"""
        value = Cache.get(key)
        if value is not None:
            return value
        
        # Если нет в кэше, вызываем callback
        value = callback()
        Cache.set(key, value, ttl)
        return value


# Глобальный экземпляр кэша
cache = Cache()

