"""
Модуль для работы с базой данных PostgreSQL
"""

import os
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import logging

logger = logging.getLogger(__name__)

# Базовый класс для моделей
Base = declarative_base()

# Глобальные переменные
engine = None
SessionLocal = None
session_factory = None


def get_database_url():
    """Получает URL базы данных из переменных окружения"""
    # Значения по умолчанию для разработки
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'ownedcore')
    
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def init_database():
    """Инициализирует подключение к базе данных"""
    global engine, SessionLocal, session_factory
    
    # Если уже инициализирован, возвращаем True
    if engine is not None and SessionLocal is not None:
        return True
    
    try:
        database_url = get_database_url()
        
        # Создаем engine с пулом соединений
        # Уменьшаем pool_size чтобы избежать "too many clients"
        engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=5,  # Уменьшено с 10 до 5
            max_overflow=10,  # Уменьшено с 20 до 10
            pool_pre_ping=True,  # Проверка соединений перед использованием
            pool_recycle=3600,  # Переиспользование соединений каждый час
            echo=False,  # Логирование SQL запросов (False для production)
            connect_args={
                "connect_timeout": 10,
                "application_name": "OwnedCore"
            }
        )
        
        # Создаем session factory
        session_factory = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False
        )
        
        # Scoped session для thread-safety
        # ВАЖНО: используем globals() для явного обновления глобальной переменной
        globals()['SessionLocal'] = scoped_session(session_factory)
        SessionLocal = globals()['SessionLocal']  # Также обновляем локальную ссылку
        
        # Получаем параметры для логирования
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'ownedcore')
        logger.info(f"База данных инициализирована: {db_host}:{db_port}/{db_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {e}")
        return False


def get_db():
    """Получает сессию базы данных (для dependency injection)"""
    if SessionLocal is None:
        logger.error("SessionLocal не инициализирован. Вызовите init_database() сначала.")
        raise RuntimeError("Database not initialized")
    
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка в сессии БД: {e}")
        raise
    finally:
        db.close()
        # Удаляем сессию из scope для scoped_session
        SessionLocal.remove()


def create_tables():
    """Создает все таблицы в базе данных"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Таблицы созданы успешно")
        return True
    except Exception as e:
        logger.error(f"Ошибка создания таблиц: {e}")
        return False


def drop_tables():
    """Удаляет все таблицы из базы данных (ОСТОРОЖНО!)"""
    try:
        Base.metadata.drop_all(bind=engine)
        logger.info("Таблицы удалены")
        return True
    except Exception as e:
        logger.error(f"Ошибка удаления таблиц: {e}")
        return False


# Инициализация при импорте
if __name__ != "__main__":
    # Инициализация будет вызвана явно через init_database()
    pass

