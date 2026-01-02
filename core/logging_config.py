"""
Утилита для настройки логирования с ротацией файлов
"""
import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path


def setup_logging_with_rotation(
    log_file: str,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB по умолчанию
    backup_count: int = 5,  # Хранить 5 резервных копий
    level: int = logging.INFO,
    format_string: str = '%(asctime)s - %(levelname)s - %(message)s',
    use_timed_rotation: bool = False,
    when: str = 'midnight',  # Для TimedRotatingFileHandler
    interval: int = 1,  # Для TimedRotatingFileHandler
) -> logging.Logger:
    """
    Настраивает логирование с ротацией файлов
    
    Args:
        log_file: Путь к файлу лога
        max_bytes: Максимальный размер файла перед ротацией (для RotatingFileHandler)
        backup_count: Количество резервных копий для хранения
        level: Уровень логирования
        format_string: Формат сообщений
        use_timed_rotation: Использовать TimedRotatingFileHandler вместо RotatingFileHandler
        when: Когда делать ротацию ('midnight', 'H', 'D', 'W0' и т.д.)
        interval: Интервал ротации (в комбинации с when)
    
    Returns:
        Настроенный logger
    """
    # Создаем директорию для логов если её нет
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Создаем форматтер
    formatter = logging.Formatter(format_string)
    
    # Выбираем тип ротации
    if use_timed_rotation:
        # Ротация по времени (например, каждый день в полночь)
        file_handler = TimedRotatingFileHandler(
            log_file,
            when=when,
            interval=interval,
            backupCount=backup_count,
            encoding='utf-8'
        )
    else:
        # Ротация по размеру файла
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
    
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Консольный handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Настраиваем root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Удаляем существующие handlers чтобы избежать дублирования
    root_logger.handlers.clear()
    
    # Добавляем новые handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger


def setup_analysis_logging() -> logging.Logger:
    """
    Настраивает логирование для анализа с ротацией по размеру (10 MB, 5 копий)
    """
    return setup_logging_with_rotation(
        log_file='logs/analysis.log',
        max_bytes=10 * 1024 * 1024,  # 10 MB
        backup_count=5,
        level=logging.INFO
    )


def setup_server_logging() -> logging.Logger:
    """
    Настраивает логирование для сервера с ротацией по размеру (10 MB, 5 копий)
    """
    return setup_logging_with_rotation(
        log_file='logs/server_multithreaded.log',
        max_bytes=10 * 1024 * 1024,  # 10 MB
        backup_count=5,
        level=logging.INFO
    )


def cleanup_old_logs(logs_dir: str = 'logs', days_to_keep: int = 7):
    """
    Удаляет старые лог-файлы старше указанного количества дней
    
    Args:
        logs_dir: Директория с логами
        days_to_keep: Количество дней для хранения логов
    """
    import time
    from pathlib import Path
    
    logs_path = Path(logs_dir)
    if not logs_path.exists():
        return
    
    current_time = time.time()
    cutoff_time = current_time - (days_to_keep * 24 * 60 * 60)
    
    deleted_count = 0
    deleted_size = 0
    
    for log_file in logs_path.glob('*.log*'):
        try:
            file_mtime = log_file.stat().st_mtime
            file_size = log_file.stat().st_size
            
            if file_mtime < cutoff_time:
                deleted_size += file_size
                log_file.unlink()
                deleted_count += 1
                print(f"Удален старый лог: {log_file.name} ({file_size / 1024 / 1024:.2f} MB)")
        except Exception as e:
            print(f"Ошибка при удалении {log_file}: {e}")
    
    if deleted_count > 0:
        print(f"Очистка завершена: удалено {deleted_count} файлов, освобождено {deleted_size / 1024 / 1024:.2f} MB")
    else:
        print("Старые лог-файлы не найдены")

