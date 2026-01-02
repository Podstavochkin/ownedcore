#!/usr/bin/env python3
"""
Скрипт для миграции данных из JSON файлов в PostgreSQL базу данных
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.database import init_database, SessionLocal, create_tables
from core.models import TradingPair, Signal, Level
from core.config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate_trading_pairs(session):
    """Мигрирует торговые пары"""
    from core.analysis_engine import TRADING_PAIRS
    
    logger.info("Миграция торговых пар...")
    
    for symbol in TRADING_PAIRS:
        # Проверяем существование
        existing = session.query(TradingPair).filter_by(symbol=symbol).first()
        if existing:
            logger.debug(f"Пара {symbol} уже существует")
            continue
        
        pair = TradingPair(
            symbol=symbol,
            exchange='binance',
            enabled=True
        )
        session.add(pair)
        logger.info(f"Добавлена пара: {symbol}")
    
    session.commit()
    logger.info("Торговые пары мигрированы")


def migrate_signals(session):
    """Мигрирует сигналы из JSON файлов"""
    signals_dir = Path("signals")
    
    if not signals_dir.exists():
        logger.warning("Директория signals не найдена")
        return
    
    logger.info("Миграция сигналов...")
    
    # Получаем все JSON файлы с сигналами
    signal_files = list(signals_dir.glob("signals_*.json"))
    
    total_signals = 0
    
    for signal_file in signal_files:
        logger.info(f"Обработка файла: {signal_file.name}")
        
        try:
            with open(signal_file, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            if not isinstance(signals_data, list):
                logger.warning(f"Файл {signal_file.name} не содержит список сигналов")
                continue
            
            for signal_dict in signals_data:
                try:
                    # Получаем или создаем пару
                    pair_symbol = signal_dict.get('pair')
                    if not pair_symbol:
                        continue
                    
                    pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
                    if not pair:
                        logger.warning(f"Пара {pair_symbol} не найдена, пропускаем сигнал")
                        continue
                    
                    # Проверяем существование сигнала (по timestamp и pair)
                    timestamp_str = signal_dict.get('timestamp')
                    if timestamp_str:
                        try:
                            if timestamp_str.endswith('Z'):
                                timestamp_str = timestamp_str[:-1]
                            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', ''))
                        except:
                            timestamp = datetime.now()
                    else:
                        timestamp = datetime.now()
                    
                    # Проверяем дубликаты
                    existing = session.query(Signal).filter_by(
                        pair_id=pair.id,
                        timestamp=timestamp,
                        level_price=signal_dict.get('level_price', 0)
                    ).first()
                    
                    if existing:
                        continue
                    
                    # Создаем сигнал
                    signal = Signal(
                        pair_id=pair.id,
                        signal_type=signal_dict.get('signal_type', 'LONG'),
                        level_price=signal_dict.get('level_price', 0),
                        entry_price=signal_dict.get('entry_price'),
                        current_price=signal_dict.get('current_price'),
                        timestamp=timestamp,
                        trend_1h=signal_dict.get('1h_trend'),
                        level_type=signal_dict.get('level_type'),
                        test_count=signal_dict.get('test_count', 1),
                        pnl=signal_dict.get('pnl', 0),
                        pnl_percent=signal_dict.get('pnl_percent', 0),
                        status=signal_dict.get('status', 'ACTIVE'),
                        notes=signal_dict.get('notes'),
                        metadata=signal_dict
                    )
                    
                    session.add(signal)
                    total_signals += 1
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки сигнала: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Ошибка обработки файла {signal_file.name}: {e}")
            continue
    
    session.commit()
    logger.info(f"Мигрировано сигналов: {total_signals}")


def migrate_levels(session):
    """Мигрирует уровни из JSON файлов"""
    levels_dir = Path("levels")
    
    if not levels_dir.exists():
        logger.warning("Директория levels не найдена")
        return
    
    logger.info("Миграция уровней...")
    
    # Обрабатываем active_levels.json
    active_levels_file = levels_dir / "active_levels.json"
    
    if active_levels_file.exists():
        try:
            with open(active_levels_file, 'r', encoding='utf-8') as f:
                levels_data = json.load(f)
            
            total_levels = 0
            
            for pair_symbol, levels_list in levels_data.items():
                # Получаем пару
                pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
                if not pair:
                    logger.warning(f"Пара {pair_symbol} не найдена")
                    continue
                
                if not isinstance(levels_list, list):
                    continue
                
                for level_dict in levels_list:
                    try:
                        # Проверяем дубликаты
                        existing = session.query(Level).filter_by(
                            pair_id=pair.id,
                            price=level_dict.get('price', 0),
                            level_type=level_dict.get('type', 'support')
                        ).first()
                        
                        if existing:
                            # Обновляем существующий уровень
                            existing.test_count = level_dict.get('test_count', existing.test_count)
                            existing.is_active = level_dict.get('is_active', True)
                            existing.last_touch = datetime.now()
                            continue
                        
                        # Создаем новый уровень
                        level = Level(
                            pair_id=pair.id,
                            price=level_dict.get('price', 0),
                            level_type=level_dict.get('type', 'support'),
                            timeframe=level_dict.get('timeframe', '15m'),
                            test_count=level_dict.get('test_count', 0),
                            strength=level_dict.get('strength', 'MEDIUM'),
                            is_active=level_dict.get('is_active', True),
                            first_touch=datetime.now(),
                            last_touch=datetime.now(),
                            metadata=level_dict
                        )
                        
                        session.add(level)
                        total_levels += 1
                        
                    except Exception as e:
                        logger.error(f"Ошибка обработки уровня: {e}")
                        continue
            
            session.commit()
            logger.info(f"Мигрировано уровней: {total_levels}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки active_levels.json: {e}")


def main():
    """Основная функция миграции"""
    logger.info("Начало миграции данных из JSON в PostgreSQL")
    
    # Инициализируем базу данных
    if not init_database():
        logger.error("Не удалось инициализировать базу данных")
        return
    
    # Проверяем что SessionLocal инициализирован
    from core.database import SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal не инициализирован. Проверьте подключение к БД.")
        return
    
    # Создаем таблицы если их нет
    create_tables()
    
    # Создаем сессию
    session = SessionLocal()
    
    try:
        # Мигрируем данные
        migrate_trading_pairs(session)
        migrate_signals(session)
        migrate_levels(session)
        
        logger.info("Миграция завершена успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка миграции: {e}")
        session.rollback()
        raise
    
    finally:
        session.close()


if __name__ == '__main__':
    main()

