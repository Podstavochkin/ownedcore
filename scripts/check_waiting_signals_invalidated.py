#!/usr/bin/env python3
"""
Скрипт для проверки всех сигналов в статусе WAITING_FOR_PRICE
и обновления их статусов, если они стали неактивными (пробитие уровня или отклонение >2%).
"""

import sys
import os

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
import logging

import core.database as database
from core.models import Signal
from core.trading.demo_trade_executor import demo_trade_executor
from core.trading.bybit_demo_client import bybit_demo_client
from core.trading.live_trade_logger import log_signal_event

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_waiting_signals():
    """Проверяет все WAITING_FOR_PRICE сигналы и обновляет их статусы, если они неактивны."""
    
    if not database.init_database() or database.SessionLocal is None:
        logger.error("❌ База данных недоступна")
        return False
    
    session = database.SessionLocal()
    now = datetime.now(timezone.utc)
    
    try:
        # Находим все активные сигналы в статусе WAITING_FOR_PRICE
        waiting_signals = (
            session.query(Signal)
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status == "WAITING_FOR_PRICE",
            )
            .all()
        )
        
        logger.info(f"🔍 Найдено {len(waiting_signals)} сигналов в статусе WAITING_FOR_PRICE")
        
        if len(waiting_signals) == 0:
            logger.info("✅ Нет сигналов для проверки")
            return True
        
        updated_count = 0
        skipped_count = 0
        
        for sig in waiting_signals:
            try:
                if not sig.pair or not sig.level_price or sig.level_price <= 0:
                    logger.warning(f"⚠️  Сигнал {sig.id}: некорректные данные (пара или уровень)")
                    skipped_count += 1
                    continue
                
                mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
                
                try:
                    current_price = bybit_demo_client.get_current_price(mapped_symbol)
                except Exception as err:
                    logger.warning(
                        f"⚠️  Сигнал {sig.id} ({sig.pair.symbol}): не удалось получить цену: {err}"
                    )
                    skipped_count += 1
                    continue
                
                if not current_price or current_price <= 0:
                    logger.warning(f"⚠️  Сигнал {sig.id} ({sig.pair.symbol}): некорректная цена {current_price}")
                    skipped_count += 1
                    continue
                
                # Проверяем пробитие уровня и отклонение цены
                is_invalidated, invalid_status, invalid_msg = demo_trade_executor.check_signal_invalidated(
                    sig, current_price
                )
                
                if is_invalidated:
                    sig.demo_status = invalid_status
                    sig.demo_error = invalid_msg
                    sig.demo_updated_at = now
                    
                    log_signal_event(
                        session,
                        sig,
                        invalid_msg,
                        event_type=invalid_status,
                        status=invalid_status,
                        details={
                            "current_price": current_price,
                            "level_price": sig.level_price,
                            "checked_at": now.isoformat(),
                        },
                    )
                    
                    logger.info(
                        f"✅ Сигнал {sig.id} ({sig.pair.symbol} {sig.signal_type}): "
                        f"обновлен статус на {invalid_status} - {invalid_msg}"
                    )
                    updated_count += 1
                else:
                    deviation_pct = abs((current_price / sig.level_price - 1) * 100.0)
                    logger.info(
                        f"ℹ️  Сигнал {sig.id} ({sig.pair.symbol} {sig.signal_type}): "
                        f"всё ещё активен (отклонение {deviation_pct:.3f}%, уровень={sig.level_price:.4f}, "
                        f"текущая={current_price:.4f})"
                    )
                    
            except Exception as err:
                logger.exception(f"❌ Ошибка при проверке сигнала {sig.id}: {err}")
                skipped_count += 1
        
        session.commit()
        
        logger.info(
            f"✅ Проверка завершена: обновлено {updated_count}, пропущено {skipped_count}, "
            f"всего проверено {len(waiting_signals)}"
        )
        
        return True
        
    except Exception as err:
        logger.exception(f"❌ Критическая ошибка: {err}")
        session.rollback()
        return False
    finally:
        session.close()


if __name__ == "__main__":
    success = check_waiting_signals()
    sys.exit(0 if success else 1)

