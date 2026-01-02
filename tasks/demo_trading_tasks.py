"""Celery-задачи для автоматической live-торговли (Bybit)."""

import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy.orm import joinedload

from tasks.celery_app import celery_app
from core.trading.demo_trade_executor import demo_trade_executor
from core.trading.bybit_demo_client import bybit_demo_client
import core.database as database
from core.models import Signal
from core.trading.live_trade_logger import log_signal_event
from core.trading.risk_manager import enforce_risk_limits, check_risk_limits
from core.config import settings

logger = logging.getLogger(__name__)


@celery_app.task(name="tasks.demo_trading_tasks.place_demo_order", queue="signals")
def place_demo_order_for_signal(signal_id: int) -> dict:
    """
    Initial‑задача: пытается сразу разместить ордер по только что созданному сигналу.

    Если цена ещё не дошла до нужного коридора, DemoTradeExecutor переведёт сигнал
    в статус WAITING_FOR_PRICE, а дальнейшее слежение возьмёт на себя watcher.
    """
    logger.info("🎯 Celery task запущен для размещения ордера: signal_id=%s", signal_id)
    try:
        result = demo_trade_executor.place_order_for_signal(signal_id, from_watcher=False)
        logger.info(
            "✅ Celery task завершен: signal_id=%s, result_status=%s",
            signal_id,
            result.get("status"),
        )
        return result
    except Exception as exc:  # pragma: no cover - зависит от внешних сервисов
        logger.exception("❌ Ошибка в Celery task для signal_id=%s: %s", signal_id, exc)
        raise


@celery_app.task(name="tasks.demo_trading_tasks.watch_waiting_signals", queue="signals")
def watch_waiting_signals() -> dict:
    """
    Периодический watcher:
    - Находит сигналы в статусе WAITING_FOR_PRICE
    - Пытается выставить по ним ордер, когда цена входит в допустимый диапазон
    - Отсекает слишком старые или ушедшие по цене сигналы
    """
    logger.info("👀 Watcher: проверка сигналов в статусе WAITING_FOR_PRICE")

    if not database.init_database() or database.SessionLocal is None:
        logger.error("❌ Watcher: База данных недоступна")
        return {"status": "db_unavailable"}

    session = database.SessionLocal()
    processed_waiting = 0
    processed_open = 0
    invalidated_waiting = 0
    try:
        now = datetime.now(timezone.utc)

        # 0) Обрабатываем активные сигналы БЕЗ ордера и demo_status
        # Это важно, когда:
        #  - live‑торговля была выключена и потом включена
        #  - ранее сработали лимиты риска, а теперь они сняты
        #  - сигнал был создан, но ордер ни разу не пытались поставить
        # ВАЖНО: Проверяем только свежие сигналы (не старше MAX_SIGNAL_AGE_SECONDS)
        max_signal_age = demo_trade_executor.MAX_SIGNAL_AGE_SECONDS
        cutoff_time = now - timedelta(seconds=max_signal_age)
        
        pending_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_order_id.is_(None),
                Signal.demo_status.is_(None),
                Signal.timestamp >= cutoff_time,  # Только свежие сигналы
            )
            .order_by(Signal.id.asc())
            .all()
        )

        if pending_signals:
            logger.info(
                "👀 Watcher: найдено %d активных сигналов без ордера и demo_status (попробуем отправить ордера, только свежие < %d сек)",
                len(pending_signals),
                max_signal_age,
            )

        for sig in pending_signals:
            try:
                if not sig.pair:
                    continue
                
                # Дополнительная проверка возраста (на случай, если фильтр не сработал)
                signal_age_seconds = (now - sig.timestamp).total_seconds()
                if signal_age_seconds > max_signal_age:
                    logger.debug(
                        "⏰ Watcher: пропускаем старый pending сигнал %s (возраст %.0f сек > %d сек)",
                        sig.id, signal_age_seconds, max_signal_age
                    )
                    continue
                
                logger.info(
                    "🔁 Watcher: пробуем отправить ордер для ранее не обработанного сигнала %s (%s %s, возраст %.0f сек)",
                    sig.id,
                    sig.pair.symbol,
                    sig.signal_type,
                    signal_age_seconds,
                )
                demo_trade_executor.place_order_for_signal(sig.id, from_watcher=False)
            except Exception as err:  # pragma: no cover
                logger.warning(
                    "⚠️  Watcher: не удалось повторно обработать сигнал %s без ордера: %s",
                    sig.id,
                    err,
                )
        
        # 0.5) Перепроверяем сигналы со статусом LEVEL_BROKEN - возможно, цена вернулась в диапазон
        # ВАЖНО: Проверяем только свежие сигналы (не старше MAX_SIGNAL_AGE_SECONDS), чтобы не обрабатывать недельные сигналы
        max_signal_age = demo_trade_executor.MAX_SIGNAL_AGE_SECONDS
        cutoff_time = now - timedelta(seconds=max_signal_age)
        
        level_broken_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status == "LEVEL_BROKEN",
                Signal.timestamp >= cutoff_time,  # Только свежие сигналы
            )
            .all()
        )
        
        reactivated_count = 0
        skipped_old_count = 0
        for sig in level_broken_signals:
            if not sig.pair or not sig.level_price or sig.level_price <= 0:
                continue
            
            # Дополнительная проверка возраста (на случай, если фильтр не сработал)
            signal_age_seconds = (now - sig.timestamp).total_seconds()
            if signal_age_seconds > max_signal_age:
                skipped_old_count += 1
                logger.debug(
                    "⏰ Watcher: пропускаем старый LEVEL_BROKEN сигнал %s (возраст %.0f сек > %d сек)",
                    sig.id, signal_age_seconds, max_signal_age
                )
                continue
            
            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
            try:
                current_price = bybit_demo_client.get_current_price(mapped_symbol)
                if not current_price or current_price <= 0:
                    continue
                
                # Перепроверяем, действительно ли уровень пробит сейчас
                is_invalidated, invalid_status, invalid_msg = demo_trade_executor.check_signal_invalidated(sig, current_price)
                
                if not is_invalidated:
                    # Уровень больше не пробит - возвращаем сигнал в активное состояние
                    old_status = sig.demo_status
                    sig.demo_status = "WAITING_FOR_PRICE"
                    sig.demo_error = None
                    sig.demo_updated_at = now
                    
                    # Логируем событие
                    log_signal_event(
                        session,
                        sig,
                        f"Уровень восстановлен: цена вернулась в допустимый диапазон (текущая: {current_price:.4f}, уровень: {sig.level_price:.4f})",
                        event_type="LEVEL_RESTORED",
                        status="WAITING_FOR_PRICE",
                    )
                    
                    logger.info(
                        "✅ Watcher: Signal %s (%s %s) - уровень восстановлен, возвращаем в WAITING_FOR_PRICE "
                        "(старый статус: %s, цена %.4f, уровень %.4f, возраст %.0f сек)",
                        sig.id, sig.pair.symbol, sig.signal_type, old_status, current_price, sig.level_price, signal_age_seconds
                    )
                    reactivated_count += 1
                    
                    # Сразу коммитим каждое восстановление, чтобы изменения сохранились
                    try:
                        session.commit()
                        logger.debug("✅ Коммит успешен для signal_id=%s", sig.id)
                    except Exception as commit_err:
                        session.rollback()
                        logger.error("❌ Ошибка коммита для signal_id=%s: %s", sig.id, commit_err)
                        # Откатываем изменения для этого сигнала
                        sig.demo_status = old_status
                        reactivated_count -= 1
            except Exception as err:
                logger.warning("⚠️  Ошибка перепроверки LEVEL_BROKEN для signal_id=%s: %s", sig.id, err)
        
        if skipped_old_count > 0:
            logger.info("⏰ Watcher: пропущено %d старых LEVEL_BROKEN сигналов (старше %d сек)", skipped_old_count, max_signal_age)
        
        # Коммит уже выполнен для каждого восстановленного сигнала индивидуально
        if reactivated_count > 0:
            logger.info("🔄 Watcher: восстановлено %d сигналов из статуса LEVEL_BROKEN", reactivated_count)
        
        # 0.6) Перепроверяем сигналы со статусом PRICE_DEVIATION_TOO_LARGE - возможно, цена вернулась ближе к уровню
        # ВАЖНО: Проверяем только свежие сигналы (не старше MAX_SIGNAL_AGE_SECONDS), чтобы не обрабатывать недельные сигналы
        max_signal_age = demo_trade_executor.MAX_SIGNAL_AGE_SECONDS
        cutoff_time = now - timedelta(seconds=max_signal_age)
        
        price_deviation_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status == "PRICE_DEVIATION_TOO_LARGE",
                Signal.timestamp >= cutoff_time,  # Только свежие сигналы
            )
            .all()
        )
        
        reactivated_price_count = 0
        skipped_old_price_count = 0
        for sig in price_deviation_signals:
            if not sig.pair or not sig.level_price or sig.level_price <= 0:
                continue
            
            # Дополнительная проверка возраста (на случай, если фильтр не сработал)
            signal_age_seconds = (now - sig.timestamp).total_seconds()
            if signal_age_seconds > max_signal_age:
                skipped_old_price_count += 1
                logger.debug(
                    "⏰ Watcher: пропускаем старый PRICE_DEVIATION_TOO_LARGE сигнал %s (возраст %.0f сек > %d сек)",
                    sig.id, signal_age_seconds, max_signal_age
                )
                continue
            
            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
            try:
                current_price = bybit_demo_client.get_current_price(mapped_symbol)
                if not current_price or current_price <= 0:
                    continue
                
                # Проверяем, вернулась ли цена в допустимый диапазон
                is_invalidated, invalid_status, invalid_msg = demo_trade_executor.check_signal_invalidated(sig, current_price)
                
                if not is_invalidated:
                    # Цена вернулась в допустимый диапазон - возвращаем сигнал в активное состояние
                    old_status = sig.demo_status
                    sig.demo_status = "WAITING_FOR_PRICE"
                    sig.demo_error = None
                    sig.demo_updated_at = now
                    
                    # Логируем событие
                    log_signal_event(
                        session,
                        sig,
                        f"Цена вернулась в допустимый диапазон: текущая {current_price:.4f}, уровень {sig.level_price:.4f}",
                        event_type="PRICE_RESTORED",
                        status="WAITING_FOR_PRICE",
                    )
                    
                    logger.info(
                        "✅ Watcher: Signal %s (%s %s) - цена вернулась в диапазон, возвращаем в WAITING_FOR_PRICE "
                        "(старый статус: %s, цена %.4f, уровень %.4f, возраст %.0f сек)",
                        sig.id, sig.pair.symbol, sig.signal_type, old_status, current_price, sig.level_price, signal_age_seconds
                    )
                    reactivated_price_count += 1
                    
                    # Сразу коммитим каждое восстановление, чтобы изменения сохранились
                    try:
                        session.commit()
                        logger.debug("✅ Коммит успешен для signal_id=%s", sig.id)
                    except Exception as commit_err:
                        session.rollback()
                        logger.error("❌ Ошибка коммита для signal_id=%s: %s", sig.id, commit_err)
                        # Откатываем изменения для этого сигнала
                        sig.demo_status = old_status
                        reactivated_price_count -= 1
            except Exception as err:
                logger.warning("⚠️  Ошибка перепроверки PRICE_DEVIATION_TOO_LARGE для signal_id=%s: %s", sig.id, err)
        
        if skipped_old_price_count > 0:
            logger.info("⏰ Watcher: пропущено %d старых PRICE_DEVIATION_TOO_LARGE сигналов (старше %d сек)", skipped_old_price_count, max_signal_age)
        
        # Коммит уже выполнен для каждого восстановленного сигнала индивидуально
        if reactivated_price_count > 0:
            logger.info("🔄 Watcher: восстановлено %d сигналов из статуса PRICE_DEVIATION_TOO_LARGE", reactivated_price_count)
        
        # 1) Обрабатываем сигналы, которые ещё ждут подхода цены
        # ВАЖНО: После коммита нужно сделать новый запрос, чтобы получить обновленные данные
        # ВАЖНО: Используем joinedload для загрузки pair, чтобы избежать ошибок "not bound to a Session"
        # ВАЖНО: Проверяем только свежие сигналы (не старше MAX_SIGNAL_AGE_SECONDS), чтобы не обрабатывать недельные сигналы
        max_signal_age = demo_trade_executor.MAX_SIGNAL_AGE_SECONDS
        cutoff_time = now - timedelta(seconds=max_signal_age)
        
        waiting_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status == "WAITING_FOR_PRICE",
                Signal.timestamp >= cutoff_time,  # Только свежие сигналы
            )
            .order_by(Signal.id.asc())
            .all()
        )

        logger.info("👀 Watcher: найдено %d сигналов в статусе WAITING_FOR_PRICE (только свежие < %d сек)", len(waiting_signals), max_signal_age)

        for sig in waiting_signals:
            processed_waiting += 1
            try:
                # Дополнительная проверка возраста (на случай, если фильтр не сработал)
                signal_age_seconds = (now - sig.timestamp).total_seconds()
                if signal_age_seconds > max_signal_age:
                    logger.debug(
                        "⏰ Watcher: пропускаем старый WAITING_FOR_PRICE сигнал %s (возраст %.0f сек > %d сек)",
                        sig.id, signal_age_seconds, max_signal_age
                    )
                    # Помечаем как слишком старый
                    sig.demo_status = "SIGNAL_TOO_OLD"
                    sig.demo_error = f"Сигнал устарел: прошло {signal_age_seconds:.1f} сек (макс {max_signal_age} сек)"
                    sig.demo_updated_at = now
                    log_signal_event(
                        session,
                        sig,
                        sig.demo_error,
                        event_type="SIGNAL_TOO_OLD",
                        status="SIGNAL_TOO_OLD",
                    )
                    continue
                
                # Проверяем, не стал ли сигнал неактивным (пробитие уровня или отклонение >2%)
                if not sig.pair or not sig.level_price or sig.level_price <= 0:
                    continue
                
                mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
                try:
                    current_price = bybit_demo_client.get_current_price(mapped_symbol)
                except Exception as err:  # pragma: no cover - зависит от внешнего API
                    logger.warning(
                        "⚠️  Watcher: не удалось получить цену для проверки сигнала %s: %s",
                        sig.id, err
                    )
                    # Продолжаем попытку поставить ордер, если не удалось получить цену
                    demo_trade_executor.place_order_for_signal(sig.id, from_watcher=True)
                    continue
                
                if not current_price or current_price <= 0:
                    continue
                
                # Проверяем пробитие уровня и отклонение цены
                is_invalidated, invalid_status, invalid_msg = demo_trade_executor.check_signal_invalidated(sig, current_price)
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
                        details={"current_price": current_price, "level_price": sig.level_price},
                    )
                    logger.warning(
                        "🚫 Watcher: сигнал %s (%s %s) помечен как неактивный: %s (статус: %s, "
                        "уровень=%.4f, текущая=%.4f)",
                        sig.id,
                        sig.pair.symbol if sig.pair else "N/A",
                        sig.signal_type,
                        invalid_msg,
                        invalid_status,
                        sig.level_price,
                        current_price,
                    )
                    invalidated_waiting += 1
                    continue
                
                # Если сигнал всё ещё валиден, пытаемся поставить ордер
                demo_trade_executor.place_order_for_signal(sig.id, from_watcher=True)
            except Exception as err:  # pragma: no cover - зависит от внешнего API
                logger.exception(
                    "⚠️  Watcher: ошибка при обработке сигнала %s: %s", sig.id, err
                )

        if invalidated_waiting > 0:
            logger.info(
                "📋 Watcher: помечено как неактивных %d из %d сигналов в WAITING_FOR_PRICE "
                "(пробитие уровня или отклонение >2%%)",
                invalidated_waiting,
                len(waiting_signals),
            )

        # 2) Проверяем уже выставленные лимитные ордера: если цена ушла >порога от уровня — снимаем
        ORDER_CANCEL_DEVIATION_PCT = settings.DEMO_ORDER_CANCEL_DEVIATION_PCT  # Настраиваемый порог (по умолчанию 1.5%)

        # Для открытых ордеров НЕ ограничиваемся 15 минутами — важно снять зависшие лимитки,
        # даже если сигнал старше cutoff.
        open_signals = (
            session.query(Signal)
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_order_id.isnot(None),
                Signal.demo_status.in_(["NEW", "OPEN", "PLACED", "SUBMITTING"]),
            )
            .all()
        )

        logger.info(
            "👀 Watcher: найдено %d сигналов с активными ордерами для проверки отклонения цены (порог: %.2f%%)",
            len(open_signals),
            ORDER_CANCEL_DEVIATION_PCT,
        )

        for sig in open_signals:
            # Защита от отсутствия пары/уровня
            if not sig.pair or not sig.level_price or sig.level_price <= 0:
                continue

            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)  # используем ту же маппинг-логику
            try:
                current_price = bybit_demo_client.get_current_price(mapped_symbol)
            except Exception as err:  # pragma: no cover - зависит от внешнего API
                logger.warning(
                    "⚠️  Watcher: не удалось получить текущую цену для %s: %s",
                    mapped_symbol,
                    err,
                )
                continue

            if not current_price or current_price <= 0:
                continue

            # Вычисляем текущее отклонение
            deviation_pct = abs((current_price / sig.level_price - 1) * 100.0)
            
            # Получаем максимальное отклонение из metadata (если есть)
            metadata = sig.meta_data or {}
            max_deviation_pct = metadata.get('max_price_deviation_pct', 0.0)
            
            # Обновляем максимальное отклонение, если текущее больше
            if deviation_pct > max_deviation_pct:
                max_deviation_pct = deviation_pct
                metadata['max_price_deviation_pct'] = max_deviation_pct
                sig.meta_data = metadata
                session.add(sig)  # Помечаем для сохранения изменений metadata
                logger.debug(
                    "📊 Обновлено максимальное отклонение для сигнала %s: %.3f%% (текущее: %.3f%%)",
                    sig.id,
                    max_deviation_pct,
                    deviation_pct,
                )

            # Отменяем ордер, если МАКСИМАЛЬНОЕ отклонение превысило порог
            # Это важно: даже если цена вернулась, но была >порога - снимаем ордер
            if max_deviation_pct > ORDER_CANCEL_DEVIATION_PCT:
                # Максимальное отклонение превысило порог — снимаем лимитку и помечаем сигнал
                try:
                    logger.warning(
                        "🚫 Watcher: максимальное отклонение > %.2f%% от уровня, отменяем ордер: signal_id=%s, symbol=%s, level=%.4f, current=%.4f, max_deviation=%.3f%% (текущее=%.3f%%)",
                        ORDER_CANCEL_DEVIATION_PCT,
                        sig.id,
                        mapped_symbol,
                        sig.level_price,
                        current_price,
                        max_deviation_pct,
                        deviation_pct,
                    )
                    bybit_demo_client.cancel_order(sig.demo_order_id, mapped_symbol)
                except Exception as cancel_err:  # pragma: no cover - зависит от внешнего API
                    logger.warning(
                        "⚠️  Watcher: не удалось отменить ордер %s для сигнала %s: %s",
                        sig.demo_order_id,
                        sig.id,
                        cancel_err,
                    )

                sig.demo_status = "ORDER_CANCELLED_PRICE_MOVED"
                cancel_msg = (
                    f"Ордер отменен: максимальное отклонение {max_deviation_pct:.3f}% превысило порог {ORDER_CANCEL_DEVIATION_PCT:.2f}% "
                    f"(текущее отклонение: {deviation_pct:.3f}%)"
                )
                sig.demo_error = cancel_msg
                sig.demo_updated_at = now
                log_signal_event(
                    session,
                    sig,
                    cancel_msg,
                    event_type="ORDER_CANCELLED",
                    status="ORDER_CANCELLED_PRICE_MOVED",
                    details={
                        "max_deviation_pct": max_deviation_pct,
                        "current_deviation_pct": deviation_pct,
                        "threshold_pct": ORDER_CANCEL_DEVIATION_PCT,
                    },
                )
                processed_open += 1

        # 3) Проверяем закрытые сигналы без ордеров и обновляем их demo_status
        closed_no_order_signals = (
            session.query(Signal)
            .filter(
                Signal.status != "ACTIVE",  # Закрытые сигналы
                Signal.demo_order_id.is_(None),  # Без ордера
                Signal.demo_status.in_([
                    "WAITING_FOR_PRICE",
                    "PRICE_DEVIATION_TOO_LARGE",
                    "SIGNAL_TOO_OLD",
                    "INVALID_ENTRY",
                    "INVALID_QUANTITY",
                    "INVALID_MARKET_PRICE",
                ]),  # В статусе ожидания
            )
            .all()
        )

        logger.info(
            "👀 Watcher: найдено %d закрытых сигналов без ордеров для обновления статуса",
            len(closed_no_order_signals),
        )

        for sig in closed_no_order_signals:
            if sig.demo_status != "SIGNAL_CLOSED_NO_ORDER":
                sig.demo_status = "SIGNAL_CLOSED_NO_ORDER"
                sig.demo_updated_at = now
                close_msg = f"Сигнал закрыт без ордера (статус: {sig.status}, причина: {sig.exit_reason or 'N/A'})"
                sig.demo_error = close_msg
                log_signal_event(
                    session,
                    sig,
                    close_msg,
                    event_type="SIGNAL_CLOSED_NO_ORDER",
                    status="SIGNAL_CLOSED_NO_ORDER",
                    details={
                        "signal_status": sig.status,
                        "exit_reason": sig.exit_reason,
                        "exit_timestamp": sig.exit_timestamp.isoformat() if sig.exit_timestamp else None,
                    },
                )
                logger.info(
                    "📋 Watcher: обновлен статус сигнала %s на SIGNAL_CLOSED_NO_ORDER (сигнал закрыт: %s)",
                    sig.id,
                    sig.status,
                )

        # 4) Проверяем ордера в статусе PLACED - возможно, они уже исполнились
        placed_orders = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status == "PLACED",
                Signal.demo_order_id.isnot(None),
                Signal.demo_filled_at.is_(None),  # Еще не помечен как исполненный
            )
            .all()
        )

        logger.info(
            "👀 Watcher: найдено %d ордеров в статусе PLACED для проверки исполнения",
            len(placed_orders),
        )

        for sig in placed_orders:
            if not sig.pair:
                continue
            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
            try:
                # СТРОГО получаем цену и время входа из исполненного ордера, а не из позиции!
                entry_price = None
                fill_timestamp = None
                fill_datetime = None
                
                if sig.demo_order_id:
                    fill_info = bybit_demo_client.get_order_fill_info(sig.demo_order_id, mapped_symbol)
                    if fill_info:
                        entry_price = fill_info.get("price")
                        fill_timestamp = fill_info.get("timestamp")
                        fill_datetime = fill_info.get("datetime")
                        if entry_price:
                            logger.info("✅ Получена информация об исполнении ордера %s: цена=%.4f, время=%s",
                                       sig.demo_order_id, entry_price, fill_datetime or fill_timestamp)
                
                # Fallback: если не удалось получить из ордера, пробуем из позиции
                if not entry_price or entry_price <= 0:
                    entry_price = bybit_demo_client.get_position_entry_price(mapped_symbol)
                    if entry_price:
                        logger.warning("⚠️  Использована цена входа из позиции (не из ордера) для сигнала %s: %.4f", sig.id, entry_price)
                
                if entry_price and entry_price > 0:
                    # Позиция открыта! Обновляем статус
                    sig.demo_status = "OPEN_POSITION"
                    sig.entry_price = entry_price
                    sig.demo_updated_at = now
                    
                    # КРИТИЧЕСКИ ВАЖНО: устанавливаем РЕАЛЬНОЕ время исполнения ордера из биржи
                    if fill_timestamp:
                        # Конвертируем timestamp (миллисекунды) в datetime UTC
                        if isinstance(fill_timestamp, (int, float)):
                            if fill_timestamp > 1e10:  # Если больше 10^10, значит в миллисекундах
                                fill_timestamp = fill_timestamp / 1000
                            sig.demo_filled_at = datetime.fromtimestamp(fill_timestamp, tz=timezone.utc)
                            logger.info("✅ Установлено реальное время исполнения ордера из биржи: %s", sig.demo_filled_at)
                        else:
                            sig.demo_filled_at = now
                            logger.warning("⚠️  Невалидный timestamp ордера, используется текущее время")
                    elif fill_datetime:
                        # Пробуем распарсить datetime строку
                        try:
                            dt_str = fill_datetime.replace('Z', '+00:00').replace('z', '+00:00')
                            if dt_str.endswith('+00:00') or dt_str.endswith('-00:00'):
                                sig.demo_filled_at = datetime.fromisoformat(dt_str)
                            else:
                                sig.demo_filled_at = datetime.fromisoformat(fill_datetime).replace(tzinfo=timezone.utc)
                            logger.info("✅ Установлено реальное время исполнения ордера из биржи: %s", sig.demo_filled_at)
                        except Exception as e:
                            logger.warning("⚠️  Не удалось распарсить datetime ордера '%s': %s, используется текущее время", fill_datetime, e)
                            sig.demo_filled_at = now
                    else:
                        # Fallback: если нет информации о времени, используем текущее время
                        sig.demo_filled_at = now
                        logger.warning("⚠️  Нет информации о времени исполнения ордера, используется текущее время")
                    
                    # Пересчитываем TP/SL от реальной цены входа, если отличается от уровня
                    if abs(entry_price / sig.level_price - 1) > 0.001:  # Если разница >0.1%
                        real_tp, real_sl = demo_trade_executor._calculate_tp_sl(sig, entry_price)
                        
                        # Обновляем TP/SL на бирже
                        bybit_demo_client.set_position_tp_sl(mapped_symbol, real_tp, real_sl)
                        sig.demo_tp_price = real_tp
                        sig.demo_sl_price = real_sl
                    
                    log_signal_event(
                        session,
                        sig,
                        f"Позиция открыта по цене {entry_price:.4f} (обнаружена watcher'ом)",
                        event_type="POSITION_FILLED",
                        status="OPEN_POSITION",
                        details={"entry_price": entry_price, "detected_by": "watcher"},
                    )
                    logger.info(
                        "✅ Watcher: обнаружена открытая позиция для сигнала %s (%s), обновлен статус на OPEN_POSITION",
                        sig.id,
                        mapped_symbol,
                    )
            except Exception as err:  # pragma: no cover
                logger.warning(
                    "⚠️  Watcher: ошибка проверки исполнения ордера для сигнала %s: %s",
                    sig.id,
                    err,
                )

        # 5) Проверяем закрытые позиции - получаем реальную цену закрытия с биржи
        # ВАЖНО: Включаем POSITION_ALREADY_OPEN, чтобы обновлять статус даже если сигнал был заблокирован
        open_position_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status.in_(["OPEN_POSITION", "FILLED", "SL_TO_BREAKEVEN", "POSITION_ALREADY_OPEN"]),
                Signal.demo_filled_at.isnot(None),
                Signal.entry_price.isnot(None),
                Signal.exit_price.is_(None),  # Позиция еще не закрыта в нашей БД
            )
            .all()
        )
        
        closed_count = 0
        for sig in open_position_signals:
            if not sig.pair:
                continue
            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
            try:
                # Проверяем, есть ли еще открытая позиция на бирже
                position_info = bybit_demo_client.get_position_info(mapped_symbol)
                
                if not position_info:  # Позиция закрыта на бирже!
                    # КРИТИЧЕСКИ ВАЖНО: получаем цену закрытия из ПЕРВОЙ закрывающей сделки/ордера после входа
                    # Метод сначала ищет среди сделок (trades), затем среди ордеров, и берет ПЕРВУЮ, а не последнюю!
                    since_timestamp = int(sig.demo_filled_at.timestamp() * 1000) if sig.demo_filled_at else None
                    exit_order_info = bybit_demo_client.get_exit_order_fill_price(
                        mapped_symbol,
                        sig.demo_order_id,  # ID ордера входа
                        since_timestamp,
                        sig.signal_type  # Направление позиции (LONG/SHORT)
                    )
                    
                    if exit_order_info:
                        actual_exit_price = exit_order_info.get("price", 0)
                        exit_timestamp_ms = exit_order_info.get("timestamp")
                        exit_reason_from_order = exit_order_info.get("exit_reason", "MANUAL_CLOSE")
                        
                        logger.info(
                            "✅ Найден ордер закрытия для сигнала %s: цена=%.4f, время=%s, причина=%s",
                            sig.id,
                            actual_exit_price,
                            exit_order_info.get("datetime"),
                            exit_reason_from_order
                        )
                        
                        if actual_exit_price > 0 and exit_timestamp_ms:
                            exit_timestamp = datetime.fromtimestamp(exit_timestamp_ms / 1000, tz=timezone.utc)
                            
                            # Обновляем сигнал реальной ценой закрытия из ордера
                            sig.exit_price = actual_exit_price
                            sig.exit_timestamp = exit_timestamp
                            sig.status = "CLOSED"
                            sig.demo_status = "CLOSED"
                            sig.demo_updated_at = now
                            sig.exit_reason = exit_reason_from_order  # Используем причину из ордера
                            
                            # Рассчитываем фактический результат
                            if sig.signal_type == "LONG":
                                actual_result_pct = ((actual_exit_price - sig.entry_price) / sig.entry_price) * 100.0
                            else:  # SHORT
                                actual_result_pct = ((sig.entry_price - actual_exit_price) / sig.entry_price) * 100.0
                            
                            log_signal_event(
                                session,
                                sig,
                                f"Позиция закрыта на бирже по цене {actual_exit_price:.4f} (фактический результат: {actual_result_pct:.2f}%)",
                                event_type="POSITION_CLOSED",
                                status="CLOSED",
                                details={
                                    "exit_price": actual_exit_price,
                                    "exit_timestamp": exit_timestamp.isoformat(),
                                    "actual_result_pct": actual_result_pct,
                                    "exit_reason": sig.exit_reason,
                                },
                            )
                            
                            logger.info(
                                "✅ Watcher: позиция закрыта для сигнала %s (%s), реальная цена закрытия: %.4f, результат: %.2f%%",
                                sig.id,
                                mapped_symbol,
                                actual_exit_price,
                                actual_result_pct,
                            )
                            closed_count += 1
                        else:
                            logger.warning(
                                "⚠️  Watcher: позиция закрыта для сигнала %s, но не удалось получить цену закрытия из ордера (нет timestamp или price)",
                                sig.id,
                            )
                    else:
                        logger.warning(
                            "⚠️  Watcher: позиция закрыта для сигнала %s, но ордер закрытия не найден (entry_order_id=%s). "
                            "Возможно, позиция была закрыта вручную или ордер еще не синхронизирован.",
                            sig.id,
                            sig.demo_order_id,
                        )
            except Exception as err:  # pragma: no cover
                logger.warning(
                    "⚠️  Watcher: ошибка проверки закрытия позиции для сигнала %s: %s",
                    sig.id,
                    err,
                )

        # 5.5) КРИТИЧЕСКАЯ ПРОВЕРКА: Убеждаемся, что у всех открытых позиций есть TP и SL
        # Это защита от ситуаций, когда биржа не установила SL при размещении ордера
        open_position_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == "ACTIVE",
                Signal.demo_status.in_(["OPEN_POSITION", "FILLED", "PLACED"]),
                Signal.demo_order_id.isnot(None),
                Signal.demo_filled_at.isnot(None),
            )
            .all()
        )
        
        for sig in open_position_signals:
            if not sig.pair:
                continue
            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
            try:
                position_info = bybit_demo_client.get_position_info(mapped_symbol)
                if not position_info:
                    continue  # Позиция уже закрыта
                
                current_tp = position_info.get("takeProfit")
                current_sl = position_info.get("stopLoss")
                expected_tp = sig.demo_tp_price
                expected_sl = sig.demo_sl_price
                
                tp_missing = not current_tp or float(current_tp) <= 0
                sl_missing = not current_sl or float(current_sl) <= 0
                
                if tp_missing or sl_missing:
                    logger.warning(
                        "🚨 КРИТИЧЕСКАЯ ПРОБЛЕМА: У позиции signal_id=%s (%s) отсутствует TP/SL на бирже! "
                        "TP=%s (нужно %.4f), SL=%s (нужно %.4f). Срочно доустанавливаем...",
                        sig.id,
                        mapped_symbol,
                        current_tp or "НЕТ",
                        expected_tp or 0.0,
                        current_sl or "НЕТ",
                        expected_sl or 0.0,
                    )
                    
                    # Доустанавливаем отсутствующие TP/SL
                    tp_to_set = float(expected_tp) if expected_tp and tp_missing else None
                    sl_to_set = float(expected_sl) if expected_sl and sl_missing else None
                    
                    # Если устанавливаем только один, сохраняем существующий другой
                    if tp_to_set and not sl_to_set:
                        sl_to_set = float(current_sl) if current_sl else None
                    if sl_to_set and not tp_to_set:
                        tp_to_set = float(current_tp) if current_tp else None
                    
                    if tp_to_set or sl_to_set:
                        success = bybit_demo_client.set_position_tp_sl(
                            mapped_symbol,
                            take_profit=tp_to_set,
                            stop_loss=sl_to_set,
                        )
                        if success:
                            logger.info("✅ TP/SL успешно доустановлены для signal_id=%s (%s)", sig.id, mapped_symbol)
                            log_signal_event(
                                session,
                                sig,
                                f"TP/SL доустановлены на бирже: TP={tp_to_set or 'сохранен'}, SL={sl_to_set or 'сохранен'}",
                                event_type="TP_SL_RESTORED",
                                status=sig.demo_status,
                            )
                        else:
                            logger.error(
                                "❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось доустановить TP/SL для signal_id=%s (%s). "
                                "Позиция открыта БЕЗ полной защиты!",
                                sig.id,
                                mapped_symbol,
                            )
                            log_signal_event(
                                session,
                                sig,
                                f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось доустановить TP/SL. TP нужен: {tp_to_set or expected_tp}, SL нужен: {sl_to_set or expected_sl}",
                                event_type="TP_SL_RESTORE_FAILED",
                                status=sig.demo_status,
                            )
            except Exception as err:
                logger.warning("⚠️  Ошибка проверки TP/SL для signal_id=%s: %s", sig.id, err)

        # 5.5) Отслеживание MFE/MAE и порогов прибыли для открытых позиций
        # Это критически важно для анализа эффективности стратегии
        for sig in open_position_signals:
            if not sig.pair or not sig.entry_price or sig.entry_price <= 0:
                continue
            mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
            try:
                # Получаем текущую цену
                current_price = bybit_demo_client.get_current_price(mapped_symbol)
                if not current_price or current_price <= 0:
                    continue
                
                entry_price = float(sig.entry_price)
                meta = sig.meta_data or {}
                needs_update = False
                
                # Вычисляем текущий PnL в процентах
                if sig.signal_type == "LONG":
                    current_pnl_pct = ((current_price - entry_price) / entry_price) * 100.0
                else:  # SHORT
                    current_pnl_pct = ((entry_price - current_price) / entry_price) * 100.0
                
                # Обновляем MFE (максимальная прибыль) - используем текущую цену (close)
                # Это консервативный подход, так как high может быть неточным из-за спреда
                current_mfe = meta.get("max_favorable_move_pct", 0.0)
                if current_pnl_pct > current_mfe:
                    meta["max_favorable_move_pct"] = current_pnl_pct
                    needs_update = True
                    logger.debug(
                        "📈 Signal %s: обновлен MFE = %.3f%% (текущая цена %.4f, вход %.4f)",
                        sig.id, current_pnl_pct, current_price, entry_price
                    )
                
                # Обновляем MAE (максимальный убыток) - используем текущую цену (close)
                current_mae = meta.get("max_adverse_move_pct", 0.0)
                if current_pnl_pct < current_mae:
                    meta["max_adverse_move_pct"] = current_pnl_pct
                    needs_update = True
                    logger.debug(
                        "📉 Signal %s: обновлен MAE = %.3f%% (текущая цена %.4f, вход %.4f)",
                        sig.id, current_pnl_pct, current_price, entry_price
                    )
                
                # Проверяем пороги прибыли и логируем первое достижение
                thresholds = [
                    (0.5, "first_touch_0_5_pct_ts", "0.5%"),
                    (1.0, "first_touch_1_0_pct_ts", "1.0%"),
                    (1.5, "first_touch_1_5_pct_ts", "1.5%"),
                ]
                
                for threshold_pct, meta_key, threshold_str in thresholds:
                    if current_pnl_pct >= threshold_pct:
                        # Проверяем, было ли уже достижение этого порога
                        if not meta.get(meta_key):
                            # Первое достижение порога - логируем событие
                            meta[meta_key] = now.isoformat()
                            needs_update = True
                            
                            log_message = (
                                f"🎯 Цена впервые достигла +{threshold_str} от входа "
                                f"(вход: {entry_price:.4f}, текущая: {current_price:.4f}, "
                                f"прибыль: +{current_pnl_pct:.3f}%)"
                            )
                            log_signal_event(
                                session,
                                sig,
                                log_message,
                                event_type="THRESHOLD_HIT",
                                status=sig.demo_status,
                                details={
                                    "threshold_pct": threshold_pct,
                                    "current_pnl_pct": current_pnl_pct,
                                    "entry_price": entry_price,
                                    "current_price": current_price,
                                },
                            )
                            logger.info(
                                "✅ Signal %s: достигнут порог +%s (текущий PnL: +%.3f%%)",
                                sig.id, threshold_str, current_pnl_pct
                            )
                
                # Сохраняем обновленные метаданные
                if needs_update:
                    sig.meta_data = meta
                    # SQLAlchemy не отслеживает изменения в JSON полях автоматически
                    from sqlalchemy.orm.attributes import flag_modified
                    flag_modified(sig, "meta_data")
                    session.flush()
                    
            except Exception as err:
                logger.warning("⚠️  Ошибка отслеживания MFE/MAE для signal_id=%s: %s", sig.id, err)

        # 6) Проверяем активные позиции для установки стопа в безубыток
        # ОТКЛЮЧЕНО: Используем фиксированный SL -1%, без перевода в безубыток
        # breakeven_signals = (
        #     session.query(Signal)
        #     .options(joinedload(Signal.pair))
        #     .filter(
        #         Signal.status == "ACTIVE",
        #         Signal.demo_status.in_(["OPEN_POSITION", "POSITION_ALREADY_OPEN"]),
        #         Signal.demo_filled_at.isnot(None),
        #     )
        #     .all()
        # )

        # for sig in breakeven_signals:
        #     if not sig.pair:
        #         continue
        #     mapped_symbol = demo_trade_executor._map_symbol(sig.pair.symbol)
        #     try:
        #         # Сначала проверяем, что позиция еще открыта на бирже
        #         position_info = bybit_demo_client.get_position_info(mapped_symbol)
        #         if not position_info:
        #             # Позиция уже закрыта - пропускаем breakeven
        #             logger.debug("Watcher: позиция для сигнала %s уже закрыта, пропускаем breakeven", sig.id)
        #             continue
        #         
        #         current_price = bybit_demo_client.get_current_price(mapped_symbol)
        #     except Exception as err:  # pragma: no cover
        #         logger.warning("⚠️  Watcher: не удалось получить цену для breakeven %s: %s", mapped_symbol, err)
        #         continue
        #     if not current_price:
        #         continue
        #     
        #     # Проверяем, не было ли недавно неудачной попытки breakeven (защита от спама в логах)
        #     should_try_breakeven = True
        #     if sig.demo_error and "безубыток" in sig.demo_error.lower() and "не удалось" in sig.demo_error.lower():
        #         if sig.demo_updated_at:
        #             time_since_last_attempt = (now - sig.demo_updated_at).total_seconds() / 60.0
        #             if time_since_last_attempt < 5.0:  # Меньше 5 минут - не пытаемся снова
        #                 should_try_breakeven = False
        #                 logger.debug("Watcher: пропускаем breakeven для сигнала %s (недавняя неудачная попытка %.1f мин назад)",
        #                            sig.id, time_since_last_attempt)
        #     
        #     if should_try_breakeven:
        #         demo_trade_executor._apply_breakeven(session, sig, mapped_symbol, current_price, now)

        session.commit()

        # КРИТИЧЕСКИ ВАЖНО: Проверяем лимиты риска после закрытия позиций
        # Это автоматически остановит торговлю при достижении дневного лимита или серии убытков
        if closed_count > 0:
            risk_stopped = enforce_risk_limits()
            if risk_stopped:
                can_trade, reason = check_risk_limits()
                logger.critical(
                    "🛑 Watcher: торговля автоматически остановлена после закрытия %d позиций: %s",
                    closed_count,
                    reason or "Лимиты риска превышены"
                )

        return {
            "status": "success",
            "processed_waiting": processed_waiting,
            "invalidated_waiting": invalidated_waiting,
            "processed_open": processed_open,
            "closed_positions": closed_count,
            "reactivated_level_broken": reactivated_count,
            "timestamp": now.isoformat(),
        }
    finally:
        try:
            session.close()
        except Exception:
            pass


