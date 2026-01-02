"""Вспомогательные функции для логирования действий live-торговли."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from core import database
from core.models import Signal, SignalLiveLog

logger = logging.getLogger(__name__)


def _ensure_session(session=None):
    if session is not None:
        return session, False
    if not database.init_database() or database.SessionLocal is None:
        raise RuntimeError("Database is not initialized")
    return database.SessionLocal(), True


def log_signal_event(
    session,
    signal: Union[Signal, int],
    message: str,
    event_type: Optional[str] = None,
    status: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    commit: bool = False,
) -> Optional[SignalLiveLog]:
    """
    Создает запись события по сигналу.

    Параметр session можно не передавать — тогда создастся временная сессия.
    Если передан ORM-объект Signal, дополнительно обновляется его demo_error/demo_updated_at.
    """
    if not message:
        return None

    own_session = False
    try:
        session, own_session = _ensure_session(session)
    except RuntimeError as err:
        logger.warning("Не удалось создать лог по сигналу: %s", err)
        return None

    signal_id = signal.id if isinstance(signal, Signal) else int(signal)
    log_entry = SignalLiveLog(
        signal_id=signal_id,
        event_type=event_type,
        status=status,
        message=message[:500],
        details=details or {},
    )
    session.add(log_entry)

    if isinstance(signal, Signal):
        signal.demo_error = message[:500]
        signal.demo_updated_at = datetime.now(timezone.utc)

    if commit:
        try:
            session.commit()
        except Exception as err:
            session.rollback()
            logger.exception("Ошибка коммита логов по сигналу %s: %s", signal_id, err)
            if own_session:
                session.close()
            return None
    else:
        try:
            session.flush()
        except Exception as err:
            session.rollback()
            logger.exception("Ошибка записи лога сигнала %s: %s", signal_id, err)
            if own_session:
                session.close()
            return None

    if own_session:
        session.close()

    return log_entry

