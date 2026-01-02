"""Управление рисками для live-торговли."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

from sqlalchemy.orm import joinedload

import core.database as database
from core.models import Signal
from core.trading.trading_mode import set_live_trading_enabled, is_live_trading_enabled

logger = logging.getLogger(__name__)

# Лимиты риска
DAILY_LOSS_LIMIT_PCT = -5.0  # Максимальный дневной убыток -5%
MAX_CONSECUTIVE_LOSSES = 5  # Максимальное количество убыточных сделок подряд


def calculate_daily_pnl_pct() -> Tuple[float, int, int]:
    """
    Рассчитывает дневной P&L в процентах на основе закрытых сделок за сегодня.
    
    Returns:
        Tuple[float, int, int]: (дневной P&L %, количество прибыльных сделок, количество убыточных сделок)
    """
    try:
        assert database.init_database() and database.SessionLocal is not None
        session = database.SessionLocal()
        
        # Получаем начало сегодняшнего дня (Москва)
        moscow_tz = timezone(timedelta(hours=3))
        now_moscow = datetime.now(moscow_tz)
        today_start = now_moscow.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start.astimezone(timezone.utc)
        
        # Получаем все закрытые сигналы за сегодня с исполненными ордерами
        closed_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.timestamp >= today_start_utc,
                Signal.status == 'CLOSED',
                Signal.exit_price.isnot(None),
                Signal.entry_price.isnot(None),
                Signal.demo_order_id.isnot(None),  # Только исполненные ордера
            )
            .all()
        )
        
        total_pnl_pct = 0.0
        profitable_count = 0
        losing_count = 0
        
        for signal in closed_signals:
            if not signal.entry_price or not signal.exit_price:
                continue
            
            # Рассчитываем P&L в процентах
            if signal.signal_type == 'LONG':
                pct = ((signal.exit_price - signal.entry_price) / signal.entry_price) * 100
            else:  # SHORT
                pct = ((signal.entry_price - signal.exit_price) / signal.entry_price) * 100
            
            # Учитываем комиссию (0.035% на вход и выход)
            commission = 0.035 * 2
            net_pct = pct - commission
            
            total_pnl_pct += net_pct
            
            if net_pct > 0:
                profitable_count += 1
            elif net_pct < 0:
                losing_count += 1
        
        session.close()
        
        return round(total_pnl_pct, 2), profitable_count, losing_count
        
    except Exception as e:
        logger.error("Ошибка расчета дневного P&L: %s", e)
        return 0.0, 0, 0


def get_consecutive_losses() -> int:
    """
    Возвращает количество убыточных сделок подряд (начиная с последней закрытой сделки).
    
    Returns:
        int: Количество убыточных сделок подряд (0 если последняя сделка прибыльная)
    """
    try:
        assert database.init_database() and database.SessionLocal is not None
        session = database.SessionLocal()
        
        # Получаем все закрытые сигналы с исполненными ордерами, отсортированные по времени закрытия
        closed_signals = (
            session.query(Signal)
            .options(joinedload(Signal.pair))
            .filter(
                Signal.status == 'CLOSED',
                Signal.exit_price.isnot(None),
                Signal.entry_price.isnot(None),
                Signal.demo_order_id.isnot(None),
            )
            .order_by(Signal.exit_timestamp.desc().nullslast(), Signal.timestamp.desc())
            .limit(10)  # Проверяем последние 10 сделок
            .all()
        )
        
        consecutive_losses = 0
        
        for signal in closed_signals:
            if not signal.entry_price or not signal.exit_price:
                continue
            
            # Рассчитываем P&L
            if signal.signal_type == 'LONG':
                pct = ((signal.exit_price - signal.entry_price) / signal.entry_price) * 100
            else:  # SHORT
                pct = ((signal.entry_price - signal.exit_price) / signal.entry_price) * 100
            
            # Учитываем комиссию
            commission = 0.035 * 2
            net_pct = pct - commission
            
            if net_pct < 0:
                # Убыточная сделка - увеличиваем счетчик
                consecutive_losses += 1
            else:
                # Прибыльная или безубыточная - прерываем серию
                break
        
        session.close()
        
        return consecutive_losses
        
    except Exception as e:
        logger.error("Ошибка получения серии убытков: %s", e)
        return 0


def check_risk_limits() -> Tuple[bool, Optional[str]]:
    """
    Проверяет лимиты риска (дневной убыток и серия убытков).
    
    Returns:
        Tuple[bool, Optional[str]]: (можно ли торговать, причина остановки если нельзя)
    """
    try:
        # Проверяем дневной убыток
        daily_pnl, profitable_count, losing_count = calculate_daily_pnl_pct()
        
        if daily_pnl <= DAILY_LOSS_LIMIT_PCT:
            reason = (
                f"Достигнут дневной лимит убытков: {daily_pnl:.2f}% "
                f"(лимит: {DAILY_LOSS_LIMIT_PCT}%). "
                f"Прибыльных: {profitable_count}, убыточных: {losing_count}"
            )
            logger.warning("🚫 %s", reason)
            return False, reason
        
        # Проверяем серию убытков
        consecutive_losses = get_consecutive_losses()
        
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            reason = (
                f"Серия из {consecutive_losses} убыточных сделок подряд "
                f"(лимит: {MAX_CONSECUTIVE_LOSSES}). "
                f"Торговля автоматически остановлена для защиты капитала."
            )
            logger.warning("🚫 %s", reason)
            return False, reason
        
        return True, None
        
    except Exception as e:
        logger.error("Ошибка проверки лимитов риска: %s", e)
        # В случае ошибки разрешаем торговлю (не блокируем из-за технических проблем)
        return True, None


def enforce_risk_limits() -> bool:
    """
    Применяет лимиты риска - останавливает торговлю, если лимиты превышены.
    
    Returns:
        bool: True если торговля остановлена, False если можно торговать
    """
    can_trade, reason = check_risk_limits()

    if not can_trade:
        # Если лимиты превышены, но live‑торговля УЖЕ включена,
        # считаем, что пользователь сознательно сделал manual override
        # и НЕ выключаем торговлю повторно в этот день.
        if is_live_trading_enabled():
            logger.warning(
                "⚠️ Лимиты риска превышены, но live‑торговля уже включена вручную. "
                "Уважаем override пользователя: %s",
                reason or "лимиты риска",
            )
            # Возвращаем False, чтобы не блокировать размещение ордеров
            return False

        # Первый раз превышены лимиты при включенной авто‑торговле — останавливаем
        set_live_trading_enabled(False)
        logger.critical(
            "🛑 LIVE-ТОРГОВЛЯ АВТОМАТИЧЕСКИ ОСТАНОВЛЕНА: %s",
            reason,
        )
        return True  # Торговля остановлена

    return False  # Лимиты не превышены, можно торговать

