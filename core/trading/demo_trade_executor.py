"""Автоматическое размещение ордеров на live-бирже по сигналам."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import joinedload

from core.config import settings
import core.database as database
from core.models import Signal
from core.trading.bybit_demo_client import bybit_demo_client
from core.trading.trading_mode import is_live_trading_enabled
from core.trading.live_trade_logger import log_signal_event  # type: ignore
from core.trading.risk_manager import enforce_risk_limits, check_risk_limits

logger = logging.getLogger(__name__)


RETRYABLE_STATUSES = {
    "FAILED",
    "CANCELLED",
    "LIVE_DISABLED",
    "NOT_CONFIGURED",
    "INVALID_ENTRY",
    "INVALID_QUANTITY",
    "INVALID_MARKET_PRICE",
    "SIGNAL_TOO_OLD",
    "WAITING_FOR_PRICE",
    # PRICE_DEVIATION_TOO_LARGE и LEVEL_BROKEN - перепроверяются watcher'ом, могут вернуться в WAITING_FOR_PRICE
}


class DemoTradeExecutor:
    """Оркестратор для автоматической отправки сигналов на live-биржу Bybit."""

    # --- Внутренние параметры ожидания и отклонения цены ---
    # Базовый коридор отклонения, если не удалось оценить волатильность
    # ОПТИМИЗИРОВАНО 31.12.2024: увеличено с 0.3% до 0.4%, чтобы порог "цена ушла" (0.4% * 3.0 = 1.2%) был больше максимального расстояния генерации (0.7%)
    BASE_MAX_DEVIATION_PCT = 0.4
    MIN_DEVIATION_PCT = 0.2
    MAX_DEVIATION_PCT = 1.0
    VOLATILITY_COEFF = 3.0  # как сильно масштабируем ATR-процент в доп. допустимое отклонение
    # Множитель, после которого считаем, что цена "улетела" и сигнал проторговать уже нельзя
    TOO_FAR_MULTIPLIER = 3.0
    # Быстрая фаза ожидания (immediate polling), сек
    FAST_WAIT_SECONDS = 30
    FAST_WAIT_STEP_SECONDS = 2
    # Максимальный "возраст" сигнала для попытки торговли (30 минут)
    MAX_SIGNAL_AGE_SECONDS = 30 * 60
    MOSCOW_TZ = timezone(timedelta(hours=3))
    MARKET_ENTRY_THRESHOLD_PCT = settings.DEMO_MARKET_ENTRY_THRESHOLD_PCT

    # Параметры перевода стопа в безубыток (пока фиксированные)
    BREAKEVEN_ENABLED = False  # Отключено: используем фиксированный SL -1%
    BREAKEVEN_MIN_MOVE_PCT = 0.4  # на сколько % цена должна уйти в плюс, чтобы имел смысл BE
    BREAKEVEN_MINUTES = 40  # стандартное время ожидания (если движение < 0.4%)
    BREAKEVEN_FAST_MINUTES = 15  # ускоренное время ожидания (если движение >= 0.4%)

    def _log_signal_event(self, session, signal, message, event_type=None, status=None, details=None):
        try:
            log_signal_event(
                session,
                signal,
                message,
                event_type=event_type,
                status=status,
                details=details,
                commit=False,
            )
        except Exception as err:  # pragma: no cover - логируем, но не падаем
            logger.warning("Не удалось записать live-лог сигнала %s: %s", getattr(signal, "id", signal), err)

    def _should_move_sl_to_breakeven(self, signal, current_price, now) -> bool:
        if not self.BREAKEVEN_ENABLED:
            return False
        if not signal.entry_price or not signal.demo_filled_at:
            return False
        entry_price = signal.entry_price
        if entry_price <= 0 or current_price <= 0:
            return False
        
        # Рассчитываем движение цены
        if signal.signal_type == "LONG":
            move_pct = (current_price / entry_price - 1.0) * 100.0
        else:
            move_pct = (entry_price / current_price - 1.0) * 100.0
        
        # Проверяем минимальное движение (должно быть >= 0.4%)
        if move_pct < self.BREAKEVEN_MIN_MOVE_PCT:
            return False
        
        # Определяем требуемое время ожидания на основе движения:
        # - Если движение >= 0.4% → достаточно 15 минут
        # - Если движение < 0.4% → нужно 40 минут (но мы уже вернули False выше, так что сюда не дойдем)
        elapsed = now - signal.demo_filled_at
        required_minutes = self.BREAKEVEN_FAST_MINUTES if move_pct >= self.BREAKEVEN_MIN_MOVE_PCT else self.BREAKEVEN_MINUTES
        
        if elapsed.total_seconds() < required_minutes * 60:
            return False
        
        # Уже в безубытке?
        # Для LONG: SL должен быть в диапазоне entry_price * 0.999 ± небольшой допуск (~-0.1% от entry)
        # Для SHORT: SL должен быть в диапазоне entry_price * 1.0001 ± небольшой допуск (~+0.1% от entry)
        if signal.demo_sl_price:
            if signal.signal_type == "LONG":
                # Для LONG: проверяем, находится ли SL в диапазоне entry_price * 0.999 ± 0.01%
                # Это соответствует безубытку с небольшим минусом (-0.1%)
                expected_breakeven = entry_price * 0.999  # -0.1%
                if abs(signal.demo_sl_price - expected_breakeven) <= entry_price * 0.0001:
                    return False  # Уже в безубытке
                # Также проверяем старую логику (ровно entry_price) для обратной совместимости
                if abs(signal.demo_sl_price - entry_price) <= entry_price * 0.0001:
                    return False  # Уже в безубытке (старая логика)
            else:  # SHORT
                # Для SHORT: проверяем, находится ли SL в диапазоне entry_price * 1.0001 ± 0.01%
                expected_breakeven = entry_price * 1.0001  # +0.1%
                if abs(signal.demo_sl_price - expected_breakeven) <= entry_price * 0.0001:
                    return False  # Уже в безубытке
                # Также проверяем, не установлен ли SL уже на entry_price или выше (старая логика)
                if signal.demo_sl_price <= entry_price * 1.0001 and signal.demo_sl_price >= entry_price:
                    # SL уже в безубытке или близко к нему
                    return False

        return True

    def _apply_breakeven(self, session, signal, mapped_symbol, current_price, now) -> bool:
        if not self._should_move_sl_to_breakeven(signal, current_price, now):
            return False

        # Проверяем, что позиция еще открыта на бирже
        position_info = bybit_demo_client.get_position_info(mapped_symbol)
        if not position_info:
            logger.warning("⚠️  Попытка установки SL в безубыток для сигнала %s, но позиция уже закрыта на бирже", signal.id)
            return False

        entry_price = signal.entry_price
        
        # КРИТИЧЕСКИ ВАЖНО: Проверяем РЕАЛЬНОЕ состояние SL на бирже, а не только в БД!
        # Если в БД записано demo_sl_price, но на бирже SL нет - сначала устанавливаем SL, а не перемещаем
        real_sl_on_exchange = position_info.get("stopLoss")
        real_sl_price = float(real_sl_on_exchange) if real_sl_on_exchange and float(real_sl_on_exchange) > 0 else None
        
        # Используем реальный SL с биржи, если он есть, иначе берем из БД
        old_sl_price = real_sl_price or signal.demo_sl_price
        
        # Если SL вообще нет на бирже - сначала устанавливаем его, а не перемещаем в безубыток
        if not real_sl_price:
            logger.warning(
                "⚠️  Попытка переместить SL в безубыток для signal_id=%s (%s), но SL НЕ УСТАНОВЛЕН на бирже! "
                "Сначала нужно установить базовый SL. Пропускаем breakeven.",
                signal.id,
                mapped_symbol,
            )
            # Устанавливаем базовый SL вместо breakeven
            sl_pct = settings.DEMO_STOP_LOSS_PERCENT / 100.0
            if signal.signal_type == "LONG":
                base_sl_price = entry_price * (1 - sl_pct)
            else:  # SHORT
                base_sl_price = entry_price * (1 + sl_pct)
            
            # Получаем существующий TP, чтобы не потерять его
            real_tp_on_exchange = position_info.get("takeProfit")
            existing_tp = float(real_tp_on_exchange) if real_tp_on_exchange and float(real_tp_on_exchange) > 0 else signal.demo_tp_price
            
            logger.info(
                "🔧 Устанавливаем базовый SL для signal_id=%s (%s), так как его не было на бирже: SL=%.4f",
                signal.id,
                mapped_symbol,
                base_sl_price,
            )
            
            success = bybit_demo_client.set_position_tp_sl(
                mapped_symbol,
                take_profit=existing_tp,
                stop_loss=base_sl_price,
            )
            
            if success:
                signal.demo_sl_price = base_sl_price
                signal.demo_updated_at = now
                self._log_signal_event(
                    session,
                    signal,
                    f"Установлен базовый стоп-лосс: {base_sl_price:.4f} (SL отсутствовал на бирже)",
                    event_type="SL_INSTALLED",
                    status=signal.demo_status,
                )
                logger.info("✅ Базовый SL установлен для signal_id=%s (%s): %.4f", signal.id, mapped_symbol, base_sl_price)
            else:
                logger.error("❌ Не удалось установить базовый SL для signal_id=%s (%s)", signal.id, mapped_symbol)
            
            return False  # Не продолжаем breakeven, если SL не был установлен
        
        # Новый формат BE: ставим SL не ровно в 0, а с небольшим минусом (~‑0.1% от entry)
        # Для LONG: SL чуть ниже entry; для SHORT: SL чуть выше entry
        move_pct = 0.001  # 0.1%
        if signal.signal_type == "LONG":
            breakeven_sl_price = entry_price * (1 - move_pct)
        else:  # SHORT
            breakeven_sl_price = entry_price * (1 + move_pct)
        
        # Проверяем, не пытаемся ли мы установить тот же SL (защита от повторных попыток)
        if old_sl_price and abs(old_sl_price - breakeven_sl_price) <= entry_price * 0.0001:
            logger.debug("SL уже установлен в безубыток для сигнала %s (старый SL=%.4f, новый SL=%.4f)", 
                        signal.id, old_sl_price, breakeven_sl_price)
            return False
        
        # Вычисляем время в позиции и движение цены для логирования
        time_in_position_minutes = (now - signal.demo_filled_at).total_seconds() / 60.0
        if signal.signal_type == "LONG":
            move_pct = (current_price / entry_price - 1.0) * 100.0
        else:
            move_pct = (entry_price / current_price - 1.0) * 100.0
        
        # Определяем, использовалось ли ускоренное время ожидания (15 мин при движении >= 0.4%)
        used_fast_time = move_pct >= self.BREAKEVEN_MIN_MOVE_PCT and time_in_position_minutes >= self.BREAKEVEN_FAST_MINUTES
        
        logger.info("🔄 Попытка установки SL в безубыток (с небольшим минусом) для сигнала %s (%s): старый SL=%.4f, новый SL=%.4f, время в позиции=%.1f мин, движение=+%.2f%% (использовано %s время ожидания)",
                   signal.id, mapped_symbol, old_sl_price or 0.0, breakeven_sl_price, time_in_position_minutes, move_pct, "ускоренное (15 мин)" if used_fast_time else "стандартное (40 мин)")
        
        success = bybit_demo_client.set_position_tp_sl(
            mapped_symbol,
            take_profit=signal.demo_tp_price,
            stop_loss=breakeven_sl_price,
        )
        if success:
            signal.demo_sl_price = breakeven_sl_price
            signal.demo_status = "SL_TO_BREAKEVEN"
            signal.demo_updated_at = now
            
            time_mode = "ускоренное (15 мин)" if used_fast_time else "стандартное (40 мин)"
            msg = (
                f"Стоп-лосс установлен в безубыток с небольшим минусом (~-0.1%): "
                f"{old_sl_price:.4f} → {breakeven_sl_price:.4f} "
                f"(время в позиции: {time_in_position_minutes:.1f} мин, движение: +{move_pct:.2f}%, {time_mode})"
            )
            self._log_signal_event(
                session,
                signal,
                msg,
                event_type="SL_TO_BREAKEVEN",
                status="SL_TO_BREAKEVEN",
                details={
                    "old_sl_price": old_sl_price,
                    "new_sl_price": breakeven_sl_price,
                    "entry_price": entry_price,
                    "time_in_position_minutes": round(time_in_position_minutes, 1),
                    "price_move_pct": round(move_pct, 2),
                    "current_price": current_price,
                },
            )
            logger.info(
                "✅ Стоп-лосс установлен в безубыток для сигнала %s (%s): %.4f → %.4f",
                signal.id,
                mapped_symbol,
                old_sl_price or 0.0,
                breakeven_sl_price,
            )
            return True
        else:
            # Логируем неудачную попытку с деталями
            # Проверяем, почему не удалось (позиция закрыта? ошибка API?)
            position_info_after_fail = bybit_demo_client.get_position_info(mapped_symbol)
            if not position_info_after_fail:
                error_detail = "Позиция уже закрыта на бирже"
            else:
                error_detail = "Ошибка API биржи при установке SL в безубыток"
            
            # Проверяем, не логировали ли мы уже эту ошибку (защита от спама в логах)
            # Если в demo_error уже есть сообщение о неудачной попытке breakeven, не логируем повторно
            should_log = True
            if signal.demo_error and "безубыток" in signal.demo_error.lower() and "не удалось" in signal.demo_error.lower():
                # Проверяем, не прошло ли достаточно времени с последней попытки (например, 5 минут)
                if signal.demo_updated_at:
                    time_since_last_attempt = (now - signal.demo_updated_at).total_seconds() / 60.0
                    if time_since_last_attempt < 5.0:  # Меньше 5 минут - не логируем повторно
                        should_log = False
                        logger.debug("Пропускаем повторное логирование ошибки breakeven для сигнала %s (последняя попытка %.1f мин назад)",
                                   signal.id, time_since_last_attempt)
            
            if should_log:
                self._log_signal_event(
                    session,
                    signal,
                    f"Не удалось перенести стоп-лосс в безубыток на бирже (время в позиции: {time_in_position_minutes:.1f} мин, причина: {error_detail})",
                    event_type="SL_TO_BREAKEVEN_FAILED",
                    status=signal.demo_status,
                    details={
                        "time_in_position_minutes": round(time_in_position_minutes, 1),
                        "error_detail": error_detail,
                        "position_exists": position_info_after_fail is not None,
                        "old_sl_price": old_sl_price,
                        "target_sl_price": breakeven_sl_price,
                        "entry_price": entry_price,
                        "current_price": current_price,
                    },
                )
                logger.warning(
                    "⚠️  Не удалось перенести стоп-лосс в безубыток на бирже для сигнала %s (%s): %s (время в позиции: %.1f мин, старый SL: %.4f, новый SL: %.4f)",
                    signal.id,
                    mapped_symbol,
                    error_detail,
                    time_in_position_minutes,
                    old_sl_price or 0.0,
                    breakeven_sl_price,
                )
        return False
    MARKET_ENTRY_THRESHOLD_PCT = settings.DEMO_MARKET_ENTRY_THRESHOLD_PCT

    def _get_allowed_price_deviation_pct(self, symbol: str, level_price: float) -> float:
        """
        Возвращает допустимое отклонение цены в процентах с учётом волатильности монеты.

        - Для тихих монет коридор сжимается к ~0.2–0.3%
        - Для волатильных может расширяться до 1.0%
        """
        # Пытаемся оценить волатильность через ATR-подобный метод
        vol_pct = bybit_demo_client.get_symbol_volatility_pct(symbol, timeframe="1m", lookback=30)

        base = self.BASE_MAX_DEVIATION_PCT
        if vol_pct and vol_pct > 0:
            # Например, если ATR% = 0.3, а коэффициент = 3 → добавим ещё ~0.9%
            dynamic_part = vol_pct * self.VOLATILITY_COEFF / 10.0
            max_dev = base + dynamic_part
        else:
            max_dev = base

        # Ограничиваем разумными пределами
        max_dev = max(self.MIN_DEVIATION_PCT, min(self.MAX_DEVIATION_PCT, max_dev))

        logger.info(
            "📏 Допустимое отклонение цены для %s: %.3f%% (volatility=%.3f%%, base=%.3f%%)",
            symbol,
            max_dev,
            vol_pct or 0.0,
            self.BASE_MAX_DEVIATION_PCT,
        )
        return max_dev

    def place_order_for_signal(self, signal_id: int, from_watcher: bool = False) -> Dict[str, Any]:
        result: Dict[str, Any] = {"signal_id": signal_id}
        
        logger.info("🚀 Начало обработки сигнала для live-торговли: signal_id=%s", signal_id)

        if not settings.DEMO_AUTO_TRADING_ENABLED:
            logger.warning("⏸️  Авто-торговля отключена в настройках (DEMO_AUTO_TRADING_ENABLED=False)")
            return {**result, "status": "disabled"}
        if not database.init_database() or database.SessionLocal is None:
            logger.error("❌ База данных недоступна для signal_id=%s", signal_id)
            return {**result, "status": "db_unavailable"}

        # КРИТИЧЕСКИ ВАЖНО: Проверяем лимиты риска перед размещением ордера
        # Это предотвращает дальнейшие убытки при достижении дневного лимита или серии убытков
        risk_stopped = enforce_risk_limits()
        if risk_stopped:
            can_trade, reason = check_risk_limits()
            logger.warning(
                "🚫 Размещение ордера для signal_id=%s заблокировано: %s",
                signal_id,
                reason or "Лимиты риска превышены"
            )
            return {**result, "status": "risk_limit_exceeded", "reason": reason}

        session = database.SessionLocal()
        try:
            # Retry логика: если сигнал только что создан, он может быть ещё не закоммичен
            # Пытаемся найти сигнал с несколькими попытками
            signal = None
            max_retries = 3
            retry_delay = 0.5  # секунды
            
            for attempt in range(max_retries):
                signal = (
                    session.query(Signal)
                    .options(joinedload(Signal.pair))
                    .filter(Signal.id == signal_id)
                    .one_or_none()
                )
                if signal and signal.pair:
                    break
                elif attempt < max_retries - 1:
                    logger.debug("⏳ Сигнал %s ещё не найден, попытка %d/%d, ждём %.1f сек...", 
                               signal_id, attempt + 1, max_retries, retry_delay)
                    session.close()
                    import time
                    time.sleep(retry_delay)
                    session = database.SessionLocal()
                    retry_delay *= 1.5  # Увеличиваем задержку с каждой попыткой
            
            if not signal or not signal.pair:
                logger.error("❌ Сигнал не найден после %d попыток: signal_id=%s", max_retries, signal_id)
                return {**result, "status": "signal_not_found"}

            logger.info("📊 Сигнал найден: ID=%s, Пара=%s, Тип=%s, Entry=%.4f", 
                       signal_id, signal.pair.symbol, signal.signal_type, 
                       signal.entry_price or signal.level_price)

            # КРИТИЧНО: Не обрабатываем закрытые сигналы (они уже в истории)
            if signal.status and signal.status.upper() in ("CLOSED", "STOP_LOSS", "TAKE_PROFIT"):
                logger.warning(
                    "⛔ Сигнал уже закрыт: signal_id=%s, статус=%s, пропускаем обработку",
                    signal_id,
                    signal.status,
                )
                return {**result, "status": "signal_closed", "signal_status": signal.status}

            if not is_live_trading_enabled():
                logger.warning("⏸️  Live-торговля отключена пользователем для signal_id=%s", signal_id)
                self._update_signal_trade_status(signal, "LIVE_DISABLED", "Live-торговля отключена пользователем")
                session.commit()
                return {**result, "status": "live_disabled"}
            
            if not bybit_demo_client.is_enabled():
                logger.warning("⏸️  Bybit API не настроен (нет ключей) для signal_id=%s", signal_id)
                self._update_signal_trade_status(signal, "NOT_CONFIGURED", "BYBIT_API_KEY / SECRET не заданы")
                session.commit()
                return {**result, "status": "bybit_not_configured"}
            
            if signal.demo_status and signal.demo_status.upper() not in RETRYABLE_STATUSES:
                logger.info("⏭️  Сигнал уже обработан: signal_id=%s, trade_status=%s", signal_id, signal.demo_status)
                return {
                    **result,
                    "status": "already_processed",
                    "trade_status": signal.demo_status,
                }

            # --- Жёсткая проверка экранов Элдера: торгуем только если оба экрана пройдены ---
            screen1_ok = bool(signal.elder_screen_1_passed)
            screen2_ok = bool(signal.elder_screen_2_passed)
            if not (screen1_ok and screen2_ok):
                msg = (
                    "Сделка отклонена: проверки Elder's Screens не пройдены "
                    f"(Экран1={signal.elder_screen_1_passed}, Экран2={signal.elder_screen_2_passed})"
                )
                logger.warning("🚫 %s (signal_id=%s, pair=%s)", msg, signal_id, signal.pair.symbol)
                self._update_signal_trade_status(
                    signal,
                    "ELDER_SCREENS_FAILED",
                    msg,
                )
                self._log_signal_event(
                    session,
                    signal,
                    msg,
                    event_type="ELDER_SCREENS_FAILED",
                    status="ELDER_SCREENS_FAILED",
                    details={
                        "screen_1_passed": signal.elder_screen_1_passed,
                        "screen_2_passed": signal.elder_screen_2_passed,
                    },
                )
                session.commit()
                return {
                    **result,
                    "status": "elder_screens_failed",
                    "screen_1_passed": signal.elder_screen_1_passed,
                    "screen_2_passed": signal.elder_screen_2_passed,
                }

            # --- Новое правило: не увеличиваем объем по паре, если уже есть активная сделка или входной ордер ---
            mapped_symbol = self._map_symbol(signal.pair.symbol)

            # 1) Проверяем, есть ли открытая позиция на бирже по этой паре
            try:
                existing_position = bybit_demo_client.get_position_info(mapped_symbol)
            except Exception as err:
                existing_position = None
                logger.warning(
                    "⚠️ Не удалось проверить наличие открытой позиции для %s перед обработкой сигнала %s: %s",
                    mapped_symbol,
                    signal_id,
                    err,
                )

            if existing_position:
                msg = (
                    f"По паре {mapped_symbol} уже есть открытая позиция на бирже, "
                    f"сигнал {signal.id} не будет проторгован, чтобы не увеличивать объем."
                )
                logger.warning("🚫 %s", msg)
                self._update_signal_trade_status(
                    signal,
                    "POSITION_ALREADY_OPEN",
                    msg,
                )
                self._log_signal_event(
                    session,
                    signal,
                    msg,
                    event_type="POSITION_ALREADY_OPEN",
                    status="POSITION_ALREADY_OPEN",
                    details={
                        "existing_position": True,
                        "position_side": existing_position.get("side"),
                        "entry_price": existing_position.get("entry_price"),
                        "contracts": existing_position.get("contracts"),
                    },
                )
                session.commit()
                return {
                    **result,
                    "status": "position_already_open",
                    "reason": "position_exists",
                }

            # 2) Проверяем активные входные ордера по этой паре (не reduceOnly), чтобы не ставить дубли
            try:
                client = bybit_demo_client._get_client()
                open_orders = client.fetch_open_orders(mapped_symbol)
                entry_orders = []
                for order in open_orders:
                    info = order.get("info", {}) or {}
                    reduce_only_flag = (
                        info.get("reduceOnly")
                        or info.get("reduce_only")
                        or order.get("reduceOnly")
                        or False
                    )
                    status = (order.get("status") or "").lower()
                    side = (order.get("side") or "").lower()

                    # Интересуют только активные не reduce-only ордера (вход в позицию)
                    if status not in ("open", "new", "partiallyfilled", "partially_filled"):
                        continue
                    if reduce_only_flag:
                        # это TP/SL, а не входной ордер
                        continue

                    # Фильтруем по направлению сигнала
                    if signal.signal_type == "LONG" and side not in ("buy",):
                        continue
                    if signal.signal_type == "SHORT" and side not in ("sell",):
                        continue

                    entry_orders.append(order.get("id") or order.get("clientOrderId") or "UNKNOWN")

                if entry_orders:
                    msg = (
                        f"По паре {mapped_symbol} уже есть активный входной ордер "
                        f"({', '.join(str(o) for o in entry_orders)}), сигнал {signal.id} не будет проторгован, "
                        f"чтобы не увеличивать объем."
                    )
                    logger.warning("🚫 %s", msg)
                    self._update_signal_trade_status(
                        signal,
                        "POSITION_ALREADY_OPEN",
                        msg,
                    )
                    self._log_signal_event(
                        session,
                        signal,
                        msg,
                        event_type="POSITION_ALREADY_OPEN",
                        status="POSITION_ALREADY_OPEN",
                        details={
                            "existing_position": False,
                            "entry_order_ids": entry_orders,
                        },
                    )
                    session.commit()
                    return {
                        **result,
                        "status": "position_already_open",
                        "reason": "entry_order_exists",
                    }
            except Exception as err:
                logger.warning(
                    "⚠️  Ошибка проверки активных ордеров по %s перед размещением сигнала %s: %s",
                    mapped_symbol,
                    signal_id,
                    err,
                )

            # Проверяем общий "возраст" сигнала (чтобы не торговать слишком старые идеи)
            signal_age_seconds = (datetime.now(timezone.utc) - signal.timestamp).total_seconds()
            if signal_age_seconds > self.MAX_SIGNAL_AGE_SECONDS:
                logger.warning(
                    "⏰ Сигнал слишком старый: signal_id=%s, возраст=%.1f сек (макс=%d сек), пропускаем",
                    signal_id,
                    signal_age_seconds,
                    self.MAX_SIGNAL_AGE_SECONDS,
                )
                self._update_signal_trade_status(
                    signal,
                    "SIGNAL_TOO_OLD",
                    f"Сигнал устарел: прошло {signal_age_seconds:.1f} сек (макс {self.MAX_SIGNAL_AGE_SECONDS} сек)",
                )
                session.commit()
                return {**result, "status": "signal_too_old", "age_seconds": signal_age_seconds}
            
            logger.info(
                "⏱️  Время от формирования сигнала до попытки размещения ордера: %.2f сек (from_watcher=%s)",
                signal_age_seconds,
                from_watcher,
            )

            # Получаем цену уровня сигнала - это наша целевая цена входа
            signal_level_price = self._resolve_entry_price(signal)
            if not signal_level_price or signal_level_price <= 0:
                logger.error("❌ Некорректная цена уровня сигнала: signal_id=%s, level_price=%s", signal_id, signal_level_price)
                self._update_signal_trade_status(signal, "INVALID_ENTRY", "Некорректная цена уровня сигнала")
                session.commit()
                return {**result, "status": "invalid_entry_price"}

            # Проверяем, не стал ли сигнал неактивным (пробитие уровня или отклонение >2%)
            current_market_price = self._get_current_market_price(mapped_symbol)
            if current_market_price and current_market_price > 0:
                is_invalidated, invalid_status, invalid_msg = self.check_signal_invalidated(signal, current_market_price)
                if is_invalidated:
                    logger.warning(
                        "🚫 Сигнал %s неактивен перед попыткой размещения ордера: %s",
                        signal_id, invalid_msg
                    )
                    self._update_signal_trade_status(signal, invalid_status, invalid_msg)
                    self._log_signal_event(
                        session,
                        signal,
                        invalid_msg,
                        event_type=invalid_status,
                        status=invalid_status,
                        details={"current_price": current_market_price, "level_price": signal_level_price},
                    )
                    session.commit()
                    return {**result, "status": "signal_invalidated", "reason": invalid_status, "message": invalid_msg}

            # Вычисляем допустимое отклонение цены с учётом волатильности
            allowed_deviation_pct = self._get_allowed_price_deviation_pct(mapped_symbol, signal_level_price)
            ideal_deviation_pct = allowed_deviation_pct * 0.7  # "идеальный" вход чуть строже допустимого

            def _get_price_deviation() -> Tuple[Optional[float], Optional[float]]:
                current_price = self._get_current_market_price(mapped_symbol)
                if not current_price or current_price <= 0:
                    return None, None
                deviation = abs((current_price / signal_level_price - 1) * 100)
                return current_price, deviation

            # --- Быстрая фаза ожидания подхода цены (immediate polling) ---
            placed = False
            too_far = False
            last_deviation_pct: Optional[float] = None

            if not from_watcher:
                logger.info(
                    "🚦 Старт быстрой фазы ожидания цены: signal_id=%s, уровень=%.4f, доп.откл=%.3f%%",
                    signal_id,
                    signal_level_price,
                    allowed_deviation_pct,
                )

                end_time = datetime.now(timezone.utc).timestamp() + self.FAST_WAIT_SECONDS
                import time

                while datetime.now(timezone.utc).timestamp() < end_time:
                    current_market_price, price_deviation_pct = _get_price_deviation()
                    if current_market_price is None:
                        logger.warning(
                            "⚠️  Не удалось получить цену в быстрой фазе: signal_id=%s, symbol=%s",
                            signal_id,
                            mapped_symbol,
                        )
                        time.sleep(self.FAST_WAIT_STEP_SECONDS)
                        continue
                    last_deviation_pct = price_deviation_pct

                    if price_deviation_pct <= allowed_deviation_pct:
                        logger.info(
                            "✅ Цена вошла в допустимый коридор: signal_id=%s, уровень=%.4f, текущая=%.4f, отклонение=%.3f%% (допустимо=%.3f%%)",
                            signal_id,
                            signal_level_price,
                            current_market_price,
                            price_deviation_pct,
                            allowed_deviation_pct,
                        )
                        placed = True
                        break

                    if price_deviation_pct >= allowed_deviation_pct * self.TOO_FAR_MULTIPLIER:
                        logger.warning(
                            "🚫 Цена ушла слишком далеко во время ожидания: signal_id=%s, уровень=%.4f, текущая=%.4f, отклонение=%.3f%% (порог=%.3f%%, x%.1f)",
                            signal_id,
                            signal_level_price,
                            current_market_price,
                            price_deviation_pct,
                            allowed_deviation_pct,
                            self.TOO_FAR_MULTIPLIER,
                        )
                        too_far = True
                        break

                        # цена ещё не в нужном коридоре, но и не улетела слишком далеко — ждём дальше
                    time.sleep(self.FAST_WAIT_STEP_SECONDS)

                if not placed and not too_far:
                    # Быстрая фаза закончилась, но цена пока не дошла — переводим сигнал в ожидание
                    logger.info(
                        "⏳ Цена пока не дошла до уровня, переводим сигнал в WAITING_FOR_PRICE: signal_id=%s",
                        signal_id,
                    )
                    self._update_signal_trade_status(
                        signal,
                        "WAITING_FOR_PRICE",
                        self._format_waiting_status_message(
                            allowed_deviation_pct, last_deviation_pct
                        ),
                    )
                    self._log_signal_event(
                        session,
                        signal,
                        "Ожидаем подхода цены к уровню",
                        event_type="WAITING",
                        status="WAITING_FOR_PRICE",
                    )
                    session.commit()
                    return {
                        **result,
                        "status": "waiting_for_price",
                        "allowed_deviation_pct": allowed_deviation_pct,
                    }

                if too_far:
                    self._update_signal_trade_status(
                        signal,
                        "PRICE_DEVIATION_TOO_LARGE",
                        f"Цена ушла слишком далеко от уровня во время ожидания (>{allowed_deviation_pct * self.TOO_FAR_MULTIPLIER:.3f}%)",
                    )
                    self._log_signal_event(
                        session,
                        signal,
                        f"Цена ушла слишком далеко: отклонение {price_deviation_pct:.3f}%",
                        event_type="PRICE_DEVIATED",
                        status="PRICE_DEVIATION_TOO_LARGE",
                    )
                    session.commit()
                    return {
                        **result,
                        "status": "price_deviation_too_large",
                        "allowed_deviation_pct": allowed_deviation_pct,
                    }

                # Если мы сюда дошли и placed=True — у нас есть актуальная current_market_price/price_deviation_pct
                current_market_price, price_deviation_pct = _get_price_deviation()
                if current_market_price is None:
                    logger.error(
                        "❌ Не удалось получить цену после быстрой фазы ожидания: signal_id=%s, symbol=%s",
                        signal_id,
                        mapped_symbol,
                    )
                    self._update_signal_trade_status(
                        signal,
                        "INVALID_ENTRY",
                        "Не удалось получить рыночную цену после ожидания",
                    )
                    session.commit()
                    return {**result, "status": "invalid_market_price"}

            else:
                # Вызов из background watcher: делаем одиночную проверку без длительного ожидания
                current_market_price, price_deviation_pct = _get_price_deviation()
                if current_market_price is None:
                    logger.warning(
                        "⚠️  Watcher: не удалось получить текущую цену: signal_id=%s, symbol=%s",
                        signal_id,
                        mapped_symbol,
                    )
                    return {**result, "status": "waiting_price_unavailable"}

                if price_deviation_pct > allowed_deviation_pct * self.TOO_FAR_MULTIPLIER:
                    logger.warning(
                        "🚫 Watcher: цена ушла слишком далеко: signal_id=%s, уровень=%.4f, текущая=%.4f, отклонение=%.3f%% (порог=%.3f%%, x%.1f)",
                        signal_id,
                        signal_level_price,
                        current_market_price,
                        price_deviation_pct,
                        allowed_deviation_pct,
                        self.TOO_FAR_MULTIPLIER,
                    )
                    self._update_signal_trade_status(
                        signal,
                        "PRICE_DEVIATION_TOO_LARGE",
                        f"Цена ушла слишком далеко от уровня в watcher (>{allowed_deviation_pct * self.TOO_FAR_MULTIPLIER:.3f}%)",
                    )
                    self._log_signal_event(
                        session,
                        signal,
                        f"Цена ушла слишком далеко: отклонение {price_deviation_pct:.3f}%",
                        event_type="PRICE_DEVIATED",
                        status="PRICE_DEVIATION_TOO_LARGE",
                    )
                    session.commit()
                    return {
                        **result,
                        "status": "price_deviation_too_large",
                        "deviation_pct": price_deviation_pct,
                    }

                if price_deviation_pct > allowed_deviation_pct:
                    logger.info(
                        "⏳ Watcher: цена ещё не в допустимом коридоре: signal_id=%s, отклонение=%.3f%% (допустимо=%.3f%%)",
                        signal_id,
                        price_deviation_pct,
                        allowed_deviation_pct,
                    )
                    # Оставляем статус WAITING_FOR_PRICE
                    self._update_signal_trade_status(
                        signal,
                        "WAITING_FOR_PRICE",
                        self._format_waiting_status_message(
                            allowed_deviation_pct, price_deviation_pct
                        ),
                    )
                    self._log_signal_event(
                        session,
                        signal,
                        f"Цена пока не в коридоре (откл. {price_deviation_pct:.3f}%)",
                        event_type="WAITING",
                        status="WAITING_FOR_PRICE",
                    )
                    session.commit()
                    return {
                        **result,
                        "status": "waiting_for_price",
                        "deviation_pct": price_deviation_pct,
                    }

            # На этом этапе current_market_price и price_deviation_pct гарантированно валидны
            logger.info(
                "📊 Цена подходит для входа: signal_id=%s, уровень=%.4f, текущая=%.4f, отклонение=%.3f%% (допустимо=%.3f%%, идеал=%.3f%%)",
                signal_id,
                signal_level_price,
                current_market_price,
                price_deviation_pct,
                allowed_deviation_pct,
                ideal_deviation_pct,
            )

            use_market_entry = (
                self.MARKET_ENTRY_THRESHOLD_PCT
                and self.MARKET_ENTRY_THRESHOLD_PCT > 0
                and price_deviation_pct <= self.MARKET_ENTRY_THRESHOLD_PCT
            )

            if use_market_entry:
                order_type = "market"
                entry_price = current_market_price
                logger.info(
                    "⚡ Переходим на MARKET-вход: текущая=%.4f, отклонение=%.3f%% (порог=%.3f%%)",
                    current_market_price,
                    price_deviation_pct,
                    self.MARKET_ENTRY_THRESHOLD_PCT,
                )
                self._log_signal_event(
                    session,
                    signal,
                    f"Маркет-вход по цене {current_market_price:.4f} (откл. {price_deviation_pct:.3f}%)",
                    event_type="ORDER_DECISION",
                    status="MARKET_ENTRY",
                )
            else:
                order_type = "limit"
                entry_price = signal_level_price
                if price_deviation_pct <= ideal_deviation_pct:
                    logger.info(
                        "✅ Limit ордер по цене уровня: цена=%.4f, текущая=%.4f, отклонение=%.3f%% (идеально)",
                        entry_price,
                        current_market_price,
                        price_deviation_pct,
                    )
                else:
                    logger.info(
                        "📊 Limit ордер по цене уровня: цена=%.4f, текущая=%.4f, отклонение=%.3f%% (в пределах нормы)",
                        entry_price,
                        current_market_price,
                        price_deviation_pct,
                    )
                self._log_signal_event(
                    session,
                    signal,
                    f"Лимитный ордер на уровне {entry_price:.4f} (откл. {price_deviation_pct:.3f}%)",
                    event_type="ORDER_DECISION",
                    status="LIMIT_ENTRY",
                )

            quantity = self._calculate_quantity(entry_price)
            if quantity <= 0:
                logger.error("❌ Некорректный объем: signal_id=%s, quantity=%s, entry_price=%s", 
                           signal_id, quantity, entry_price)
                self._update_signal_trade_status(signal, "INVALID_QUANTITY", "Некорректный объем позиции")
                session.commit()
                return {**result, "status": "invalid_quantity"}

            # Рассчитываем TP/SL от выбранной цены входа
            # После исполнения проверим реальную цену и при необходимости обновим TP/SL
            tp_price, sl_price = self._calculate_tp_sl(signal, entry_price)
            price = entry_price if order_type == "limit" else None
            params = self._build_order_params(tp_price, sl_price)

            logger.info("📋 Параметры ордера (TP/SL от ожидаемой цены): signal_id=%s, symbol=%s, side=%s, type=%s, quantity=%.6f, entry=%.4f, TP=%.4f, SL=%.4f",
                       signal_id, mapped_symbol, "buy" if signal.signal_type == "LONG" else "sell",
                       order_type, quantity, entry_price, tp_price, sl_price)

            self._apply_leverage(mapped_symbol)

            now = datetime.now(timezone.utc)
            signal.demo_status = "SUBMITTING"
            signal.demo_quantity = quantity
            signal.demo_tp_price = None  # Будет установлен после исполнения
            signal.demo_sl_price = None  # Будет установлен после исполнения
            signal.demo_error = None
            signal.demo_submitted_at = now
            signal.demo_updated_at = now
            session.commit()
            logger.info("💾 Статус сигнала обновлен на SUBMITTING: signal_id=%s", signal_id)

            logger.info("📤 Отправка ордера на биржу (без TP/SL): signal_id=%s, symbol=%s", signal_id, mapped_symbol)
            order = bybit_demo_client.place_order(
                symbol=mapped_symbol,
                side="buy" if signal.signal_type == "LONG" else "sell",
                order_type=order_type,
                amount=quantity,
                price=price,
                params=params,
            )

            status = (order.get("status") or "placed").upper()
            signal.demo_order_id = order.get("id")
            signal.demo_status = status
            signal.demo_updated_at = datetime.now(timezone.utc)
            self._log_signal_event(
                session,
                signal,
                f"Ордер отправлен ({order_type.upper()}) id={signal.demo_order_id or 'N/A'}",
                event_type="ORDER_SUBMITTED",
                status=status,
                details={"type": order_type, "quantity": quantity, "price": entry_price if order_type == "limit" else None},
            )
            
            # Обновляем цену входа в сигнале на цену уровня (для limit ордеров это ожидаемая цена входа)
            if entry_price and entry_price != signal.entry_price:
                old_entry = signal.entry_price
                signal.entry_price = entry_price
                logger.info("💰 Обновлена ожидаемая цена входа: signal_id=%s, было=%.4f, стало=%.4f",
                           signal_id, old_entry or signal.level_price, entry_price)
            
            session.commit()
            
            # Сохраняем ожидаемые TP/SL (рассчитанные от цены limit ордера)
            expected_tp_price = tp_price
            expected_sl_price = sl_price
            
            # Обновляем сигнал с ожидаемыми значениями
            signal.entry_price = entry_price
            signal.demo_tp_price = expected_tp_price
            signal.demo_sl_price = expected_sl_price
            signal.demo_status = status
            signal.demo_updated_at = datetime.now(timezone.utc)
            session.commit()
            
            # Ждем исполнения ордера и получаем реальную цену и время входа
            fill_info = self._wait_for_order_fill_and_get_entry_info(mapped_symbol, signal.demo_order_id, signal.signal_type)
            
            if fill_info and fill_info.get("price") and fill_info.get("price") > 0:
                real_entry_price = fill_info.get("price")
                signal.demo_status = "OPEN_POSITION"
                
                # КРИТИЧЕСКИ ВАЖНО: используем РЕАЛЬНОЕ время исполнения ордера из биржи, а не текущее время системы!
                fill_timestamp = fill_info.get("timestamp")
                fill_datetime_str = fill_info.get("datetime")
                
                if fill_timestamp:
                    # Конвертируем timestamp (миллисекунды) в datetime UTC
                    if isinstance(fill_timestamp, (int, float)):
                        # Если timestamp в миллисекундах (обычно для бирж)
                        if fill_timestamp > 1e10:  # Если больше 10^10, значит в миллисекундах
                            fill_timestamp = fill_timestamp / 1000
                        signal.demo_filled_at = datetime.fromtimestamp(fill_timestamp, tz=timezone.utc)
                        logger.info("✅ Установлено реальное время исполнения ордера из биржи: %s (timestamp=%s)",
                                   signal.demo_filled_at, fill_timestamp)
                    else:
                        # Fallback: используем текущее время, если timestamp невалидный
                        signal.demo_filled_at = datetime.now(timezone.utc)
                        logger.warning("⚠️  Невалидный timestamp ордера, используется текущее время")
                elif fill_datetime_str:
                    # Пробуем распарсить datetime строку (ISO format или другие форматы)
                    try:
                        # Пробуем ISO format сначала
                        if 'T' in fill_datetime_str or ' ' in fill_datetime_str:
                            # Убираем 'Z' в конце и добавляем timezone если нужно
                            dt_str = fill_datetime_str.replace('Z', '+00:00').replace('z', '+00:00')
                            if dt_str.endswith('+00:00') or dt_str.endswith('-00:00'):
                                signal.demo_filled_at = datetime.fromisoformat(dt_str)
                            else:
                                # Если нет timezone, добавляем UTC
                                signal.demo_filled_at = datetime.fromisoformat(fill_datetime_str).replace(tzinfo=timezone.utc)
                        else:
                            # Если формат не ISO, пробуем dateutil
                            from dateutil import parser
                            signal.demo_filled_at = parser.parse(fill_datetime_str)
                            if signal.demo_filled_at.tzinfo is None:
                                signal.demo_filled_at = signal.demo_filled_at.replace(tzinfo=timezone.utc)
                        logger.info("✅ Установлено реальное время исполнения ордера из биржи: %s (datetime=%s)",
                                   signal.demo_filled_at, fill_datetime_str)
                    except Exception as e:
                        logger.warning("⚠️  Не удалось распарсить datetime ордера '%s': %s, используется текущее время", fill_datetime_str, e)
                        signal.demo_filled_at = datetime.now(timezone.utc)
                else:
                    # Fallback: если нет ни timestamp, ни datetime, используем текущее время
                    signal.demo_filled_at = datetime.now(timezone.utc)
                    logger.warning("⚠️  Нет информации о времени исполнения ордера, используется текущее время")
                
                # Сначала логируем исполнение ордера
                self._log_signal_event(
                    session,
                    signal,
                    f"Ордер исполнен по цене {real_entry_price:.4f}",
                    event_type="ORDER_FILLED",
                    status="FILLED",
                )
                
                # Затем логируем открытие позиции
                self._log_signal_event(
                    session,
                    signal,
                    f"Позиция открыта по цене {real_entry_price:.4f}",
                    event_type="POSITION_FILLED",
                    status="OPEN_POSITION",
                )
                # Проверяем, отличается ли реальная цена от ожидаемой
                price_diff_pct = abs((real_entry_price / entry_price - 1) * 100)
                
                if price_diff_pct > 0.1:  # Если разница больше 0.1%
                    logger.warning("⚠️  Реальная цена входа отличается от ожидаемой: signal_id=%s, реальная=%.4f, ожидаемая=%.4f, разница=%.3f%%",
                                 signal_id, real_entry_price, entry_price, price_diff_pct)
                    
                    # Пересчитываем TP/SL от РЕАЛЬНОЙ цены входа
                    real_tp_price, real_sl_price = self._calculate_tp_sl(signal, real_entry_price)
                    
                    logger.info("🔄 Пересчет TP/SL от реальной цены входа: signal_id=%s, старый TP=%.4f→%.4f, старый SL=%.4f→%.4f",
                               signal_id, expected_tp_price, real_tp_price, expected_sl_price, real_sl_price)
                    
                    # Пытаемся обновить TP/SL в позиции
                    tp_sl_updated = bybit_demo_client.set_position_tp_sl(mapped_symbol, real_tp_price, real_sl_price)
                    if tp_sl_updated:
                        logger.info("✅ TP/SL обновлены в позиции от реальной цены входа: signal_id=%s", signal_id)
                    else:
                        logger.warning("⚠️  Не удалось обновить TP/SL в позиции, используем значения от ожидаемой цены: signal_id=%s", signal_id)
                        real_tp_price = expected_tp_price
                        real_sl_price = expected_sl_price
                    
                    # Обновляем сигнал с реальными значениями
                    signal.entry_price = real_entry_price
                    signal.demo_tp_price = real_tp_price
                    signal.demo_sl_price = real_sl_price
                else:
                    logger.info("✅ Реальная цена входа совпадает с ожидаемой: signal_id=%s, цена=%.4f, разница=%.3f%% (в пределах нормы)",
                               signal_id, real_entry_price, price_diff_pct)
                    # Обновляем только цену входа (TP/SL уже правильные)
                    signal.entry_price = real_entry_price
            else:
                logger.warning("⚠️  Не удалось получить реальную цену входа позиции: signal_id=%s, используем ожидаемую цену %.4f",
                             signal_id, entry_price)
                # Оставляем ожидаемые значения
            
            if signal.demo_status != "OPEN_POSITION":
                signal.demo_status = "FILLED" if status in ("FILLED", "CLOSED") else status
            signal.demo_updated_at = datetime.now(timezone.utc)
            session.commit()
            
            # КРИТИЧЕСКИ ВАЖНО: Проверяем, что TP и SL действительно установлены на бирже
            # Если ордер был размещен с TP/SL в params, но биржа не установила оба - доустанавливаем
            if fill_info and fill_info.get("price"):
                logger.info("🔍 Проверка установки TP/SL на бирже для signal_id=%s...", signal_id)
                position_info = bybit_demo_client.get_position_info(mapped_symbol)
                
                if position_info:
                    current_tp = position_info.get("takeProfit")
                    current_sl = position_info.get("stopLoss")
                    final_tp = signal.demo_tp_price or expected_tp_price
                    final_sl = signal.demo_sl_price or expected_sl_price
                    
                    tp_missing = not current_tp or float(current_tp) <= 0
                    sl_missing = not current_sl or float(current_sl) <= 0
                    
                    if tp_missing or sl_missing:
                        logger.warning(
                            "⚠️  TP/SL не полностью установлены на бирже для signal_id=%s: TP=%s (нужно %.4f), SL=%s (нужно %.4f). "
                            "Попытка доустановить...",
                            signal_id,
                            current_tp or "НЕТ",
                            final_tp,
                            current_sl or "НЕТ",
                            final_sl,
                        )
                        
                        # Доустанавливаем отсутствующие TP/SL
                        tp_to_set = final_tp if tp_missing else None
                        sl_to_set = final_sl if sl_missing else None
                        
                        # Если устанавливаем только один, сохраняем существующий другой
                        if tp_to_set and not sl_to_set:
                            sl_to_set = float(current_sl) if current_sl else None
                        if sl_to_set and not tp_to_set:
                            tp_to_set = float(current_tp) if current_tp else None
                        
                        if tp_to_set or sl_to_set:
                            tp_sl_updated = bybit_demo_client.set_position_tp_sl(
                                mapped_symbol,
                                take_profit=tp_to_set,
                                stop_loss=sl_to_set,
                            )
                            if tp_sl_updated:
                                logger.info("✅ TP/SL успешно доустановлены для signal_id=%s", signal_id)
                                # Обновляем значения в сигнале
                                if tp_to_set:
                                    signal.demo_tp_price = tp_to_set
                                if sl_to_set:
                                    signal.demo_sl_price = sl_to_set
                                session.commit()
                            else:
                                logger.error(
                                    "❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось доустановить TP/SL для signal_id=%s. "
                                    "Позиция открыта БЕЗ полной защиты!",
                                    signal_id,
                                )
                                self._log_signal_event(
                                    session,
                                    signal,
                                    f"КРИТИЧЕСКАЯ ОШИБКА: TP/SL не установлены на бирже. TP нужен: {final_tp:.4f}, SL нужен: {final_sl:.4f}",
                                    event_type="TP_SL_MISSING",
                                    status=signal.demo_status,
                                )
                    else:
                        logger.info("✅ TP/SL проверены и установлены на бирже для signal_id=%s: TP=%.4f, SL=%.4f", 
                                   signal_id, float(current_tp), float(current_sl))
            
            total_delay_seconds = (datetime.now(timezone.utc) - signal.timestamp).total_seconds()
            final_tp = signal.demo_tp_price or expected_tp_price
            final_sl = signal.demo_sl_price or expected_sl_price
            final_entry = signal.entry_price or entry_price
            logger.info("✅ Ордер размещен: signal_id=%s, order_id=%s, entry=%.4f, TP=%.4f, SL=%.4f, задержка=%.2f сек", 
                       signal_id, signal.demo_order_id, final_entry, final_tp, final_sl, total_delay_seconds)

            result.update(
                {
                    "status": "submitted",
                    "exchange_status": status,
                    "order_id": signal.demo_order_id,
                    "symbol": mapped_symbol,
                    "quantity": quantity,
                    "tp_price": tp_price,
                    "sl_price": sl_price,
                }
            )
            return result

        except Exception as exc:  # pragma: no cover - зависит от внешнего API
            logger.exception("Не удалось разместить ордер на бирже для сигнала %s: %s", signal_id, exc)
            error_text = str(exc)
            self._log_signal_event(
                session,
                signal,
                f"Ошибка размещения ордера: {error_text}",
                event_type="ERROR",
                status="FAILED",
            )
            session_fail = None
            try:
                session.rollback()
                session_fail = database.SessionLocal()
                failed_signal = (
                    session_fail.query(Signal)
                    .options(joinedload(Signal.pair))
                    .filter(Signal.id == signal_id)
                    .one_or_none()
                )
                if failed_signal:
                    failed_signal.demo_status = "FAILED"
                    failed_signal.demo_error = error_text[:500]
                    failed_signal.demo_updated_at = datetime.now(timezone.utc)
                    session_fail.commit()
            except Exception as update_error:  # pragma: no cover - fail-safe
                logger.warning("Не удалось обновить статус ордера: %s", update_error)
            finally:
                if session_fail is not None:
                    try:
                        session_fail.close()
                    except Exception:
                        pass
            return {**result, "status": "failed", "error": error_text}
        finally:
            session.close()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _resolve_entry_price(self, signal: Signal) -> Optional[float]:
        return signal.entry_price or signal.level_price

    def _calculate_quantity(self, entry_price: float) -> float:
        order_value = max(settings.DEMO_ORDER_SIZE_USDT, 0.0)
        if entry_price <= 0 or order_value <= 0:
            return 0.0
        raw_qty = order_value / entry_price
        precision = max(settings.DEMO_QUANTITY_PRECISION, 0)
        factor = 10 ** precision
        return max(0.0, int(raw_qty * factor) / factor)

    def _calculate_tp_sl(self, signal: Signal, entry_price: float) -> Tuple[Optional[float], Optional[float]]:
        tp_pct = settings.DEMO_TAKE_PROFIT_PERCENT / 100.0
        sl_pct = settings.DEMO_STOP_LOSS_PERCENT / 100.0

        if signal.signal_type == "LONG":
            tp_price = entry_price * (1 + tp_pct)
            # ВАЖНО: Всегда используем расчетный SL на основе entry_price, игнорируем signal.stop_loss
            sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct)
            # ВАЖНО: Всегда используем расчетный SL на основе entry_price, игнорируем signal.stop_loss
            sl_price = entry_price * (1 + sl_pct)

        return round(tp_price, 6), round(sl_price, 6)

    def _map_symbol(self, symbol: str) -> str:
        if not symbol:
            return symbol
        suffix = settings.DEMO_SYMBOL_SUFFIX or ""
        if suffix and symbol.endswith("/USDT") and not symbol.endswith(suffix):
            return f"{symbol}{suffix}"
        return symbol

    def _build_order_params(self, tp_price: Optional[float], sl_price: Optional[float]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if tp_price:
            params["takeProfit"] = tp_price
        if sl_price:
            params["stopLoss"] = sl_price
        if settings.DEMO_TIME_IN_FORCE:
            params["timeInForce"] = settings.DEMO_TIME_IN_FORCE
        if settings.DEMO_POSITION_IDX is not None:
            params["positionIdx"] = settings.DEMO_POSITION_IDX
        return params

    def _apply_leverage(self, symbol: str) -> None:
        leverage = settings.DEMO_LEVERAGE
        if leverage and leverage > 0:
            try:
                bybit_demo_client.ensure_leverage(symbol, leverage)
            except Exception as err:  # pragma: no cover - зависит от API
                logger.warning("Не удалось применить плечо %s для %s: %s", leverage, symbol, err)

    def check_signal_invalidated(self, signal: Signal, current_price: float) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Проверяет, стал ли сигнал неактивным из-за пробития уровня или большого отклонения цены.
        
        Returns:
            (is_invalidated, status, message) - True если сигнал неактивен, статус и сообщение
        """
        if not signal.level_price or signal.level_price <= 0:
            return False, None, None
        
        level_price = signal.level_price
        deviation_pct = abs((current_price / level_price - 1) * 100.0)
        
        # Проверка 1: Отклонение цены на >2% от уровня
        MAX_DEVIATION_PCT = 2.0
        if deviation_pct > MAX_DEVIATION_PCT:
            msg = f"Цена ушла на {deviation_pct:.3f}% от уровня (порог {MAX_DEVIATION_PCT:.2f}%)"
            logger.warning(
                "🚫 Сигнал %s неактивен: %s (уровень=%.4f, текущая=%.4f)",
                signal.id, msg, level_price, current_price
            )
            return True, "PRICE_DEVIATION_TOO_LARGE", msg
        
        # Проверка 2: Пробитие уровня против нашего направления
        # Пробитие считается только если отклонение значительное (>0.2%), 
        # чтобы не блокировать сигналы, когда цена практически на уровне
        LEVEL_BREAK_THRESHOLD_PCT = 0.2
        
        if signal.signal_type == "LONG":
            # Для LONG: если цена ушла ниже уровня на >0.2% (пробитие вниз) → сигнал неактивен
            if current_price < level_price:
                break_pct = ((level_price - current_price) / level_price) * 100.0
                if break_pct > LEVEL_BREAK_THRESHOLD_PCT:
                    msg = f"Уровень пробит вниз: цена {current_price:.4f} < уровня {level_price:.4f} (пробитие {break_pct:.3f}%)"
                    logger.warning(
                        "🚫 Сигнал %s неактивен: %s",
                        signal.id, msg
                    )
                    return True, "LEVEL_BROKEN", msg
        else:  # SHORT
            # Для SHORT: если цена ушла выше уровня на >0.2% (пробитие вверх) → сигнал неактивен
            if current_price > level_price:
                break_pct = ((current_price - level_price) / level_price) * 100.0
                if break_pct > LEVEL_BREAK_THRESHOLD_PCT:
                    msg = f"Уровень пробит вверх: цена {current_price:.4f} > уровня {level_price:.4f} (пробитие {break_pct:.3f}%)"
                    logger.warning(
                        "🚫 Сигнал %s неактивен: %s",
                        signal.id, msg
                    )
                    return True, "LEVEL_BROKEN", msg
        
        return False, None, None

    def _update_signal_trade_status(self, signal: Signal, status: str, error: Optional[str] = None) -> None:
        signal.demo_status = status
        if error:
            signal.demo_error = error[:500]
        signal.demo_updated_at = datetime.now(timezone.utc)

    def _format_waiting_status_message(
        self, allowed_deviation_pct: float, current_deviation_pct: Optional[float]
    ) -> str:
        """Строит текст для статуса WAITING_FOR_PRICE с последней проверкой."""
        timestamp_str = datetime.now(self.MOSCOW_TZ).strftime("%H:%M:%S МСК")
        if current_deviation_pct is None:
            return (
                f"Ожидание подхода цены: допустимое отклонение ≤ {allowed_deviation_pct:.3f}%. "
                f"Обновлено {timestamp_str}"
            )
        return (
            f"Ожидание подхода цены: отклонение {current_deviation_pct:.3f}% "
            f"(порог {allowed_deviation_pct:.3f}%). Обновлено {timestamp_str}"
        )

    def _get_current_market_price(self, symbol: str) -> Optional[float]:
        """Получает текущую рыночную цену через API биржи."""
        price = bybit_demo_client.get_current_price(symbol)
        if price:
            logger.debug("📈 Текущая рыночная цена для %s: %.4f", symbol, price)
        return price

    def _wait_for_order_fill_and_get_entry_price(self, symbol: str, order_id: str, signal_type: str, max_wait_seconds: int = 10) -> Optional[float]:
        """
        Ждет исполнения ордера и возвращает реальную цену входа из исполненного ордера.
        СТРОГО использует данные из ордера, а не из позиции!
        УСТАРЕВШИЙ МЕТОД: используйте _wait_for_order_fill_and_get_entry_info для получения времени исполнения.
        """
        fill_info = self._wait_for_order_fill_and_get_entry_info(symbol, order_id, signal_type, max_wait_seconds)
        if fill_info:
            return fill_info.get("price")
        return None

    def _wait_for_order_fill_and_get_entry_info(self, symbol: str, order_id: str, signal_type: str, max_wait_seconds: int = 10) -> Optional[Dict[str, Any]]:
        """
        Ждет исполнения ордера и возвращает реальную цену и время входа из исполненного ордера.
        СТРОГО использует данные из ордера, а не из позиции!
        
        Returns:
            Dict с ключами: price, timestamp, datetime или None
        """
        import time
        from typing import Dict, Any
        
        start_time = time.time()
        
        # Сначала пробуем получить информацию из ордера (возможно ордер уже исполнился)
        fill_info = bybit_demo_client.get_order_fill_info(order_id, symbol)
        if fill_info and fill_info.get("price") and fill_info.get("price") > 0:
            logger.info("✅ Получена информация об исполнении ордера: order_id=%s, entry_price=%.4f, время=%s",
                       order_id, fill_info.get("price"), fill_info.get("datetime") or fill_info.get("timestamp"))
            return fill_info
        
        # Если ордер еще не исполнен, ждем
        while time.time() - start_time < max_wait_seconds:
            try:
                # Пробуем получить информацию из ордера (ордер мог исполниться)
                fill_info = bybit_demo_client.get_order_fill_info(order_id, symbol)
                if fill_info and fill_info.get("price") and fill_info.get("price") > 0:
                    logger.info("✅ Ордер исполнен, получена информация: order_id=%s, entry_price=%.4f, время=%s",
                               order_id, fill_info.get("price"), fill_info.get("datetime") or fill_info.get("timestamp"))
                    return fill_info
                
                # Если ордер еще не исполнен, ждем
                time.sleep(0.5)
                
            except Exception as exc:
                logger.warning("⚠️  Ошибка проверки ордера %s: %s, продолжаем ожидание...", order_id, exc)
                time.sleep(0.5)
        
        # Если не дождались исполнения, возвращаем None
        logger.warning("⏰ Превышено время ожидания исполнения ордера: order_id=%s, ордер еще не исполнен", order_id)
        return None


demo_trade_executor = DemoTradeExecutor()


