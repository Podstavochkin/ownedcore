import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional

import ccxt
import requests

from core.config import settings

logger = logging.getLogger(__name__)


class BybitDemoClient:
    """Обертка над ccxt.bybit для демо-торговли.

    Используется только когда указаны API ключи Bybit и включен демо-режим.
    """

    def __init__(self) -> None:
        self._client: Optional[ccxt.bybit] = None

    def is_enabled(self) -> bool:
        return bool(settings.BYBIT_API_KEY and settings.BYBIT_API_SECRET)

    def _get_client(self) -> ccxt.bybit:
        if self._client:
            return self._client

        if not self.is_enabled():
            raise RuntimeError("Bybit demo client is not configured")

        exchange = ccxt.bybit(
            {
                "apiKey": settings.BYBIT_API_KEY,
                "secret": settings.BYBIT_API_SECRET,
                "enableRateLimit": True,
                "options": {
                    "defaultType": settings.DEMO_MARKET_TYPE or "contract",
                },
            }
        )

        custom_api = settings.DEMO_BYBIT_API_BASE_URL
        if custom_api:
            api_urls = exchange.urls.get("api", {})
            for key in ("public", "private", "v3", "publicLinear", "privateLinear", "publicInverse", "privateInverse"):
                if key in api_urls:
                    api_urls[key] = custom_api
            exchange.urls["api"] = api_urls
            try:
                exchange.set_sandbox_mode(False)
            except Exception:
                pass
        else:
            try:
                exchange.set_sandbox_mode(settings.BYBIT_DEMO)
            except Exception as err:  # pragma: no cover - зависит от версии ccxt
                logger.warning("Не удалось переключить Bybit в sandbox-режим: %s", err)

        if settings.DEMO_BYBIT_DEMO_HEADER:
            exchange.headers = exchange.headers or {}
            exchange.headers["X-BAPI-Demo-Trading"] = "1"
            exchange.headers["X-BAPI-Simulated-Trading"] = "1"

        self._client = exchange
        return exchange

    # --------- Публичные методы ---------

    def ensure_leverage(self, symbol: str, leverage: Optional[float]) -> None:
        """Устанавливает плечо для пары, если указано."""
        if not leverage:
            return
        client = self._get_client()
        try:
            client.set_leverage(
                leverage,
                symbol,
                {
                    "buyLeverage": leverage,
                    "sellLeverage": leverage,
                },
            )
        except Exception as err:  # pragma: no cover - зависит от API
            logger.warning("Не удалось установить плечо %s для %s: %s", leverage, symbol, err)

    def get_status(self) -> Dict[str, Any]:
        """Возвращает балансы, позиции и открытые ордера."""
        if not self.is_enabled():
            return {
                "enabled": False,
                "connected": False,
                "message": "Укажите BYBIT_API_KEY / BYBIT_API_SECRET в .env для активации демо-торговли.",
            }

        client = self._get_client()

        try:
            balance_raw = client.fetch_balance()
            positions_raw = client.fetch_positions()
            orders_raw = client.fetch_open_orders()

            account = self._format_balance(balance_raw)
            positions = self._format_positions(positions_raw)
            orders = self._format_orders(orders_raw)

            return {
                "enabled": True,
                "connected": True,
                "account": account,
                "positions": positions,
                "orders": orders,
            }
        except Exception as err:
            logger.exception("Ошибка получения статуса Bybit demo: %s", err)
            return {
                "enabled": True,
                "connected": False,
                "message": str(err),
            }

    def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: Optional[float] = None,
        reduce_only: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Размещение ордера (market или limit)."""
        if not self.is_enabled():
            raise RuntimeError("Bybit demo client is not configured")

        client = self._get_client()

        ccxt_params: Dict[str, Any] = params.copy() if params else {}
        if reduce_only:
            ccxt_params["reduceOnly"] = True

        try:
            order = client.create_order(symbol, order_type, side, amount, price, ccxt_params)
            return self._format_order(order)
        except Exception as err:
            logger.exception("Ошибка размещения ордера Bybit demo: %s", err)
            raise

    def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        if not self.is_enabled():
            raise RuntimeError("Bybit demo client is not configured")

        client = self._get_client()
        try:
            order = client.cancel_order(order_id, symbol)
            return self._format_order(order)
        except Exception as err:
            logger.exception("Ошибка отмены ордера Bybit demo: %s", err)
            raise

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Получает текущую рыночную цену для символа."""
        if not self.is_enabled():
            return None
        
        try:
            client = self._get_client()
            ticker = client.fetch_ticker(symbol)
            price = ticker.get("last") or ticker.get("close")
            if price and price > 0:
                return float(price)
            return None
        except Exception as err:
            logger.exception("Ошибка получения цены для %s: %s", symbol, err)
            return None

    def get_symbol_volatility_pct(
        self,
        symbol: str,
        timeframe: str = "1m",
        lookback: int = 30,
    ) -> Optional[float]:
        """
        Оценивает текущую волатильность инструмента в процентах.

        Используем простой ATR-подобный подход:
        - Берем последние N свечей
        - Считаем средний диапазон (high - low)
        - Делим на среднюю цену закрытия
        """
        if not self.is_enabled():
            return None

        try:
            client = self._get_client()
            ohlcv = client.fetch_ohlcv(symbol, timeframe=timeframe, limit=lookback)
            if not ohlcv:
                return None

            ranges = []
            closes = []
            for ts, o, h, l, c, v in ohlcv:
                if h is None or l is None or c is None:
                    continue
                ranges.append(h - l)
                closes.append(c)

            if not ranges or not closes:
                return None

            avg_range = sum(ranges) / len(ranges)
            avg_close = sum(closes) / len(closes)
            if avg_close <= 0:
                return None

            volatility_pct = (avg_range / avg_close) * 100.0
            return float(volatility_pct)
        except Exception as err:
            # Волатильность — вспомогательный параметр, поэтому не считаем ошибку критичной
            logger.warning("Не удалось получить волатильность для %s: %s", symbol, err)
            return None

    def get_order_fill_info(self, order_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Получает реальную цену и время исполнения из исполненного ордера.
        Возвращает словарь с ключами: price, timestamp, datetime.
        
        Использует fetch_closed_orders, так как fetch_order может получить только последние 500 ордеров.
        """
        if not self.is_enabled():
            return None
        
        try:
            client = self._get_client()
            
            # Сначала пробуем получить через fetch_order (для недавних ордеров)
            try:
                order = client.fetch_order(order_id, symbol)
                if order:
                    status = order.get("status", "").lower()
                    if status in ("closed", "filled"):
                        fill_price = order.get("average") or order.get("price")
                        if fill_price and fill_price > 0:
                            timestamp = order.get("timestamp")
                            datetime_str = order.get("datetime")
                            logger.info("✅ Получена информация об исполнении ордера %s (fetch_order): цена=%.4f, время=%s",
                                       order_id, fill_price, datetime_str or timestamp)
                            return {
                                "price": float(fill_price),
                                "timestamp": timestamp,
                                "datetime": datetime_str,
                            }
            except Exception:
                # Если не удалось через fetch_order (ордер слишком старый), ищем в закрытых
                pass
            
            # Ищем ордер среди закрытых ордеров
            closed_orders = client.fetch_closed_orders(symbol, limit=500)
            
            for order in closed_orders:
                if order.get("id") == order_id:
                    status = order.get("status", "").lower()
                    if status in ("closed", "filled"):
                        fill_price = order.get("average") or order.get("price")
                        if fill_price and fill_price > 0:
                            timestamp = order.get("timestamp")
                            datetime_str = order.get("datetime")
                            logger.info("✅ Получена информация об исполнении ордера %s (fetch_closed_orders): цена=%.4f, время=%s",
                                       order_id, fill_price, datetime_str or timestamp)
                            return {
                                "price": float(fill_price),
                                "timestamp": timestamp,
                                "datetime": datetime_str,
                            }
            
            logger.warning("Ордер %s не найден среди закрытых ордеров для %s", order_id, symbol)
            return None
            
        except Exception as err:
            logger.exception("Ошибка получения информации об исполнении ордера %s для %s: %s", order_id, symbol, err)
            return None

    def get_order_fill_price(self, order_id: str, symbol: str) -> Optional[float]:
        """
        Получает реальную цену исполнения из исполненного ордера.
        Это основной метод для получения фактической цены входа/выхода.
        
        Использует get_order_fill_info для получения данных.
        """
        fill_info = self.get_order_fill_info(order_id, symbol)
        if fill_info:
            return fill_info.get("price")
        return None

    def get_position_entry_price(self, symbol: str) -> Optional[float]:
        """Получает реальную цену входа открытой позиции для символа."""
        if not self.is_enabled():
            return None
        
        try:
            client = self._get_client()
            positions = client.fetch_positions([symbol])
            for pos in positions:
                contracts = float(pos.get("contracts") or 0)
                if abs(contracts) > 1e-8:  # Есть открытая позиция
                    entry_price = pos.get("entryPrice") or pos.get("entry_price")
                    if entry_price and entry_price > 0:
                        return float(entry_price)
            return None
        except Exception as err:
            logger.exception("Ошибка получения цены входа позиции для %s: %s", symbol, err)
            return None

    def get_exit_order_fill_price(self, symbol: str, entry_order_id: str, since_timestamp: Optional[int] = None, position_side: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Получает реальную цену закрытия из исполненного TP/SL ордера или сделки.
        КРИТИЧЕСКИ ВАЖНО: ищет ПЕРВУЮ закрывающую сделку после входа, а не последнюю!
        Сначала ищет среди сделок (trades) - это более точно, затем среди ордеров.
        
        Args:
            symbol: Символ торговой пары
            entry_order_id: ID ордера входа
            since_timestamp: Временная метка входа (для фильтрации)
            position_side: Направление позиции ('LONG' или 'SHORT') для определения закрывающих ордеров
        
        Returns:
            Dict с ключами: price, timestamp, datetime, order_id, exit_reason (TP/SL)
        """
        if not self.is_enabled():
            return None
        
        try:
            client = self._get_client()
            
            # КРИТИЧЕСКИ ВАЖНО: сначала ищем среди сделок (trades) - это более точно
            # Сделки показывают реальное исполнение, а не только размещенные ордера
            logger.info("Поиск закрывающей сделки для %s (entry_order_id=%s, position_side=%s)", 
                       symbol, entry_order_id, position_side)
            
            if since_timestamp:
                all_trades = client.fetch_my_trades(symbol, since=since_timestamp, limit=100)
                
                # Определяем, какая сторона закрывает позицию
                closing_side = None
                if position_side == "LONG":
                    closing_side = "sell"
                elif position_side == "SHORT":
                    closing_side = "buy"
                
                # Ищем закрывающие сделки
                closing_trades = []
                for trade in all_trades:
                    trade_side = trade.get("side", "").lower()
                    trade_timestamp = trade.get("timestamp", 0)
                    trade_price = trade.get("price", 0)
                    info = trade.get("info", {})
                    reduce_only = info.get("reduceOnly") or info.get("reduce_only") or False
                    
                    # Сделка закрывает позицию если:
                    # 1. Это reduce-only сделка (явно закрывающая)
                    # 2. ИЛИ это сделка противоположного направления после входа
                    is_closing = False
                    if reduce_only:
                        is_closing = True
                    elif closing_side and trade_side == closing_side:
                        is_closing = True
                    
                    if is_closing and trade_price > 0:
                        closing_trades.append({
                            "price": float(trade_price),
                            "timestamp": trade_timestamp,
                            "datetime": trade.get("datetime"),
                            "side": trade_side,
                            "reduce_only": reduce_only,
                        })
                
                # КРИТИЧЕСКИ ВАЖНО: берем ПЕРВУЮ закрывающую сделку (самую раннюю), а не последнюю!
                # Это та сделка, которая реально закрыла позицию
                if closing_trades:
                    closing_trades.sort(key=lambda x: x["timestamp"])  # Сортируем от старых к новым
                    first_closing_trade = closing_trades[0]  # Берем ПЕРВУЮ
                    
                    logger.info("✅ Найдена закрывающая сделка: цена=%.4f, время=%s (первая после входа)",
                               first_closing_trade["price"], first_closing_trade["datetime"])
                    
                    # Определяем причину закрытия
                    exit_reason = "STOP_LOSS" if first_closing_trade["reduce_only"] else "MANUAL_CLOSE"
                    
                    return {
                        "price": first_closing_trade["price"],
                        "timestamp": first_closing_trade["timestamp"],
                        "datetime": first_closing_trade["datetime"],
                        "order_id": None,  # Это сделка, а не ордер
                        "exit_reason": exit_reason,
                        "order_type": "trade",
                    }
            
            # Fallback: если не нашли в сделках, ищем среди ордеров
            logger.info("Закрывающая сделка не найдена, ищем среди ордеров...")
            closed_orders = client.fetch_closed_orders(symbol, limit=500)
            
            logger.info("Получено %d закрытых ордеров для %s", len(closed_orders), symbol)
            
            # Получаем информацию об ордере входа для определения его side
            entry_order_side = None
            try:
                entry_order = None
                for order in closed_orders:
                    if order.get("id") == entry_order_id:
                        entry_order = order
                        break
                if entry_order:
                    entry_order_side = entry_order.get("side", "").lower()
            except Exception:
                pass
            
            # Определяем, какая сторона закрывает позицию
            # Для LONG: закрывающий ордер - это 'sell'
            # Для SHORT: закрывающий ордер - это 'buy'
            closing_side = None
            if position_side == "LONG":
                closing_side = "sell"
            elif position_side == "SHORT":
                closing_side = "buy"
            elif entry_order_side:
                # Если знаем side ордера входа, определяем противоположный
                closing_side = "sell" if entry_order_side == "buy" else "buy"
            
            # Ищем ордер, который закрыл позицию
            # Сортируем по времени (от новых к старым), чтобы найти последний закрывающий ордер
            candidate_orders = []
            
            for order in closed_orders:
                order_id = order.get("id")
                order_status = order.get("status", "").lower()
                order_type = order.get("type", "").lower()
                order_side = order.get("side", "").lower()
                
                # Пропускаем ордер входа
                if order_id == entry_order_id:
                    continue
                
                # Проверяем, что ордер исполнен
                if order_status not in ("closed", "filled"):
                    continue
                
                # Проверяем время (должен быть после входа)
                order_timestamp = order.get("timestamp", 0)
                if since_timestamp and order_timestamp < since_timestamp:
                    continue
                
                # Проверяем, что это закрывающий ордер
                info = order.get("info", {})
                reduce_only = info.get("reduceOnly") or info.get("reduce_only") or False
                is_conditional = order_type in ("stop_market", "take_profit_market", "stop", "take_profit")
                
                # Ордер закрывает позицию если:
                # 1. Это reduce-only ордер
                # 2. ИЛИ это условный ордер (TP/SL)
                # 3. ИЛИ это ордер противоположного направления (если знаем closing_side)
                is_closing = reduce_only or is_conditional
                if not is_closing and closing_side and order_side == closing_side:
                    is_closing = True
                
                if not is_closing:
                    continue
                
                # Получаем цену исполнения
                fill_price = order.get("average") or order.get("price")
                if not fill_price or fill_price <= 0:
                    continue
                
                candidate_orders.append({
                    "order": order,
                    "timestamp": order_timestamp,
                    "price": float(fill_price),
                })
            
            # КРИТИЧЕСКИ ВАЖНО: берем ПЕРВЫЙ закрывающий ордер после входа (самый ранний), а не последний!
            # Это тот ордер, который реально закрыл позицию
            if candidate_orders:
                candidate_orders.sort(key=lambda x: x["timestamp"])  # Сортируем от старых к новым
                first_order_info = candidate_orders[0]  # Берем ПЕРВЫЙ
                order = first_order_info["order"]
                order_id = order.get("id")
                order_type = order.get("type", "").lower()
                
                # Определяем причину закрытия
                exit_reason = "MANUAL_CLOSE"
                if "take_profit" in order_type.lower() or "tp" in str(order_id).lower():
                    exit_reason = "TAKE_PROFIT"
                elif "stop" in order_type.lower() or "sl" in str(order_id).lower():
                    exit_reason = "STOP_LOSS"
                
                logger.info("✅ Найден ордер закрытия: id=%s, тип=%s, цена=%.4f, причина=%s (первый после входа)",
                           order_id, order_type, first_order_info["price"], exit_reason)
                
                return {
                    "price": first_order_info["price"],
                    "timestamp": first_order_info["timestamp"],
                    "datetime": order.get("datetime"),
                    "order_id": order_id,
                    "exit_reason": exit_reason,
                    "order_type": order_type,
                }
            
            logger.warning("Не найден ордер закрытия для %s (entry_order_id=%s, position_side=%s)", 
                          symbol, entry_order_id, position_side)
            return None
            
        except Exception as err:
            logger.exception("Ошибка получения ордера закрытия для %s: %s", symbol, err)
            return None

    def get_closed_trades(self, symbol: str, since: Optional[int] = None, limit: int = 100, position_side: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает историю закрытых сделок (trades) для символа.
        
        Args:
            symbol: Символ торговой пары
            since: Временная метка начала поиска (в миллисекундах)
            limit: Максимальное количество сделок
            position_side: Направление позиции ('LONG' или 'SHORT') для определения закрывающих сделок
        """
        if not self.is_enabled():
            return []
        
        try:
            client = self._get_client()
            # Получаем историю сделок (trades)
            trades = client.fetch_my_trades(symbol, since=since, limit=limit)
            
            logger.info("Получено %d сделок для %s (since=%s, position_side=%s)", len(trades), symbol, since, position_side)
            
            # Определяем, какая сторона сделки закрывает позицию
            # Для LONG позиции: закрывающая сделка - это 'sell'
            # Для SHORT позиции: закрывающая сделка - это 'buy'
            closing_side = None
            if position_side == "LONG":
                closing_side = "sell"
            elif position_side == "SHORT":
                closing_side = "buy"
            
            # Фильтруем закрывающие сделки
            closed_trades = []
            for trade in trades:
                info = trade.get("info", {})
                trade_side = trade.get("side", "").lower()
                reduce_only = info.get("reduceOnly") or info.get("reduce_only") or False
                trade_timestamp = trade.get("timestamp", 0)
                
                # Сделка закрывает позицию если:
                # 1. Это reduce-only сделка (явно закрывающая)
                # 2. ИЛИ это сделка противоположного направления после входа (для позиции)
                is_closing = False
                if reduce_only:
                    is_closing = True
                elif closing_side and trade_side == closing_side and since and trade_timestamp >= since:
                    # Это сделка противоположного направления после входа - вероятно закрывает позицию
                    is_closing = True
                
                if is_closing:
                    closed_trades.append({
                        "id": trade.get("id"),
                        "symbol": trade.get("symbol"),
                        "side": trade.get("side"),
                        "price": float(trade.get("price") or 0),
                        "amount": float(trade.get("amount") or 0),
                        "cost": float(trade.get("cost") or 0),
                        "timestamp": trade.get("timestamp"),
                        "datetime": trade.get("datetime"),
                        "fee": trade.get("fee"),
                        "info": info,
                    })
            
            # Сортируем по времени (от старых к новым)
            closed_trades.sort(key=lambda x: x.get("timestamp", 0))
            
            logger.info("Отфильтровано %d закрывающих сделок для %s (position_side=%s)", len(closed_trades), symbol, position_side)
            return closed_trades
        except Exception as err:
            logger.exception("Ошибка получения истории сделок для %s: %s", symbol, err)
            return []
    
    def get_position_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Получает полную информацию об открытой позиции для символа."""
        if not self.is_enabled():
            return None
        
        try:
            client = self._get_client()
            positions = client.fetch_positions([symbol])
            for pos in positions:
                contracts = float(pos.get("contracts") or 0)
                if abs(contracts) > 1e-8:  # Есть открытая позиция
                    info = pos.get("info", {}) or {}
                    raw_symbol = info.get("symbol") or pos.get("symbol")
                    return {
                        # Символы:
                        #  - raw_symbol: формат Bybit v5, например 'SOLUSDT'
                        #  - ccxt_symbol: формат ccxt, например 'SOL/USDT:USDT'
                        "raw_symbol": raw_symbol,
                        "symbol": pos.get("symbol"),
                        "side": pos.get("side"),
                        "contracts": contracts,
                        "entry_price": float(pos.get("entryPrice") or pos.get("entry_price") or 0),
                        "mark_price": float(pos.get("markPrice") or pos.get("mark_price") or 0),
                        "unrealized_pnl": float(pos.get("unrealizedPnl") or pos.get("unrealized_pnl") or 0),
                        "leverage": pos.get("leverage"),
                        "percentage": pos.get("percentage"),
                        # Индекс позиции и текущие TP/SL из сырой info Bybit
                        "positionIdx": int(info.get("positionIdx") or 0),
                        "takeProfit": float(info.get("takeProfit") or 0) if info.get("takeProfit") not in (None, "", "0", "0.0") else 0.0,
                        "stopLoss": float(info.get("stopLoss") or 0) if info.get("stopLoss") not in (None, "", "0", "0.0") else 0.0,
                    }
            return None
        except Exception as err:
            logger.exception("Ошибка получения информации о позиции для %s: %s", symbol, err)
            return None

    def close_position(self, symbol: str, side: Optional[str] = None) -> Dict[str, Any]:
        """
        Закрывает открытую позицию по рыночной цене.
        
        Args:
            symbol: Символ торговой пары
            side: Сторона позиции ('buy' для LONG, 'sell' для SHORT). Если не указана, определяется автоматически.
        
        Returns:
            Dict с информацией о закрытии: order, pnl, entry_price, exit_price
        """
        if not self.is_enabled():
            raise RuntimeError("Bybit demo client is not configured")
        
        client = self._get_client()
        
        # Нормализуем symbol (убираем :USDT если есть, CCXT сам добавит)
        normalized_symbol = symbol.replace(":USDT", "") if ":USDT" in symbol else symbol
        
        logger.info("🔄 Попытка закрытия позиции: symbol=%s (нормализован: %s), side_param=%s", 
                   symbol, normalized_symbol, side)
        
        # Получаем информацию о позиции
        position_info = self.get_position_info(normalized_symbol)
        if not position_info:
            # Пробуем с оригинальным symbol
            position_info = self.get_position_info(symbol)
            if not position_info:
                raise ValueError(f"Нет открытой позиции для {symbol} (проверено: {normalized_symbol} и {symbol})")
        
        # Определяем сторону для закрытия (противоположная стороне позиции)
        position_side_raw = position_info.get("side", "")
        position_side = str(position_side_raw).lower() if position_side_raw else ""
        
        logger.info("🔍 Определение стороны закрытия: position_side_raw=%s, position_side=%s, side_param=%s", 
                    position_side_raw, position_side, side)
        
        # Нормализуем side параметр, если передан
        if side:
            side_normalized = str(side).lower()
            # Конвертируем Long/Short в buy/sell
            if side_normalized in ("long", "buy"):
                close_side = "sell"
            elif side_normalized in ("short", "sell"):
                close_side = "buy"
            else:
                close_side = side_normalized
            logger.info("   Используем переданный side параметр: %s -> close_side=%s", side, close_side)
        elif position_side in ("long", "buy"):
            close_side = "sell"
            logger.info("   Определено из позиции: LONG -> close_side=SELL")
        elif position_side in ("short", "sell"):
            close_side = "buy"
            logger.info("   Определено из позиции: SHORT -> close_side=BUY")
        else:
            raise ValueError(f"Не удалось определить сторону для закрытия позиции: position_side='{position_side}' (raw: '{position_side_raw}'), side_param='{side}'")
        
        # Получаем количество контрактов для закрытия
        contracts = abs(position_info.get("contracts", 0))
        if contracts < 1e-8:
            raise ValueError(f"Позиция уже закрыта или количество контрактов равно нулю: {contracts}")
        
        entry_price = position_info.get("entry_price", 0)
        unrealized_pnl = position_info.get("unrealized_pnl", 0)
        
        try:
            # Получаем positionIdx для Bybit v5 API
            position_idx = position_info.get("positionIdx", 0)
            raw_symbol = position_info.get("raw_symbol") or normalized_symbol
            ccxt_symbol = position_info.get("symbol") or normalized_symbol
            
            # Размещаем market ордер с reduce_only=True для закрытия позиции
            logger.info("🔄 Закрытие позиции %s (ccxt: %s, raw: %s): side=%s, contracts=%.3f, entry_price=%.4f, unrealized_pnl=%.2f, positionIdx=%s",
                       symbol, ccxt_symbol, raw_symbol, close_side, contracts, entry_price, unrealized_pnl, position_idx)
            
            # Для Bybit v5 API нужно указать positionIdx в params
            # positionIdx: 0 = One-Way Mode, 1 = Buy side (hedge), 2 = Sell side (hedge)
            order_params = {
                "reduceOnly": True
            }
            
            # Добавляем positionIdx только если он больше 0 (hedge mode)
            if position_idx > 0:
                order_params["positionIdx"] = position_idx
            
            # Используем ccxt_symbol (формат CCXT) для create_order
            # CCXT автоматически конвертирует его в нужный формат для Bybit
            order_symbol = ccxt_symbol
            
            logger.debug("📤 Параметры ордера: symbol=%s (raw: %s), side=%s, amount=%.6f, params=%s", 
                        order_symbol, raw_symbol, close_side, contracts, order_params)
            
            try:
                order = client.create_order(
                    symbol=order_symbol,
                    type='market',
                    side=close_side,
                    amount=contracts,
                    price=None,
                    params=order_params
                )
                
                logger.info("📥 Ответ от биржи: order_id=%s, status=%s, filled=%s", 
                           order.get("id"), order.get("status"), order.get("filled"))
                
                # Проверяем, что ордер действительно размещен
                if not order.get("id"):
                    raise ValueError("Ордер не был размещен: отсутствует order_id в ответе биржи")
                    
            except Exception as order_err:
                logger.error("❌ Ошибка размещения ордера на закрытие позиции: %s", order_err)
                logger.error("   Параметры: symbol=%s, side=%s, amount=%.6f, params=%s", 
                            order_symbol, close_side, contracts, order_params)
                raise ValueError(f"Не удалось разместить ордер на закрытие позиции: {order_err}") from order_err
            
            # Получаем цену исполнения из ордера
            exit_price = float(order.get("average") or order.get("price") or 0)
            if not exit_price or exit_price <= 0:
                # Если цена не в ордере, получаем текущую рыночную цену
                exit_price = self.get_current_price(symbol) or entry_price
            
            # Рассчитываем реальный PnL
            if position_side == "long" or position_side == "buy":
                pnl = (exit_price - entry_price) * contracts
            else:  # short
                pnl = (entry_price - exit_price) * contracts
            
            logger.info("✅ Позиция закрыта: %s, exit_price=%.4f, pnl=%.2f", symbol, exit_price, pnl)
            
            return {
                "success": True,
                "order": self._format_order(order),
                "pnl": pnl,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "contracts": contracts,
                "unrealized_pnl_before_close": unrealized_pnl,
            }
        except Exception as err:
            logger.exception("Ошибка закрытия позиции %s: %s", symbol, err)
            raise

    def set_position_tp_sl(self, symbol: str, take_profit: Optional[float] = None, stop_loss: Optional[float] = None) -> bool:
        """
        Устанавливает TP/SL для открытой позиции ЧЕРЕЗ Bybit v5 positionTradingStop,
        не добавляя объем в позицию и не открывая новых сделок.
        """
        if not self.is_enabled():
            return False

        # Ничего не делать, если ни TP, ни SL не переданы
        if take_profit is None and stop_loss is None:
            logger.warning("⚠️ Не указаны ни take_profit, ни stop_loss для установки TP/SL на %s", symbol)
            return False

        try:
            client = self._get_client()

            # Получаем информацию о позиции, чтобы знать raw_symbol и positionIdx
            position_info = self.get_position_info(symbol)
            if not position_info:
                logger.warning("⚠️ Нет открытой позиции для %s при попытке установки TP/SL", symbol)
                return False

            raw_symbol = position_info.get("raw_symbol") or position_info.get("symbol")
            position_idx = int(position_info.get("positionIdx") or 0)

            # КРИТИЧЕСКИ ВАЖНО: Bybit API v5 требует "linear" для USDT фьючерсов
            # settings.DEMO_MARKET_TYPE может быть "contract", но API ожидает "linear"
            category = "linear" if (settings.DEMO_MARKET_TYPE or "").lower() in ("contract", "linear") else (settings.DEMO_MARKET_TYPE or "linear")
            
            params: Dict[str, Any] = {
                "category": category,
                "symbol": raw_symbol,
                "positionIdx": position_idx,
            }

            if stop_loss is not None:
                params["stopLoss"] = str(stop_loss)
                params["slTriggerBy"] = "LastPrice"

            if take_profit is not None:
                params["takeProfit"] = str(take_profit)
                params["tpTriggerBy"] = "LastPrice"

            logger.info(
                "🔄 Установка TP/SL через positionTradingStop для %s: params=%s",
                raw_symbol,
                params,
            )
            res = client.private_post_v5_position_trading_stop(params)
            ret_code = str(res.get("retCode") or res.get("ret_code") or "")
            if ret_code != "0":
                logger.error("❌ Ошибка positionTradingStop для %s: %s", raw_symbol, res)
                return False

            logger.info("✅ TP/SL для %s успешно обновлены через positionTradingStop", raw_symbol)
            return True
        except Exception as err:
            logger.exception("Ошибка установки TP/SL для %s: %s", symbol, err)
            return False

    # --------- Форматирование данных ---------

    def _format_balance(self, balance: Dict[str, Any]) -> Dict[str, Any]:
        total = balance.get("total", {})
        free = balance.get("free", {})

        usdt_total = float(total.get("USDT", 0) or 0)
        usdt_free = float(free.get("USDT", 0) or 0)

        return {
            "timestamp": balance.get("datetime"),
            "total_usdt": usdt_total,
            "free_usdt": usdt_free,
        }

    def _format_positions(self, positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        formatted: List[Dict[str, Any]] = []
        for pos in positions:
            contracts = float(pos.get("contracts") or 0)
            if abs(contracts) < 1e-8:
                continue

            entry_price = float(pos.get("entryPrice") or 0)
            mark_price = float(pos.get("markPrice") or 0)
            unrealized = float(pos.get("unrealizedPnl") or 0)

            formatted.append(
                {
                    "symbol": pos.get("symbol"),
                    "side": pos.get("side"),
                    "contracts": contracts,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "leverage": pos.get("leverage"),
                    "unrealized_pnl": unrealized,
                    "percentage": pos.get("percentage"),
                }
            )

        return formatted

    def _format_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self._format_order(order) for order in orders]

    def _format_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": order.get("id"),
            "symbol": order.get("symbol"),
            "type": order.get("type"),
            "side": order.get("side"),
            "price": float(order.get("price") or 0),
            "amount": float(order.get("amount") or 0),
            "filled": float(order.get("filled") or 0),
            "remaining": float(order.get("remaining") or 0),
            "status": order.get("status"),
            "timestamp": order.get("datetime"),
            "info": order.get("info"),
        }


bybit_demo_client = BybitDemoClient()


