"""
SQLAlchemy модели для базы данных
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, Index, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from core.database import Base
import json


class TradingPair(Base):
    """Модель торговой пары"""
    __tablename__ = 'trading_pairs'
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    exchange = Column(String(50), default='binance')
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Связи
    signals = relationship("Signal", back_populates="pair")
    levels = relationship("Level", back_populates="pair")
    analysis_data = relationship("AnalysisData", back_populates="pair")
    
    def __repr__(self):
        return f"<TradingPair(symbol={self.symbol})>"


class Signal(Base):
    """Модель торгового сигнала"""
    __tablename__ = 'signals'
    
    id = Column(Integer, primary_key=True, index=True)
    pair_id = Column(Integer, ForeignKey('trading_pairs.id'), nullable=False, index=True)
    signal_type = Column(String(10), nullable=False, index=True)  # LONG, SHORT
    level_price = Column(Float, nullable=False)
    entry_price = Column(Float)
    current_price = Column(Float)
    stop_loss = Column(Float)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    
    # Анализ тренда
    trend_1h = Column(String(20))  # UP_STRONG, UP_WEAK, DOWN_STRONG, etc.
    level_type = Column(String(20))  # support, resistance
    test_count = Column(Integer, default=1)
    
    # Результаты (P&L)
    pnl = Column(Float, default=0.0)
    pnl_percent = Column(Float, default=0.0)
    max_profit = Column(Float, default=0.0)
    max_drawdown = Column(Float, default=0.0)
    status = Column(String(20), default='ACTIVE')  # ACTIVE, CLOSED, STOP_LOSS, TAKE_PROFIT
    level_timeframe = Column(String(10))
    historical_touches = Column(Integer, default=0)
    live_test_count = Column(Integer, default=0)
    level_score = Column(Float)
    distance_percent = Column(Float)
    exit_price = Column(Float)
    exit_timestamp = Column(DateTime(timezone=True))
    exit_reason = Column(String(50))
    
    # Демо-торговля (Bybit)
    demo_order_id = Column(String(100))
    demo_status = Column(String(30))
    demo_quantity = Column(Float)
    demo_tp_price = Column(Float)
    demo_sl_price = Column(Float)
    demo_error = Column(Text)
    demo_submitted_at = Column(DateTime(timezone=True))
    demo_updated_at = Column(DateTime(timezone=True))
    demo_filled_at = Column(DateTime(timezone=True))
    
    # Информация о фиксации результата (какой порог был достигнут первым)
    result_fixed = Column(Float)  # 1.5 для прибыли, -0.5 для убытка, NULL если не зафиксирован
    result_fixed_at = Column(DateTime(timezone=True))  # Когда был зафиксирован результат
    
    # Дополнительные данные
    notes = Column(Text)
    meta_data = Column(JSON, name='metadata')  # Для дополнительных данных (metadata - зарезервированное имя в SQLAlchemy)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Архивация
    archived = Column(Boolean, default=False, index=True)  # Флаг архивации сигнала
    archived_at = Column(DateTime(timezone=True))  # Дата архивации
    
    # Elder's Triple Screen System
    elder_screen_1_passed = Column(Boolean)  # Прошел ли Экран 1 (4H тренд)
    elder_screen_1_blocked_reason = Column(Text)  # Причина блокировки Экран 1
    elder_screen_2_passed = Column(Boolean)  # Прошел ли Экран 2 (1H анализ)
    elder_screen_2_blocked_reason = Column(Text)  # Причина блокировки Экран 2
    elder_screen_3_passed = Column(Boolean)  # Прошел ли Экран 3 (15M вход) - пока не используется
    elder_screen_3_blocked_reason = Column(Text)  # Причина блокировки Экран 3
    elder_screens_metadata = Column(JSON)  # Детальные метаданные проверок всех экранов
    
    # Связи
    pair = relationship("TradingPair", back_populates="signals")
    live_logs = relationship(
        "SignalLiveLog",
        back_populates="signal",
        order_by="SignalLiveLog.created_at",
        cascade="all, delete-orphan",
    )
    
    # Индексы для быстрого поиска
    __table_args__ = (
        Index('idx_signals_pair_timestamp', 'pair_id', 'timestamp'),
        Index('idx_signals_status', 'status'),
        Index('idx_signals_type', 'signal_type'),
    )
    
    def __repr__(self):
        return f"<Signal(pair={self.pair.symbol if self.pair else 'N/A'}, type={self.signal_type}, price={self.level_price})>"
    
    def to_dict(self):
        """Преобразует объект в словарь"""
        # Конвертируем max_profit и max_drawdown в проценты для отображения "Макс. движение"
        # Формат всегда: "+X% / -Y%" где:
        # - X = процент изменения по росту стоимости пары (вверх)
        # - Y = процент изменения до ближайшей минимальной точки (вниз)
        # 
        # Для LONG:
        # - max_favorable_move = рост цены вверх (max_profit) → это первое значение "+X%"
        # - max_adverse_move = падение цены вниз (max_drawdown) → это второе значение "-Y%"
        # 
        # Для SHORT:
        # - max_favorable_move = падение цены вниз (max_profit) → это второе значение "-Y%"
        # - max_adverse_move = рост цены вверх (max_drawdown) → это первое значение "+X%"
        # 
        # Но для единообразия отображения, мы всегда показываем:
        # - Первое значение = рост цены вверх (max_adverse_move для SHORT, max_favorable_move для LONG)
        # - Второе значение = падение цены вниз (max_favorable_move для SHORT, max_adverse_move для LONG)
        
        # Для LONG: рост вверх = max_profit, падение вниз = max_drawdown
        # Для SHORT: рост вверх = max_drawdown, падение вниз = max_profit
        entry_price = self.entry_price or self.level_price
        
        # Вычисляем проценты роста и падения
        growth_percent = 0.0  # Процент роста цены вверх
        decline_percent = 0.0  # Процент падения цены вниз
        
        if entry_price and entry_price > 0:
            if self.signal_type == 'LONG':
                # Для LONG: рост = max_profit, падение = max_drawdown
                if self.max_profit is not None and self.max_profit != 0:
                    growth_percent = float((self.max_profit / entry_price) * 100)
                if self.max_drawdown is not None and self.max_drawdown != 0:
                    decline_percent = float((self.max_drawdown / entry_price) * 100)
            else:  # SHORT
                # Для SHORT: рост = max_drawdown, падение = max_profit
                if self.max_drawdown is not None and self.max_drawdown != 0:
                    growth_percent = float((self.max_drawdown / entry_price) * 100)
                if self.max_profit is not None and self.max_profit != 0:
                    decline_percent = float((self.max_profit / entry_price) * 100)
        
        # Для обратной совместимости сохраняем старые названия
        # Но теперь они означают: рост вверх / падение вниз (независимо от типа сигнала)
        max_favorable_move = growth_percent
        max_adverse_move = decline_percent
        
        # Конвертируем в calculated_result на основе первого достигнутого порога
        # ВАЖНО: calculated_result должен быть установлен ТОЛЬКО если result_fixed установлен
        # Это означает, что порог был достигнут и сделка закрыта
        # Для активных сделок (status != 'CLOSED') calculated_result всегда должен быть 0,
        # даже если max_profit показывает большие значения (это исторические данные)
        calculated_result = 0
        if self.result_fixed is not None:
            # Результат уже был зафиксирован при первом достижении порога
            calculated_result = self.result_fixed
        # НЕ устанавливаем calculated_result на основе max_profit для активных сделок!
        # Это может привести к неправильному отображению результата для сделок,
        # которые были возвращены в статус ACTIVE после ошибочного закрытия
        
        return {
            'id': self.id,
            'pair': self.pair.symbol if self.pair else None,
            'signal_type': self.signal_type,
            'level_price': self.level_price,
            'entry_price': self.entry_price,
            'current_price': self.current_price,
            'stop_loss': self.stop_loss,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'trend_1h': self.trend_1h,
            'level_type': self.level_type,
            'test_count': self.test_count,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'max_profit': self.max_profit,
            'max_drawdown': self.max_drawdown,
            'status': self.status,
            'notes': self.notes,
            'timeframe': self.level_timeframe,
            'historical_touches': self.historical_touches,
            'live_test_count': self.live_test_count,
            'level_score': self.level_score,
            'distance_percent': self.distance_percent,
            'exit_price': self.exit_price,
            'exit_timestamp': self.exit_timestamp.isoformat() if self.exit_timestamp else None,
            'exit_reason': self.exit_reason,
            'metadata': self.meta_data or {},  # Для совместимости
            # Дополнительные поля для фронтенда
            'calculated_result': calculated_result,
            'max_favorable_move': max_favorable_move,
            'max_adverse_move': max_adverse_move,
            'result_fixed': self.result_fixed,
            'result_fixed_at': self.result_fixed_at.isoformat() if self.result_fixed_at else None,
            'demo_status': self.demo_status,
            'demo_order_id': self.demo_order_id,
            'demo_quantity': self.demo_quantity,
            'demo_tp_price': self.demo_tp_price,
            'demo_sl_price': self.demo_sl_price,
            'demo_error': self.demo_error,
            'demo_submitted_at': self.demo_submitted_at.isoformat() if self.demo_submitted_at else None,
            'demo_updated_at': self.demo_updated_at.isoformat() if self.demo_updated_at else None,
            'demo_filled_at': self.demo_filled_at.isoformat() if self.demo_filled_at else None,
            'archived': self.archived,
            'archived_at': self.archived_at.isoformat() if self.archived_at else None,
            # Elder's Triple Screen System
            'elder_screen_1_passed': self.elder_screen_1_passed,
            'elder_screen_1_blocked_reason': self.elder_screen_1_blocked_reason,
            'elder_screen_2_passed': self.elder_screen_2_passed,
            'elder_screen_2_blocked_reason': self.elder_screen_2_blocked_reason,
            'elder_screen_3_passed': self.elder_screen_3_passed,
            'elder_screen_3_blocked_reason': self.elder_screen_3_blocked_reason,
            'elder_screens_metadata': self.elder_screens_metadata or {},
        }


class Level(Base):
    """Модель уровня поддержки/сопротивления"""
    __tablename__ = 'levels'
    
    id = Column(Integer, primary_key=True, index=True)
    pair_id = Column(Integer, ForeignKey('trading_pairs.id'), nullable=False, index=True)
    price = Column(Float, nullable=False, index=True)
    level_type = Column(String(20), nullable=False)  # support, resistance
    timeframe = Column(String(10), nullable=False)  # 15m, 1h, 4h
    
    # Характеристики уровня
    test_count = Column(Integer, default=0)
    strength = Column(String(20))  # STRONG, MEDIUM, WEAK
    is_active = Column(Boolean, default=True, index=True)
    
    # Временные метки
    first_touch = Column(DateTime(timezone=True))
    last_touch = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Дополнительные данные
    meta_data = Column(JSON, name='metadata')  # metadata - зарезервированное имя в SQLAlchemy
    
    # Связи
    pair = relationship("TradingPair", back_populates="levels")
    
    # Индексы
    __table_args__ = (
        Index('idx_levels_pair_price', 'pair_id', 'price'),
        Index('idx_levels_active', 'is_active', 'pair_id'),
    )
    
    def __repr__(self):
        return f"<Level(pair={self.pair.symbol if self.pair else 'N/A'}, price={self.price}, type={self.level_type})>"
    
    def to_dict(self):
        """Преобразует объект в словарь"""
        metadata = self.meta_data or {}
        return {
            'id': self.id,
            'pair': self.pair.symbol if self.pair else None,
            'price': self.price,
            'level_type': self.level_type,
            'timeframe': self.timeframe,
            'test_count': self.test_count,
            'strength': self.strength,
            'is_active': self.is_active,
            'first_touch': self.first_touch.isoformat() if self.first_touch else None,
            'last_touch': self.last_touch.isoformat() if self.last_touch else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'historical_touches': metadata.get('historical_touches', self.test_count),
            'live_test_count': metadata.get('live_test_count'),
            'score': metadata.get('score'),
            'distance_percent': metadata.get('distance_percent'),
            'trend_context': metadata.get('trend_context'),
            'signal_generated': metadata.get('signal_generated'),
            'metadata': metadata  # Возвращаем как metadata для совместимости
        }


class SignalLiveLog(Base):
    """Журнал действий по каждому сигналу (ордера, переносы SL и т.д.)."""
    __tablename__ = "signal_live_logs"

    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, ForeignKey("signals.id", ondelete="CASCADE"), nullable=False, index=True)
    event_type = Column(String(50))
    status = Column(String(30))
    message = Column(Text, nullable=False)
    details = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    signal = relationship("Signal", back_populates="live_logs")

    def to_dict(self):
        return {
            "id": self.id,
            "signal_id": self.signal_id,
            "event_type": self.event_type,
            "status": self.status,
            "message": self.message,
            "details": self.details or {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class AnalysisData(Base):
    """Модель для хранения данных анализа торговых пар"""
    __tablename__ = 'analysis_data'
    
    id = Column(Integer, primary_key=True, index=True)
    pair_id = Column(Integer, ForeignKey('trading_pairs.id'), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)  # 5m, 15m, 1h, 4h
    
    # Данные анализа
    current_price = Column(Float)
    trend = Column(String(20))  # UP_STRONG, UP_WEAK, DOWN_STRONG, etc.
    trend_strength = Column(Float)  # ADX значение
    price_change_24h = Column(Float)
    volume_24h = Column(Float)
    
    # Индикаторы
    ema20 = Column(Float)
    ema50 = Column(Float)
    rsi = Column(Float)
    adx = Column(Float)
    
    # Кэшированные данные свечей (JSON)
    candles_data = Column(JSON)  # Последние N свечей
    
    # Временные метки
    analyzed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Связи
    pair = relationship("TradingPair", back_populates="analysis_data")
    
    # Индексы
    __table_args__ = (
        Index('idx_analysis_pair_timeframe', 'pair_id', 'timeframe', 'analyzed_at'),
    )
    
    def __repr__(self):
        return f"<AnalysisData(pair={self.pair.symbol if self.pair else 'N/A'}, timeframe={self.timeframe})>"


class User(Base):
    """Модель пользователя (базовая, для будущей аутентификации)"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255))
    is_active = Column(Boolean, default=True)
    is_premium = Column(Boolean, default=False)
    
    # Подписка
    subscription_tier = Column(String(20), default='FREE')  # FREE, BASIC, PRO, ENTERPRISE
    subscription_expires_at = Column(DateTime(timezone=True))
    
    # Временные метки
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    last_login = Column(DateTime(timezone=True))
    
    def __repr__(self):
        return f"<User(email={self.email}, tier={self.subscription_tier})>"


class OHLCV(Base):
    """Модель для хранения свечных данных (OHLCV)"""
    __tablename__ = 'ohlcv'
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)  # TRX, BTC, etc.
    timeframe = Column(String(10), nullable=False, index=True)  # 1m, 5m, 15m, 1h, 4h
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)  # Время начала свечи
    
    # OHLCV данные
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False, default=0.0)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Составной уникальный индекс: одна свеча = одна комбинация symbol+timeframe+timestamp
    __table_args__ = (
        Index('idx_ohlcv_symbol_tf_ts', 'symbol', 'timeframe', 'timestamp', unique=True),
        Index('idx_ohlcv_symbol_tf', 'symbol', 'timeframe'),
        Index('idx_ohlcv_timestamp', 'timestamp'),
    )
    
    def __repr__(self):
        return f"<OHLCV(symbol={self.symbol}, tf={self.timeframe}, ts={self.timestamp})>"


class ChartPattern(Base):
    """Модель для хранения обнаруженных ценовых фигур (chart patterns)"""
    __tablename__ = 'chart_patterns'
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)  # BTC/USDT
    timeframe = Column(String(10), nullable=False, index=True)  # 1m, 5m, 15m, 1h, 4h
    pattern_type = Column(String(50), nullable=False)  # head_and_shoulders, flag, triangle, etc.
    pattern_category = Column(String(20), nullable=False)  # reversal, continuation, consolidation
    direction = Column(String(10), nullable=False)  # bullish, bearish, neutral
    reliability = Column(Float, default=0.5)  # Надежность фигуры (0.0-1.0)
    
    # Геометрия фигуры
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=False, index=True)
    confirmation_time = Column(DateTime(timezone=True))  # Время подтверждения (пробой)
    
    # Ключевые уровни фигуры
    support_level = Column(Float)  # Уровень поддержки/нижняя граница
    resistance_level = Column(Float)  # Уровень сопротивления/верхняя граница
    neckline = Column(Float)  # Для Head and Shoulders, Double Top/Bottom
    target_price = Column(Float)  # Целевая цена после пробоя
    
    # Характеристики фигуры
    pattern_height = Column(Float)  # Высота фигуры в процентах
    pattern_width = Column(Integer)  # Ширина в свечах
    volume_confirmation = Column(Boolean, default=False)  # Подтверждение объемом
    
    # Статус
    is_active = Column(Boolean, default=True, index=True)
    is_confirmed = Column(Boolean, default=False)  # Пробой произошел?
    
    # Метаданные
    candles_count = Column(Integer)  # Количество свечей в фигуре
    pattern_data = Column(JSON)  # Детальные данные (точки, линии и т.д.)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_chart_pattern_symbol_tf', 'symbol', 'timeframe'),
        Index('idx_chart_pattern_active', 'is_active'),
        Index('idx_chart_pattern_confirmed', 'is_confirmed'),
        Index('idx_chart_pattern_start_time', 'start_time'),
    )
    
    def __repr__(self):
        return f"<ChartPattern(symbol={self.symbol}, pattern={self.pattern_type}, category={self.pattern_category}, direction={self.direction})>"
    
    def to_dict(self):
        """Преобразует объект в словарь"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'pattern_type': self.pattern_type,
            'pattern_category': self.pattern_category,
            'direction': self.direction,
            'reliability': self.reliability,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'confirmation_time': self.confirmation_time.isoformat() if self.confirmation_time else None,
            'support_level': self.support_level,
            'resistance_level': self.resistance_level,
            'neckline': self.neckline,
            'target_price': self.target_price,
            'pattern_height': self.pattern_height,
            'pattern_width': self.pattern_width,
            'volume_confirmation': self.volume_confirmation,
            'is_active': self.is_active,
            'is_confirmed': self.is_confirmed,
            'candles_count': self.candles_count,
            'pattern_data': self.pattern_data,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


# Импортируем все модели для Alembic
__all__ = ['TradingPair', 'Signal', 'Level', 'AnalysisData', 'User', 'SignalLiveLog', 'OHLCV', 'ChartPattern', 'Base']

