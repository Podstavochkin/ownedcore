"""
API Gateway - единая точка входа для всех запросов
Использует FastAPI для современного и быстрого API
"""

import asyncio
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from datetime import datetime
import uvicorn
import logging
from pathlib import Path
import sys
import math
from typing import Literal, Optional

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.database import init_database, get_db
from core.cache import init_redis, cache
from core.config import settings
from core.models import ChartPattern
from sqlalchemy.orm import Session
from sqlalchemy import and_
from core.trading.bybit_demo_client import bybit_demo_client
from core.trading.trading_mode import is_live_trading_enabled, set_live_trading_enabled
from tasks.celery_app import celery_app
from tasks.analysis_tasks import analyze_all_pairs, analyze_pair
from tasks.signals_tasks import process_new_signal, update_signals_pnl_sync
from tasks.demo_trading_tasks import place_demo_order_for_signal
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Инициализация при запуске
    logger.info("Инициализация API Gateway...")
    
    # Инициализируем БД
    if not init_database():
        logger.error("Не удалось инициализировать БД")
    
    # Инициализируем Redis
    if not init_redis():
        logger.warning("Не удалось инициализировать Redis")
    
    yield
    
    # Очистка при остановке
    logger.info("Остановка API Gateway...")


# Создаем FastAPI приложение
app = FastAPI(
    title="OwnedCore API",
    description="API Gateway для торговых сигналов и анализа",
    version="1.0.0",
    lifespan=lifespan
)


class DemoOrderRequest(BaseModel):
    """Запрос на размещение ордера на live-бирже."""

    symbol: str = Field(..., description="Торговая пара, например BTC/USDT")
    side: Literal["buy", "sell"] = Field(..., description="Направление сделки")
    order_type: Literal["market", "limit"] = Field(..., description="Тип ордера")
    amount: float = Field(..., gt=0, description="Количество контрактов/монет")
    price: Optional[float] = Field(None, gt=0, description="Цена для лимитного ордера")
    reduce_only: bool = Field(False, description="Только закрытие позиции (reduceOnly)")


class DemoCancelOrderRequest(BaseModel):
    """Запрос на отмену ордера."""

    order_id: str = Field(..., description="Идентификатор ордера Bybit")
    symbol: Optional[str] = Field(None, description="Пара (опционально)")


class LiveTradingToggleRequest(BaseModel):
    """Тоггл режима live-торговли."""

    enabled: bool = Field(..., description="True - включено, False - выключено")


class ClosePositionRequest(BaseModel):
    """Запрос на закрытие позиции."""

    symbol: str = Field(..., description="Торговая пара, например BTC/USDT")
    side: Optional[Literal["buy", "sell"]] = Field(None, description="Сторона позиции (опционально, определяется автоматически)")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В production указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "OwnedCore API Gateway",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    try:
        # Проверка БД
        db_status = "connected" if init_database() else "disconnected"
        # Проверка Redis
        redis_status = "disconnected"
        try:
            cache.get("health_check")
            redis_status = "connected"
        except:
            pass
    except Exception as e:
        logger.error(f"Ошибка проверки здоровья: {e}")
        db_status = "error"
        redis_status = "error"
    
    return {
        "status": "healthy",
        "database": db_status,
        "redis": redis_status
    }


@app.get("/api/pairs-status")
async def get_pairs_status(db=Depends(get_db)):
    """Получает статус всех торговых пар"""
    try:
        # Пытаемся получить из кэша
        cached_data = cache.get('analysis:all_pairs')
        if cached_data and cached_data.get('status') == 'success':
            return JSONResponse(content=cached_data)
        
        # Если нет в кэше, пытаемся получить из БД
        from core.models import TradingPair, AnalysisData
        from sqlalchemy.orm import joinedload
        from sqlalchemy import desc
        
        pairs = db.query(TradingPair).filter_by(enabled=True).all()
        
        if pairs:
            from core.models import Level
            
            results = {}
            active_levels = {}
            
            for pair in pairs:
                # Получаем активные уровни для этой пары
                levels = db.query(Level).filter_by(
                    pair_id=pair.id,
                    is_active=True
                ).all()
                
                pair_levels = [level.to_dict() for level in levels]
                active_levels[pair.symbol] = pair_levels
                
                # Получаем последний анализ
                latest_analysis = db.query(AnalysisData).filter_by(
                    pair_id=pair.id
                ).order_by(desc(AnalysisData.analyzed_at)).first()
                
                if latest_analysis:
                    results[pair.symbol] = {
                        "pair": pair.symbol,
                        "status": "success",
                        "trend_1h": latest_analysis.trend or "UNKNOWN",
                        "current_price": float(latest_analysis.current_price) if latest_analysis.current_price else 0.0,
                        "price_change_24h": float(latest_analysis.price_change_24h) if latest_analysis.price_change_24h else 0.0,
                        "volume_24h": float(latest_analysis.volume_24h) if latest_analysis.volume_24h else 0.0,
                        "active_levels": len(pair_levels),
                        "signals_generated": 0
                    }
                else:
                    # Если нет анализа, возвращаем базовую информацию
                    results[pair.symbol] = {
                        "pair": pair.symbol,
                        "status": "pending",
                        "trend_1h": "UNKNOWN",
                        "current_price": 0.0,
                        "price_change_24h": 0.0,
                        "volume_24h": 0.0,
                        "active_levels": len(pair_levels),
                        "signals_generated": 0
                    }
            
            # Если нет данных анализа, запускаем анализ в фоне для обновления
            has_analysis_data = any(r.get('status') == 'success' for r in results.values())
            if not has_analysis_data:
                try:
                    analyze_all_pairs.delay()
                except:
                    pass  # Игнорируем если Celery не доступен
            
            return {
                "status": "success",
                "pairs_analyzed": len(results),
                "timestamp": datetime.now().isoformat(),
                "results": results,
                "active_levels": active_levels
            }
        
        # Если нет данных в БД, запускаем анализ
        task = analyze_all_pairs.delay()
        
        return {
            "status": "processing",
            "task_id": task.id,
            "message": "Анализ запущен в фоне"
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса пар: {e}")
        import traceback
        traceback.print_exc()
        # Возвращаем более информативную ошибку
        error_detail = str(e)
        if "too many clients" in error_detail.lower():
            error_detail = "Слишком много соединений с БД. Попробуйте позже."
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": error_detail}
        )


@app.get("/api/signals-by-pair")
async def get_signals_by_pair(pair: str, db=Depends(get_db)):
    """Получает все сигналы для конкретной пары"""
    try:
        from core.models import Signal, TradingPair
        from sqlalchemy.orm import joinedload
        
        # Получаем пару
        pair_obj = db.query(TradingPair).filter(TradingPair.symbol == pair).first()
        if not pair_obj:
            return JSONResponse(content={
                "success": False,
                "error": f"Пара {pair} не найдена",
                "signals": [],
                "total": 0
            })
        
        # Получаем все сигналы для этой пары
        signals = db.query(Signal).options(
            joinedload(Signal.pair)
        ).filter(
            Signal.pair_id == pair_obj.id
        ).order_by(Signal.timestamp.desc()).all()
        
        signals_data = [signal.to_dict() for signal in signals]
        
        return JSONResponse(content={
            "success": True,
            "signals": signals_data,
            "total": len(signals_data),
            "pair": pair
        })
    except Exception as e:
        logger.error(f"Ошибка получения сигналов для пары {pair}: {e}", exc_info=True)
        return JSONResponse(content={
            "success": False,
            "error": str(e),
            "signals": [],
            "total": 0
        })


@app.get("/api/signals")
async def get_signals(include_archived: bool = False, db=Depends(get_db)):
    """Получает все сигналы (по умолчанию без заархивированных)"""
    try:
        from core.models import Signal, TradingPair
        from sqlalchemy.orm import joinedload
        
        # Пытаемся получить из кэша (только для неархивированных)
        if not include_archived:
            cached_signals = cache.get('signals:all')
            if cached_signals:
                # Проверяем формат кэша
                if isinstance(cached_signals, dict) and cached_signals.get('success'):
                    return JSONResponse(content=cached_signals)
                else:
                    # Старый формат - возвращаем в правильном формате
                    return {
                        "success": True,
                        "signals": cached_signals if isinstance(cached_signals, list) else [],
                        "total": len(cached_signals) if isinstance(cached_signals, list) else 0,
                        "timestamp": datetime.now().isoformat()
                    }
        
        # Получаем из БД
        query = db.query(Signal).options(joinedload(Signal.pair))
        
        # Фильтруем заархивированные сигналы (если не запрошен архив)
        if not include_archived:
            query = query.filter(Signal.archived == False)
        
        signals = query.order_by(Signal.timestamp.desc()).limit(500).all()
        
        signals_data = [signal.to_dict() for signal in signals]
        
        # Рассчитываем статистику
        from datetime import datetime, timedelta, timezone
        
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        summary = {
            # Теоретическая статистика (все сигналы)
            'profit_count': 0,
            'loss_count': 0,
            'in_progress_count': 0,
            'today_count': 0,
            'week_count': 0,
            'month_count': 0,
            'today_result': 0.0,
            'week_result': 0.0,
            'month_result': 0.0,
            'closed_count': 0,
            'active_count': 0,
            # Реальная статистика (только исполненные ордера)
            'real_profit_count': 0,
            'real_loss_count': 0,
            'real_in_progress_count': 0,
            'real_today_result_pct': 0.0,
            'real_week_result_pct': 0.0,
            'real_month_result_pct': 0.0,
            'real_today_result_usdt': 0.0,
            'real_week_result_usdt': 0.0,
            'real_month_result_usdt': 0.0,
            'not_executed_count': 0,  # Сигналы, по которым ордер не был исполнен
        }
        
        for signal_dict in signals_data:
            # Парсим timestamp
            try:
                signal_time = datetime.fromisoformat(signal_dict['timestamp'].replace('Z', '+00:00'))
            except:
                continue
            
            result_fixed = signal_dict.get('result_fixed')
            status = signal_dict.get('status', 'ACTIVE')
            
            # ТЕОРЕТИЧЕСКАЯ СТАТИСТИКА (все сигналы)
            if result_fixed == 1.5:
                summary['profit_count'] += 1
                summary['closed_count'] += 1
            elif result_fixed == -0.5:
                summary['loss_count'] += 1
                summary['closed_count'] += 1
            else:
                summary['in_progress_count'] += 1
                summary['active_count'] += 1 if status in ('ACTIVE', 'OPEN', 'PENDING') else 0
            
            # Подсчет по периодам (теоретическая статистика)
            if signal_time >= today:
                summary['today_count'] += 1
            if signal_time >= week_ago:
                summary['week_count'] += 1
            if signal_time >= month_ago:
                summary['month_count'] += 1
            
            if result_fixed is not None:
                exit_ts = signal_dict.get('exit_timestamp') or signal_dict.get('result_fixed_at') or signal_dict.get('timestamp')
                try:
                    exit_time = datetime.fromisoformat(exit_ts.replace('Z', '+00:00'))
                except Exception:
                    exit_time = signal_time
                
                if exit_time >= today:
                    summary['today_result'] += result_fixed
                if exit_time >= week_ago:
                    summary['week_result'] += result_fixed
                if exit_time >= month_ago:
                    summary['month_result'] += result_fixed
            
            # РЕАЛЬНАЯ СТАТИСТИКА (только исполненные ордера)
            # Проверяем, был ли ордер исполнен: demo_filled_at IS NOT NULL и entry_price IS NOT NULL
            demo_filled_at = signal_dict.get('demo_filled_at')
            entry_price = signal_dict.get('entry_price')
            demo_status = signal_dict.get('demo_status', '')
            
            # КРИТИЧЕСКАЯ ПРОВЕРКА: если нет demo_order_id, значит ордер вообще не был отправлен на биржу
            # В этом случае не должно быть никакого результата, даже если есть exit_price (он мог быть из теоретического расчета)
            demo_order_id = signal_dict.get('demo_order_id')
            if not demo_order_id:
                # Ордер не был отправлен - не считаем в статистику и не рассчитываем результат
                if demo_status and demo_status not in ('NOT_SENT', 'LIVE_DISABLED', 'NOT_CONFIGURED', 'SIGNAL_CLOSED_NO_ORDER'):
                    summary['not_executed_count'] += 1
                continue  # Пропускаем этот сигнал для реальной статистики
            
            # Ордер считается исполненным если:
            # 1. Есть demo_filled_at (ордер был исполнен) И
            # 2. Есть entry_price (цена входа установлена) И
            # 3. Статус указывает на исполнение или закрытие
            is_order_executed = (
                demo_filled_at is not None and 
                entry_price is not None and 
                entry_price > 0 and
                demo_status in ('FILLED', 'OPEN_POSITION', 'SL_TO_BREAKEVEN', 'CLOSED')
            )
            
            if not is_order_executed:
                # Ордер не был исполнен - считаем в статистику неотработанных
                if demo_status and demo_status not in ('NOT_SENT', 'LIVE_DISABLED', 'NOT_CONFIGURED'):
                    summary['not_executed_count'] += 1
                continue  # Пропускаем этот сигнал для реальной статистики
            
            # Ордер исполнен - рассчитываем реальную статистику
            exit_price = signal_dict.get('exit_price')
            exit_timestamp = signal_dict.get('exit_timestamp')
            signal_type = signal_dict.get('signal_type', 'LONG')
            demo_quantity = signal_dict.get('demo_quantity', 0)
            
            # Проверяем, закрыта ли позиция
            is_closed = exit_price is not None and exit_timestamp is not None
            
            if is_closed:
                # Позиция закрыта - рассчитываем результат
                try:
                    exit_time = datetime.fromisoformat(exit_timestamp.replace('Z', '+00:00'))
                except Exception:
                    exit_time = signal_time
                
                # Расчет результата в процентах от реальной цены входа
                # Комиссия Bybit фьючерсы: Taker 0.035%, Maker 0.014%
                # Используем Taker комиссию (0.035%) как консервативный вариант
                COMMISSION_RATE = 0.00035  # 0.035%
                
                if signal_type == 'LONG':
                    gross_result_pct = ((exit_price - entry_price) / entry_price) * 100.0
                    gross_result_usdt = (exit_price - entry_price) * demo_quantity if demo_quantity else 0.0
                else:  # SHORT
                    gross_result_pct = ((entry_price - exit_price) / entry_price) * 100.0
                    gross_result_usdt = (entry_price - exit_price) * demo_quantity if demo_quantity else 0.0
                
                # Учитываем комиссии (вход + выход)
                if demo_quantity and demo_quantity > 0:
                    entry_commission = entry_price * demo_quantity * COMMISSION_RATE
                    exit_commission = exit_price * demo_quantity * COMMISSION_RATE
                    total_commission = entry_commission + exit_commission
                    
                    # Чистая прибыль = валовая прибыль - комиссии
                    net_result_usdt = gross_result_usdt - total_commission
                    
                    # Чистый результат в процентах
                    position_value = entry_price * demo_quantity
                    net_result_pct = (net_result_usdt / position_value) * 100.0 if position_value > 0 else 0.0
                else:
                    net_result_pct = gross_result_pct
                    net_result_usdt = gross_result_usdt
                
                result_pct = net_result_pct
                result_usdt = net_result_usdt
                
                # Учитываем в статистике по периодам
                if exit_time >= today:
                    summary['real_today_result_pct'] += result_pct
                    summary['real_today_result_usdt'] += result_usdt
                if exit_time >= week_ago:
                    summary['real_week_result_pct'] += result_pct
                    summary['real_week_result_usdt'] += result_usdt
                if exit_time >= month_ago:
                    summary['real_month_result_pct'] += result_pct
                    summary['real_month_result_usdt'] += result_usdt
                
                # Подсчет прибыльных/убыточных
                if result_pct > 0:
                    summary['real_profit_count'] += 1
                elif result_pct < 0:
                    summary['real_loss_count'] += 1
            else:
                # Позиция открыта, но еще не закрыта
                summary['real_in_progress_count'] += 1
        
        # Формируем ответ в формате, ожидаемом фронтендом
        response_data = {
            "success": True,
            "signals": signals_data,
            "summary": summary,
            "total": len(signals_data),
            "timestamp": datetime.now().isoformat()
        }
        
        # Кэшируем только неархивированные сигналы (TTL уменьшен до 30 секунд для более частого обновления данных)
        if not include_archived:
            cache.set('signals:all', response_data, ttl=30)
        
        return response_data
        
    except Exception as e:
        logger.error(f"Ошибка получения сигналов: {e}")
        return {
            "success": False,
            "error": str(e),
            "signals": [],
            "total": 0
        }


@app.get("/api/signals/{signal_id}/live-log")
async def get_signal_live_log(signal_id: int, limit: int = 50, db=Depends(get_db)):
    """Возвращает историю действий по сигналу."""
    from core.models import SignalLiveLog

    try:
        limit = max(1, min(limit, 200))
        logs = (
            db.query(SignalLiveLog)
            .filter(SignalLiveLog.signal_id == signal_id)
            .order_by(SignalLiveLog.created_at.desc())
            .limit(limit)
            .all()
        )
        return {"success": True, "logs": [log.to_dict() for log in logs]}
    except Exception as e:
        logger.error("Ошибка получения live-логов сигнала %s: %s", signal_id, e)
        raise HTTPException(status_code=500, detail="Не удалось получить историю сигнала")


@app.get("/api/levels")
async def get_levels(db=Depends(get_db)):
    """Получает все активные уровни"""
    try:
        from core.models import Level, TradingPair
        from sqlalchemy.orm import joinedload
        
        # Получаем только уровни для включенных пар
        levels = db.query(Level).options(
            joinedload(Level.pair)
        ).join(TradingPair).filter(
            Level.is_active == True,
            TradingPair.enabled == True
        ).all()
        
        # Группируем по парам (пропускаем отключенные пары)
        levels_by_pair = {}
        for level in levels:
            if not level.pair or not level.pair.enabled:
                continue  # Пропускаем уровни для отключенных пар
            pair_symbol = level.pair.symbol
            if pair_symbol not in levels_by_pair:
                levels_by_pair[pair_symbol] = []
            levels_by_pair[pair_symbol].append(level.to_dict())
        
        return {
            "levels": levels_by_pair,
            "total_pairs": len(levels_by_pair),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения уровней: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/potential-signals")
async def get_potential_signals(db=Depends(get_db)):
    """
    Elder's Screen (ES) - страница анализа и фильтрации уровней.
    
    Получает все активные уровни с данными Elder's Triple Screen System.
    Только уровни, прошедшие все проверки ES (ready_for_signal=True), 
    могут генерировать сигналы на странице Signals.
    
    Оптимизированная версия: проверяет Elder's Screens для всех уровней и кэширует результаты.
    """
    try:
        from core.models import Level, TradingPair
        from core.analysis_engine import analysis_engine
        from sqlalchemy.orm import joinedload
        from sqlalchemy import func
        import json
        
        # Пытаемся получить из кэша
        cache_key = "potential_signals:all"
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.debug("Используем кэшированные данные потенциальных сигналов")
            return JSONResponse(content=cached_data)
        
        # Получаем все активные уровни только для включенных пар
        levels = db.query(Level).options(
            joinedload(Level.pair)
        ).join(TradingPair).filter(
            Level.is_active == True,
            TradingPair.enabled == True
        ).all()
        
        if not levels:
            result = {
                "levels": [],
                "total_levels": 0,
                "ready_for_signal": 0,
                "blocked_screen_1": 0,
                "blocked_screen_2": 0,
                "not_checked": 0,
                "timestamp": datetime.now().isoformat()
            }
            cache.set(cache_key, result, ttl=300)  # Кэш на 5 минут (300 секунд) - синхронизировано с analyze_pair
            return result
        
        # Группируем уровни по парам для оптимизации запросов данных
        # Пропускаем уровни для отключенных пар
        levels_by_pair = {}
        for level in levels:
            if not level.pair or not level.pair.enabled:
                continue  # Пропускаем уровни для отключенных пар
            pair_symbol = level.pair.symbol
            if pair_symbol not in levels_by_pair:
                levels_by_pair[pair_symbol] = []
            levels_by_pair[pair_symbol].append(level)
        
        # Проверяем Elder's Screens для всех уровней
        potential_levels = []
        ready_count = 0
        blocked_screen_1_count = 0
        blocked_screen_2_count = 0
        not_checked_count = 0
        levels_from_db_count = 0  # Счетчик уровней, использующих данные из БД
        levels_recalculated_count = 0  # Счетчик уровней, для которых пересчитаны Elder's Screens
        
        for pair_symbol, pair_levels in levels_by_pair.items():
            try:
                # Получаем данные для пары один раз
                candles_1h = await analysis_engine.fetch_ohlcv(pair_symbol, '1h', 200)
                candles_4h = await analysis_engine.fetch_ohlcv(pair_symbol, '4h', 200)
                
                if not candles_1h or len(candles_1h) == 0:
                    logger.warning(f"Нет данных 1H для {pair_symbol}, пропускаем проверку Elder's Screens")
                    # Добавляем уровни с правильной структурой данных (не проверено)
                    for level in pair_levels:
                        level_dict = level.to_dict()
                        level_dict['elder_screens'] = {
                            'screen_1': {
                                'passed': None,  # Не проверено
                                'blocked_reason': 'Нет данных 1H для проверки Экран 1',
                                'checks': {'error': 'Нет данных 1H'}
                            },
                            'screen_2': {
                                'passed': None,  # Не проверено
                                'blocked_reason': 'Нет данных 1H для проверки Экран 2',
                                'checks': {'error': 'Нет данных 1H'}
                            },
                            'final_decision': 'NOT_CHECKED',
                            'error': 'Нет данных 1H'
                        }
                        potential_levels.append(level_dict)
                        not_checked_count += 1
                    continue
                
                # Получаем свечи 15m для проверки пробития и текущей цены (более актуальная, чем 1H)
                candles_15m = await analysis_engine.fetch_ohlcv(pair_symbol, '15m', 50)
                if candles_15m and len(candles_15m) > 0:
                    current_price = candles_15m[-1]['close']  # Текущая цена из последней 15m свечи
                else:
                    current_price = candles_1h[-1]['close']  # Fallback на 1H
                
                for level in pair_levels:
                    # ПРОВЕРКА ПРОБИТИЯ: пропускаем пробитые уровни
                    level_data = {
                        'price': float(level.price),
                        'type': level.level_type
                    }
                    if candles_15m and analysis_engine.is_level_broken(level_data, candles_15m, current_price):
                        logger.debug(f"Пропускаем пробитый уровень {pair_symbol} @ {level.price}")
                        continue  # Пропускаем пробитый уровень
                    
                    level_dict = level.to_dict()
                    meta = level.meta_data or {}
                    
                    # Проверяем, был ли создан АКТУАЛЬНЫЙ сигнал для этого уровня
                    try:
                        from core.models import Signal
                        from sqlalchemy import func
                        from datetime import timedelta, timezone as tz
                        price_tolerance = level.price * 0.001  # 0.1%
                        
                        # Максимальный возраст актуального сигнала (30 минут)
                        MAX_SIGNAL_AGE_SECONDS = 30 * 60
                        cutoff_time = datetime.now(tz.utc) - timedelta(seconds=MAX_SIGNAL_AGE_SECONDS)
                        
                        # Ищем только АКТУАЛЬНЫЕ сигналы (не старше 30 минут и активные)
                        existing_signal = db.query(Signal).filter(
                            Signal.pair_id == level.pair_id,
                            func.abs(Signal.level_price - level.price) < price_tolerance,
                            Signal.timestamp >= cutoff_time,  # Только свежие сигналы
                            Signal.status == 'ACTIVE'  # Только активные сигналы
                        ).order_by(Signal.timestamp.desc()).first()
                        
                        if existing_signal:
                            level_dict['signal_created'] = True
                            level_dict['signal_timestamp'] = existing_signal.timestamp.isoformat() if existing_signal.timestamp else None
                            level_dict['signal_id'] = existing_signal.id
                            level_dict['signal_status'] = existing_signal.status
                        else:
                            level_dict['signal_created'] = False
                            level_dict['signal_timestamp'] = None
                            level_dict['signal_id'] = None
                            level_dict['signal_status'] = None
                    except Exception as sig_err:
                        # Если ошибка при проверке сигнала, откатываем транзакцию и продолжаем
                        logger.warning(f"Ошибка проверки сигнала для уровня {level.price}: {sig_err}")
                        try:
                            db.rollback()
                        except:
                            pass
                        level_dict['signal_created'] = False
                        level_dict['signal_timestamp'] = None
                        level_dict['signal_id'] = None
                        level_dict['signal_status'] = None
                    
                    # Проверяем, есть ли свежие данные Elder's Screens в метаданных
                    # В analysis_engine.py данные сохраняются в level['metadata']['elder_screens']
                    # а затем весь level сохраняется в meta_data, поэтому структура: meta_data['metadata']['elder_screens']
                    metadata = meta.get('metadata', {}) or {}
                    elder_screens_data = metadata.get('elder_screens')
                    elder_screens_checked_at = metadata.get('elder_screens_checked_at')
                    
                    # ОПТИМИЗАЦИЯ: Проверяем, нужно ли обновить данные (если старше 5 минут или нет данных)
                    # Приоритетно используем данные из БД, которые обновляются в analyze_pair каждые 5 минут
                    needs_check = True
                    if elder_screens_data and elder_screens_checked_at:
                        try:
                            checked_time = datetime.fromisoformat(elder_screens_checked_at.replace('Z', '+00:00'))
                            time_diff = (datetime.now(checked_time.tzinfo) - checked_time).total_seconds()
                            if time_diff < 300:  # 5 минут - данные свежие из analyze_pair
                                needs_check = False
                                levels_from_db_count += 1
                                logger.debug(f"✅ Используем Elder's Screens из БД для {pair_symbol} @ {level.price} (проверено {time_diff:.0f} сек назад)")
                                
                                # Определяем signal_type для кэшированных данных
                                cached_signal_type = 'LONG' if level.level_type == 'support' else 'SHORT'
                                
                                # Убеждаемся, что blocked_reason присутствует в кэшированных данных
                                # Убеждаемся, что blocked_reason всегда присутствует в screen_1 (для кэшированных данных)
                                if 'screen_1' in elder_screens_data:
                                    screen_1 = elder_screens_data['screen_1']
                                    if not screen_1.get('passed'):
                                        # Всегда формируем blocked_reason, даже если он уже есть (может быть пустым или некорректным)
                                        checks = screen_1.get('checks', {})
                                        blocked_parts = []
                                        
                                        if checks.get("btc_blocked"):
                                            btc_trend = checks.get("btc_trend", "N/A")
                                            blocked_parts.append(f"BTC тренд {btc_trend} блокирует {cached_signal_type} сигналы")
                                        
                                        if checks.get("pair_blocked"):
                                            pair_trend = checks.get("pair_trend", {})
                                            if isinstance(pair_trend, dict):
                                                trend = pair_trend.get("trend", "N/A")
                                                blocked_parts.append(f"Тренд пары {trend} блокирует {cached_signal_type} сигналы")
                                            else:
                                                blocked_parts.append(f"Тренд пары блокирует {cached_signal_type} сигналы")
                                        
                                        if checks.get("error"):
                                            blocked_parts.append(f"Ошибка: {checks.get('error')}")
                                        
                                        # Если есть конкретные причины, используем их; иначе формируем общую
                                        if blocked_parts:
                                            screen_1['blocked_reason'] = "; ".join(blocked_parts)
                                        else:
                                            btc_trend = checks.get("btc_trend", "N/A")
                                            pair_trend_info = checks.get("pair_trend", {})
                                            pair_trend = pair_trend_info.get("trend", "N/A") if isinstance(pair_trend_info, dict) else "N/A"
                                            screen_1['blocked_reason'] = f"Экран 1 не пройден: BTC тренд={btc_trend}, тренд пары={pair_trend}"
                                
                                # Убеждаемся, что blocked_reason всегда присутствует в screen_2 (для кэшированных данных)
                                if 'screen_2' in elder_screens_data:
                                    screen_2 = elder_screens_data['screen_2']
                                    if not screen_2.get('passed') and not screen_2.get('blocked_reason'):
                                        # Если экран не пройден, но blocked_reason отсутствует, формируем его из checks
                                        checks = screen_2.get('checks', {})
                                        blocked_parts = []
                                        
                                        if checks.get("approach_blocked") or checks.get("price_approach", {}).get("valid") is False:
                                            price_approach = checks.get("price_approach", {})
                                            reason = price_approach.get("reason", f"Направление подхода: {price_approach.get('direction', 'N/A')}")
                                            blocked_parts.append(reason)
                                        
                                        rsi_check = checks.get("rsi", {})
                                        if rsi_check.get("blocked"):
                                            rsi_value = rsi_check.get("value", "N/A")
                                            # Правильное форматирование: сначала проверяем тип, потом форматируем
                                            rsi_str = f"{rsi_value:.2f}" if isinstance(rsi_value, (int, float)) else str(rsi_value)
                                            blocked_reason = rsi_check.get("blocked_reason", f"RSI {rsi_str}")
                                            blocked_parts.append(blocked_reason)
                                        
                                        macd_check = checks.get("macd", {})
                                        if macd_check.get("blocked"):
                                            macd_value = macd_check.get("macd", "N/A")
                                            # Правильное форматирование: сначала проверяем тип, потом форматируем
                                            macd_str = f"{macd_value:.4f}" if isinstance(macd_value, (int, float)) else str(macd_value)
                                            blocked_reason = macd_check.get("blocked_reason", f"MACD {macd_str}")
                                            blocked_parts.append(blocked_reason)
                                        
                                        if checks.get("error"):
                                            blocked_parts.append(f"Ошибка: {checks.get('error')}")
                                        
                                        if checks.get("oscillator_error"):
                                            blocked_parts.append(f"Ошибка расчета осцилляторов: {checks.get('oscillator_error')}")
                                        
                                        if blocked_parts:
                                            screen_2['blocked_reason'] = "; ".join(blocked_parts)
                                        else:
                                            # Если checks пустой, но экран не пройден, формируем общую причину
                                            if not checks or len(checks) == 0:
                                                screen_2['blocked_reason'] = "Экран 2 не пройден: проверки не были выполнены (недостаточно данных или ошибка)"
                                            elif checks.get("price_approach", {}).get("error"):
                                                screen_2['blocked_reason'] = f"Экран 2 не пройден: {checks['price_approach']['error']}"
                                            elif checks.get("oscillators", {}).get("error"):
                                                screen_2['blocked_reason'] = f"Экран 2 не пройден: {checks['oscillators']['error']}"
                                            else:
                                                screen_2['blocked_reason'] = "Экран 2 не пройден: проверки не выполнены или данные недоступны"
                        except:
                            pass
                    
                    if needs_check:
                        # Определяем потенциальный тип сигнала
                        signal_type = 'LONG' if level.level_type == 'support' else 'SHORT'
                        levels_recalculated_count += 1
                        logger.debug(f"🔄 Пересчитываем Elder's Screens для {pair_symbol} @ {level.price} (данные устарели или отсутствуют)")
                        
                        # Проверяем Elder's Screens
                        try:
                            level_data = {
                                'price': float(level.price),
                                'type': level.level_type,
                                'score': meta.get('score', 0)
                            }
                            
                            screens_passed, screens_details = await analysis_engine.check_elder_screens(
                                pair=pair_symbol,
                                signal_type=signal_type,
                                level=level_data,
                                current_price=current_price,
                                candles_4h=candles_4h if candles_4h else [],
                                candles_1h=candles_1h,
                                level_score=meta.get('score')
                            )
                            
                            # Сохраняем результаты в метаданные уровня
                            # Используем ту же структуру, что и в analysis_engine.py: meta_data['metadata']['elder_screens']
                            updated_meta = meta.copy()
                            if 'metadata' not in updated_meta:
                                updated_meta['metadata'] = {}
                            updated_meta['metadata']['elder_screens'] = screens_details
                            updated_meta['metadata']['elder_screens_checked_at'] = datetime.now().isoformat()
                            updated_meta['metadata']['elder_screens_passed'] = screens_passed
                            
                            # Обновляем в БД
                            level.meta_data = updated_meta
                            db.commit()
                            
                            elder_screens_data = screens_details
                            
                            # Убеждаемся, что blocked_reason всегда присутствует в screen_1
                            if elder_screens_data and 'screen_1' in elder_screens_data:
                                screen_1 = elder_screens_data['screen_1']
                                if not screen_1.get('passed'):
                                    # Всегда формируем blocked_reason, даже если он уже есть (может быть пустым или некорректным)
                                    checks = screen_1.get('checks', {})
                                    blocked_parts = []
                                    
                                    if checks.get("btc_blocked"):
                                        btc_trend = checks.get("btc_trend", "N/A")
                                        blocked_parts.append(f"BTC тренд {btc_trend} блокирует {signal_type} сигналы")
                                    
                                    if checks.get("pair_blocked"):
                                        pair_trend = checks.get("pair_trend", {})
                                        if isinstance(pair_trend, dict):
                                            trend = pair_trend.get("trend", "N/A")
                                            blocked_parts.append(f"Тренд пары {trend} блокирует {signal_type} сигналы")
                                        else:
                                            blocked_parts.append(f"Тренд пары блокирует {signal_type} сигналы")
                                    
                                    if checks.get("error"):
                                        blocked_parts.append(f"Ошибка: {checks.get('error')}")
                                    
                                    # Если есть конкретные причины, используем их; иначе формируем общую
                                    if blocked_parts:
                                        screen_1['blocked_reason'] = "; ".join(blocked_parts)
                                    else:
                                        btc_trend = checks.get("btc_trend", "N/A")
                                        pair_trend_info = checks.get("pair_trend", {})
                                        pair_trend = pair_trend_info.get("trend", "N/A") if isinstance(pair_trend_info, dict) else "N/A"
                                        screen_1['blocked_reason'] = f"Экран 1 не пройден: BTC тренд={btc_trend}, тренд пары={pair_trend}"
                            
                            # Убеждаемся, что blocked_reason всегда присутствует в screen_2
                            if elder_screens_data and 'screen_2' in elder_screens_data:
                                screen_2 = elder_screens_data['screen_2']
                                if not screen_2.get('passed'):
                                    if not screen_2.get('blocked_reason'):
                                        # Если экран не пройден, но blocked_reason отсутствует, формируем его из checks
                                        checks = screen_2.get('checks', {})
                                        blocked_parts = []
                                        
                                        # Проверяем направление подхода
                                        if checks.get("approach_blocked") or checks.get("price_approach", {}).get("valid") is False:
                                            price_approach = checks.get("price_approach", {})
                                            direction = price_approach.get("direction", "N/A")
                                            reason = price_approach.get("reason", f"Направление подхода некорректно: {direction}")
                                            blocked_parts.append(reason)
                                        
                                        # Проверяем RSI
                                        rsi_check = checks.get("rsi", {})
                                        if rsi_check.get("blocked"):
                                            rsi_value = rsi_check.get("value", "N/A")
                                            threshold = rsi_check.get("threshold", "N/A")
                                            # Правильное форматирование: сначала проверяем тип, потом форматируем
                                            rsi_str = f"{rsi_value:.2f}" if isinstance(rsi_value, (int, float)) else str(rsi_value)
                                            blocked_reason = rsi_check.get("blocked_reason", f"RSI {rsi_str} {'<' if signal_type == 'SHORT' else '>'} {threshold}")
                                            blocked_parts.append(blocked_reason)
                                        
                                        # Проверяем MACD
                                        macd_check = checks.get("macd", {})
                                        if macd_check.get("blocked"):
                                            macd_value = macd_check.get("macd", "N/A")
                                            signal_value = macd_check.get("signal", "N/A")
                                            expected = macd_check.get("expected", "N/A")
                                            # Правильное форматирование: сначала проверяем тип, потом форматируем
                                            macd_str = f"{macd_value:.4f}" if isinstance(macd_value, (int, float)) else str(macd_value)
                                            signal_str = f"{signal_value:.4f}" if isinstance(signal_value, (int, float)) else str(signal_value)
                                            blocked_reason = macd_check.get("blocked_reason", f"MACD {macd_str} не соответствует {expected} (Signal: {signal_str})")
                                            blocked_parts.append(blocked_reason)
                                        
                                        # Проверяем ошибки
                                        if checks.get("error"):
                                            blocked_parts.append(f"Ошибка проверки: {checks.get('error')}")
                                        
                                        if checks.get("oscillator_error"):
                                            blocked_parts.append(f"Ошибка расчета осцилляторов: {checks.get('oscillator_error')}")
                                        
                                        if checks.get("price_approach", {}).get("error"):
                                            blocked_parts.append(f"Ошибка проверки направления подхода: {checks['price_approach']['error']}")
                                        
                                        if blocked_parts:
                                            screen_2['blocked_reason'] = "; ".join(blocked_parts)
                                        else:
                                            # Если нет конкретных причин, формируем общую
                                            screen_2['blocked_reason'] = f"Экран 2 не пройден: проверки не выполнены или данные недоступны"
                            
                        except Exception as e:
                            logger.error(f"Ошибка проверки Elder's Screens для {pair_symbol} @ {level.price}: {e}")
                            import traceback
                            traceback.print_exc()
                            # Формируем правильную структуру данных даже при ошибке
                            elder_screens_data = {
                                'screen_1': {
                                    'passed': False,
                                    'blocked_reason': f'Ошибка проверки Экран 1: {str(e)}',
                                    'checks': {'error': str(e)}
                                },
                                'screen_2': {
                                    'passed': False,
                                    'blocked_reason': f'Ошибка проверки Экран 2: {str(e)}',
                                    'checks': {'error': str(e)}
                                },
                                'final_decision': 'ERROR',
                                'error': str(e)
                            }
                    
                    # Формируем данные уровня с Elder's Screens
                    # Убеждаемся, что elder_screens_data имеет правильную структуру
                    if not elder_screens_data or not isinstance(elder_screens_data, dict):
                        elder_screens_data = {
                            'screen_1': {'passed': None, 'blocked_reason': 'Данные Elder\'s Screens отсутствуют'},
                            'screen_2': {'passed': None, 'blocked_reason': 'Данные Elder\'s Screens отсутствуют'},
                            'final_decision': 'NOT_CHECKED'
                        }
                    elif 'screen_1' not in elder_screens_data or 'screen_2' not in elder_screens_data:
                        # Если структура неполная, дополняем её
                        if 'screen_1' not in elder_screens_data:
                            elder_screens_data['screen_1'] = {'passed': None, 'blocked_reason': 'Экран 1 не проверен'}
                        if 'screen_2' not in elder_screens_data:
                            elder_screens_data['screen_2'] = {'passed': None, 'blocked_reason': 'Экран 2 не проверен'}
                        if 'final_decision' not in elder_screens_data:
                            elder_screens_data['final_decision'] = 'NOT_CHECKED'
                    
                    level_dict['elder_screens'] = elder_screens_data
                    level_dict['ready_for_signal'] = elder_screens_data.get('final_decision') == 'PASSED' if elder_screens_data else False
                    
                    # Подсчитываем статистику
                    if elder_screens_data:
                        if elder_screens_data.get('final_decision') == 'PASSED':
                            ready_count += 1
                        elif elder_screens_data.get('final_decision') == 'BLOCKED_SCREEN_1':
                            blocked_screen_1_count += 1
                        elif elder_screens_data.get('final_decision') == 'BLOCKED_SCREEN_2':
                            blocked_screen_2_count += 1
                        else:
                            not_checked_count += 1
                    else:
                        not_checked_count += 1
                    
                    # Вычисляем расстояние до уровня (используем актуальную цену из 15m свечей)
                    if current_price and level.price:
                        distance_pct = abs(current_price - level.price) / level.price * 100
                        # Округляем до 2 знаков после запятой, но сохраняем реальное значение (не заменяем на 0.00)
                        level_dict['distance_pct'] = round(distance_pct, 2)
                        
                        # ОПТИМИЗАЦИЯ: Определяем частоту обновления в зависимости от расстояния
                        # <1%: каждые 30 секунд, 1-2.5%: каждую минуту, 2.5-5%: каждые 5 минут, >5%: каждые 10 минут
                        if distance_pct < 1.0:
                            update_interval_seconds = 30
                        elif distance_pct < 2.5:
                            update_interval_seconds = 60
                        elif distance_pct < 5.0:
                            update_interval_seconds = 300
                        else:
                            update_interval_seconds = 600
                        
                        level_dict['update_interval_seconds'] = update_interval_seconds
                        
                        # Логируем для отладки готовых сигналов
                        if level_dict.get('ready_for_signal'):
                            logger.info(f"Готовый сигнал {pair_symbol} @ {level.price}: текущая цена={current_price:.4f}, расстояние={distance_pct:.4f}%, интервал обновления={update_interval_seconds}с")
                    else:
                        # Если нет цены, устанавливаем большое значение вместо None
                        level_dict['distance_pct'] = 999.0
                        level_dict['update_interval_seconds'] = 600  # По умолчанию 10 минут
                    
                    potential_levels.append(level_dict)
                    
            except Exception as e:
                logger.error(f"Ошибка обработки уровней для {pair_symbol}: {e}")
                import traceback
                traceback.print_exc()
                # Откатываем транзакцию при ошибке
                try:
                    db.rollback()
                except:
                    pass
                # Добавляем уровни с правильной структурой данных при ошибке
                for level in pair_levels:
                    level_dict = level.to_dict()
                    level_dict['elder_screens'] = {
                        'screen_1': {
                            'passed': None,  # Не проверено
                            'blocked_reason': f'Ошибка обработки уровней: {str(e)}',
                            'checks': {'error': str(e)}
                        },
                        'screen_2': {
                            'passed': None,  # Не проверено
                            'blocked_reason': f'Ошибка обработки уровней: {str(e)}',
                            'checks': {'error': str(e)}
                        },
                        'final_decision': 'ERROR',
                        'error': str(e)
                    }
                    level_dict['signal_created'] = False
                    level_dict['signal_timestamp'] = None
                    level_dict['signal_id'] = None
                    level_dict['signal_status'] = None
                    potential_levels.append(level_dict)
                    not_checked_count += 1
        
        # Группируем уровни по парам
        levels_by_pair = {}
        for level in potential_levels:
            pair = level.get('pair', 'UNKNOWN')
            if pair not in levels_by_pair:
                levels_by_pair[pair] = []
            levels_by_pair[pair].append(level)
        
        # Сортируем уровни внутри каждой пары: готовые → по расстоянию → по score
        for pair in levels_by_pair:
            levels_by_pair[pair].sort(key=lambda x: (
                not x.get('ready_for_signal', False),  # Готовые первыми
                x.get('distance_pct') if x.get('distance_pct') is not None else 999.0,  # Затем по расстоянию (ближайшие первыми), 0.00 это валидное значение
                -(x.get('score') or 0)  # Затем по score (высокий score первым), обрабатываем None
            ))
        
        # Сортируем пары: сначала те, у которых есть готовые сигналы, затем по ближайшему расстоянию, затем по количеству готовых
        def get_pair_priority(pair_levels):
            has_ready = any(l.get('ready_for_signal', False) for l in pair_levels)
            ready_count = sum(1 for l in pair_levels if l.get('ready_for_signal', False))
            # Правильно обрабатываем distance_pct: 0.00 это валидное значение, не заменяем на 999
            distances = [l.get('distance_pct') for l in pair_levels if l.get('distance_pct') is not None]
            min_distance = min(distances) if distances else 999.0
            max_score = max((l.get('score') or 0 for l in pair_levels), default=0)
            # Приоритет: готовые пары → больше готовых → ближе → выше score
            return (not has_ready, -ready_count, min_distance, -max_score)
        
        sorted_pairs = sorted(levels_by_pair.items(), key=lambda x: get_pair_priority(x[1]))
        
        # Формируем финальный список: сначала все уровни из пар с готовыми сигналами, затем остальные
        sorted_levels = []
        for pair, pair_levels in sorted_pairs:
            sorted_levels.extend(pair_levels)
        
        result = {
            "levels": sorted_levels,  # Плоский список для обратной совместимости
            "levels_by_pair": {pair: levels for pair, levels in sorted_pairs},  # Группированный по парам
            "total_levels": len(sorted_levels),
            "ready_for_signal": ready_count,
            "blocked_screen_1": blocked_screen_1_count,
            "blocked_screen_2": blocked_screen_2_count,
            "not_checked": not_checked_count,
            "timestamp": datetime.now().isoformat()
        }
        
        # ОПТИМИЗАЦИЯ: Кэшируем на 30 секунд для ближайших уровней (<1%)
        # Уровни обновляются с разной частотой в зависимости от расстояния:
        # Кэш устанавливаем на 5 минут (300 секунд) - синхронизировано с analyze_pair
        # Это уменьшает нагрузку на биржу и ускоряет загрузку страницы
        cache.set(cache_key, result, ttl=300)
        logger.info(f"✅ Кэш потенциальных сигналов обновлен: {len(sorted_levels)} уровней, готовых: {ready_count}, "
                   f"из БД: {levels_from_db_count}, пересчитано: {levels_recalculated_count}, "
                   f"оптимизация: {levels_from_db_count / len(sorted_levels) * 100:.1f}% данных из БД, "
                   f"TTL кэша: 300 секунд (5 минут)")
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка получения потенциальных сигналов: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/force-analysis")
async def force_analysis():
    """Принудительно запускает анализ всех пар (без предварительной очистки уровней)."""
    try:
        task = analyze_all_pairs.delay()
        return {
            "success": True,
            "status": "started",
            "task_id": task.id,
            "message": "Анализ запущен в фоне"
        }
    except Exception as e:
        logger.error(f"Ошибка запуска анализа: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cleanup-levels")
async def cleanup_levels_endpoint():
    """Очищает неактуальные уровни из БД"""
    try:
        from core.analysis_engine import analysis_engine
        
        logger.info("🧹 Запуск очистки неактуальных уровней...")
        cleanup_result = await analysis_engine.cleanup_outdated_levels()
        logger.info(f"Результат очистки: {cleanup_result}")
        
        return {
            "success": cleanup_result.get('status') == 'success',
            "message": "Очистка уровней завершена",
            "result": cleanup_result
        }
        
    except Exception as e:
        logger.error(f"Ошибка очистки уровней: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/demo-trading/status")
async def get_demo_trading_status():
    """Возвращает статус live-торговли (Bybit)."""
    status = bybit_demo_client.get_status()
    status["live_trading_enabled"] = is_live_trading_enabled()
    return status


@app.post("/api/demo-trading/order")
async def create_demo_trading_order(payload: DemoOrderRequest):
    """Размещает ордер на live-бирже."""
    if not bybit_demo_client.is_enabled():
        raise HTTPException(
            status_code=400,
            detail="Live-торговля недоступна. Укажите BYBIT_API_KEY/BYBIT_API_SECRET в .env.",
        )

    if payload.order_type == "limit" and payload.price is None:
        raise HTTPException(status_code=400, detail="Для лимитного ордера необходимо указать цену.")

    try:
        order = bybit_demo_client.place_order(
            symbol=payload.symbol,
            side=payload.side,
            order_type=payload.order_type,
            amount=payload.amount,
            price=payload.price,
            reduce_only=payload.reduce_only,
        )
        return {"success": True, "order": order}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/demo-trading/order/cancel")
async def cancel_demo_trading_order(payload: DemoCancelOrderRequest):
    """Отменяет ордер live-торговли."""
    if not bybit_demo_client.is_enabled():
        raise HTTPException(
            status_code=400,
            detail="Live-торговля недоступна. Укажите BYBIT_API_KEY/BYBIT_API_SECRET в .env.",
        )

    try:
        order = bybit_demo_client.cancel_order(order_id=payload.order_id, symbol=payload.symbol)
        return {"success": True, "order": order}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/demo-trading/position/close")
async def close_demo_position(payload: ClosePositionRequest):
    """Закрывает открытую позицию по рыночной цене."""
    if not bybit_demo_client.is_enabled():
        raise HTTPException(
            status_code=400,
            detail="Live-торговля недоступна. Укажите BYBIT_API_KEY/BYBIT_API_SECRET в .env.",
        )

    try:
        result = bybit_demo_client.close_position(symbol=payload.symbol, side=payload.side)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/trading/live/status")
async def get_live_trading_mode():
    """Текущее состояние live-торговли."""
    return {"enabled": is_live_trading_enabled()}


@app.post("/api/trading/live/status")
async def set_live_trading_mode(payload: LiveTradingToggleRequest):
    """Включает или выключает live-торговлю."""
    enabled = set_live_trading_enabled(payload.enabled)
    return {"enabled": enabled}


@app.post("/api/demo-trading/signals/{signal_id}/execute")
async def trigger_demo_trade(signal_id: int):
    """Запускает размещение ордера на бирже по конкретному сигналу."""
    if not settings.DEMO_AUTO_TRADING_ENABLED:
        raise HTTPException(status_code=400, detail="Автоматическая live-торговля отключена.")
    try:
        task = place_demo_order_for_signal.delay(signal_id)
        return {"success": True, "task_id": task.id}
    except Exception as err:
        logger.error("Не удалось поставить задачу demo-trade для сигнала %s: %s", signal_id, err)
        raise HTTPException(status_code=500, detail=str(err))


@app.post("/api/reload-historical-ohlcv")
async def reload_historical_ohlcv(
    pair: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    days: int = Query(3),
    force_update_closed: bool = Query(False)
):
    """
    Перезагружает исторические данные OHLCV с биржи для исправления неправильных данных в БД
    
    ВАЖНО: Используйте эту функцию только при обнаружении проблем с историческими данными.
    По умолчанию обновляет только незакрытые свечи. Если force_update_closed=True,
    то обновляет ВСЕ свечи, включая закрытые (для исправления ошибок).
    
    Args:
        pair: Торговая пара (если None - для всех пар)
        timeframe: Таймфрейм (если None - для всех таймфреймов: 15m, 1h, 4h)
        days: Количество дней истории для перезагрузки (по умолчанию 3 дня)
        force_update_closed: Если True, обновляет даже закрытые свечи
    
    Returns:
        Результат задачи Celery
    """
    try:
        from tasks.ohlcv_tasks import reload_historical_data
        
        task = reload_historical_data.delay(
            pair=pair,
            timeframe=timeframe,
            days=days,
            force_update_closed=force_update_closed
        )
        
        return {
            "success": True,
            "task_id": task.id,
            "message": f"Запущена перезагрузка исторических данных (пара={pair or 'все'}, таймфрейм={timeframe or 'все'}, дней={days})"
        }
    except Exception as e:
        logger.error(f"Ошибка запуска перезагрузки исторических данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/update-signals-pnl")
async def update_signals_pnl_endpoint():
    """Принудительно обновляет P&L и результаты сигналов."""
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, update_signals_pnl_sync)
        status = result.get('status', 'success') if isinstance(result, dict) else 'success'
        return {
            "status": status,
            "mode": "sync",
            "message": "Обновление P&L выполнено локально",
            "result": result
        }
    except Exception as e:
        logger.error(f"Ошибка обновления P&L: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tasks")
async def get_tasks_list():
    """Получает список всех активных задач Celery"""
    try:
        # Получаем активные задачи
        inspect = celery_app.control.inspect()
        active_tasks = inspect.active()
        
        # Получаем зарезервированные задачи
        reserved_tasks = inspect.reserved()
        
        # Получаем запланированные задачи
        scheduled_tasks = inspect.scheduled()
        
        tasks_list = {
            "active": [],
            "reserved": [],
            "scheduled": []
        }
        
        # Обрабатываем активные задачи
        if active_tasks:
            for worker, tasks in active_tasks.items():
                for task in tasks:
                    tasks_list["active"].append({
                        "task_id": task.get("id"),
                        "name": task.get("name"),
                        "worker": worker,
                        "args": task.get("args", []),
                        "kwargs": task.get("kwargs", {}),
                        "time_start": task.get("time_start")
                    })
        
        # Обрабатываем зарезервированные задачи
        if reserved_tasks:
            for worker, tasks in reserved_tasks.items():
                for task in tasks:
                    tasks_list["reserved"].append({
                        "task_id": task.get("id"),
                        "name": task.get("name"),
                        "worker": worker,
                        "args": task.get("args", []),
                        "kwargs": task.get("kwargs", {})
                    })
        
        # Обрабатываем запланированные задачи
        if scheduled_tasks:
            for worker, tasks in scheduled_tasks.items():
                for task in tasks:
                    tasks_list["scheduled"].append({
                        "task_id": task.get("request", {}).get("id"),
                        "name": task.get("request", {}).get("task"),
                        "worker": worker,
                        "eta": task.get("eta"),
                        "priority": task.get("priority")
                    })
        
        return {
            "total_active": len(tasks_list["active"]),
            "total_reserved": len(tasks_list["reserved"]),
            "total_scheduled": len(tasks_list["scheduled"]),
            "tasks": tasks_list,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения списка задач: {e}")
        return {
            "error": str(e),
            "tasks": {"active": [], "reserved": [], "scheduled": []},
            "total_active": 0,
            "total_reserved": 0,
            "total_scheduled": 0
        }


@app.get("/api/task/{task_id}")
async def get_task_status(task_id: str):
    """Получает статус задачи Celery с прогрессом"""
    try:
        task = celery_app.AsyncResult(task_id)
        
        response = {
            "task_id": task_id,
            "status": task.status,
            "result": task.result if task.ready() else None
        }
        
        # Добавляем информацию о прогрессе, если задача выполняется
        if task.state == 'PROGRESS':
            response['progress'] = task.info
        elif task.state == 'PENDING':
            response['progress'] = {'status': 'Ожидание запуска...', 'current': 0, 'total': 0, 'percent': 0}
        elif task.state == 'STARTED':
            response['progress'] = {'status': 'Запущено...', 'current': 0, 'total': 0, 'percent': 0}
        
        return response
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса задачи: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/chart-data")
async def get_chart_data(pair: str, timeframe: str = "15m", db=Depends(get_db)):
    """Получает данные OHLCV для графиков с кэшированием"""
    try:
        from core.analysis_engine import analysis_engine
        from core.models import Level, TradingPair
        import asyncio
        
        if not pair:
            return {
                "success": False,
                "error": "Не указана пара"
            }
        
        # Пытаемся получить из кэша сначала
        cache_key = f"chart_data:{pair}:{timeframe}"
        cached_data = cache.get(cache_key)
        
        candles_list = None
        cache_hit = False
        
        if cached_data and isinstance(cached_data, dict) and cached_data.get('candles'):
            # Используем кэшированные данные, НО валидируем их тоже
            cached_candles = cached_data.get('candles')
            if cached_candles and isinstance(cached_candles, list) and len(cached_candles) > 0:
                candles_list = cached_candles
                cache_hit = True
                logger.info(f"Используем кэшированные данные для {pair} {timeframe} (будет валидировано)")
            else:
                candles_list = None
                cache_hit = False
        else:
            # Используем локальное хранилище свечей (ohlcv_store) с fallback на API биржи
            try:
                from core.ohlcv_store import ohlcv_store
                
                # ohlcv_store.get_ohlcv() - синхронный метод, оборачиваем в asyncio.to_thread()
                # Увеличиваем лимит в 3 раза для лучшей визуализации
                candles_limit = 600  # Было 200, теперь 600
                candles = await asyncio.to_thread(
                    ohlcv_store.get_ohlcv,
                    pair,
                    timeframe,
                    candles_limit
                )
                
                # ohlcv_store.get_ohlcv() возвращает список словарей в правильном формате
                if candles and len(candles) > 0:
                    candles_list = candles  # Уже в правильном формате от ohlcv_store
                    
                    # Кэшируем данные на 2 минуты (120 секунд)
                    cache.set(cache_key, {
                        'candles': candles_list,
                        'timestamp': datetime.now().isoformat()
                    }, ttl=120)
                    logger.info(f"✅ Получены свежие данные из ohlcv_store для {pair} {timeframe}, количество свечей: {len(candles_list)}")
                else:
                    # Если нет данных, но есть кэш - используем его
                    if cached_data:
                        candles_list = cached_data.get('candles', [])
                        logger.warning(f"Нет свежих данных для {pair} {timeframe}, используем старый кэш")
                    else:
                        logger.warning(f"Нет данных для {pair} {timeframe} и нет кэша")
            except Exception as fetch_error:
                logger.error(f"Ошибка получения данных для {pair} {timeframe}: {fetch_error}")
                # Fallback на кэш или пробуем через старый метод (analysis_engine) как последний резерв
                if cached_data:
                    candles_list = cached_data.get('candles', [])
                    logger.warning(f"Ошибка получения данных, используем кэш для {pair} {timeframe}")
                else:
                    # Последний fallback: пробуем через старый метод (analysis_engine)
                    try:
                        logger.warning(f"Пробуем получить данные через старый метод (analysis_engine) для {pair} {timeframe}")
                        candles = await analysis_engine.fetch_ohlcv(pair, timeframe, 200)
                        if candles and len(candles) > 0:
                            if isinstance(candles, list):
                                candles_list = candles
                            else:
                                candles_list = list(candles)
                            logger.info(f"✅ Данные получены через fallback (analysis_engine) для {pair} {timeframe}")
                        else:
                            return {
                                "success": False,
                                "error": f"Нет данных для пары {pair}. Возможно, превышен лимит запросов к Binance API. Попробуйте позже.",
                                "cached": False
                            }
                    except Exception as fallback_error:
                        logger.error(f"Ошибка fallback для {pair} {timeframe}: {fallback_error}")
                        return {
                            "success": False,
                            "error": f"Нет данных для пары {pair}. Возможно, превышен лимит запросов к Binance API. Попробуйте позже.",
                            "cached": False
                        }
        
        # Если все еще нет данных
        if not candles_list or len(candles_list) == 0:
            return {
                "success": False,
                "error": f"Нет данных для пары {pair}",
                "cached": cache_hit
            }
        
        # ВАЛИДАЦИЯ ДАННЫХ: фильтруем некорректные свечи перед отправкой клиенту
        validated_candles = []
        for idx, candle in enumerate(candles_list):
            if not candle or not isinstance(candle, dict):
                logger.warning(f"Пропущена свеча {idx} для {pair} {timeframe}: не является словарем")
                continue
            
            # Проверяем наличие всех обязательных полей
            timestamp = candle.get('timestamp')
            open_val = candle.get('open')
            high_val = candle.get('high')
            low_val = candle.get('low')
            close_val = candle.get('close')
            
            # Проверяем, что все значения не null/undefined
            if timestamp is None or open_val is None or high_val is None or low_val is None or close_val is None:
                logger.warning(f"Пропущена свеча {idx} для {pair} {timeframe}: содержит null значения")
                continue
            
            # Преобразуем в числа и проверяем валидность
            try:
                # КРИТИЧЕСКАЯ ПРОВЕРКА: убеждаемся, что значения не None перед преобразованием
                if timestamp is None or open_val is None or high_val is None or low_val is None or close_val is None:
                    logger.warning(f"Пропущена свеча {idx} для {pair} {timeframe}: содержит None перед преобразованием")
                    continue
                
                timestamp_num = float(timestamp)
                open_num = float(open_val)
                high_num = float(high_val)
                low_num = float(low_val)
                close_num = float(close_val)
                
                # Проверяем, что преобразование не дало NaN или Infinity
                if (math.isnan(timestamp_num) or math.isinf(timestamp_num) or timestamp_num <= 0 or
                    math.isnan(open_num) or math.isinf(open_num) or open_num <= 0 or
                    math.isnan(high_num) or math.isinf(high_num) or high_num <= 0 or
                    math.isnan(low_num) or math.isinf(low_num) or low_num <= 0 or
                    math.isnan(close_num) or math.isinf(close_num) or close_num <= 0):
                    logger.warning(f"Пропущена свеча {idx} для {pair} {timeframe}: NaN, Infinity или неположительные значения")
                    continue
                
                # Проверяем логику OHLC
                if (high_num < low_num or high_num < open_num or high_num < close_num or 
                    low_num > open_num or low_num > close_num):
                    logger.warning(f"Пропущена свеча {idx} для {pair} {timeframe}: нарушена логика OHLC")
                    continue
                
                # Сохраняем валидированную свечу
                validated_candles.append({
                    'timestamp': timestamp_num,
                    'open': open_num,
                    'high': high_num,
                    'low': low_num,
                    'close': close_num,
                    'volume': candle.get('volume', 0.0)  # volume может быть опциональным
                })
            except (ValueError, TypeError) as e:
                logger.warning(f"Пропущена свеча {idx} для {pair} {timeframe}: ошибка преобразования - {e}")
                continue
        
        if len(validated_candles) == 0:
            logger.error(f"Нет валидных свечей для {pair} {timeframe} после фильтрации")
            return {
                "success": False,
                "error": f"Нет валидных данных для пары {pair}",
                "cached": cache_hit
            }
        
        # Получаем активные уровни для этой пары (только если пара включена)
        pair_obj = db.query(TradingPair).filter_by(symbol=pair).first()
        pair_levels = []
        
        if pair_obj and pair_obj.enabled:
            levels_query = db.query(Level).filter_by(
                pair_id=pair_obj.id,
                is_active=True
            )
            levels = levels_query.all()

            # Фильтруем уровни относительно текущей цены, чтобы не показывать
            # явно неактуальные уровни (поддержка далеко выше цены и т.п.)
            if validated_candles:
                current_price = validated_candles[-1]['close']
                # Допустимая погрешность для "коридора" вокруг цены (в процентах)
                support_tolerance_pct = 0.01  # 1% сверху для поддержки
                resistance_tolerance_pct = 0.01  # 1% снизу для сопротивления

                filtered_levels = []
                for lvl in levels:
                    price = lvl.price or 0
                    if price <= 0:
                        continue

                    # Поддержка должна быть ниже или очень близко к цене
                    if lvl.level_type == 'support':
                        # Если поддержка значительно выше текущей цены (>1%), считаем её пробитой и скрываем
                        if price > current_price * (1 + support_tolerance_pct):
                            continue
                    # Сопротивление должно быть выше или очень близко к цене
                    elif lvl.level_type == 'resistance':
                        # Если сопротивление значительно ниже текущей цены (>1%), считаем его пробитым и скрываем
                        if price < current_price * (1 - resistance_tolerance_pct):
                            continue

                    filtered_levels.append(lvl)

                pair_levels = [level.to_dict() for level in filtered_levels]
            else:
                pair_levels = [level.to_dict() for level in levels]
        
        return {
            "success": True,
            "pair": pair,
            "timeframe": timeframe,
            "candles": validated_candles,  # Используем валидированные свечи
            "levels": pair_levels,
            "count": len(validated_candles),
            "cached": cache_hit
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения данных графика для {pair}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "cached": False
        }


@app.get("/api/signal-chart-data/{signal_id}")
async def get_signal_chart_data(signal_id: int, timeframe: str = "15m", db=Depends(get_db)):
    """Получает данные графика для конкретного сигнала с точками входа и выхода"""
    try:
        from core.analysis_engine import analysis_engine
        from core.models import Signal, TradingPair
        from sqlalchemy.orm import joinedload
        import asyncio
        
        # Получаем сигнал из БД
        signal = db.query(Signal).options(
            joinedload(Signal.pair)
        ).filter(Signal.id == signal_id).first()
        
        if not signal:
            return {
                "success": False,
                "error": f"Сигнал с ID {signal_id} не найден"
            }
        
        if not signal.pair:
            return {
                "success": False,
                "error": "Пара не найдена для сигнала"
            }
        
        pair_symbol = signal.pair.symbol
        entry_price = signal.entry_price or signal.level_price
        signal_timestamp = signal.timestamp
        
        # Получаем данные графика
        cache_key = f"signal_chart_data:{signal_id}:{timeframe}"
        cached_data = cache.get(cache_key)
        
        candles_list = None
        cache_hit = False
        
        if cached_data and isinstance(cached_data, dict) and cached_data.get('candles'):
            candles_list = cached_data.get('candles')
            cache_hit = True
        else:
            try:
                # Используем локальное хранилище свечей (ohlcv_store) с fallback на API биржи
                from core.ohlcv_store import ohlcv_store
                
                # Оптимизация: загружаем только необходимое количество свечей (100-150 достаточно для графика)
                # Для 15m: 150 свечей = ~37.5 часов истории (достаточно для анализа)
                candles_limit = 150 if timeframe == '15m' else 100
                
                # ohlcv_store.get_ohlcv() - синхронный метод, оборачиваем в asyncio.to_thread()
                candles = await asyncio.to_thread(
                    ohlcv_store.get_ohlcv,
                    pair_symbol,
                    timeframe,
                    candles_limit
                )
                
                # ohlcv_store.get_ohlcv() возвращает список словарей в правильном формате
                # Формат: [{'timestamp': int, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float}]
                if candles and len(candles) > 0:
                    candles_list = candles  # Уже в правильном формате от ohlcv_store
                    
                    # Логируем для отладки
                    logger.debug(f"✅ Получено {len(candles_list)} свечей из ohlcv_store для сигнала {signal_id}")
                    
                    # Кэшируем на 10 минут
                    cache.set(cache_key, {
                        'candles': candles_list,
                        'timestamp': datetime.now().isoformat()
                    }, ttl=600)
            except Exception as fetch_error:
                logger.error(f"Ошибка получения данных для сигнала {signal_id}: {fetch_error}")
                # Fallback на кэш или пробуем через старый метод (analysis_engine) как последний резерв
                if cached_data:
                    candles_list = cached_data.get('candles', [])
                    logger.info(f"Используем кэшированные данные для сигнала {signal_id} после ошибки")
                else:
                    # Последний fallback: пробуем через старый метод (analysis_engine)
                    try:
                        logger.warning(f"Пробуем получить данные через старый метод (analysis_engine) для сигнала {signal_id}")
                        candles_limit = 150 if timeframe == '15m' else 100
                        candles = await asyncio.wait_for(
                            analysis_engine.fetch_ohlcv(pair_symbol, timeframe, candles_limit),
                            timeout=5.0
                        )
                        if candles and len(candles) > 0:
                            if isinstance(candles, list):
                                candles_list = candles
                            else:
                                candles_list = list(candles)
                            logger.info(f"✅ Данные получены через fallback (analysis_engine) для сигнала {signal_id}")
                        else:
                            return {
                                "success": False,
                                "error": f"Не удалось получить данные графика: {fetch_error}",
                                "cached": False
                            }
                    except Exception as fallback_error:
                        logger.error(f"Ошибка fallback для сигнала {signal_id}: {fallback_error}")
                        return {
                            "success": False,
                            "error": f"Не удалось получить данные графика: {fetch_error}",
                            "cached": False
                        }
        
        if not candles_list or len(candles_list) == 0:
            return {
                "success": False,
                "error": f"Нет данных для пары {pair_symbol}",
                "cached": cache_hit
            }
        
        # ВАЛИДАЦИЯ ДАННЫХ: фильтруем некорректные свечи ПЕРЕД использованием
        validated_candles = []
        for idx, candle in enumerate(candles_list):
            if not candle or not isinstance(candle, dict):
                logger.warning(f"Пропущена свеча {idx} для сигнала {signal_id}: не является словарем")
                continue
            
            # Проверяем наличие всех обязательных полей
            timestamp = candle.get('timestamp')
            open_val = candle.get('open')
            high_val = candle.get('high')
            low_val = candle.get('low')
            close_val = candle.get('close')
            
            # Проверяем, что все значения не null/undefined
            if timestamp is None or open_val is None or high_val is None or low_val is None or close_val is None:
                logger.warning(f"Пропущена свеча {idx} для сигнала {signal_id}: содержит null значения")
                continue
            
            # Преобразуем в числа и проверяем валидность
            try:
                # КРИТИЧЕСКАЯ ПРОВЕРКА: убеждаемся, что значения не None перед преобразованием
                if timestamp is None or open_val is None or high_val is None or low_val is None or close_val is None:
                    logger.warning(f"Пропущена свеча {idx} для сигнала {signal_id}: содержит None перед преобразованием")
                    continue
                
                timestamp_num = float(timestamp)
                open_num = float(open_val)
                high_num = float(high_val)
                low_num = float(low_val)
                close_num = float(close_val)
                
                # Проверяем, что преобразование не дало NaN или Infinity
                if (math.isnan(timestamp_num) or math.isinf(timestamp_num) or timestamp_num <= 0 or
                    math.isnan(open_num) or math.isinf(open_num) or open_num <= 0 or
                    math.isnan(high_num) or math.isinf(high_num) or high_num <= 0 or
                    math.isnan(low_num) or math.isinf(low_num) or low_num <= 0 or
                    math.isnan(close_num) or math.isinf(close_num) or close_num <= 0):
                    logger.warning(f"Пропущена свеча {idx} для сигнала {signal_id}: NaN, Infinity или неположительные значения")
                    continue
                
                # Проверяем логику OHLC
                if (high_num < low_num or high_num < open_num or high_num < close_num or 
                    low_num > open_num or low_num > close_num):
                    logger.warning(f"Пропущена свеча {idx} для сигнала {signal_id}: нарушена логика OHLC")
                    continue
                
                # Сохраняем валидированную свечу
                validated_candles.append({
                    'timestamp': timestamp_num,
                    'open': open_num,
                    'high': high_num,
                    'low': low_num,
                    'close': close_num,
                    'volume': candle.get('volume', 0.0)  # volume может быть опциональным
                })
            except (ValueError, TypeError) as e:
                logger.warning(f"Пропущена свеча {idx} для сигнала {signal_id}: ошибка преобразования - {e}")
                continue
        
        if len(validated_candles) == 0:
            logger.error(f"Нет валидных свечей для сигнала {signal_id} после фильтрации")
            return {
                "success": False,
                "error": f"Нет валидных данных для пары {pair_symbol}",
                "cached": cache_hit
            }
        
        # Используем валидированные свечи для дальнейших вычислений
        candles_list = validated_candles
        
        # Вычисляем точку выхода (exit price)
        exit_price = None
        exit_timestamp = None
        exit_reason = None
        exit_timestamp_ts = None
        
        if signal.result_fixed is not None and signal.result_fixed_at:
            exit_timestamp = signal.result_fixed_at
            exit_reason = "Прибыль +1.5%" if signal.result_fixed == 1.5 else "Убыток -0.5%"
            
            # Вычисляем цену выхода на основе result_fixed (базовая цена для поиска в свечах)
            if signal.signal_type == 'LONG':
                if signal.result_fixed == 1.5:
                    # Прибыль: цена выросла на 1.5%
                    exit_price = entry_price * 1.015
                elif signal.result_fixed == -0.5:
                    # Убыток: цена упала на 0.5%
                    exit_price = entry_price * 0.995
            else:  # SHORT
                if signal.result_fixed == 1.5:
                    # Прибыль: цена упала на 1.5%
                    exit_price = entry_price * 0.985
                elif signal.result_fixed == -0.5:
                    # Убыток: цена выросла на 0.5%
                    exit_price = entry_price * 1.005
            
            # Конвертируем время выхода в timestamp (миллисекунды)
            # exit_timestamp может быть datetime объектом из SQLAlchemy
            try:
                if hasattr(exit_timestamp, 'timestamp'):
                    # Это datetime объект
                    exit_timestamp_ts = int(exit_timestamp.timestamp() * 1000)
                elif isinstance(exit_timestamp, str):
                    # Это строка в ISO формате
                    dt = datetime.fromisoformat(exit_timestamp.replace('Z', '+00:00'))
                    exit_timestamp_ts = int(dt.timestamp() * 1000)
                else:
                    logger.warning(f"Неизвестный тип exit_timestamp: {type(exit_timestamp)}")
                    exit_timestamp_ts = None
            except Exception as e:
                logger.error(f"Ошибка конвертации времени выхода: {e}, тип: {type(exit_timestamp)}")
                exit_timestamp_ts = None
            
            # Находим реальную цену выхода из свечей
            # result_fixed_at - это время свечи, когда был достигнут порог
            if exit_timestamp_ts:
                # Определяем период свечи в миллисекундах на основе таймфрейма
                timeframe_periods = {
                    '1m': 1 * 60 * 1000,
                    '5m': 5 * 60 * 1000,
                    '15m': 15 * 60 * 1000,
                    '30m': 30 * 60 * 1000,
                    '1h': 60 * 60 * 1000,
                    '4h': 4 * 60 * 60 * 1000,
                    '1d': 24 * 60 * 60 * 1000,
                }
                candle_period = timeframe_periods.get(timeframe, 15 * 60 * 1000)  # По умолчанию 15 минут
                
                # Ищем свечу, которая соответствует времени выхода (или ближайшую)
                closest_candle = None
                min_diff = float('inf')
                
                for candle in candles_list:
                    candle_ts = candle.get('timestamp', 0)
                    if isinstance(candle_ts, str):
                        # Если timestamp в строковом формате, пропускаем
                        continue
                    
                    # Ищем свечу, которая содержит время выхода (в пределах периода свечи)
                    if abs(candle_ts - exit_timestamp_ts) < candle_period:
                        # Эта свеча содержит время выхода
                        closest_candle = candle
                        break
                    else:
                        # Ищем ближайшую свечу
                        diff = abs(candle_ts - exit_timestamp_ts)
                        if diff < min_diff:
                            min_diff = diff
                            closest_candle = candle
                
                if closest_candle:
                    # Используем реальную цену из свечи в зависимости от направления и результата
                    if signal.signal_type == 'LONG':
                        if signal.result_fixed == 1.5:
                            # Прибыль LONG: цена выросла, берем high свечи (максимальная цена достижения 1.5%)
                            exit_price = closest_candle.get('high', exit_price)
                            # Убеждаемся, что цена действительно выше entry_price на 1.5%
                            min_exit_price = entry_price * 1.015
                            if exit_price < min_exit_price:
                                exit_price = min_exit_price
                        else:  # -0.5
                            # Убыток LONG: цена упала, берем low свечи (минимальная цена достижения -0.5%)
                            exit_price = closest_candle.get('low', exit_price)
                            # Убеждаемся, что цена действительно ниже entry_price на 0.5%
                            max_exit_price = entry_price * 0.995
                            if exit_price > max_exit_price:
                                exit_price = max_exit_price
                    else:  # SHORT
                        if signal.result_fixed == 1.5:
                            # Прибыль SHORT: цена упала, берем low свечи (минимальная цена достижения 1.5%)
                            exit_price = closest_candle.get('low', exit_price)
                            # Убеждаемся, что цена действительно ниже entry_price на 1.5%
                            max_exit_price = entry_price * 0.985
                            if exit_price > max_exit_price:
                                exit_price = max_exit_price
                        else:  # -0.5
                            # Убыток SHORT: цена выросла, берем high свечи (максимальная цена достижения -0.5%)
                            exit_price = closest_candle.get('high', exit_price)
                            # Убеждаемся, что цена действительно выше entry_price на 0.5%
                            min_exit_price = entry_price * 1.005
                            if exit_price < min_exit_price:
                                exit_price = min_exit_price
        
        # Формируем информацию о сигнале
        signal_info = {
            "id": signal.id,
            "pair": pair_symbol,
            "signal_type": signal.signal_type,
            "entry_price": float(entry_price) if entry_price else None,
            "level_price": float(signal.level_price) if signal.level_price else None,
            "entry_timestamp": signal_timestamp.isoformat() if signal_timestamp else None,
            "exit_price": float(exit_price) if exit_price else None,
            "exit_timestamp": exit_timestamp.isoformat() if exit_timestamp else None,
            "exit_reason": exit_reason,
            "result_fixed": float(signal.result_fixed) if signal.result_fixed is not None else None,
            "pnl_percent": float(signal.pnl_percent) if signal.pnl_percent else 0.0,
            "max_profit": float(signal.max_profit) if signal.max_profit else 0.0,
            "max_drawdown": float(signal.max_drawdown) if signal.max_drawdown else 0.0,
            "status": signal.status,
            "trend_1h": signal.trend_1h,
            "level_type": signal.level_type,
            # Elder's Triple Screen System
            "elder_screen_1_passed": signal.elder_screen_1_passed,
            "elder_screen_1_blocked_reason": signal.elder_screen_1_blocked_reason,
            "elder_screen_2_passed": signal.elder_screen_2_passed,
            "elder_screen_2_blocked_reason": signal.elder_screen_2_blocked_reason,
            "elder_screen_3_passed": signal.elder_screen_3_passed,
            "elder_screen_3_blocked_reason": signal.elder_screen_3_blocked_reason,
            "elder_screens_metadata": signal.elder_screens_metadata if signal.elder_screens_metadata else {}
        }
        
        logger.info(f"Возвращаем данные для сигнала {signal_id}: {len(candles_list)} свечей")
        
        return {
            "success": True,
            "signal": signal_info,
            "pair": pair_symbol,
            "timeframe": timeframe,
            "candles": candles_list,
            "count": len(candles_list),
            "cached": cache_hit
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения данных графика для сигнала {signal_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "cached": False
        }


# Статические файлы
web_path = Path(__file__).parent.parent.parent / "web"

@app.get("/dashboard")
async def serve_dashboard():
    """Сервирует главную страницу"""
    return FileResponse(web_path / "dashboard.html")


@app.get("/signals")
async def serve_signals():
    """Сервирует страницу сигналов"""
    return FileResponse(web_path / "signals.html")


@app.get("/demo-trading")
async def serve_demo_trading():
    """Сервирует страницу live-торговли"""
    return FileResponse(web_path / "demo_trading.html")


@app.get("/potential-signals")
async def serve_potential_signals():
    """Сервирует страницу потенциальных сигналов"""
    return FileResponse(web_path / "potential-signals.html")


@app.get("/charts")
async def serve_charts():
    """Сервирует страницу графиков"""
    return FileResponse(web_path / "charts.html")


@app.get("/api/chart-patterns")
async def get_chart_patterns(
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    pattern_category: Optional[str] = None,
    direction: Optional[str] = None,
    pattern_type: Optional[str] = None,
    is_active: bool = True,
    is_confirmed: Optional[bool] = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Получает список обнаруженных ценовых фигур (chart patterns)
    
    ВАЖНО: По умолчанию возвращает ТОЛЬКО треугольники для оптимизации производительности.
    Для получения других паттернов используйте параметр pattern_type.
    
    Args:
        symbol: Фильтр по символу пары (например, 'BTC/USDT')
        timeframe: Фильтр по таймфрейму ('15m', '1h', '4h')
        pattern_category: Фильтр по категории ('reversal', 'continuation', 'consolidation')
        direction: Фильтр по направлению ('bullish', 'bearish', 'neutral')
        pattern_type: Фильтр по типу фигуры. Если не указан, возвращаются только треугольники.
        is_active: Показывать только активные фигуры (по умолчанию True)
        is_confirmed: Фильтр по подтверждению (True/False/None - все)
        limit: Максимальное количество фигур (по умолчанию 100)
    
    Returns:
        Список фигур с информацией о типе, категории, направлении, надежности и геометрии
    """
    try:
        query = db.query(ChartPattern)
        
        # ВАЖНО: По умолчанию показываем ТОЛЬКО треугольники для оптимизации
        # Это ускоряет загрузку графиков и уменьшает нагрузку
        if not pattern_type:
            triangle_types = ['ascending_triangle', 'descending_triangle', 'symmetrical_triangle']
            query = query.filter(ChartPattern.pattern_type.in_(triangle_types))
        
        # Применяем фильтры
        if symbol:
            query = query.filter(ChartPattern.symbol == symbol)
        if timeframe:
            query = query.filter(ChartPattern.timeframe == timeframe)
        if pattern_category:
            query = query.filter(ChartPattern.pattern_category == pattern_category)
        if direction:
            query = query.filter(ChartPattern.direction == direction)
        if pattern_type:
            query = query.filter(ChartPattern.pattern_type == pattern_type)
        if is_active:
            query = query.filter(ChartPattern.is_active == True)
        if is_confirmed is not None:
            query = query.filter(ChartPattern.is_confirmed == is_confirmed)
        
        # Сортируем по времени начала (новые первыми)
        query = query.order_by(ChartPattern.start_time.desc())
        
        # Ограничиваем количество
        patterns = query.limit(limit).all()
        
        # Преобразуем в словари
        result = []
        for pattern in patterns:
            try:
                result.append(pattern.to_dict())
            except Exception as e:
                logger.warning(f"Ошибка преобразования фигуры {pattern.id} в словарь: {e}")
                # Создаем словарь вручную, если to_dict() не работает
                result.append({
                    'id': pattern.id,
                    'symbol': pattern.symbol,
                    'timeframe': pattern.timeframe,
                    'pattern_type': pattern.pattern_type,
                    'pattern_category': pattern.pattern_category,
                    'direction': pattern.direction,
                    'reliability': pattern.reliability,
                    'start_time': pattern.start_time.isoformat() if pattern.start_time else None,
                    'end_time': pattern.end_time.isoformat() if pattern.end_time else None,
                    'confirmation_time': pattern.confirmation_time.isoformat() if pattern.confirmation_time else None,
                    'support_level': pattern.support_level,
                    'resistance_level': pattern.resistance_level,
                    'neckline': pattern.neckline,
                    'target_price': pattern.target_price,
                    'pattern_height': pattern.pattern_height,
                    'pattern_width': pattern.pattern_width,
                    'volume_confirmation': pattern.volume_confirmation,
                    'is_active': pattern.is_active,
                    'is_confirmed': pattern.is_confirmed,
                    'candles_count': pattern.candles_count,
                    'pattern_data': pattern.pattern_data,
                    'created_at': pattern.created_at.isoformat() if pattern.created_at else None,
                    'updated_at': pattern.updated_at.isoformat() if pattern.updated_at else None,
                })
        
        return {
            'success': True,
            'patterns': result,
            'count': len(result)
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения ценовых фигур: {e}", exc_info=True)
        import traceback
        logger.error(traceback.format_exc())
        # Возвращаем пустой список вместо ошибки, если таблица не существует
        if 'does not exist' in str(e).lower() or 'relation' in str(e).lower():
            logger.warning("Таблица chart_patterns не существует, возвращаем пустой список")
            return {
                'success': True,
                'patterns': [],
                'count': 0,
                'message': 'Таблица chart_patterns не создана. Примените миграцию: alembic upgrade head'
            }
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/chart-patterns/{symbol}")
async def get_chart_patterns_for_symbol(
    symbol: str,
    timeframe: Optional[str] = None,
    is_active: bool = True,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """
    Получает ценовые фигуры для конкретной пары
    
    Args:
        symbol: Символ пары (например, 'BTC/USDT')
        timeframe: Опциональный фильтр по таймфрейму
        is_active: Показывать только активные фигуры
        limit: Максимальное количество фигур
    
    Returns:
        Список фигур для указанной пары
    """
    return await get_chart_patterns(
        symbol=symbol,
        timeframe=timeframe,
        is_active=is_active,
        limit=limit,
        db=db
    )




if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )

