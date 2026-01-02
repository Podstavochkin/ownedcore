import ccxt
import asyncio
import numpy as np
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
import logging
from core.signal_manager import signal_manager
import pandas as pd

logger = logging.getLogger(__name__)

# Список торговых пар (существующие на Binance и Bybit фьючерсах)
# ИСКЛЮЧЕНЫ: FTM/USDT, MKR/USDT (недоступны на Bybit)
TRADING_PAIRS = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT",
    "XRP/USDT", "DOT/USDT", "AVAX/USDT", "LINK/USDT", "UNI/USDT", 
    "ATOM/USDT", "LTC/USDT", "BCH/USDT", "ETC/USDT", "FIL/USDT", 
    "NEAR/USDT", "ALGO/USDT", "VET/USDT", "ICP/USDT", 
    "THETA/USDT", "XLM/USDT", "TRX/USDT", "AAVE/USDT", "SUSHI/USDT", 
    "COMP/USDT", "SNX/USDT", "APT/USDT", "OP/USDT"
]

class AnalysisEngine:
    def __init__(self):
        self.exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'options': {
                'defaultType': 'future'
            }
        })
        
        # Кэш данных
        self.data_cache = {}
        self.trend_cache = {}
        self.last_update = {}
        
        # Интервалы обновления (в секундах)
        self.update_intervals = {
            "15m": 60,      # каждую 1 минуту
            "1h": 180,      # каждые 3 минуты
            "5m": 10        # каждые 10 секунд
        }
        
        # Настройки скальпинговой стратегии "отскок"
        self.level_settings = {
            "min_distance_percent": 1.0,          # Минимальная дистанция от текущей цены до уровня
            "max_distance_percent": 5.0,          # Максимальная дистанция (чтобы не брать слишком дальние уровни)
            "historical_touch_tolerance": 0.003,  # 0.3% - точность подсчета исторических касаний
            "live_touch_tolerance": 0.004,        # 0.4% - касание в режиме онлайн
            "break_tolerance": 0.007,             # 0.7% - пробой уровня
            "max_live_tests": 5,                  # После скольких живых тестов уровень считается "мертвым"
            "min_historical_touches": 2,          # Минимум подтверждений в истории
            "max_historical_touches": 8,          # Слишком много касаний = уровень слаб
            "exclude_recent_minutes": 60          # Сколько минут последних данных исключаем при создании уровня
        }
    
    def _calculate_candles_to_exclude(self, candles: List[Dict], minutes: int = 60) -> int:
        """
        Вычисляет количество свечей, которые нужно исключить из анализа,
        чтобы не формировать уровни по "горячим" данным.
        """
        if not candles or len(candles) < 2 or minutes <= 0:
            return 0
        
        latest_ts = candles[-1]['timestamp']
        prev_ts = candles[-2]['timestamp']
        candle_duration_ms = abs(latest_ts - prev_ts)
        if candle_duration_ms <= 0:
            return 0
        
        minutes_per_candle = candle_duration_ms / 1000 / 60
        if minutes_per_candle <= 0:
            return 0
        
        candles_to_exclude = int(minutes / minutes_per_candle)
        return max(0, min(len(candles) - 1, candles_to_exclude))
    
    def _prepare_candles_for_levels(self, candles: List[Dict]) -> Tuple[List[Dict], int]:
        """
        Возвращает список свечей без последнего часа (или заданного интервала)
        и количество исключенных свечей.
        """
        exclude_minutes = self.level_settings["exclude_recent_minutes"]
        exclude_count = self._calculate_candles_to_exclude(candles, exclude_minutes)
        if exclude_count <= 0 or exclude_count >= len(candles):
            return candles[:], 0
        return candles[:-exclude_count], exclude_count
    
    def _calculate_approach_score(self, candles: List[Dict], fractal_index: int,
                                  level_price: float, level_type: str, window: int = 5) -> float:
        """Оценивает угол/скорость подхода цены к уровню."""
        if fractal_index <= 0 or not candles:
            return 0.0
        start = max(0, fractal_index - window)
        segment = candles[start:fractal_index + 1]
        if len(segment) < 2:
            return 0.0
        
        start_price = segment[0]['close']
        end_price = segment[-1]['close']
        price_change = end_price - start_price
        normalized = abs(price_change) / max(level_price, 1e-9) * 100
        
        if level_type == 'support' and price_change >= 0:
            return 0.0  # Цена поднималась, подход слабый
        if level_type == 'resistance' and price_change <= 0:
            return 0.0
        
        return min(100.0, normalized * 4)  # Усиливаем вклад
    
    def _trend_bonus(self, level_type: str, trend: str) -> float:
        """Бонус за соответствие уровней контексту тренда."""
        if not trend:
            return 10.0
        direction = trend.split('_')[0]
        if level_type == 'support':
            if direction == 'UP':
                return 25.0
            if direction == 'SIDEWAYS':
                return 15.0
            if direction == 'DOWN':
                return 5.0
        if level_type == 'resistance':
            if direction == 'DOWN':
                return 25.0
            if direction == 'SIDEWAYS':
                return 15.0
            if direction == 'UP':
                return 5.0
        return 10.0
    
    def _deactivate_level_in_db(self, pair_symbol: str, level_price: float, price_tolerance: float = 0.005) -> None:
        """Удаляет пробитый/использованный уровень из БД (не храним мертвые уровни)."""
        try:
            from core.database import init_database, SessionLocal
            from core.models import TradingPair, Level

            if not init_database():
                return

            session = SessionLocal()
            try:
                pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
                if not pair:
                    return

                levels = session.query(Level).filter(
                    Level.pair_id == pair.id,
                    Level.is_active == True
                ).all()

                # Находим ближайший по цене уровень
                target = None
                min_diff = None
                for lvl in levels:
                    diff = abs(lvl.price - level_price) / max(level_price, 1e-9)
                    if diff <= price_tolerance and (min_diff is None or diff < min_diff):
                        target = lvl
                        min_diff = diff

                if target is not None:
                    session.delete(target)
                    session.commit()
                    logger.info(f"DB: удален уровень {pair_symbol} @ {target.price}")
            except Exception as e:
                session.rollback()
                logger.warning(f"Не удалось деактивировать уровень {pair_symbol} @ {level_price} в БД: {e}")
            finally:
                session.close()
        except Exception:
            # База может быть недоступна в некоторых окружениях анализа — не мешаем основной логике
            pass
    
    def _delete_level_from_db(self, pair_symbol: str, level_price: float, price_tolerance: float = 0.005) -> None:
        """Алиас для _deactivate_level_in_db для совместимости"""
        self._deactivate_level_in_db(pair_symbol, level_price, price_tolerance)
    
    async def cleanup_outdated_levels(self) -> Dict[str, Any]:
        """
        Проверяет и удаляет неактуальные уровни из БД.
        Критерии для удаления:
        1. test_count >= 5 (слишком много касаний)
        2. Устаревшие уровни (созданные более 48 часов назад)
        3. Пробитые уровни (цена пробила уровень более чем на 0.5%)
        """
        try:
            from core.database import init_database, SessionLocal
            from core.models import TradingPair, Level
            
            if not init_database():
                return {'status': 'error', 'message': 'БД не инициализирована'}
            
            session = SessionLocal()
            removed_count = 0
            removed_by_test_count = 0
            removed_by_broken = 0
            removed_by_age = 0
            
            try:
                # Получаем все активные уровни с загрузкой пар
                from sqlalchemy.orm import joinedload
                active_levels = session.query(Level).options(
                    joinedload(Level.pair)
                ).filter(
                    Level.is_active == True
                ).all()
                
                logger.info(f"🔍 Проверка {len(active_levels)} активных уровней на актуальность...")
                
                now = datetime.now(timezone.utc)
                max_age_hours = 168  # Удаляем уровни старше 7 дней (168 часов) - улучшено с 48 часов
                
                # Группируем уровни по парам для оптимизации запросов к API
                levels_by_pair = {}
                levels_to_remove = []  # Уровни, которые нужно удалить без проверки пробития
                
                for level in active_levels:
                    pair_symbol = level.pair.symbol if level.pair else None
                    if not pair_symbol:
                        continue
                    
                    should_remove = False
                    remove_reason = ""
                    
                    meta = dict(level.meta_data or {})
                    historical_touch = meta.get('historical_touches', max(level.test_count or 1, 1))
                    live_tests = meta.get('live_test_count')
                    if live_tests is None:
                        live_tests = max((level.test_count or historical_touch) - historical_touch, 0)
                    meta['historical_touches'] = historical_touch
                    meta['live_test_count'] = live_tests
                    level.meta_data = meta
                    
                    # 1. Проверка количества ЖИВЫХ касаний
                    if live_tests >= self.level_settings["max_live_tests"]:
                        should_remove = True
                        remove_reason = f"live_tests={live_tests} >= {self.level_settings['max_live_tests']}"
                        removed_by_test_count += 1
                        levels_to_remove.append((level, remove_reason))
                    
                    # 2. Проверка возраста уровня (старше 48 часов)
                    elif level.created_at:
                        age_hours = (now - level.created_at).total_seconds() / 3600
                        if age_hours > max_age_hours:
                            should_remove = True
                            remove_reason = f"возраст {age_hours:.1f}ч > {max_age_hours}ч"
                            removed_by_age += 1
                            levels_to_remove.append((level, remove_reason))
                    
                    # Если уровень еще не помечен на удаление, добавляем в группу для проверки пробития
                    if not should_remove:
                        if pair_symbol not in levels_by_pair:
                            levels_by_pair[pair_symbol] = []
                        levels_by_pair[pair_symbol].append(level)
                
                # Удаляем уровни, которые уже помечены на удаление (test_count или возраст)
                for level, reason in levels_to_remove:
                    pair_symbol = level.pair.symbol if level.pair else "UNKNOWN"
                    logger.info(f"🗑️ Удаление неактуального уровня {pair_symbol} @ {level.price}: {reason}")
                    session.delete(level)
                    removed_count += 1
                
                # Проверяем пробитие и расстояние от цены для оставшихся уровней (группируем по парам)
                max_distance_pct = 5.0  # Удаляем уровни дальше 5% от текущей цены
                removed_by_distance = 0
                
                for pair_symbol, levels in levels_by_pair.items():
                    if not levels:
                        continue
                    
                    try:
                        # Получаем свечи один раз для всех уровней пары
                        candles_15m = await self.fetch_ohlcv(pair_symbol, '15m', 20)
                        if not candles_15m or len(candles_15m) == 0:
                            continue
                        
                        current_price = candles_15m[-1]['close']
                        
                        # Проверяем каждый уровень на пробитие и расстояние
                        for level in levels:
                            level_dict = {
                                'price': level.price,
                                'type': level.level_type
                            }
                            
                            # Проверка пробития
                            if self.is_level_broken(level_dict, candles_15m, current_price):
                                logger.info(f"🗑️ Удаление пробитого уровня {pair_symbol} @ {level.price}")
                                session.delete(level)
                                removed_count += 1
                                removed_by_broken += 1
                                continue
                            
                            # Проверка расстояния от текущей цены
                            distance_pct = abs(level.price - current_price) / current_price * 100
                            if distance_pct > max_distance_pct:
                                logger.info(f"🗑️ Удаление уровня далеко от цены {pair_symbol} @ {level.price} (расстояние: {distance_pct:.2f}%)")
                                session.delete(level)
                                removed_count += 1
                                removed_by_distance += 1
                                
                    except Exception as e:
                        logger.warning(f"Ошибка проверки уровней для {pair_symbol}: {e}")
                        # Продолжаем проверку других пар даже при ошибке
                        continue
                
                session.commit()
                logger.info(f"✅ Очистка завершена: удалено {removed_count} уровней (test_count: {removed_by_test_count}, пробитые: {removed_by_broken}, устаревшие: {removed_by_age}, далеко от цены: {removed_by_distance})")
                
                # Очищаем кэш уровней
                from core.cache import cache
                cache.delete('levels:all')
                cache.delete('signals:all')  # Также очищаем кэш сигналов, так как уровни изменились
                
                return {
                    'status': 'success',
                    'removed_count': removed_count,
                    'removed_by_test_count': removed_by_test_count,
                    'removed_by_broken': removed_by_broken,
                    'removed_by_age': removed_by_age,
                    'removed_by_distance': removed_by_distance,
                    'total_checked': len(active_levels)
                }
                
            except Exception as e:
                session.rollback()
                logger.error(f"Ошибка очистки уровней: {e}")
                import traceback
                traceback.print_exc()
                return {'status': 'error', 'message': str(e)}
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Критическая ошибка очистки уровней: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}

    def _upsert_level_in_db(self, pair_symbol: str, level: Dict, timeframe: str = '15m', price_tolerance: float = 0.005) -> None:
        """Создает/обновляет активный уровень в БД, чтобы фронтенд видел актуальные уровни."""
        try:
            logger.info(f"🔄 _upsert_level_in_db вызван для {pair_symbol} @ {level.get('price')}")
            from core.database import init_database, SessionLocal
            from core.models import TradingPair, Level

            if not init_database():
                logger.error(f"❌ Не удалось инициализировать БД для {pair_symbol}")
                return

            session = SessionLocal()
            logger.info(f"✅ Сессия БД создана для {pair_symbol}")
            try:
                pair = session.query(TradingPair).filter_by(symbol=pair_symbol).first()
                if not pair:
                    return

                # Пытаемся найти существующий близкий уровень того же типа
                existing = session.query(Level).filter(
                    Level.pair_id == pair.id,
                    Level.level_type == (level.get('type') or level.get('level_type')),
                    Level.is_active == True
                ).all()

                target = None
                min_diff = None
                for lvl in existing:
                    diff = abs(lvl.price - level['price']) / max(level['price'], 1e-9)
                    if diff <= price_tolerance and (min_diff is None or diff < min_diff):
                        target = lvl
                        min_diff = diff

                if target is None:
                    # Создаем новый уровень
                    # ВАЖНО: время создания уровня - ТЕКУЩЕЕ время, а не время фрактала
                    # Фрактал может быть старым, но уровень создается СЕЙЧАС
                    level_time = datetime.now(timezone.utc)
                    
                    # Время первого касания (first_touch) берем из фрактала для информации
                    first_touch_time = None
                    if level.get('timestamp'):
                        # timestamp может быть в миллисекундах или секундах
                        ts = level['timestamp']
                        if ts > 1e10:  # Если в миллисекундах
                            first_touch_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                        else:  # Если в секундах
                            first_touch_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                    elif level.get('created_at'):
                        # Парсим из ISO формата (это время фрактала, не создания уровня)
                        try:
                            first_touch_time = datetime.fromisoformat(level['created_at'].replace('Z', '+00:00'))
                            if first_touch_time.tzinfo is None:
                                first_touch_time = first_touch_time.replace(tzinfo=timezone.utc)
                        except:
                            first_touch_time = level_time
                    else:
                        first_touch_time = level_time
                    
                    target = Level(
                        pair_id=pair.id,
                        price=float(level['price']),
                        level_type=(level.get('type') or level.get('level_type') or 'support'),
                        timeframe=timeframe,
                        test_count=int(level.get('test_count', 1)),
                        strength=None,
                        is_active=True,
                        first_touch=first_touch_time,  # Время первого касания из фрактала
                        last_touch=None,
                        created_at=level_time,  # ТЕКУЩЕЕ время создания уровня (НЕ время фрактала!)
                        meta_data=level
                    )
                    session.add(target)
                    logger.info(f"✅ DB: Создан НОВЫЙ уровень {pair_symbol} @ {target.price} ({target.level_type}), created_at={level_time}, first_touch={first_touch_time}")
                else:
                    # Обновляем счетчики/метки
                    # ВАЖНО: если в level есть обновленный test_count, используем его
                    new_test_count = level.get('test_count')
                    if new_test_count is not None:
                        target.test_count = int(new_test_count)
                    else:
                        # Если test_count не передан, сохраняем текущее значение
                        target.test_count = target.test_count or 1
                    
                    # Обновляем last_touch если есть в level
                    if level.get('last_test'):
                        # last_test может быть в миллисекундах или datetime
                        last_test = level.get('last_test')
                        if isinstance(last_test, (int, float)):
                            if last_test > 1e10:  # миллисекунды
                                target.last_touch = datetime.fromtimestamp(last_test / 1000, tz=timezone.utc)
                            else:  # секунды
                                target.last_touch = datetime.fromtimestamp(last_test, tz=timezone.utc)
                        elif isinstance(last_test, str):
                            try:
                                target.last_touch = datetime.fromisoformat(last_test.replace('Z', '+00:00'))
                            except:
                                target.last_touch = datetime.now(timezone.utc)
                        elif hasattr(last_test, 'timestamp'):
                            target.last_touch = last_test
                    elif level.get('timestamp'):
                        # Используем timestamp как fallback
                        ts = level.get('timestamp')
                        if isinstance(ts, (int, float)):
                            if ts > 1e10:  # миллисекунды
                                ts_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                            else:  # секунды
                                ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                            target.last_touch = ts_dt
                    
                    # Обновляем метаданные
                    target.meta_data = level
                    target.updated_at = datetime.now(timezone.utc)

                session.commit()
                logger.info(f"✅ DB: upsert уровня {pair_symbol} @ {target.price} ({target.level_type}), ID={target.id}, created_at={target.created_at}")
            except Exception as e:
                session.rollback()
                logger.error(f"❌ Не удалось upsert уровня {pair_symbol} @ {level.get('price')}: {e}")
                import traceback
                traceback.print_exc()
            finally:
                session.close()
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в _upsert_level_in_db для {pair_symbol}: {e}")
            import traceback
            traceback.print_exc()

    async def fetch_ohlcv(self, pair: str, timeframe: str, limit: int = 100) -> List[Dict]:
        """
        Получает OHLCV данные из локального хранилища с fallback на API биржи
        
        Сначала пытается использовать ohlcv_store (локальное хранилище),
        если не получается - использует старый метод (прямой запрос к бирже)
        """
        try:
            # Пытаемся использовать локальное хранилище (ohlcv_store)
            try:
                from core.ohlcv_store import ohlcv_store
                
                # ohlcv_store.get_ohlcv() - синхронный метод, оборачиваем в asyncio.to_thread()
                candles = await asyncio.to_thread(
                    ohlcv_store.get_ohlcv,
                    pair,
                    timeframe,
                    limit
                )
                
                if candles and len(candles) > 0:
                    logger.debug(f"✅ Данные из ohlcv_store для {pair} {timeframe}: {len(candles)} свечей")
                    return candles
                else:
                    logger.warning(f"⚠️ ohlcv_store вернул пустой результат для {pair} {timeframe}, используем fallback")
            except Exception as ohlcv_error:
                logger.warning(f"⚠️ Ошибка ohlcv_store для {pair} {timeframe}: {ohlcv_error}, используем fallback")
            
            # Fallback: используем старый метод (прямой запрос к бирже)
            logger.debug(f"🔄 Fallback на прямой запрос к бирже для {pair} {timeframe}")
            ohlcv = self.exchange.fetch_ohlcv(pair, timeframe, limit=limit)
            return [
                {
                    'timestamp': candle[0],
                    'open': candle[1],
                    'high': candle[2],
                    'low': candle[3],
                    'close': candle[4],
                    'volume': candle[5]
                }
                for candle in ohlcv
            ]
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных для {pair} {timeframe}: {e}")
            return []
    
    def calculate_sma(self, prices: List[float], period: int) -> List[float]:
        """Вычисляет простое скользящее среднее"""
        if len(prices) < period:
            return []
        
        sma = []
        for i in range(period - 1, len(prices)):
            sma.append(sum(prices[i-period+1:i+1]) / period)
        return sma
    
    def determine_trend_1h(self, candles: List[Dict]) -> str:
        """Определяет тренд на 1H таймфрейме с помощью EMA20, EMA50 и ADX, возвращает направление и силу тренда (с debug-print)"""
        if len(candles) < 50:
            print('Not enough candles for trend')
            return "UNKNOWN"
        closes = [candle['close'] for candle in candles]
        ema20 = pd.Series(closes).ewm(span=20).mean()
        ema50 = pd.Series(closes).ewm(span=50).mean()
        try:
            import ta
            df = pd.DataFrame(candles)
            adx = ta.trend.adx(df['high'], df['low'], df['close'], window=14).iloc[-1]
        except Exception as e:
            print('TA-Lib/ta error:', e)
            adx = 0
        if ema20.iloc[-1] > ema50.iloc[-1]:
            direction = 'UP'
        elif ema20.iloc[-1] < ema50.iloc[-1]:
            direction = 'DOWN'
        else:
            direction = 'SIDEWAYS'
        if adx >= 25 and abs(ema20.iloc[-1] - ema50.iloc[-1]) / ema50.iloc[-1] > 0.01:
            strength = 'STRONG'
        elif adx >= 15:
            strength = 'WEAK'
        else:
            strength = 'SIDEWAYS'
        print(f"[TREND] EMA20={ema20.iloc[-1]:.2f}, EMA50={ema50.iloc[-1]:.2f}, ADX={adx:.2f}, direction={direction}, strength={strength}")
        return f"{direction}_{strength}"
    
    def should_block_signal_by_filters(
        self,
        level: Dict,
        trend_1h: str,
        timeframe: str, 
        price_distance_pct: float,
        test_count: int,
        signal_type: str = None,
        triangle: Optional[Dict] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Проверяет, должен ли сигнал быть заблокирован на основе фильтров.
        Возвращает (should_block, reason).
        На основе анализа от 10.12.2024.
        Теперь учитывает активные треугольники.
        """
        from core.config import settings, get_timeframe_min_score
        
        level_score = level.get('score', 0) or 0
        
        # Этап 1.1: Фильтр по минимальному level_score
        min_score = get_timeframe_min_score(timeframe)
        if level_score < min_score:
            return True, f"level_score={level_score:.1f} < {min_score} (таймфрейм {timeframe})"
        
        # Этап 1.2: Исключение боковых трендов
        if settings.SIGNAL_FILTER_BLOCK_SIDEWAYS:
            if trend_1h and ('SIDEWAYS' in trend_1h or trend_1h.startswith('SIDEWAYS')):
                return True, f"боковой тренд ({trend_1h}) - 0% winrate"
        
        # НОВЫЙ: Фильтр на основе треугольника
        if triangle and signal_type:
            pattern_type = triangle.get('pattern_type', '')
            direction = triangle.get('direction', '')
            is_confirmed = triangle.get('is_confirmed', False)
            
            # Если треугольник пробит, не блокируем (может быть продолжение движения)
            if is_confirmed:
                pass  # Не блокируем пробитые треугольники
            else:
                # Восходящий треугольник (bullish) - приоритет лонгам
                if pattern_type == 'ascending_triangle' and direction == 'bullish':
                    if signal_type == 'SHORT':
                        # Блокируем шорты в восходящем треугольнике (кроме очень сильных уровней)
                        if level_score < 50:  # Только очень сильные уровни могут перевесить
                            return True, f"восходящий треугольник - приоритет лонгам (level_score={level_score:.1f})"
                
                # Нисходящий треугольник (bearish) - приоритет шортам
                elif pattern_type == 'descending_triangle' and direction == 'bearish':
                    if signal_type == 'LONG':
                        # Блокируем лонги в нисходящем треугольнике (кроме очень сильных уровней)
                        if level_score < 50:
                            return True, f"нисходящий треугольник - приоритет шортам (level_score={level_score:.1f})"
                
                # Симметричный треугольник - нейтральный, не блокируем
        
        # Этап 4: Фильтр по расстоянию до уровня
        if price_distance_pct > settings.SIGNAL_FILTER_MAX_DISTANCE_PCT:
            return True, f"расстояние {price_distance_pct:.2f}% > {settings.SIGNAL_FILTER_MAX_DISTANCE_PCT}%"
        
        # Этап 5: Фильтр по количеству тестов
        if test_count > settings.SIGNAL_FILTER_MAX_TEST_COUNT:
            return True, f"слишком много тестов ({test_count} > {settings.SIGNAL_FILTER_MAX_TEST_COUNT})"
        
        return False, None
    
    def calculate_signal_priority(
        self, 
        trend_1h: str, 
        level_score: float, 
        timeframe: str
    ) -> int:
        """
        Рассчитывает приоритет сигнала на основе лучших комбинаций из анализа.
        Возвращает приоритет (чем выше, тем лучше).
        """
        from core.config import settings
        
        if not settings.SIGNAL_FILTER_ENABLE_PRIORITY:
            return 0
        
        priority = 0
        
        # Бонусы за лучшие комбинации (на основе анализа)
        if timeframe == '1h' and trend_1h == 'UP_STRONG':
            priority += 10  # Максимальный приоритет (83.3% winrate)
        elif timeframe == '1h' and trend_1h == 'DOWN_STRONG':
            priority += 8  # 62.5% winrate
        elif trend_1h == 'DOWN_WEAK' and 30 <= level_score < 40:
            priority += 7  # 60% winrate
        elif trend_1h == 'UP_STRONG' and level_score < 30:
            priority += 6  # 75% winrate
        
        # Штрафы за худшие комбинации
        if timeframe == '15m' and trend_1h == 'UP_STRONG':
            priority -= 5  # 10% winrate - сильно снижаем приоритет
        elif trend_1h == 'UP_WEAK' and level_score < 30:
            priority -= 4  # 10.5% winrate
        
        return priority
    
    def get_pair_trend_4h(self, candles_4h: List[Dict]) -> Dict[str, Any]:
        """
        Рассчитывает тренд пары на 4H таймфрейме (Экран 1).
        Возвращает словарь с трендом, EMA20, EMA50, ADX и силой тренда.
        """
        if not candles_4h or len(candles_4h) < 50:
            return {
                "trend": "UNKNOWN",
                "ema20": None,
                "ema50": None,
                "adx": None,
                "strength": "UNKNOWN",
                "confidence": 0.0,
                "error": "Недостаточно свечей для расчета тренда"
            }
        
        try:
            closes = [candle['close'] for candle in candles_4h]
            ema20_series = pd.Series(closes).ewm(span=20).mean()
            ema50_series = pd.Series(closes).ewm(span=50).mean()
            
            ema20 = float(ema20_series.iloc[-1])
            ema50 = float(ema50_series.iloc[-1])
            
            # Рассчитываем ADX
            try:
                import ta
                df = pd.DataFrame(candles_4h)
                adx_series = ta.trend.adx(df['high'], df['low'], df['close'], window=14)
                adx = float(adx_series.iloc[-1]) if not adx_series.empty else 0.0
            except Exception as e:
                logger.warning(f"Ошибка расчета ADX: {e}")
                adx = 0.0
            
            # Определяем направление тренда
            ema_diff_pct = abs(ema20 - ema50) / ema50 if ema50 > 0 else 0.0
            if ema_diff_pct < 0.005:  # Разница < 0.5% = боковой тренд
                trend = "SIDEWAYS"
            elif ema20 > ema50:
                trend = "UP"
            else:
                trend = "DOWN"
            
            # Определяем силу тренда
            if adx >= 25 and ema_diff_pct > 0.01:
                strength = "STRONG"
                confidence = min(1.0, (adx / 50.0) * 0.8 + (ema_diff_pct / 0.05) * 0.2)
            elif adx >= 15:
                strength = "WEAK"
                confidence = min(0.7, (adx / 25.0) * 0.5 + (ema_diff_pct / 0.02) * 0.5)
            else:
                strength = "SIDEWAYS"
                confidence = 0.3
            
            return {
                "trend": trend,
                "ema20": ema20,
                "ema50": ema50,
                "adx": adx,
                "strength": strength,
                "confidence": confidence,
                "error": None
            }
        except Exception as e:
            logger.error(f"Ошибка расчета тренда пары на 4H: {e}")
            return {
                "trend": "UNKNOWN",
                "ema20": None,
                "ema50": None,
                "adx": None,
                "strength": "UNKNOWN",
                "confidence": 0.0,
                "error": str(e)
            }
    
    def check_price_approach_direction(self, level_type: str, level_price: float, current_price: float, candles_1h: List[Dict]) -> Tuple[bool, str, Dict]:
        """
        Проверяет направление подхода цены к уровню (Экран 2).
        Блокирует входы в пробой.
        
        Returns:
            (is_valid, reason, details)
        """
        if not candles_1h or len(candles_1h) < 3:
            return False, "Недостаточно свечей для проверки направления подхода", {}
        
        try:
            # Проверяем текущую цену относительно уровня
            price_diff_pct = abs(current_price - level_price) / level_price if level_price > 0 else 0.0
            
            # Проверяем последние 3-5 свечей для определения направления подхода
            recent_candles = candles_1h[-5:] if len(candles_1h) >= 5 else candles_1h
            
            if level_type == 'support':
                # Для LONG на support: цена должна быть ВЫШЕ уровня (подход сверху для отскока)
                BREAKOUT_THRESHOLD_PCT = 0.01  # 1% - значительный пробой
                
                # КРИТИЧЕСКАЯ ПРОВЕРКА: если цена пробила поддержку вниз на >1%, блокируем ВСЕГДА
                if current_price < level_price * (1 - BREAKOUT_THRESHOLD_PCT):  # Пробой вниз на >1%
                    return False, f"Пробой поддержки вниз: цена {current_price:.4f} ниже уровня {level_price:.4f} (-{price_diff_pct*100:.2f}%). Уровень пробит, LONG сигнал недействителен.", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "price_diff_pct": price_diff_pct * 100,
                        "direction": "BREAKOUT_DOWN"
                    }
                
                # Проверяем историю свечей для определения направления подхода
                candles_below = sum(1 for c in recent_candles if c['close'] < level_price)
                candles_above = sum(1 for c in recent_candles if c['close'] > level_price)
                
                # Если большинство свечей ниже уровня поддержки → уровень пробит вниз → блокировка
                if candles_below >= len(recent_candles) * 0.6:  # Большинство свечей ниже уровня
                    return False, f"Уровень поддержки пробит вниз: большинство свечей ({candles_below}/{len(recent_candles)}) ниже уровня. LONG сигнал недействителен.", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "candles_below": candles_below,
                        "candles_above": candles_above,
                        "total_candles": len(recent_candles),
                        "direction": "BREAKOUT_DOWN"
                    }
                
                # Если пробой небольшой (<1%), разрешаем только если большинство свечей выше уровня
                if price_diff_pct < BREAKOUT_THRESHOLD_PCT:
                    if candles_above >= len(recent_candles) * 0.6:  # Большинство свечей выше уровня
                        return True, f"Небольшой пробой поддержки (<1%): большинство свечей выше уровня, разрешаем отскок", {
                            "level_type": level_type,
                            "level_price": level_price,
                            "current_price": current_price,
                            "price_diff_pct": price_diff_pct * 100,
                            "direction": "SMALL_BREAKOUT_DOWN_ALLOWED",
                            "candles_above": candles_above,
                            "candles_below": candles_below,
                            "total_candles": len(recent_candles)
                        }
                    else:
                        return False, f"Небольшой пробой поддержки, но большинство свечей ниже уровня: только {candles_above}/{len(recent_candles)} свечей выше", {
                            "level_type": level_type,
                            "level_price": level_price,
                            "current_price": current_price,
                            "price_diff_pct": price_diff_pct * 100,
                            "direction": "BREAKOUT_DOWN",
                            "candles_above": candles_above,
                            "candles_below": candles_below,
                            "total_candles": len(recent_candles)
                        }
                
                # Правильный подход: цена выше уровня и большинство свечей выше уровня (подход сверху для отскока)
                if current_price > level_price and candles_above >= len(recent_candles) * 0.4:  # Хотя бы 40% свечей выше уровня
                    return True, f"Правильный подход к поддержке: цена {current_price:.4f} выше уровня {level_price:.4f} (подход сверху для отскока)", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "price_diff_pct": price_diff_pct * 100,
                        "direction": "APPROACH_FROM_ABOVE",
                        "candles_above": candles_above,
                        "candles_below": candles_below,
                        "total_candles": len(recent_candles)
                    }
                else:
                    return False, f"Некорректный подход к поддержке: цена {current_price:.4f}, только {candles_above}/{len(recent_candles)} свечей выше уровня", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "candles_above": candles_above,
                        "candles_below": candles_below,
                        "total_candles": len(recent_candles)
                    }
            
            else:  # resistance
                # Для SHORT на resistance: цена должна быть НИЖЕ уровня (подход снизу для отскока)
                BREAKOUT_THRESHOLD_PCT = 0.01  # 1% - значительный пробой
                
                # КРИТИЧЕСКАЯ ПРОВЕРКА: если цена пробила сопротивление вверх на >1%, блокируем ВСЕГДА
                if current_price > level_price * (1 + BREAKOUT_THRESHOLD_PCT):  # Пробой вверх на >1%
                    return False, f"Пробой сопротивления вверх: цена {current_price:.4f} выше уровня {level_price:.4f} (+{price_diff_pct*100:.2f}%). Уровень пробит, SHORT сигнал недействителен.", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "price_diff_pct": price_diff_pct * 100,
                        "direction": "BREAKOUT_UP"
                    }
                
                # Проверяем историю свечей для определения направления подхода
                candles_above = sum(1 for c in recent_candles if c['close'] > level_price)
                candles_below = sum(1 for c in recent_candles if c['close'] < level_price)
                
                # Если большинство свечей выше уровня сопротивления → уровень пробит вверх → блокировка
                if candles_above >= len(recent_candles) * 0.6:  # Большинство свечей выше уровня
                    return False, f"Уровень сопротивления пробит вверх: большинство свечей ({candles_above}/{len(recent_candles)}) выше уровня. SHORT сигнал недействителен.", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "candles_above": candles_above,
                        "candles_below": candles_below,
                        "total_candles": len(recent_candles),
                        "direction": "BREAKOUT_UP"
                    }
                
                # Если пробой небольшой (<1%), разрешаем только если большинство свечей ниже уровня
                if price_diff_pct < BREAKOUT_THRESHOLD_PCT:
                    if candles_below >= len(recent_candles) * 0.6:  # Большинство свечей ниже уровня
                        return True, f"Небольшой пробой сопротивления (<1%): большинство свечей ниже уровня, разрешаем отскок", {
                            "level_type": level_type,
                            "level_price": level_price,
                            "current_price": current_price,
                            "price_diff_pct": price_diff_pct * 100,
                            "direction": "SMALL_BREAKOUT_UP_ALLOWED",
                            "candles_above": candles_above,
                            "candles_below": candles_below,
                            "total_candles": len(recent_candles)
                        }
                    else:
                        return False, f"Небольшой пробой сопротивления, но большинство свечей выше уровня: только {candles_below}/{len(recent_candles)} свечей ниже", {
                            "level_type": level_type,
                            "level_price": level_price,
                            "current_price": current_price,
                            "price_diff_pct": price_diff_pct * 100,
                            "direction": "BREAKOUT_UP",
                            "candles_above": candles_above,
                            "candles_below": candles_below,
                            "total_candles": len(recent_candles)
                        }
                
                # Правильный подход: цена ниже уровня и большинство свечей ниже уровня (подход снизу для отскока)
                if current_price < level_price and candles_below >= len(recent_candles) * 0.4:  # Хотя бы 40% свечей ниже уровня
                    return True, f"Правильный подход к сопротивлению: цена {current_price:.4f} ниже уровня {level_price:.4f} (подход снизу для отскока)", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "price_diff_pct": price_diff_pct * 100,
                        "direction": "APPROACH_FROM_BELOW",
                        "candles_above": candles_above,
                        "candles_below": candles_below,
                        "total_candles": len(recent_candles)
                    }
                else:
                    return False, f"Некорректный подход к сопротивлению: цена {current_price:.4f}, только {candles_below}/{len(recent_candles)} свечей ниже уровня", {
                        "level_type": level_type,
                        "level_price": level_price,
                        "current_price": current_price,
                        "candles_above": candles_above,
                        "candles_below": candles_below,
                        "total_candles": len(recent_candles)
                    }
        
        except Exception as e:
            logger.error(f"Ошибка проверки направления подхода: {e}")
            return False, f"Ошибка проверки: {str(e)}", {"error": str(e)}
    
    def calculate_oscillators(self, candles_1h: List[Dict]) -> Dict[str, Any]:
        """
        Рассчитывает осцилляторы RSI и MACD на 1H таймфрейме (Экран 2).
        Возвращает словарь с RSI, MACD, Signal и Histogram.
        """
        if not candles_1h or len(candles_1h) < 26:  # Нужно минимум 26 свечей для MACD (26 + signal 9)
            return {
                "rsi": None,
                "macd": None,
                "macd_signal": None,
                "macd_histogram": None,
                "error": "Недостаточно свечей для расчета осцилляторов (нужно минимум 26)"
            }
        
        try:
            # Преобразуем в DataFrame
            df = pd.DataFrame(candles_1h)
            closes = df['close']
            
            # ========== RSI (Relative Strength Index) ==========
            # Период RSI: 14 (стандарт Элдера)
            rsi_period = 14
            if len(closes) < rsi_period + 1:
                rsi = None
            else:
                delta = closes.diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=rsi_period, min_periods=1).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_period, min_periods=1).mean()
                
                # Избегаем деления на ноль - используем метод Уайлдера (Wilder's smoothing)
                # Если loss = 0 и gain > 0, то RSI = 100 (только рост)
                # Если gain = 0 и loss > 0, то RSI = 0 (только падение)
                # Если gain = 0 и loss = 0, то RSI = 50 (нейтральный, цена не меняется)
                rs = gain / loss.replace(0, np.nan)
                
                # Правильная обработка edge cases
                last_gain = gain.iloc[-1] if len(gain) > 0 else 0
                last_loss = loss.iloc[-1] if len(loss) > 0 else 0
                
                if last_loss == 0 and last_gain > 0:
                    # Только рост - RSI = 100
                    rsi = 100.0
                elif last_gain == 0 and last_loss > 0:
                    # Только падение - RSI = 0
                    rsi = 0.0
                elif last_gain == 0 and last_loss == 0:
                    # Цена не меняется - RSI = 50 (нейтральный)
                    rsi = 50.0
                else:
                    # Нормальный расчет
                    rs = rs.fillna(float('inf') if last_gain > 0 else 0)
                    rsi_calc = 100 - (100 / (1 + rs))
                    rsi = float(rsi_calc.iloc[-1]) if not pd.isna(rsi_calc.iloc[-1]) else 50.0
            
            # ========== MACD (Moving Average Convergence Divergence) ==========
            # Параметры MACD по Элдеру: Fast=12, Slow=26, Signal=9
            macd_fast = 12
            macd_slow = 26
            macd_signal_period = 9
            
            if len(closes) < macd_slow + macd_signal_period:
                macd = None
                macd_signal = None
                macd_histogram = None
            else:
                # Быстрая EMA
                ema_fast = closes.ewm(span=macd_fast, adjust=False).mean()
                # Медленная EMA
                ema_slow = closes.ewm(span=macd_slow, adjust=False).mean()
                # MACD линия = разница между быстрой и медленной EMA
                macd_line = ema_fast - ema_slow
                # Сигнальная линия = EMA от MACD линии
                macd_signal_line = macd_line.ewm(span=macd_signal_period, adjust=False).mean()
                # Гистограмма = разница между MACD и Signal
                macd_histogram = macd_line - macd_signal_line
                
                # Берем последние значения
                macd = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else None
                macd_signal = float(macd_signal_line.iloc[-1]) if not pd.isna(macd_signal_line.iloc[-1]) else None
                macd_histogram = float(macd_histogram.iloc[-1]) if not pd.isna(macd_histogram.iloc[-1]) else None
            
            return {
                "rsi": rsi,
                "macd": macd,
                "macd_signal": macd_signal,
                "macd_histogram": macd_histogram,
                "error": None
            }
        
        except Exception as e:
            logger.error(f"Ошибка расчета осцилляторов: {e}")
            return {
                "rsi": None,
                "macd": None,
                "macd_signal": None,
                "macd_histogram": None,
                "error": str(e)
            }
    
    async def get_btc_market_trend_4h(self) -> str:
        """
        Получает тренд BTC/USDT на 4H таймфрейме (Экран 1).
        Возвращает: 'UP', 'DOWN', 'SIDEWAYS'
        """
        try:
            candles_4h = await self.fetch_ohlcv('BTC/USDT', '4h', 200)
            if not candles_4h or len(candles_4h) < 50:
                logger.warning("Недостаточно свечей BTC для определения тренда на 4H")
                return 'SIDEWAYS'
            
            trend_data = self.get_pair_trend_4h(candles_4h)
            return trend_data.get('trend', 'SIDEWAYS')
        except Exception as e:
            logger.error(f"Ошибка получения BTC тренда на 4H: {e}")
            return 'SIDEWAYS'  # При ошибке разрешаем все сигналы
    
    async def check_elder_screens(
        self,
        pair: str,
        signal_type: str,
        level: Dict,
        current_price: float,
        candles_4h: List[Dict],
        candles_1h: List[Dict],
        level_score: float = None
    ) -> Tuple[bool, Dict]:
        """
        Проверяет все экраны Элдера перед генерацией сигнала.
        
        Returns:
            (passed, details) - passed=True если все экраны пройдены, details содержит детали проверок
        """
        details = {
            "screen_1": {"passed": False, "blocked_reason": None, "checks": {}},
            "screen_2": {"passed": False, "blocked_reason": None, "checks": {}},
            "screen_3": {"passed": True, "blocked_reason": None, "checks": {}},  # Экран 3 проверяется позже
            "final_decision": "BLOCKED"
        }
        
        level_price = level['price']
        level_type = level['type']
        level_score = level_score or level.get('score', 0)
        
        # ========== ЭКРАН 1: 4H - ДОЛГОСРОЧНЫЙ ТРЕНД ==========
        screen_1_passed = False  # ИЗМЕНЕНО: по умолчанию НЕ пройден
        screen_1_reason = None
        screen_1_data_available = False  # Флаг наличия данных
        
        try:
            # 1.1. Проверка BTC тренда
            btc_trend = await self.get_btc_market_trend_4h()
            details["screen_1"]["checks"]["btc_trend"] = btc_trend
            # Сохраняем BTC тренд в screen_1 для отображения в UI
            details["screen_1"]["btc_trend"] = btc_trend
            
            # Получаем полные данные BTC для анализа SIDEWAYS
            btc_trend_data = None
            if btc_trend == 'SIDEWAYS':
                try:
                    btc_candles_4h = await self.fetch_ohlcv('BTC/USDT', '4h', 200)
                    if btc_candles_4h and len(btc_candles_4h) >= 50:
                        btc_trend_data = self.get_pair_trend_4h(btc_candles_4h)
                        details["screen_1"]["checks"]["btc_trend_data"] = btc_trend_data
                        # Сохраняем детальные данные BTC для отображения в UI
                        details["screen_1"]["btc_trend_data"] = btc_trend_data
                except Exception as e:
                    logger.warning(f"Не удалось получить данные BTC для анализа SIDEWAYS: {e}")
            
            if btc_trend and btc_trend != 'UNKNOWN':
                screen_1_data_available = True
                btc_allows = False
                # СМЯГЧЕННЫЕ УСЛОВИЯ: снижаем порог level_score с 60 до 30
                if btc_trend == 'UP':
                    btc_allows = signal_type == 'LONG' or (signal_type == 'SHORT' and level_score > 30)
                elif btc_trend == 'DOWN':
                    btc_allows = signal_type == 'SHORT' or (signal_type == 'LONG' and level_score > 30)
                else:  # SIDEWAYS - НОВАЯ ЛОГИКА (Вариант 2: ADX >= 20)
                    # При SIDEWAYS используем направление EMA и ADX
                    if btc_trend_data:
                        btc_adx = btc_trend_data.get('adx')
                        btc_ema20 = btc_trend_data.get('ema20')
                        btc_ema50 = btc_trend_data.get('ema50')
                        
                        if btc_adx is not None and btc_adx < 20:
                            # Слабый тренд (ADX < 20) - блокируем все сигналы
                            btc_allows = False
                            screen_1_reason = f"BTC тренд SIDEWAYS с ADX={btc_adx:.1f} < 20 (слабый тренд) - блокируем все сигналы"
                        elif btc_ema20 is not None and btc_ema50 is not None:
                            # Используем направление EMA
                            if signal_type == 'LONG':
                                # LONG разрешен только если EMA20 > EMA50 (восходящий тренд)
                                btc_allows = btc_ema20 > btc_ema50
                                if btc_allows:
                                    # Явное сообщение о разрешении LONG при SIDEWAYS
                                    screen_1_reason = f"BTC тренд SIDEWAYS: LONG разрешен (EMA20={btc_ema20:.2f} > EMA50={btc_ema50:.2f}, ADX={btc_adx:.1f} >= 20)"
                                    details["screen_1"]["checks"]["btc_sideways_long_allowed"] = True
                                    details["screen_1"]["checks"]["btc_sideways_reason"] = screen_1_reason
                                else:
                                    screen_1_reason = f"BTC тренд SIDEWAYS: LONG заблокирован (EMA20={btc_ema20:.2f} <= EMA50={btc_ema50:.2f})"
                            else:  # SHORT
                                # SHORT разрешен только если EMA20 < EMA50 (нисходящий тренд)
                                btc_allows = btc_ema20 < btc_ema50
                                if btc_allows:
                                    # Явное сообщение о разрешении SHORT при SIDEWAYS
                                    screen_1_reason = f"BTC тренд SIDEWAYS: SHORT разрешен (EMA20={btc_ema20:.2f} < EMA50={btc_ema50:.2f}, ADX={btc_adx:.1f} >= 20)"
                                    details["screen_1"]["checks"]["btc_sideways_short_allowed"] = True
                                    details["screen_1"]["checks"]["btc_sideways_reason"] = screen_1_reason
                                else:
                                    screen_1_reason = f"BTC тренд SIDEWAYS: SHORT заблокирован (EMA20={btc_ema20:.2f} >= EMA50={btc_ema50:.2f})"
                        else:
                            # Нет данных EMA - блокируем
                            btc_allows = False
                            screen_1_reason = "BTC тренд SIDEWAYS: нет данных EMA для анализа"
                    else:
                        # Не удалось получить данные BTC - блокируем для безопасности
                        btc_allows = False
                        screen_1_reason = "BTC тренд SIDEWAYS: не удалось получить данные для анализа"
                
                if not btc_allows:
                    screen_1_passed = False
                    # Сохраняем детальную причину блокировки, если она уже установлена (для SIDEWAYS)
                    if not screen_1_reason:
                        screen_1_reason = f"BTC тренд {btc_trend} блокирует {signal_type} сигналы (level_score={level_score:.1f})"
                    details["screen_1"]["checks"]["btc_blocked"] = True
                    details["screen_1"]["blocked_reason"] = screen_1_reason
                else:
                    details["screen_1"]["checks"]["btc_blocked"] = False
                    screen_1_passed = True  # BTC тренд разрешает
            else:
                # BTC тренд не рассчитан
                screen_1_passed = False
                screen_1_reason = "BTC тренд не рассчитан (недостаточно данных)"
                details["screen_1"]["checks"]["btc_trend"] = None
                details["screen_1"]["checks"]["btc_blocked"] = True
            
            # 1.2. Проверка тренда пары на 4H
            if candles_4h and len(candles_4h) >= 50:
                screen_1_data_available = True
                pair_trend_data = self.get_pair_trend_4h(candles_4h)
                details["screen_1"]["checks"]["pair_trend"] = pair_trend_data
                # Сохраняем тренд пары в screen_1 для отображения в UI
                details["screen_1"]["pair_trend"] = pair_trend_data.get('trend', 'UNKNOWN')
                details["screen_1"]["pair_trend_data"] = pair_trend_data
                
                pair_trend = pair_trend_data.get('trend', 'UNKNOWN')
                pair_allows = False
                
                # РАЗРЕШЕН SIDEWAYS для 4H: разрешаем сигналы при боковом тренде
                if pair_trend == 'SIDEWAYS':
                    pair_allows = True  # Разрешаем SIDEWAYS для обоих типов сигналов
                elif signal_type == 'LONG':
                    # Для LONG: EMA20 > EMA50 (восходящий тренд)
                    # Разрешаем если: тренд UP, или тренд не DOWN и level_score > 30
                    pair_allows = (pair_trend == 'UP' or 
                                 (pair_trend != 'DOWN' and level_score > 30))
                else:  # SHORT
                    # Для SHORT: EMA20 < EMA50 (нисходящий тренд)
                    # Разрешаем если: тренд DOWN, или тренд не UP и level_score > 30
                    pair_allows = (pair_trend == 'DOWN' or 
                                 (pair_trend != 'UP' and level_score > 30))
                
                if not pair_allows:
                    # Тренд пары блокирует - экран НЕ пройден (независимо от BTC)
                    screen_1_passed = False
                    if screen_1_reason:
                        screen_1_reason += f"; Тренд пары {pair_trend} блокирует {signal_type}"
                    else:
                        screen_1_reason = f"Тренд пары {pair_trend} блокирует {signal_type} сигналы (level_score={level_score:.1f})"
                    details["screen_1"]["checks"]["pair_blocked"] = True
                else:
                    # Тренд пары разрешает - экран ПРОЙДЕН
                    # Приоритет: тренд пары важнее для конкретной пары, чем общий BTC тренд
                    details["screen_1"]["checks"]["pair_blocked"] = False
                    screen_1_passed = True
                    if details["screen_1"]["checks"].get("btc_blocked"):
                        # Добавляем примечание, что BTC блокирует, но тренд пары разрешает
                        logger.info(f"[{pair}] Тренд пары {pair_trend} разрешает {signal_type}, несмотря на блокировку BTC")
            else:
                # Если нет данных 4H, экран НЕ пройден
                details["screen_1"]["checks"]["pair_trend"] = {"error": "Недостаточно данных 4H"}
                logger.warning(f"[{pair}] Недостаточно свечей 4H для проверки тренда пары")
                if not screen_1_reason:
                    screen_1_reason = "Недостаточно данных 4H для проверки тренда пары"
                screen_1_passed = False
        
        except Exception as e:
            logger.error(f"[{pair}] Ошибка проверки Экран 1: {e}")
            # ИЗМЕНЕНО: При ошибке экран НЕ пройден
            screen_1_passed = False
            screen_1_reason = f"Ошибка проверки Экран 1: {str(e)}"
            details["screen_1"]["checks"]["error"] = str(e)
        
        details["screen_1"]["passed"] = screen_1_passed
        
        # Убеждаемся, что blocked_reason всегда установлен, даже если явная причина не найдена
        if not screen_1_passed:
            # Формируем причину из доступных данных (даже если screen_1_reason уже установлен, дополняем его)
            blocked_parts = []
            checks = details["screen_1"].get("checks", {})
            
            # Если screen_1_reason уже установлен, добавляем его в начало
            if screen_1_reason:
                blocked_parts.append(screen_1_reason)
            
            # Дополняем деталями из checks
            if checks.get("btc_blocked"):
                btc_trend = checks.get("btc_trend", "N/A")
                reason = f"BTC тренд {btc_trend} блокирует {signal_type} сигналы"
                if reason not in blocked_parts:
                    blocked_parts.append(reason)
            
            if checks.get("pair_blocked"):
                pair_trend = checks.get("pair_trend", {})
                if isinstance(pair_trend, dict):
                    trend = pair_trend.get("trend", "N/A")
                    reason = f"Тренд пары {trend} блокирует {signal_type} сигналы"
                else:
                    reason = f"Тренд пары блокирует {signal_type} сигналы"
                if reason not in blocked_parts:
                    blocked_parts.append(reason)
            
            if checks.get("error"):
                error_reason = f"Ошибка проверки: {checks.get('error')}"
                if error_reason not in blocked_parts:
                    blocked_parts.append(error_reason)
            
            # Если нет конкретных причин, формируем общую
            if not blocked_parts:
                btc_trend = checks.get("btc_trend", "N/A")
                pair_trend_info = checks.get("pair_trend", {})
                pair_trend = pair_trend_info.get("trend", "N/A") if isinstance(pair_trend_info, dict) else "N/A"
                screen_1_reason = f"Экран 1 не пройден: BTC тренд={btc_trend}, тренд пары={pair_trend}"
            else:
                screen_1_reason = "; ".join(blocked_parts)
        
        # Всегда устанавливаем blocked_reason (даже если экран пройден, для консистентности)
        # Если экран пройден и есть явное сообщение (например, для SIDEWAYS), сохраняем его
        if screen_1_passed and screen_1_reason and ("SIDEWAYS" in screen_1_reason or "разрешен" in screen_1_reason):
            details["screen_1"]["blocked_reason"] = None  # Не блокирован
            details["screen_1"]["passed_reason"] = screen_1_reason  # Сохраняем причину прохождения
        else:
            details["screen_1"]["blocked_reason"] = screen_1_reason if not screen_1_passed else None
        
        if not screen_1_passed:
            details["final_decision"] = "BLOCKED_SCREEN_1"
            return False, details
        
        # ========== ЭКРАН 2: 1H - СРЕДНЕСРОЧНЫЙ АНАЛИЗ ==========
        screen_2_passed = False  # ИЗМЕНЕНО: по умолчанию НЕ пройден
        screen_2_reason = None
        screen_2_data_available = False  # Флаг наличия данных
        
        try:
            # 2.1. Проверка направления подхода цены к уровню
            if candles_1h and len(candles_1h) >= 3:
                screen_2_data_available = True
                # СМЯГЧЕННЫЕ УСЛОВИЯ: если уровень очень близок к цене (<0.5%), разрешаем даже если направление не идеальное
                # НО: не применяем обход для пробоев >1% - это критическая ошибка
                price_diff_pct = abs(current_price - level_price) / level_price if level_price > 0 else 1.0
                is_very_close = price_diff_pct < 0.005  # 0.5%
                
                approach_valid, approach_reason, approach_details = self.check_price_approach_direction(
                    level_type, level_price, current_price, candles_1h
                )
                details["screen_2"]["checks"]["price_approach"] = approach_details
                
                # Проверяем, является ли это пробоем уровня (BREAKOUT_DOWN или BREAKOUT_UP)
                is_breakout = approach_details.get("direction") in ("BREAKOUT_DOWN", "BREAKOUT_UP")
                
                # Если уровень очень близок к цене И это не пробой, разрешаем даже при неидеальном направлении
                if not approach_valid and is_very_close and not is_breakout:
                    logger.warning(f"[{pair}] Уровень очень близок к цене ({price_diff_pct*100:.2f}%), разрешаем несмотря на неидеальное направление подхода")
                    approach_valid = True
                    approach_details["close_to_price_override"] = True
                elif not approach_valid and is_breakout:
                    # Пробой уровня - НЕ разрешаем обход, даже если уровень близок
                    logger.warning(f"[{pair}] Обнаружен пробой уровня ({approach_details.get('direction')}), блокируем сигнал независимо от расстояния")
                
                if not approach_valid:
                    screen_2_passed = False
                    screen_2_reason = approach_reason
                    details["screen_2"]["checks"]["approach_blocked"] = True
                else:
                    details["screen_2"]["checks"]["approach_blocked"] = False
                    screen_2_passed = True  # Направление подхода корректно
            else:
                # Если нет данных 1H, экран НЕ пройден
                details["screen_2"]["checks"]["price_approach"] = {"error": "Недостаточно данных 1H"}
                logger.warning(f"[{pair}] Недостаточно свечей 1H для проверки направления подхода")
                screen_2_passed = False
                screen_2_reason = "Недостаточно данных 1H для проверки направления подхода"
            
            # 2.2. Проверка осцилляторов (RSI и MACD)
            if candles_1h and len(candles_1h) >= 26:
                screen_2_data_available = True
                oscillators = self.calculate_oscillators(candles_1h)
                details["screen_2"]["checks"]["oscillators"] = oscillators
                
                if oscillators.get("error"):
                    # ИЗМЕНЕНО: При ошибке расчета экран НЕ пройден
                    logger.warning(f"[{pair}] Ошибка расчета осцилляторов: {oscillators.get('error')}")
                    details["screen_2"]["checks"]["oscillator_error"] = oscillators.get("error")
                    screen_2_passed = False
                    if screen_2_reason:
                        screen_2_reason += f"; Ошибка расчета осцилляторов: {oscillators.get('error')}"
                    else:
                        screen_2_reason = f"Ошибка расчета осцилляторов: {oscillators.get('error')}"
                else:
                    rsi = oscillators.get("rsi")
                    macd = oscillators.get("macd")
                    macd_signal = oscillators.get("macd_signal")
                    
                    # 2.2.1. Проверка RSI
                    if rsi is not None:
                        rsi_blocked = False
                        rsi_warning = False
                        
                        if signal_type == 'LONG':
                            # Для LONG: RSI < 70 (не перекуплен)
                            if rsi > 75:
                                rsi_blocked = True
                                screen_2_passed = False
                                if screen_2_reason:
                                    screen_2_reason += f"; RSI {rsi:.2f} > 75 (перекуплен)"
                                else:
                                    screen_2_reason = f"RSI {rsi:.2f} > 75 (перекуплен, блокировка LONG)"
                            elif rsi >= 70:
                                rsi_warning = True
                                logger.warning(f"[{pair}] RSI {rsi:.2f} в зоне перекупленности (70-75), но разрешаем")
                        else:  # SHORT
                            # Для SHORT: RSI > 30 (не перепродан)
                            if rsi < 25:
                                rsi_blocked = True
                                screen_2_passed = False
                                if screen_2_reason:
                                    screen_2_reason += f"; RSI {rsi:.2f} < 25 (перепродан)"
                                else:
                                    screen_2_reason = f"RSI {rsi:.2f} < 25 (перепродан, блокировка SHORT)"
                            elif rsi <= 30:
                                rsi_warning = True
                                logger.warning(f"[{pair}] RSI {rsi:.2f} в зоне перепроданности (25-30), но разрешаем")
                        
                        rsi_blocked_reason = None
                        if rsi_blocked:
                            threshold = 75 if signal_type == 'LONG' else 25
                            rsi_blocked_reason = f"RSI {rsi:.2f} {'<' if signal_type == 'SHORT' else '>'} {threshold} ({'перепродан' if signal_type == 'SHORT' else 'перекуплен'})"
                        
                        details["screen_2"]["checks"]["rsi"] = {
                            "value": rsi,
                            "blocked": rsi_blocked,
                            "warning": rsi_warning,
                            "threshold": 75 if signal_type == 'LONG' else 25,
                            "blocked_reason": rsi_blocked_reason
                        }
                    
                    # 2.2.2. Проверка MACD
                    if macd is not None and macd_signal is not None:
                        macd_blocked = False
                        macd_diff = macd - macd_signal

                        # Нейтральная зона: допускаем слабый/нулевой импульс
                        # Толеранс: 0.5% от Signal, но не меньше 0.0005
                        macd_tolerance = max(abs(macd_signal) * 0.005, 0.0005)
                        neutral_zone = abs(macd_diff) <= macd_tolerance
                        
                        if signal_type == 'LONG':
                            # Для LONG: ожидаем MACD > Signal; блокируем только явный медвежий импульс ниже порога
                            if macd < macd_signal - macd_tolerance:
                                macd_blocked = True
                                screen_2_passed = False
                                if screen_2_reason:
                                    screen_2_reason += f"; MACD {macd:.4f} < Signal {macd_signal:.4f} (медвежий, разница: {macd_diff:.4f}, допуск ±{macd_tolerance:.4f})"
                                else:
                                    screen_2_reason = f"MACD {macd:.4f} < Signal {macd_signal:.4f} (медвежий сигнал, блокировка LONG, разница: {macd_diff:.4f}, допуск ±{macd_tolerance:.4f})"
                        else:  # SHORT
                            # Для SHORT: ожидаем MACD < Signal; блокируем только явный бычий импульс выше порога
                            if macd > macd_signal + macd_tolerance:
                                macd_blocked = True
                                screen_2_passed = False
                                if screen_2_reason:
                                    screen_2_reason += f"; MACD {macd:.4f} > Signal {macd_signal:.4f} (бычий, разница: {macd_diff:.4f}, допуск ±{macd_tolerance:.4f})"
                                else:
                                    screen_2_reason = f"MACD {macd:.4f} > Signal {macd_signal:.4f} (бычий сигнал, блокировка SHORT, разница: {macd_diff:.4f}, допуск ±{macd_tolerance:.4f})"
                        
                        macd_blocked_reason = None
                        if macd_blocked:
                            expected = "MACD > Signal" if signal_type == 'LONG' else "MACD < Signal"
                            macd_blocked_reason = (
                                f"MACD {macd:.4f} не соответствует {expected} "
                                f"(Signal: {macd_signal:.4f}, разница: {macd_diff:.4f}, допуск ±{macd_tolerance:.4f})"
                            )
                        
                        details["screen_2"]["checks"]["macd"] = {
                            "macd": macd,
                            "signal": macd_signal,
                            "histogram": oscillators.get("macd_histogram"),
                            "diff": macd_diff,
                            "blocked": macd_blocked,
                            "expected": "MACD > Signal" if signal_type == 'LONG' else "MACD < Signal",
                            "blocked_reason": macd_blocked_reason,
                            "tolerance": macd_tolerance,
                            "neutral_zone": neutral_zone
                        }
            else:
                # Если нет данных 1H, экран НЕ пройден
                details["screen_2"]["checks"]["oscillators"] = {"error": "Недостаточно данных 1H для расчета осцилляторов"}
                logger.warning(f"[{pair}] Недостаточно свечей 1H для расчета осцилляторов (нужно минимум 26)")
                if not screen_2_reason:
                    screen_2_reason = "Недостаточно данных 1H для расчета осцилляторов (нужно минимум 26 свечей)"
                screen_2_passed = False
        
        except Exception as e:
            logger.error(f"[{pair}] Ошибка проверки Экран 2: {e}")
            # ИЗМЕНЕНО: При ошибке экран НЕ пройден
            screen_2_passed = False
            screen_2_reason = f"Ошибка проверки Экран 2: {str(e)}"
            details["screen_2"]["checks"]["error"] = str(e)
        
        details["screen_2"]["passed"] = screen_2_passed
        
        # Убеждаемся, что blocked_reason всегда установлен, даже если явная причина не найдена
        if not screen_2_passed:
            # Формируем причину из доступных данных (даже если screen_2_reason уже установлен, дополняем его)
            blocked_parts = []
            checks = details["screen_2"].get("checks", {})
            
            # Если screen_2_reason уже установлен, добавляем его в начало
            if screen_2_reason:
                blocked_parts.append(screen_2_reason)
            
            # Дополняем деталями из checks
            if checks.get("approach_blocked"):
                price_approach = checks.get("price_approach", {})
                direction = price_approach.get("direction", "N/A")
                price_diff_pct = price_approach.get("price_diff_pct", 0)
                if direction == "BREAKOUT_DOWN":
                    # Для LONG на support: пробой поддержки вниз
                    reason = f"Пробой поддержки вниз: цена {price_approach.get('current_price', 0):.4f} ниже уровня {price_approach.get('level_price', 0):.4f} (-{price_diff_pct:.2f}%). Уровень пробит, LONG сигнал недействителен."
                    if reason not in blocked_parts:
                        blocked_parts.append(reason)
                elif direction == "BREAKOUT_UP":
                    # Для SHORT на resistance: пробой сопротивления вверх
                    reason = f"Пробой сопротивления вверх: цена {price_approach.get('current_price', 0):.4f} выше уровня {price_approach.get('level_price', 0):.4f} (+{price_diff_pct:.2f}%). Уровень пробит, SHORT сигнал недействителен."
                    if reason not in blocked_parts:
                        blocked_parts.append(reason)
                else:
                    reason = f"Направление подхода некорректно: {direction}"
                    if reason not in blocked_parts:
                        blocked_parts.append(reason)
            
            rsi_check = checks.get("rsi", {})
            if rsi_check.get("blocked"):
                rsi_value = rsi_check.get("value", "N/A")
                threshold = rsi_check.get("threshold", "N/A")
                # Правильное форматирование: сначала проверяем тип, потом форматируем
                rsi_str = f"{rsi_value:.2f}" if isinstance(rsi_value, (int, float)) else str(rsi_value)
                rsi_reason = f"RSI {rsi_str} {'<' if signal_type == 'SHORT' else '>'} {threshold} ({'перепродан' if signal_type == 'SHORT' else 'перекуплен'})"
                # Добавляем только если еще не добавлено
                if rsi_reason not in blocked_parts:
                    blocked_parts.append(rsi_reason)
            
            macd_check = checks.get("macd", {})
            if macd_check.get("blocked"):
                macd_value = macd_check.get("macd", "N/A")
                signal_value = macd_check.get("signal", "N/A")
                expected = macd_check.get("expected", "N/A")
                # Правильное форматирование: сначала проверяем тип, потом форматируем
                macd_str = f"{macd_value:.4f}" if isinstance(macd_value, (int, float)) else str(macd_value)
                signal_str = f"{signal_value:.4f}" if isinstance(signal_value, (int, float)) else str(signal_value)
                macd_reason = f"MACD {macd_str} не соответствует {expected} (Signal: {signal_str})"
                # Добавляем только если еще не добавлено
                if macd_reason not in blocked_parts:
                    blocked_parts.append(macd_reason)
            
            # Проверяем другие возможные причины блокировки
            if checks.get("error"):
                error_reason = f"Ошибка проверки: {checks.get('error')}"
                if error_reason not in blocked_parts:
                    blocked_parts.append(error_reason)
            
            if checks.get("oscillator_error"):
                osc_error_reason = f"Ошибка расчета осцилляторов: {checks.get('oscillator_error')}"
                if osc_error_reason not in blocked_parts:
                    blocked_parts.append(osc_error_reason)
            
            # Если checks пустой или не содержит информации, но экран не пройден, формируем общую причину
            if not blocked_parts:
                # Пытаемся найти любую информацию о том, почему экран не пройден
                if not checks or len(checks) == 0:
                    screen_2_reason = "Экран 2 не пройден: проверки не были выполнены (недостаточно данных)"
                elif checks.get("price_approach", {}).get("error"):
                    screen_2_reason = f"Экран 2 не пройден: {checks['price_approach']['error']}"
                elif checks.get("oscillators", {}).get("error"):
                    screen_2_reason = f"Экран 2 не пройден: {checks['oscillators']['error']}"
                else:
                    screen_2_reason = "Экран 2 не пройден (детали проверок недоступны, проверьте логи системы)"
            else:
                screen_2_reason = "; ".join(blocked_parts)
        
        # Всегда устанавливаем blocked_reason (даже если экран пройден, для консистентности)
        details["screen_2"]["blocked_reason"] = screen_2_reason if not screen_2_passed else None
        
        if not screen_2_passed:
            details["final_decision"] = "BLOCKED_SCREEN_2"
            return False, details
        
        # Все экраны пройдены
        details["final_decision"] = "PASSED"
        return True, details
    
    def find_fractals(self, candles: List[Dict], lookback: int = 2, exclude_last_hours: int = 1) -> Tuple[List[Dict], List[Dict]]:
        """
        Находит фракталы (локальные минимумы и максимумы) с улучшенной логикой.
        
        Args:
            candles: Список свечей
            lookback: Количество свечей для проверки экстремума
            exclude_last_hours: Количество часов в конце, которые нужно исключить (по умолчанию 1 час)
                                Для 15m таймфрейма: 1 час = 4 свечи, для 1h = 1 свеча
        """
        if len(candles) < lookback * 2 + 1:
            return [], []
        
        # ВАЖНО: Исключаем последние свечи из анализа (не формируем уровни по последним данным)
        # Для 15m таймфрейма: 1 час = 4 свечи
        # Для 1h таймфрейма: 1 час = 1 свеча
        # Определяем таймфрейм на основе интервала между свечами
        candles_excluded = 4  # По умолчанию исключаем 4 свечи (1 час для 15m)
        if len(candles) >= 2:
            # Пытаемся определить таймфрейм по разнице timestamps
            time_diff = abs(candles[-1]['timestamp'] - candles[-2]['timestamp'])
            if time_diff > 3600000:  # Более 1 часа в миллисекундах (1h таймфрейм)
                candles_excluded = 1  # Для 1h исключаем 1 свечу
            elif time_diff > 900000:  # Более 15 минут (30m или больше)
                candles_excluded = 2
            else:  # 15m или меньше
                candles_excluded = 4  # 1 час для 15m = 4 свечи
        
        # Убеждаемся, что не исключаем слишком много свечей
        effective_end = len(candles) - candles_excluded
        if effective_end < lookback * 2 + 1:
            # Если после исключения слишком мало свечей, исключаем минимум
            candles_excluded = max(1, len(candles) - (lookback * 2 + 1))
            effective_end = len(candles) - candles_excluded
        
        print(f"[FRACTALS] Исключаем последние {candles_excluded} свечей из анализа (последний час)")
        
        minima = []
        maxima = []
        
        # Основной поиск фракталов (исключаем последние свечи)
        for i in range(lookback, effective_end - lookback):
            current = candles[i]
            
            # Проверяем минимум
            is_minimum = True
            for j in range(i - lookback, i + lookback + 1):
                if j != i and j < len(candles) and candles[j]['low'] <= current['low']:
                    is_minimum = False
                    break
            
            if is_minimum:
                minima.append({
                    'index': i,
                    'price': current['low'],
                    'timestamp': current['timestamp'],
                    'volume': current['volume'],
                    'candle_length': current['high'] - current['low']
                })
            
            # Проверяем максимум
            is_maximum = True
            for j in range(i - lookback, i + lookback + 1):
                if j != i and j < len(candles) and candles[j]['high'] >= current['high']:
                    is_maximum = False
                    break
            
            if is_maximum:
                maxima.append({
                    'index': i,
                    'price': current['high'],
                    'timestamp': current['timestamp'],
                    'volume': current['volume'],
                    'candle_length': current['high'] - current['low']
                })
        
        # Если не нашли фракталы, попробуем более простой подход (тоже исключаем последние свечи)
        if not minima and not maxima and lookback > 1:
            # Ищем простые экстремумы (сравниваем только с соседними свечами)
            for i in range(1, effective_end - 1):
                if i >= len(candles):
                    break
                current = candles[i]
                prev = candles[i-1]
                if i+1 < len(candles):
                    next_candle = candles[i+1]
                else:
                    continue
                
                # Простой минимум
                if current['low'] < prev['low'] and current['low'] < next_candle['low']:
                    minima.append({
                        'index': i,
                        'price': current['low'],
                        'timestamp': current['timestamp'],
                        'volume': current['volume'],
                        'candle_length': current['high'] - current['low']
                    })
                
                # Простой максимум
                if current['high'] > prev['high'] and current['high'] > next_candle['high']:
                    maxima.append({
                        'index': i,
                        'price': current['high'],
                        'timestamp': current['timestamp'],
                        'volume': current['volume'],
                        'candle_length': current['high'] - current['low']
                    })
        
        # Сортируем по индексу (времени)
        minima.sort(key=lambda x: x['index'])
        maxima.sort(key=lambda x: x['index'])
        
        print(f"[FRACTALS] Найдено {len(minima)} минимумов и {len(maxima)} максимумов (исключено последних {candles_excluded} свечей)")
        
        return minima, maxima
    
    def is_high_volume_candle(self, candle: Dict, avg_volume: float) -> bool:
        """Проверяет, является ли свеча высокообъемной"""
        return candle['volume'] > avg_volume * 1.5  # В 1.5 раза больше среднего
    
    def is_long_candle(self, candle: Dict, avg_length: float) -> bool:
        """Проверяет, является ли свеча длинной"""
        candle_length = candle['high'] - candle['low']
        return candle_length > avg_length * 1.2  # В 1.2 раза больше среднего
    
    def find_volume_profile_levels(self, candles: List[Dict], bins: int = 40, value_area: float = 0.7) -> list:
        """Находит уровни по Volume Profile: POC, Value Area High/Low"""
        if len(candles) < 20:
            return []
        lows = [c['low'] for c in candles]
        highs = [c['high'] for c in candles]
        vols = [c['volume'] for c in candles]
        min_price = min(lows)
        max_price = max(highs)
        bin_size = (max_price - min_price) / bins
        # Гистограмма объёма по ценам
        hist = [0] * bins
        for c in candles:
            price_range = np.arange(c['low'], c['high']+bin_size, bin_size)
            for p in price_range:
                idx = int((p - min_price) / bin_size)
                if 0 <= idx < bins:
                    hist[idx] += c['volume'] / len(price_range)
        # POC
        poc_idx = int(np.argmax(hist))
        poc_price = min_price + poc_idx * bin_size
        # Value Area (например, 70% объёма)
        total_vol = sum(hist)
        sorted_bins = sorted(enumerate(hist), key=lambda x: x[1], reverse=True)
        acc = 0
        value_bins = set()
        for idx, v in sorted_bins:
            acc += v
            value_bins.add(idx)
            if acc >= total_vol * value_area:
                break
        va_low = min(value_bins)
        va_high = max(value_bins)
        va_low_price = min_price + va_low * bin_size
        va_high_price = min_price + va_high * bin_size
        # Возвращаем уровни
        levels = [
            {'type': 'poc', 'price': poc_price},
            {'type': 'value_area_low', 'price': va_low_price},
            {'type': 'value_area_high', 'price': va_high_price},
        ]
        return levels
    
    def get_active_triangle_for_pair(self, pair: str, timeframe: str, current_candle_index: int = None) -> Optional[Dict]:
        """
        Получает активный треугольник для пары и таймфрейма.
        
        Args:
            pair: Торговая пара (например, 'ATOM/USDT')
            timeframe: Таймфрейм ('15m', '1h', '4h')
            current_candle_index: Индекс текущей свечи (для проверки, находится ли цена внутри треугольника)
        
        Returns:
            Dict с данными треугольника или None
        """
        try:
            from core.database import init_database, SessionLocal
            from core.models import ChartPattern
            init_database()
            db = SessionLocal()
            
            try:
                triangle_types = ['ascending_triangle', 'descending_triangle', 'symmetrical_triangle']
                triangle = db.query(ChartPattern).filter(
                    ChartPattern.symbol == pair,
                    ChartPattern.timeframe == timeframe,
                    ChartPattern.pattern_type.in_(triangle_types),
                    ChartPattern.is_active == True,
                    ChartPattern.is_confirmed == False  # Только не пробитые треугольники
                ).order_by(ChartPattern.reliability.desc(), ChartPattern.end_time.desc()).first()
                
                if not triangle:
                    return None
                
                # Преобразуем в словарь
                triangle_dict = triangle.to_dict()
                
                # Вычисляем текущие границы треугольника, если есть pattern_data
                if triangle_dict.get('pattern_data') and current_candle_index is not None:
                    pd = triangle_dict['pattern_data']
                    res_slope = pd.get('resistance_slope', 0)
                    res_intercept = pd.get('resistance_intercept', 0)
                    sup_slope = pd.get('support_slope', 0)
                    sup_intercept = pd.get('support_intercept', 0)
                    
                    # Текущие границы на позиции current_candle_index
                    current_resistance = res_slope * current_candle_index + res_intercept
                    current_support = sup_slope * current_candle_index + sup_intercept
                    
                    triangle_dict['current_resistance'] = current_resistance
                    triangle_dict['current_support'] = current_support
                    triangle_dict['breakout_point'] = (current_resistance + current_support) / 2
                
                return triangle_dict
            finally:
                db.close()
                SessionLocal.remove()
        except Exception as e:
            logger.error(f"Ошибка получения треугольника для {pair} {timeframe}: {e}")
            return None
    
    def calculate_triangle_level_bonus(self, level: Dict, triangle: Dict, current_price: float) -> float:
        """
        Вычисляет бонус к score уровня на основе треугольника.
        
        Args:
            level: Данные уровня (price, type, etc.)
            triangle: Данные треугольника
            current_price: Текущая цена
        
        Returns:
            Бонус к score (0-50)
        """
        if not triangle:
            return 0.0
        
        level_price = level.get('price', 0)
        level_type = level.get('type', '')
        pattern_type = triangle.get('pattern_type', '')
        direction = triangle.get('direction', '')
        
        # Получаем границы треугольника
        support_level = triangle.get('support_level') or triangle.get('current_support')
        resistance_level = triangle.get('resistance_level') or triangle.get('current_resistance')
        
        if not support_level or not resistance_level:
            return 0.0
        
        # Толерантность для совпадения с границей треугольника (0.2%)
        tolerance_pct = 0.002
        tolerance = (support_level + resistance_level) / 2 * tolerance_pct
        
        bonus = 0.0
        
        # Бонус за совпадение с границей треугольника (максимальный бонус)
        if level_type == 'support' and abs(level_price - support_level) < tolerance:
            bonus += 30.0  # Очень сильный уровень - граница треугольника
        elif level_type == 'resistance' and abs(level_price - resistance_level) < tolerance:
            bonus += 30.0
        
        # Бонус за уровень внутри треугольника
        if support_level < level_price < resistance_level:
            # Расстояние до ближайшей границы
            dist_to_support = abs(level_price - support_level)
            dist_to_resistance = abs(level_price - resistance_level)
            triangle_height = resistance_level - support_level
            
            if triangle_height > 0:
                # Чем ближе к границе, тем больше бонус
                min_dist = min(dist_to_support, dist_to_resistance)
                proximity_score = 1.0 - (min_dist / triangle_height)
                bonus += 15.0 * proximity_score
        
        # Бонус за соответствие направлению треугольника
        if pattern_type == 'ascending_triangle' and direction == 'bullish':
            # В восходящем треугольнике приоритет лонгам (support)
            if level_type == 'support':
                bonus += 10.0
        elif pattern_type == 'descending_triangle' and direction == 'bearish':
            # В нисходящем треугольнике приоритет шортам (resistance)
            if level_type == 'resistance':
                bonus += 10.0
        
        # Штраф за уровни вне треугольника (если треугольник активен)
        if level_price < support_level or level_price > resistance_level:
            # Небольшой штраф, но не блокируем полностью
            bonus -= 5.0
        
        return max(0.0, min(50.0, bonus))  # Ограничиваем бонус 0-50
    
    def find_potential_levels(self, pair: str, candles: List[Dict], trend: str = None,
                              timeframe_label: str = '15m', max_levels: int = 5) -> List[Dict]:
        """
        Поиск релевантных уровней для скальпинга "отскок".
        Уровни формируются только на истории (без последнего часа данных) и
        содержат расширенную мета-информацию (таймфрейм, качество, касания).
        Теперь учитывает активные треугольники для приоритизации уровней.
        """
        settings = self.level_settings
        if len(candles) < 40:
            print(f"[{pair}] Not enough candles for levels (need 40, have {len(candles)})")
            return []
        
        candles_for_analysis, excluded = self._prepare_candles_for_levels(candles)
        if len(candles_for_analysis) < 40:
            print(f"[{pair}] Недостаточно свечей после исключения последних {excluded} для {pair}")
            return []
        
        reference_index = len(candles_for_analysis) - 1
        current_price = candles_for_analysis[reference_index]['close']
        current_timestamp = candles_for_analysis[reference_index]['timestamp']
        latest_price = candles[-1]['close']
        
        print(f"[{pair}] ⚠️ Исключено последних {excluded} свечей (последний {settings['exclude_recent_minutes']} мин)")
        print(f"[{pair}] Историческая цена для расчета уровней: {current_price:.4f}, текущая цена: {latest_price:.4f}")
        
        # Получаем активный треугольник для этой пары и таймфрейма
        triangle = self.get_active_triangle_for_pair(pair, timeframe_label, reference_index)
        if triangle:
            pattern_type = triangle.get('pattern_type', '')
            direction = triangle.get('direction', '')
            support_level = triangle.get('support_level') or triangle.get('current_support')
            resistance_level = triangle.get('resistance_level') or triangle.get('current_resistance')
            print(f"[{pair}] 🔺 Найден активный треугольник: {pattern_type} ({direction}), "
                  f"поддержка: {support_level:.4f}, сопротивление: {resistance_level:.4f}")
        else:
            print(f"[{pair}] ℹ️ Активных треугольников не найдено")
        
        # История сигналов, чтобы не дублировать уровни
        try:
            all_signals = signal_manager.load_recent_signals(limit=1000)
            tested_prices = {
                round(signal.get('level_price', 0), 3)
                for signal in all_signals
                if signal.get('pair') == pair and signal.get('level_price', 0) > 0
            }
            print(f"[{pair}] Найдено {len(tested_prices)} исторических уровней с сигналами")
        except Exception as e:
            print(f"[{pair}] Ошибка загрузки сигналов: {e}")
            tested_prices = set()
        
        # Фильтр по тренду: в боковике уровни всё равно нужны, поэтому не выходим сразу
        # но запоминаем контекст тренда в метаданных уровня
        trend_context = trend or "UNKNOWN"
        
        # Параметры поиска фракталов
        optimal_lookback = 5
        if len(candles_for_analysis) < optimal_lookback * 2:
            optimal_lookback = max(2, len(candles_for_analysis) // 4)
        
        minima, maxima = self.find_fractals(
            candles_for_analysis,
            lookback=optimal_lookback,
            exclude_last_hours=0  # Уже исключили нужное окно
        )
        print(f"[{pair}] Фракталы: {len(minima)} минимумов, {len(maxima)} максимумов (lookback={optimal_lookback})")
        
        min_distance = settings["min_distance_percent"]
        max_distance = settings["max_distance_percent"]
        if timeframe_label in ('1h', '4h'):
            max_distance *= 1.5  # Разрешаем чуть более дальние уровни на старших ТФ
        min_touches = settings["min_historical_touches"]
        max_touches = settings["max_historical_touches"]
        touch_tolerance = settings["historical_touch_tolerance"]
        
        potential_levels: List[Dict] = []
        
        def build_level(fractal: Dict, level_type: str, source_suffix: str) -> Optional[Dict]:
            price = fractal['price']
            distance_percent = abs(price - current_price) / current_price * 100
            
            if distance_percent < min_distance or distance_percent > max_distance:
                return None
            
            rounded_price = round(price, 3)
            if rounded_price in tested_prices:
                return None
            
            total_touches = self.count_total_level_touches(
                candles_for_analysis,
                price,
                tolerance=touch_tolerance,
                exclude_last_hours=0
            )
            
            if total_touches < min_touches or total_touches > max_touches:
                return None
            
            volume_score = min(100, (fractal.get('volume', 0) / 1_000_000) * 10)
            distance_score = max(0, 100 - distance_percent * 20)
            touch_score = min(100, (total_touches - min_touches + 1) * 20)
            approach_score = self._calculate_approach_score(
                candles_for_analysis,
                fractal.get('index', 0),
                price,
                level_type
            )
            trend_bonus = self._trend_bonus(level_type, trend_context)
            fractal_age_hours = max(0, (current_timestamp - fractal['timestamp']) / (1000 * 3600))
            freshness_score = max(0, 100 - fractal_age_hours * 10)
            
            # Бонус за треугольник
            triangle_bonus = 0.0
            if triangle:
                temp_level = {'price': price, 'type': level_type}
                triangle_bonus = self.calculate_triangle_level_bonus(temp_level, triangle, current_price)
            
            # Базовый score (без треугольника)
            base_score = round(
                distance_score * 0.25 +
                volume_score * 0.15 +
                touch_score * 0.2 +
                freshness_score * 0.15 +
                approach_score * 0.15 +
                trend_bonus * 0.1,
                2
            )
            
            # Итоговый score с учетом треугольника
            total_score = round(base_score + triangle_bonus, 2)
            
            created_at_iso = datetime.now(timezone.utc).isoformat()
            level_dict = {
                'pair': pair,
                'type': level_type,
                'timeframe': timeframe_label,
                'price': float(price),
                'timestamp': fractal['timestamp'],
                'volume': fractal.get('volume', 0),
                'candle_length': fractal.get('candle_length', 0),
                'created_at': created_at_iso,
                'test_count': max(1, total_touches),
                'historical_touches': max(1, total_touches),
                'live_test_count': 0,
                'fractal_touch_number': total_touches,
                'last_test': None,
                'source': f'fractal_{source_suffix}_N{optimal_lookback}',
                'signal_generated': False,
                'trend_context': trend_context,
                'score': total_score,
                'base_score': base_score,  # Базовый score без треугольника
                'triangle_bonus': round(triangle_bonus, 2),  # Бонус от треугольника
                'approach_score': round(approach_score, 2),
                'trend_bonus': round(trend_bonus, 2),
                'distance_percent': distance_percent,
                'fractal_age_hours': fractal_age_hours,
                'excluded_recent_candles': excluded,
                'triangle_pattern': triangle.get('pattern_type') if triangle else None,  # Тип треугольника
                'triangle_direction': triangle.get('direction') if triangle else None  # Направление треугольника
            }
            return level_dict
        
        if minima:
            for fractal in minima:
                level = build_level(fractal, 'support', 'min')
                if level:
                    potential_levels.append(level)
                    print(f"[{pair}] Уровень поддержки @ {level['price']} (касания: {level['historical_touches']}, score: {level['score']})")
        
        if maxima:
            for fractal in maxima:
                level = build_level(fractal, 'resistance', 'max')
                if level:
                    potential_levels.append(level)
                    print(f"[{pair}] Уровень сопротивления @ {level['price']} (касания: {level['historical_touches']}, score: {level['score']})")
        
        potential_levels.sort(key=lambda x: x['score'], reverse=True)
        trimmed = potential_levels[:max_levels]
        print(f"[{pair}] Отобрано {len(trimmed)} уровней из {len(potential_levels)} найденных")
        return trimmed
    
    def count_level_touches(self, candles: List[Dict], fractal_index: int, level_price: float, tolerance: float = 0.005, signal_type: str = None) -> int:
        """Подсчитывает количество касаний уровня после фрактала - ТОЧНОЕ КАСАНИЕ"""
        touch_count = 0
        
        # ТОЧНОЕ КАСАНИЕ: используем очень узкий диапазон 0.01%
        actual_tolerance = 0.0001  # 0.01% для всех типов сигналов
            
        for i in range(fractal_index + 1, len(candles)):
            candle = candles[i]
            # Проверяем касание уровня (high, low, close)
            if (abs(candle['low'] - level_price) / level_price < actual_tolerance or
                abs(candle['high'] - level_price) / level_price < actual_tolerance or
                abs(candle['close'] - level_price) / level_price < actual_tolerance):
                touch_count += 1
        return touch_count
    
    def count_total_level_touches(
        self,
        candles: List[Dict],
        level_price: float,
        tolerance: float = 0.003,
        exclude_last_hours: int = 1
    ) -> int:
        """
        Подсчитывает ВСЕ исторические касания уровня (low/high/close попадают в tol).
        exclude_last_hours > 0 позволяет исключить последние свечи при необходимости.
        """
        if not candles or level_price == 0:
            return 0
        
        candles_excluded = 0
        if exclude_last_hours > 0:
            candles_excluded = self._calculate_candles_to_exclude(
                candles,
                minutes=exclude_last_hours * 60
            )
        
        effective_end = len(candles) - candles_excluded
        if effective_end <= 0:
            effective_end = len(candles)
            candles_excluded = 0
        
        total_touches = 0
        for i in range(effective_end):
            candle = candles[i]
            low_touch = abs(candle['low'] - level_price) / level_price <= tolerance
            high_touch = abs(candle['high'] - level_price) / level_price <= tolerance
            close_touch = abs(candle['close'] - level_price) / level_price <= tolerance
            if low_touch or high_touch or close_touch:
                total_touches += 1
                diff_percent = abs(candle['close'] - level_price) / level_price * 100
                print(f"[TOUCH #{total_touches}] Свеча {i}: ts={candle['timestamp']}, close={candle['close']:.4f}, уровень={level_price:.4f}, Δ={diff_percent:.4f}%")
        
        print(f"[TOTAL TOUCHES] {level_price} → {total_touches} касаний (минус {candles_excluded} свечей)")
        return total_touches
    
    def is_level_touch(self, candle: Dict, level_price: float, tolerance: float = 0.0001) -> bool:
        """Проверяет, коснулась ли свеча уровня"""
        return (abs(candle['low'] - level_price) / level_price < tolerance or
                abs(candle['high'] - level_price) / level_price < tolerance or
                abs(candle['close'] - level_price) / level_price < tolerance)
    
    def check_level_touch(self, current_price: float, level_price: float, tolerance: float = 0.005, signal_type: str = None) -> bool:
        """Проверяет, коснулась ли цена уровня - УЛУЧШЕННАЯ ЛОГИКА"""
        if level_price == 0:
            return False
        
        price_diff = abs(current_price - level_price) / level_price
        live_tolerance = tolerance or self.level_settings["live_touch_tolerance"]
        is_touch = price_diff <= live_tolerance
        
        if is_touch:
            print(f"[TOUCH CHECK] ✅ КАСАНИЕ! Цена: {current_price}, Уровень: {level_price}, Разница: {price_diff:.4f} ({price_diff*100:.2f}%), Сигнал: {signal_type}")
        else:
            print(f"[TOUCH CHECK] Нет касания. Цена: {current_price}, Уровень: {level_price}, Разница: {price_diff:.4f} ({price_diff*100:.2f}%), Сигнал: {signal_type}")
        
        return is_touch
    
    def check_level_break(self, current_price: float, level_price: float, level_type: str, tolerance: float = None) -> bool:
        """Проверяет, пробит ли уровень (0.5% против позиции)"""
        if tolerance is None:
            tolerance = self.level_settings["break_tolerance"]
        if level_type == 'support':
            # Для поддержки - цена упала ниже уровня
            price_diff = (level_price - current_price) / level_price
            is_broken = price_diff > tolerance
            print(f"[BREAK CHECK] Поддержка: цена={current_price}, уровень={level_price}, разница={price_diff:.4f} ({price_diff*100:.2f}%), пробой={is_broken} (макс {tolerance*100:.1f}%)")
            return is_broken
        else:
            # Для сопротивления - цена поднялась выше уровня
            price_diff = (current_price - level_price) / level_price
            is_broken = price_diff > tolerance
            print(f"[BREAK CHECK] Сопротивление: цена={current_price}, уровень={level_price}, разница={price_diff:.4f} ({price_diff*100:.2f}%), пробой={is_broken} (макс {tolerance*100:.1f}%)")
            return is_broken
    
    def is_level_broken(self, level: Dict, candles: List[Dict], current_price: float) -> bool:
        """
        Проверяет, пробит ли уровень (для скальпинга - более строгая проверка).
        Уровень считается пробитым если:
        1. Текущая цена пробила уровень более чем на 0.5%
        2. В последних 20 свечах уровень был пробит (для поддержки - цена упала ниже, для сопротивления - выше)
        3. Цена ушла от уровня на значительное расстояние (>2% для агрессивной очистки)
        """
        level_price = level['price']
        level_type = level['type']
        
        # 1. ПРОВЕРКА ТЕКУЩЕГО ПРОБОЯ (0.5% толерантность)
        if self.check_level_break(current_price, level_price, level_type):
            print(f"[BROKEN LEVEL] Уровень {level_type} @ {level_price} пробит текущей ценой {current_price}")
            return True
        
        # 2. АГРЕССИВНАЯ ПРОВЕРКА: если цена ушла от уровня на >2%, считаем пробитым
        price_diff_pct = abs(current_price - level_price) / level_price * 100
        if level_type == 'support':
            # Для поддержки: если цена ниже уровня на >2%
            if current_price < level_price and price_diff_pct > 2.0:
                print(f"[BROKEN LEVEL] Уровень поддержки {level_price} пробит: цена {current_price} ниже на {price_diff_pct:.2f}%")
                return True
        else:  # resistance
            # Для сопротивления: если цена выше уровня на >2%
            if current_price > level_price and price_diff_pct > 2.0:
                print(f"[BROKEN LEVEL] Уровень сопротивления {level_price} пробит: цена {current_price} выше на {price_diff_pct:.2f}%")
                return True
        
        # 3. ПРОВЕРКА ИСТОРИЧЕСКОГО ПРОБОЯ (последние 20 свечей для более надежной проверки)
        recent_candles = candles[-20:] if len(candles) >= 20 else candles
        
        tolerance = self.level_settings["break_tolerance"]
        for i, candle in enumerate(recent_candles):
            if level_type == 'support':
                if candle['low'] < level_price * (1 - tolerance) or candle['close'] < level_price * (1 - tolerance):
                    print(f"[BROKEN LEVEL] Уровень поддержки {level_price} пробит в свече {i} (low: {candle['low']}, close: {candle['close']})")
                    return True
            else:  # resistance
                if candle['high'] > level_price * (1 + tolerance) or candle['close'] > level_price * (1 + tolerance):
                    print(f"[BROKEN LEVEL] Уровень сопротивления {level_price} пробит в свече {i} (high: {candle['high']}, close: {candle['close']})")
                    return True
        
        return False
    
    def clean_broken_levels(self, pair: str, pair_levels: List[Dict], candles: List[Dict], current_price: float) -> List[Dict]:
        """
        Очищает пробитые уровни каждые 5 минут (для скальпинга).
        Удаляет только пробитые уровни, не по возрасту.
        """
        if not pair_levels:
            return []
        
        original_count = len(pair_levels)
        cleaned_levels = []
        
        print(f"[{pair}] 🔍 Проверка {original_count} уровней на пробитие...")
        
        for level in pair_levels:
            if self.is_level_broken(level, candles, current_price):
                print(f"[{pair}] 🗑️ УДАЛЯЕМ пробитый уровень: {level['type']} @ {level['price']}")
                # Удаляем из БД
                self._delete_level_from_db(pair, level['price'])
                continue
            else:
                cleaned_levels.append(level)
                print(f"[{pair}] ✅ Активный уровень: {level['type']} @ {level['price']} (расстояние: {abs(current_price - level['price']) / level['price'] * 100:.2f}%)")
        
        removed_count = original_count - len(cleaned_levels)
        if removed_count > 0:
            print(f"[{pair}] ✅ Очистка завершена: удалено {removed_count} пробитых уровней, осталось {len(cleaned_levels)} активных")
        
        return cleaned_levels
    
    def calculate_stop_loss(self, entry_price: float, signal_type: str, stop_percent: float = 0.005) -> float:
        """Вычисляет стоп-лосс для сигнала"""
        if signal_type == 'LONG':
            return entry_price * (1 - stop_percent)
        elif signal_type == 'SHORT':
            return entry_price * (1 + stop_percent)
        else:
            return entry_price

    def calculate_price_change_24h(self, candles: List[Dict]) -> float:
        """Вычисляет изменение цены за 24 часа в процентах"""
        if len(candles) < 24:
            return 0.0
        
        # Берем цену закрытия 24 свечи назад и текущую
        old_price = candles[-24]['close']
        current_price = candles[-1]['close']
        
        if old_price == 0:
            return 0.0
        
        change_percent = ((current_price - old_price) / old_price) * 100
        return round(change_percent, 2)

    def calculate_volume_24h(self, candles: List[Dict]) -> float:
        """Вычисляет объем торгов за 24 часа в миллионах долларов"""
        if len(candles) < 24:
            print(f"[VOLUME] Недостаточно свечей для расчета объема: {len(candles)} < 24")
            return 0.0
        
        # Суммируем объемы за последние 24 свечи
        total_volume = sum(candle['volume'] for candle in candles[-24:])
        
        # Конвертируем в миллионы долларов
        volume_millions = total_volume / 1_000_000
        
        # Отладочная информация
        print(f"[VOLUME] Объем за 24ч: {total_volume:.2f} -> {volume_millions:.2f}M")
        
        return round(volume_millions, 2)
    
    def fix_existing_levels(self, pair_levels: List[Dict]) -> List[Dict]:
        """Исправляет существующие уровни, добавляя недостающие поля"""
        fixed_levels = []
        for level in pair_levels:
            # Добавляем поле signal_generated, если его нет
            if 'signal_generated' not in level:
                level['signal_generated'] = False
                print(f"[{level.get('pair', 'UNKNOWN')}] Исправлен уровень {level['price']}: добавлено signal_generated=False")
            if 'historical_touches' not in level:
                level['historical_touches'] = level.get('test_count', 1)
            if 'live_test_count' not in level:
                hist = level.get('historical_touches', level.get('test_count', 1))
                level['live_test_count'] = max(level.get('test_count', 1) - hist, 0)
            if 'timeframe' not in level:
                level['timeframe'] = level.get('metadata', {}).get('timeframe', '15m')
            fixed_levels.append(level)
        return fixed_levels

    async def analyze_pair(self, pair: str) -> Dict[str, Any]:
        """Анализирует одну торговую пару"""
        try:
            print(f"\n=== АНАЛИЗ ПАРЫ {pair} ===")
            
            # Получаем данные - увеличиваем лимит до 200 свечей
            candles_1h = await self.fetch_ohlcv(pair, '1h', 200)
            candles_15m = await self.fetch_ohlcv(pair, '15m', 200)
            if not candles_1h or not candles_15m:
                print(f"[{pair}] Нет данных для анализа")
                return {'pair': pair, 'status': 'error', 'message': 'Нет данных'}
            
            print(f"[{pair}] Получено свечей: 1H={len(candles_1h)}, 15M={len(candles_15m)}")
            
            trend_1h = self.determine_trend_1h(candles_1h)
            current_price = candles_15m[-1]['close']
            
            # Вычисляем изменение цены и объем за 24 часа
            price_change_24h = self.calculate_price_change_24h(candles_15m)
            volume_24h = self.calculate_volume_24h(candles_15m)
            
            print(f"[{pair}] Тренд 1H: {trend_1h}, Текущая цена: {current_price}")
            print(f"[{pair}] Изменение 24ч: {price_change_24h}%, Объем 24ч: {volume_24h}M")
            
            # ИСКЛЮЧЕНИЕ: при боковом тренде удаляем все существующие уровни
            # Проверяем все варианты бокового тренда: SIDEWAYS_*, UP_SIDEWAYS, DOWN_SIDEWAYS
            is_sideways_trend = False
            if not trend_1h:
                is_sideways_trend = True
            elif trend_1h.startswith('SIDEWAYS'):
                is_sideways_trend = True
            elif '_SIDEWAYS' in trend_1h:  # UP_SIDEWAYS или DOWN_SIDEWAYS (слабая сила тренда)
                is_sideways_trend = True
            elif trend_1h == 'UNKNOWN':
                is_sideways_trend = True
            
            if is_sideways_trend:
                print(f"[{pair}] ⚠️ Боковой или неопределенный тренд ({trend_1h}) — расширяем поиск уровней для отскока")
            
            candles_4h = await self.fetch_ohlcv(pair, '4h', 200)
            
            potential_levels = []
            potential_levels += self.find_potential_levels(pair, candles_15m, trend=trend_1h, timeframe_label='15m')
            if candles_1h:
                potential_levels += self.find_potential_levels(pair, candles_1h, trend=trend_1h, timeframe_label='1h', max_levels=4)
            if candles_4h:
                potential_levels += self.find_potential_levels(pair, candles_4h, trend=trend_1h, timeframe_label='4h', max_levels=2)
            
            print(f"[{pair}] Найдено потенциальных уровней: {len(potential_levels)} (15m+1h+4h)")
            
            # Загружаем активные уровни
            active_levels = signal_manager.load_active_levels()
            pair_levels = active_levels.get(pair, [])
            if not isinstance(pair_levels, list):
                pair_levels = []
            
            # Исправляем существующие уровни (добавляем недостающие поля)
            pair_levels = self.fix_existing_levels(pair_levels)
            
            print(f"[{pair}] 📊 Начальное количество уровней: {len(pair_levels)}")
            for i, level in enumerate(pair_levels):
                price_diff = abs(current_price - level['price']) / level['price'] * 100
                hist = level.get('historical_touches', level.get('test_count', 1))
                live_tests = level.get('live_test_count', max(level.get('test_count', 1) - hist, 0))
                print(f"[{pair}] Уровень {i+1}: {level['type']} @ {level['price']}, расстояние: {price_diff:.2f}%, historical={hist}, live_tests={live_tests}")
            
            signals = []
            
            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: проверяем пробитые уровни каждые 5 минут (не по возрасту!)
            print(f"[{pair}] 🔍 Проверка уровней на пробитие (каждые 5 минут)...")
            pair_levels = self.clean_broken_levels(pair, pair_levels, candles_15m, current_price)
            
            print(f"[{pair}] ✅ Активных уровней после проверки на пробитие: {len(pair_levels)}")
            
            # ОПТИМИЗАЦИЯ: Проверяем Elder's Screens для всех уровней (с кэшированием)
            # Это позволяет избежать дублирования проверок и сохранить результаты в метаданные
            print(f"[{pair}] 🔍 Проверка Elder's Screens для всех уровней...")
            for level in pair_levels:
                try:
                    # Проверяем, нужно ли обновить Elder's Screens (если старше 5 минут или нет данных)
                    meta = level.get('metadata', {}) or {}
                    elder_screens_data = meta.get('elder_screens')
                    elder_screens_checked_at = meta.get('elder_screens_checked_at')
                    
                    needs_check = True
                    if elder_screens_data and elder_screens_checked_at:
                        try:
                            from datetime import datetime, timezone
                            checked_time = datetime.fromisoformat(elder_screens_checked_at.replace('Z', '+00:00'))
                            time_diff = (datetime.now(checked_time.tzinfo) - checked_time).total_seconds()
                            if time_diff < 300:  # 5 минут
                                needs_check = False
                                # Устанавливаем elder_screens_passed из кэшированных данных
                                if 'metadata' not in level:
                                    level['metadata'] = {}
                                level['metadata']['elder_screens_passed'] = meta.get('elder_screens_passed', False)
                                print(f"[{pair}] Используем кэшированные Elder's Screens для уровня {level['price']} (passed={level['metadata']['elder_screens_passed']})")
                        except:
                            pass
                    
                    if needs_check:
                        # Определяем потенциальный тип сигнала
                        signal_type = 'LONG' if level['type'] == 'support' else 'SHORT'
                        
                        # Проверяем Elder's Screens
                        screens_passed, screens_details = await self.check_elder_screens(
                            pair=pair,
                            signal_type=signal_type,
                            level=level,
                            current_price=current_price,
                            candles_4h=candles_4h if candles_4h else [],
                            candles_1h=candles_1h,
                            level_score=level.get('score')
                        )
                        
                        # Сохраняем результаты в метаданные уровня
                        if 'metadata' not in level:
                            level['metadata'] = {}
                        level['metadata']['elder_screens'] = screens_details
                        level['metadata']['elder_screens_checked_at'] = datetime.now(timezone.utc).isoformat()
                        level['metadata']['elder_screens_passed'] = screens_passed
                        
                        # Обновляем в БД
                        self._upsert_level_in_db(pair, level, timeframe=level.get('timeframe', '15m'))
                        
                        print(f"[{pair}] Elder's Screens проверены для уровня {level['price']}: {'✅ ПРОЙДЕН' if screens_passed else '❌ ЗАБЛОКИРОВАН'}")
                except Exception as e:
                    logger.error(f"[{pair}] Ошибка проверки Elder's Screens для уровня {level.get('price', 'N/A')}: {e}")
            
            # Проверяем все активные уровни на касание и генерацию сигналов
            for level in pair_levels[:]:  # Итерируемся по копии, чтобы можно было удалять
                # Определяем потенциальный тип сигнала для проверки касания
                potential_signal_type = 'LONG' if level['type'] == 'support' else 'SHORT'
                
                print(f"[{pair}] Проверяем уровень {level['type']} @ {level['price']} (текущая цена: {current_price}, потенциальный сигнал: {potential_signal_type})")
                
                # Проверяем касание ИЛИ пробой уровня
                is_touching = self.check_level_touch(current_price, level['price'], signal_type=potential_signal_type)
                is_breakthrough = False
                
                # Для поддержки: пробой = цена была ниже уровня, а теперь выше (LONG сигнал)
                # Для сопротивления: пробой = цена была выше уровня, а теперь ниже (SHORT сигнал)
                # Проверяем последние 10 свечей для определения пробоя
                if level['type'] == 'support' and trend_1h.startswith('UP'):
                    # Для поддержки в восходящем тренде: проверяем, была ли цена ниже уровня
                    price_above_level = current_price > level['price']
                    if price_above_level:
                        # Проверяем последние свечи - была ли цена ниже уровня
                        recent_candles = candles_15m[-10:] if len(candles_15m) >= 10 else candles_15m
                        was_below_level = any(candle['low'] < level['price'] for candle in recent_candles)
                        price_diff_percent = ((current_price - level['price']) / level['price']) * 100
                        if was_below_level and price_diff_percent > 0.1:  # Пробой на 0.1% выше уровня
                            is_breakthrough = True
                            print(f"[{pair}] ПРОБОЙ ПОДДЕРЖКИ! Цена {current_price} пробила уровень {level['price']} снизу вверх (+{price_diff_percent:.2f}%)")
                elif level['type'] == 'resistance' and trend_1h.startswith('DOWN'):
                    # Для сопротивления в нисходящем тренде: проверяем, была ли цена выше уровня
                    price_below_level = current_price < level['price']
                    if price_below_level:
                        # Проверяем последние свечи - была ли цена выше уровня
                        recent_candles = candles_15m[-10:] if len(candles_15m) >= 10 else candles_15m
                        was_above_level = any(candle['high'] > level['price'] for candle in recent_candles)
                        price_diff_percent = ((level['price'] - current_price) / level['price']) * 100
                        if was_above_level and price_diff_percent > 0.1:  # Пробой на 0.1% ниже уровня
                            is_breakthrough = True
                            print(f"[{pair}] ПРОБОЙ СОПРОТИВЛЕНИЯ! Цена {current_price} пробила уровень {level['price']} сверху вниз (-{price_diff_percent:.2f}%)")
                
                # ОПТИМИЗАЦИЯ: Проверяем готовые уровни (прошли Elder's Screens) при приближении, не только при касании
                meta = level.get('metadata', {}) or {}
                elder_screens_passed = meta.get('elder_screens_passed', False)
                price_distance_pct = abs(current_price - level['price']) / level['price'] * 100
                is_price_close = price_distance_pct <= 0.6  # В пределах 0.6% для готовых уровней
                ready_for_signal = elder_screens_passed and is_price_close
                
                if is_touching or is_breakthrough or ready_for_signal:
                    historical_touches = level.get('historical_touches', level.get('test_count', 1))
                    live_tests = level.get('live_test_count')
                    if live_tests is None:
                        live_tests = max(level.get('test_count', 1) - historical_touches, 0)
                    level['live_test_count'] = live_tests
                    
                    if is_touching:
                        live_tests += 1
                        level['live_test_count'] = live_tests
                        level['test_count'] = historical_touches + live_tests
                        level['last_test'] = candles_15m[-1]['timestamp']
                        print(f"[{pair}] КАСАНИЕ! {level['type']} @ {level['price']} → historical={historical_touches}, live={live_tests}")
                        self._upsert_level_in_db(pair, level, timeframe='15m')
                    elif ready_for_signal and not is_touching:
                        print(f"[{pair}] 🎯 ГОТОВЫЙ УРОВЕНЬ приближается! {level['type']} @ {level['price']} (расстояние: {price_distance_pct:.2f}%)")
                    
                    should_generate_signal = False
                    signal_reason = ""
                    
                    # Проверяем, был ли уже сгенерирован сигнал для этого уровня
                    signal_already_generated = level.get('signal_generated', False)
                    if not signal_already_generated:
                        # Проверяем в БД, был ли уже сигнал для этого уровня
                        try:
                            from core.database import init_database, SessionLocal
                            from core.models import Signal, TradingPair
                            if init_database():
                                session = SessionLocal()
                                try:
                                    pair_obj = session.query(TradingPair).filter_by(symbol=pair).first()
                                    if pair_obj:
                                        # Используем строгую толерантность 0.1% для проверки дубликатов
                                        level_price = level['price']
                                        price_tolerance = level_price * 0.001  # 0.1%
                                        
                                        existing_signal = session.query(Signal).filter(
                                            Signal.pair_id == pair_obj.id,
                                            abs(Signal.level_price - level_price) < price_tolerance
                                        ).order_by(Signal.timestamp.desc()).first()
                                        if existing_signal:
                                            signal_already_generated = True
                                            print(f"[{pair}] ⚠️ Сигнал для уровня {level_price} уже существует (ID: {existing_signal.id}, создан: {existing_signal.timestamp}). Пропускаем.")
                                finally:
                                    session.close()
                        except Exception as e:
                            print(f"[{pair}] Ошибка проверки существующего сигнала: {e}")
                    
                    trend_dir = trend_1h.split('_')[0] if trend_1h else 'UNKNOWN'
                    is_up = trend_dir == 'UP'
                    is_down = trend_dir == 'DOWN'
                    is_sideways = trend_dir not in ('UP', 'DOWN')
                    condition1 = level['type'] == 'support' and (is_up or is_sideways)
                    condition2 = level['type'] == 'resistance' and (is_down or is_sideways)
                    if is_sideways and level.get('historical_touches', 1) < 3:
                        if level['type'] == 'support':
                            condition1 = False
                        if level['type'] == 'resistance':
                            condition2 = False
                    
                    if condition1 or condition2:
                        live_tests = level.get('live_test_count', 0)
                        first_touch = live_tests == 1
                        second_touch = live_tests == 2
                        too_many_live = live_tests >= self.level_settings["max_live_tests"]
                        
                        # Проверяем, прошел ли уровень Elder's Screens (для генерации сигнала даже при поздних касаниях)
                        # elder_screens_passed и price_distance_pct уже определены выше
                        is_price_close_for_touch = price_distance_pct <= 0.5  # В пределах 0.5% от уровня для касания
                        
                        # КРИТИЧНО: Сигналы генерируются ТОЛЬКО из Elder's Screen (ES)
                        # ready_for_signal = True означает, что уровень прошел все проверки ES
                        if ready_for_signal and not signal_already_generated:
                            # Готовый уровень из ES - генерируем сигнал
                            should_generate_signal = True
                            signal_reason = f"ES: Готовый уровень (расстояние: {price_distance_pct:.2f}%, Elder's Screens пройдены)"
                        elif is_touching and elder_screens_passed and is_price_close_for_touch and not signal_already_generated:
                            # Касание уровня с пройденными Elder's Screens (ES) - генерируем сигнал
                            should_generate_signal = True
                            signal_reason = f"ES: Касание уровня с пройденными Elder's Screens (касание #{live_tests}, расстояние: {price_distance_pct:.2f}%)"
                        # Все остальные случаи (пробой, первое/второе касание без ES) - НЕ генерируем сигналы
                        elif is_touching and too_many_live:
                            print(f"[{pair}] МЕРТВЫЙ УРОВЕНЬ! Живых касаний={live_tests} (>{self.level_settings['max_live_tests']}), удаляем {level['type']} @ {level['price']}")
                            pair_levels.remove(level)
                            self._delete_level_from_db(pair, level['price'])
                            continue
                        else:
                            print(f"[{pair}] Касание без сигнала: живых касаний={live_tests}, signal_generated={signal_already_generated}, elder_screens_passed={elder_screens_passed}, price_close={is_price_close}")
                    
                    if should_generate_signal:
                        # ========== ПРИМЕНЕНИЕ ФИЛЬТРОВ ==========
                        # Этап 1-5: Применяем все фильтры перед генерацией сигнала
                        timeframe_label = level.get('timeframe', '15m')
                        test_count = level.get('test_count', 0) or 0
                        
                        should_block, block_reason = self.should_block_signal_by_filters(
                            level=level,
                            trend_1h=trend_1h,
                            timeframe=timeframe_label,
                            price_distance_pct=price_distance_pct,
                            test_count=test_count
                        )
                        
                        if should_block:
                            print(f"[{pair}] 🚫 БЛОКИРОВКА сигнала: {block_reason}")
                            continue
                        
                        # Этап 2-3: Проверка приоритета (опционально, для логирования)
                        level_score = level.get('score', 0) or 0
                        priority = self.calculate_signal_priority(trend_1h, level_score, timeframe_label)
                        if priority < -3:
                            print(f"[{pair}] ⚠️ Низкий приоритет сигнала ({priority}), но не блокируем")
                        
                        print(f"[{pair}] {signal_reason}! Генерируем сигнал... (приоритет: {priority})")
                        
                        # Определяем тип сигнала
                        signal_type = 'LONG' if level['type'] == 'support' else 'SHORT'
                        
                        # ========== ПРОВЕРКА ЭКРАНОВ ЭЛДЕРА ==========
                        # ОПТИМИЗАЦИЯ: Используем уже проверенные Elder's Screens из метаданных, если они свежие
                        meta = level.get('metadata', {}) or {}
                        elder_screens_data = meta.get('elder_screens')
                        elder_screens_checked_at = meta.get('elder_screens_checked_at')
                        
                        # Используем кэшированные данные, если они свежие (менее 1 минуты)
                        use_cached = False
                        if elder_screens_data and elder_screens_checked_at:
                            try:
                                from datetime import datetime, timezone
                                checked_time = datetime.fromisoformat(elder_screens_checked_at.replace('Z', '+00:00'))
                                time_diff = (datetime.now(checked_time.tzinfo) - checked_time).total_seconds()
                                if time_diff < 60:  # 1 минута - достаточно свежие данные для генерации сигнала
                                    use_cached = True
                                    screens_passed = meta.get('elder_screens_passed', False)
                                    screens_details = elder_screens_data
                                    print(f"[{pair}] Используем свежие Elder's Screens из метаданных для генерации сигнала (проверено {time_diff:.0f} сек назад)")
                            except:
                                pass
                        
                        if not use_cached:
                            # Проверяем Elder's Screens заново (данные устарели или отсутствуют)
                            screens_passed, screens_details = await self.check_elder_screens(
                                pair=pair,
                                signal_type=signal_type,
                                level=level,
                                current_price=current_price,
                                candles_4h=candles_4h if candles_4h else [],
                                candles_1h=candles_1h,
                                level_score=level.get('score')
                            )
                            
                            # Обновляем метаданные уровня
                            if 'metadata' not in level:
                                level['metadata'] = {}
                            level['metadata']['elder_screens'] = screens_details
                            level['metadata']['elder_screens_checked_at'] = datetime.now(timezone.utc).isoformat()
                            level['metadata']['elder_screens_passed'] = screens_passed
                        
                        if not screens_passed:
                            blocked_screen = screens_details.get('final_decision', 'UNKNOWN')
                            blocked_reason = None
                            if blocked_screen == 'BLOCKED_SCREEN_1':
                                blocked_reason = screens_details['screen_1'].get('blocked_reason', 'Экран 1 не пройден')
                            elif blocked_screen == 'BLOCKED_SCREEN_2':
                                blocked_reason = screens_details['screen_2'].get('blocked_reason', 'Экран 2 не пройден')
                            
                            print(f"[{pair}] ❌ Сигнал {signal_type} @ {level['price']} ЗАБЛОКИРОВАН экранами Элдера: {blocked_reason}")
                            logger.info(f"[{pair}] Сигнал заблокирован: {blocked_reason}, детали: {screens_details}")
                            continue  # Пропускаем генерацию сигнала
                        
                        print(f"[{pair}] ✅ Сигнал {signal_type} @ {level['price']} прошел все экраны Элдера")
                        
                        # Рассчитываем Stop Loss на основе цены уровня
                        stop_loss_percent = 0.004  # 0.4% (обновлено согласно настройкам)
                        if signal_type == 'LONG':
                            stop_loss = level['price'] * (1 - stop_loss_percent)  # Ниже цены уровня
                        else:  # SHORT
                            stop_loss = level['price'] * (1 + stop_loss_percent)  # Выше цены уровня
                        
                        # Создаем простую запись сигнала
                        signal_data = {
                            'pair': pair,
                            'signal_type': signal_type,
                            'level_price': level['price'],
                            'entry_price': level['price'],  # ТОЧКА ВХОДА = ЦЕНА УРОВНЯ
                            'current_price': current_price,
                            'stop_loss': round(stop_loss, 4),  # Stop Loss с округлением
                            '1h_trend': trend_1h,
                            'trend_direction': trend_1h.split('_')[0] if '_' in trend_1h else 'UNKNOWN',  # UP/DOWN/SIDEWAYS
                            'trend_strength': trend_1h.split('_')[1] if '_' in trend_1h else 'UNKNOWN',  # STRONG/WEAK/SIDEWAYS
                            'level_type': level['type'],
                            'test_count': level['test_count'],
                            'timeframe': level.get('timeframe', '15m'),
                            'historical_touches': level.get('historical_touches', level.get('test_count', 1)),
                            'live_test_count': level.get('live_test_count', 0),
                            'level_score': level.get('score'),
                            'distance_percent': level.get('distance_percent'),
                            'approach_score': level.get('approach_score'),
                            'trend_bonus': level.get('trend_bonus'),
                            'trend_context': level.get('trend_context'),
                            'status': 'ACTIVE',
                            'timestamp': datetime.now().isoformat(),
                            'notes': f"Сигнал {signal_type} на уровне {level['type']} @ {level['price']} (тест #{level['test_count']}, тренд: {trend_1h})",
                            'elder_screens_metadata': screens_details  # Сохраняем детали проверок экранов
                        }
                        
                        print(f"[{pair}] ГЕНЕРИРУЕМ СИГНАЛ {signal_type} на уровне {level['price']}")
                        signal_saved = signal_manager.save_signal(signal_data)
                        signals.append(signal_data)
                        
                        # Логируем результаты проверок экранов (после сохранения сигнала)
                        if signal_saved:
                            try:
                                from core.trading.live_trade_logger import log_signal_event
                                from core.database import init_database, SessionLocal
                                from core.models import Signal, TradingPair
                                if init_database():
                                    session = SessionLocal()
                                    try:
                                        # Находим только что сохраненный сигнал
                                        pair_obj = session.query(TradingPair).filter_by(symbol=pair).first()
                                        if pair_obj:
                                            level_price = level['price']
                                            price_tolerance = level_price * 0.001  # 0.1%
                                            saved_signal = session.query(Signal).filter(
                                                Signal.pair_id == pair_obj.id,
                                                Signal.signal_type == signal_type,
                                                abs(Signal.level_price - level_price) < price_tolerance
                                            ).order_by(Signal.timestamp.desc()).first()
                                            
                                            if saved_signal:
                                                # Логируем Экран 1
                                                screen_1 = screens_details.get('screen_1', {})
                                                if screen_1.get('passed'):
                                                    log_signal_event(
                                                        session, saved_signal.id,
                                                        f"Экран 1 пройден: BTC тренд={screens_details['screen_1']['checks'].get('btc_trend')}, тренд пары={screens_details['screen_1']['checks'].get('pair_trend', {}).get('trend')}",
                                                        event_type='SCREEN_1_RESULT',
                                                        status='PASSED',
                                                        details=screens_details['screen_1'],
                                                        commit=False
                                                    )
                                                
                                                # Логируем Экран 2
                                                screen_2 = screens_details.get('screen_2', {})
                                                if screen_2.get('passed'):
                                                    # Логируем отдельно каждую проверку Экран 2
                                                    # Направление подхода
                                                    if 'price_approach' in screen_2.get('checks', {}):
                                                        approach_details = screen_2['checks']['price_approach']
                                                        log_signal_event(
                                                            session, saved_signal.id,
                                                            f"Экран 2: Направление подхода корректно - {approach_details.get('direction', 'N/A')}",
                                                            event_type='SCREEN_2_PRICE_APPROACH',
                                                            status='PASSED',
                                                            details=approach_details,
                                                            commit=False
                                                        )
                                                    
                                                    # RSI
                                                    if 'rsi' in screen_2.get('checks', {}):
                                                        rsi_details = screen_2['checks']['rsi']
                                                        rsi_value = rsi_details.get('value')
                                                        if rsi_value is not None:
                                                            log_signal_event(
                                                                session, saved_signal.id,
                                                                f"Экран 2: RSI={rsi_value:.2f} {'⚠️ предупреждение' if rsi_details.get('warning') else '✅ OK'}",
                                                                event_type='SCREEN_2_RSI',
                                                                status='WARNING' if rsi_details.get('warning') else 'PASSED',
                                                                details=rsi_details,
                                                                commit=False
                                                            )
                                                    
                                                    # MACD
                                                    if 'macd' in screen_2.get('checks', {}):
                                                        macd_details = screen_2['checks']['macd']
                                                        log_signal_event(
                                                            session, saved_signal.id,
                                                            f"Экран 2: MACD={macd_details.get('macd', 0):.4f}, Signal={macd_details.get('signal', 0):.4f}, Histogram={macd_details.get('histogram', 0):.4f}",
                                                            event_type='SCREEN_2_MACD',
                                                            status='PASSED',
                                                            details=macd_details,
                                                            commit=False
                                                        )
                                                    
                                                    # Итоговый результат Экран 2
                                                    log_signal_event(
                                                        session, saved_signal.id,
                                                        f"Экран 2 пройден: все проверки пройдены",
                                                        event_type='SCREEN_2_RESULT',
                                                        status='PASSED',
                                                        details=screens_details['screen_2'],
                                                        commit=False
                                                    )
                                                else:
                                                    # Логируем блокировку
                                                    blocked_reason = screen_2.get('blocked_reason', 'Неизвестная причина')
                                                    log_signal_event(
                                                        session, saved_signal.id,
                                                        f"Экран 2 заблокирован: {blocked_reason}",
                                                        event_type='SCREEN_2_OSCILLATOR_BLOCKED',
                                                        status='BLOCKED',
                                                        details=screens_details['screen_2'],
                                                        commit=False
                                                    )
                                                
                                                session.commit()
                                    finally:
                                        session.close()
                            except Exception as e:
                                logger.error(f"[{pair}] Ошибка логирования экранов Элдера: {e}")
                        
                        # НЕ УДАЛЯЕМ уровень после генерации сигнала - уровень может использоваться для отскока
                        # Помечаем только как использованный
                        level['signal_generated'] = True
                        level['signal_timestamp'] = datetime.now().isoformat()
                        # Обновляем в БД
                        self._upsert_level_in_db(pair, level, timeframe='15m')
                        print(f"[{pair}] Сигнал сохранен, уровень помечен как использованный (оставляем для возможного отскока)")
                    else:
                        print(f"[{pair}] Условия для сигнала не выполнены")
                            
                # Пробитые уровни уже удалены в clean_broken_levels, здесь только проверяем касания
            
            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: добавляем ВСЕ релевантные уровни из potential_levels (не только один!)
            # Для скальпинга нужно несколько уровней на пару
            print(f"[{pair}] 🔍 Проверка {len(potential_levels)} потенциальных уровней для добавления...")
            
            # Загружаем историю сигналов для проверки
            all_signals = signal_manager.load_recent_signals(limit=1000)
            tested_prices = set()
            for signal in all_signals:
                if signal.get('pair') == pair:
                    level_price = signal.get('level_price', 0)
                    if level_price > 0:
                        tested_prices.add(round(level_price, 3))
            
            added_count = 0
            for level in potential_levels:
                # Проверяем, не был ли уровень уже использован для сигнала
                price_rounded = round(level['price'], 3)
                if price_rounded in tested_prices:
                    print(f"[{pair}] Уровень {level['price']} уже использовался для сигнала, пропускаем")
                    continue
                
                # Проверяем, не существует ли уже такой уровень (толерантность 0.5%)
                existing_level = None
                for l in pair_levels:
                    price_diff_percent = abs(l['price'] - level['price']) / level['price'] * 100
                    if price_diff_percent < 0.5:  # 0.5% толерантность для дубликатов
                        existing_level = l
                        print(f"[{pair}] Уровень {level['price']} уже существует (близкий уровень {l['price']}), пропускаем")
                        break
                
                if not existing_level:
                    print(f"[{pair}] ✅ Добавляем новый уровень: {level['type']} @ {level['price']} (score: {level.get('score', 0):.1f}, расстояние: {level.get('distance_percent', 0):.2f}%)")
                    pair_levels.append(level)
                    # Синхронизируем в БД
                    self._upsert_level_in_db(pair, level, timeframe='15m')
                    added_count += 1
                    
                    # ВАЖНО: проверяем касание/пробой СРАЗУ после создания уровня
                    # Это позволяет генерировать сигналы, если уровень уже касается или пробит
                    potential_signal_type = 'LONG' if level['type'] == 'support' else 'SHORT'
                    is_touching_new = self.check_level_touch(current_price, level['price'], signal_type=potential_signal_type)
                    is_breakthrough_new = False
                    
                    # Проверяем пробой для нового уровня
                    if level['type'] == 'support' and trend_1h.startswith('UP'):
                        price_above_level = current_price > level['price']
                        if price_above_level:
                            recent_candles = candles_15m[-10:] if len(candles_15m) >= 10 else candles_15m
                            was_below_level = any(candle['low'] < level['price'] for candle in recent_candles)
                            price_diff_percent = ((current_price - level['price']) / level['price']) * 100
                            if was_below_level and price_diff_percent > 0.1:
                                is_breakthrough_new = True
                                print(f"[{pair}] НОВЫЙ УРОВЕНЬ ПРОБИТ! Цена {current_price} пробила уровень {level['price']} снизу вверх (+{price_diff_percent:.2f}%)")
                    elif level['type'] == 'resistance' and trend_1h.startswith('DOWN'):
                        price_below_level = current_price < level['price']
                        if price_below_level:
                            recent_candles = candles_15m[-10:] if len(candles_15m) >= 10 else candles_15m
                            was_above_level = any(candle['high'] > level['price'] for candle in recent_candles)
                            price_diff_percent = ((level['price'] - current_price) / level['price']) * 100
                            if was_above_level and price_diff_percent > 0.1:
                                is_breakthrough_new = True
                                print(f"[{pair}] НОВЫЙ УРОВЕНЬ ПРОБИТ! Цена {current_price} пробила уровень {level['price']} сверху вниз (-{price_diff_percent:.2f}%)")
                    
                    # Если новый уровень касается или пробит - генерируем сигнал
                    if (is_touching_new or is_breakthrough_new) and level.get('test_count', 1) == 1:
                        condition1 = level['type'] == 'support' and trend_1h.startswith('UP')
                        condition2 = level['type'] == 'resistance' and trend_1h.startswith('DOWN')
                        
                        if condition1 or condition2:
                            # Проверяем, был ли уже сигнал для этого уровня
                            signal_already_generated = False
                            try:
                                from core.database import init_database, SessionLocal
                                from core.models import Signal, TradingPair
                                if init_database():
                                    session = SessionLocal()
                                    try:
                                        pair_obj = session.query(TradingPair).filter_by(symbol=pair).first()
                                        if pair_obj:
                                            # Используем строгую толерантность 0.1% для проверки дубликатов
                                            level_price = level['price']
                                            price_tolerance = level_price * 0.001  # 0.1%
                                            
                                            existing_signal = session.query(Signal).filter(
                                                Signal.pair_id == pair_obj.id,
                                                abs(Signal.level_price - level_price) < price_tolerance
                                            ).order_by(Signal.timestamp.desc()).first()
                                            if existing_signal:
                                                signal_already_generated = True
                                                print(f"[{pair}] ⚠️ Сигнал для уровня {level_price} уже существует (ID: {existing_signal.id}, создан: {existing_signal.timestamp}). Пропускаем.")
                                    finally:
                                        session.close()
                            except Exception as e:
                                print(f"[{pair}] Ошибка проверки существующего сигнала для нового уровня: {e}")
                                import traceback
                                traceback.print_exc()
                            
                            if not signal_already_generated:
                                # Генерируем сигнал для нового уровня
                                signal_type = 'LONG' if level['type'] == 'support' else 'SHORT'
                                
                                # ========== ПРОВЕРКА ЭКРАНОВ ЭЛДЕРА ==========
                                # ОПТИМИЗАЦИЯ: Используем уже проверенные Elder's Screens из метаданных, если они свежие
                                meta = level.get('metadata', {}) or {}
                                elder_screens_data = meta.get('elder_screens')
                                elder_screens_checked_at = meta.get('elder_screens_checked_at')
                                
                                # Используем кэшированные данные, если они свежие (менее 1 минуты)
                                use_cached = False
                                if elder_screens_data and elder_screens_checked_at:
                                    try:
                                        from datetime import datetime, timezone
                                        checked_time = datetime.fromisoformat(elder_screens_checked_at.replace('Z', '+00:00'))
                                        time_diff = (datetime.now(checked_time.tzinfo) - checked_time).total_seconds()
                                        if time_diff < 60:  # 1 минута - достаточно свежие данные для генерации сигнала
                                            use_cached = True
                                            screens_passed = meta.get('elder_screens_passed', False)
                                            screens_details = elder_screens_data
                                            print(f"[{pair}] Используем свежие Elder's Screens из метаданных для нового уровня (проверено {time_diff:.0f} сек назад)")
                                    except:
                                        pass
                                
                                if not use_cached:
                                    # Проверяем Elder's Screens заново (данные устарели или отсутствуют)
                                    screens_passed, screens_details = await self.check_elder_screens(
                                        pair=pair,
                                        signal_type=signal_type,
                                        level=level,
                                        current_price=current_price,
                                        candles_4h=candles_4h if candles_4h else [],
                                        candles_1h=candles_1h,
                                        level_score=level.get('score')
                                    )
                                    
                                    # Обновляем метаданные уровня
                                    if 'metadata' not in level:
                                        level['metadata'] = {}
                                    level['metadata']['elder_screens'] = screens_details
                                    level['metadata']['elder_screens_checked_at'] = datetime.now(timezone.utc).isoformat()
                                    level['metadata']['elder_screens_passed'] = screens_passed
                                
                                if not screens_passed:
                                    blocked_screen = screens_details.get('final_decision', 'UNKNOWN')
                                    blocked_reason = None
                                    if blocked_screen == 'BLOCKED_SCREEN_1':
                                        blocked_reason = screens_details['screen_1'].get('blocked_reason', 'Экран 1 не пройден')
                                    elif blocked_screen == 'BLOCKED_SCREEN_2':
                                        blocked_reason = screens_details['screen_2'].get('blocked_reason', 'Экран 2 не пройден')
                                    
                                    print(f"[{pair}] ❌ Сигнал {signal_type} @ {level['price']} для нового уровня ЗАБЛОКИРОВАН экранами Элдера: {blocked_reason}")
                                    logger.info(f"[{pair}] Сигнал для нового уровня заблокирован: {blocked_reason}, детали: {screens_details}")
                                    continue  # Пропускаем генерацию сигнала
                                
                                print(f"[{pair}] ✅ Сигнал {signal_type} @ {level['price']} для нового уровня прошел все экраны Элдера")
                                
                                stop_loss_percent = 0.004  # 0.4% (обновлено согласно настройкам)
                                if signal_type == 'LONG':
                                    stop_loss = level['price'] * (1 - stop_loss_percent)
                                else:
                                    stop_loss = level['price'] * (1 + stop_loss_percent)
                                
                                signal_data = {
                                    'pair': pair,
                                    'signal_type': signal_type,
                                    'level_price': level['price'],
                                    'entry_price': level['price'],
                                    'current_price': current_price,
                                    'stop_loss': round(stop_loss, 4),
                                    '1h_trend': trend_1h,
                                    'trend_direction': trend_1h.split('_')[0] if '_' in trend_1h else 'UNKNOWN',
                                    'trend_strength': trend_1h.split('_')[1] if '_' in trend_1h else 'UNKNOWN',
                                    'level_type': level['type'],
                                    'test_count': 1,
                                    'status': 'ACTIVE',
                                    'timestamp': datetime.now().isoformat(),
                                    'notes': f"Сигнал {signal_type} на новом уровне {level['type']} @ {level['price']} (пробой: {is_breakthrough_new}, касание: {is_touching_new}, тренд: {trend_1h})",
                                    'elder_screens_metadata': screens_details  # Сохраняем детали проверок экранов
                                }
                                
                                print(f"[{pair}] ГЕНЕРИРУЕМ СИГНАЛ для нового уровня {signal_type} @ {level['price']}")
                                signal_saved = signal_manager.save_signal(signal_data)
                                signals.append(signal_data)
                                
                                # Логируем результаты проверок экранов (после сохранения сигнала)
                                if signal_saved:
                                    try:
                                        from core.trading.live_trade_logger import log_signal_event
                                        from core.database import init_database, SessionLocal
                                        from core.models import Signal, TradingPair
                                        if init_database():
                                            session = SessionLocal()
                                            try:
                                                # Находим только что сохраненный сигнал
                                                pair_obj = session.query(TradingPair).filter_by(symbol=pair).first()
                                                if pair_obj:
                                                    level_price = level['price']
                                                    price_tolerance = level_price * 0.001  # 0.1%
                                                    saved_signal = session.query(Signal).filter(
                                                        Signal.pair_id == pair_obj.id,
                                                        Signal.signal_type == signal_type,
                                                        abs(Signal.level_price - level_price) < price_tolerance
                                                    ).order_by(Signal.timestamp.desc()).first()
                                                    
                                                    if saved_signal:
                                                        # Логируем Экран 1
                                                        screen_1 = screens_details.get('screen_1', {})
                                                        if screen_1.get('passed'):
                                                            log_signal_event(
                                                                session, saved_signal.id,
                                                                f"Экран 1 пройден: BTC тренд={screens_details['screen_1']['checks'].get('btc_trend')}, тренд пары={screens_details['screen_1']['checks'].get('pair_trend', {}).get('trend')}",
                                                                event_type='SCREEN_1_RESULT',
                                                                status='PASSED',
                                                                details=screens_details['screen_1'],
                                                                commit=False
                                                            )
                                                        
                                                        # Логируем Экран 2
                                                        screen_2 = screens_details.get('screen_2', {})
                                                        if screen_2.get('passed'):
                                                            # Логируем отдельно каждую проверку Экран 2
                                                            # Направление подхода
                                                            if 'price_approach' in screen_2.get('checks', {}):
                                                                approach_details = screen_2['checks']['price_approach']
                                                                log_signal_event(
                                                                    session, saved_signal.id,
                                                                    f"Экран 2: Направление подхода корректно - {approach_details.get('direction', 'N/A')}",
                                                                    event_type='SCREEN_2_PRICE_APPROACH',
                                                                    status='PASSED',
                                                                    details=approach_details,
                                                                    commit=False
                                                                )
                                                            
                                                            # RSI
                                                            if 'rsi' in screen_2.get('checks', {}):
                                                                rsi_details = screen_2['checks']['rsi']
                                                                rsi_value = rsi_details.get('value')
                                                                if rsi_value is not None:
                                                                    log_signal_event(
                                                                        session, saved_signal.id,
                                                                        f"Экран 2: RSI={rsi_value:.2f} {'⚠️ предупреждение' if rsi_details.get('warning') else '✅ OK'}",
                                                                        event_type='SCREEN_2_RSI',
                                                                        status='WARNING' if rsi_details.get('warning') else 'PASSED',
                                                                        details=rsi_details,
                                                                        commit=False
                                                                    )
                                                            
                                                            # MACD
                                                            if 'macd' in screen_2.get('checks', {}):
                                                                macd_details = screen_2['checks']['macd']
                                                                log_signal_event(
                                                                    session, saved_signal.id,
                                                                    f"Экран 2: MACD={macd_details.get('macd', 0):.4f}, Signal={macd_details.get('signal', 0):.4f}, Histogram={macd_details.get('histogram', 0):.4f}",
                                                                    event_type='SCREEN_2_MACD',
                                                                    status='PASSED',
                                                                    details=macd_details,
                                                                    commit=False
                                                                )
                                                            
                                                            # Итоговый результат Экран 2
                                                            log_signal_event(
                                                                session, saved_signal.id,
                                                                f"Экран 2 пройден: все проверки пройдены",
                                                                event_type='SCREEN_2_RESULT',
                                                                status='PASSED',
                                                                details=screens_details['screen_2'],
                                                                commit=False
                                                            )
                                                        else:
                                                            # Логируем блокировку
                                                            blocked_reason = screen_2.get('blocked_reason', 'Неизвестная причина')
                                                            log_signal_event(
                                                                session, saved_signal.id,
                                                                f"Экран 2 заблокирован: {blocked_reason}",
                                                                event_type='SCREEN_2_OSCILLATOR_BLOCKED',
                                                                status='BLOCKED',
                                                                details=screens_details['screen_2'],
                                                                commit=False
                                                            )
                                                        
                                                        session.commit()
                                            finally:
                                                session.close()
                                    except Exception as e:
                                        logger.error(f"[{pair}] Ошибка логирования экранов Элдера для нового уровня: {e}")
                                
                                # НЕ УДАЛЯЕМ уровень после генерации сигнала - уровень может использоваться для отскока
                                # Помечаем только как использованный
                                level['signal_generated'] = True
                                level['signal_timestamp'] = datetime.now().isoformat()
                                # Обновляем в БД
                                self._upsert_level_in_db(pair, level, timeframe='15m')
                                print(f"[{pair}] Сигнал для нового уровня сохранен, уровень помечен как использованный (оставляем для возможного отскока)")
            
            # НЕ удаляем уровни с signal_generated=True - они могут использоваться для отскока
            # Уровни остаются активными до пробития

            # Fallback: если после всех фильтров нет уровня — добавляем ближайший по тренду
            # ВАЖНО: создаем уровень близко к текущей цене, чтобы он был актуальным
            if not pair_levels:
                try:
                    print(f"[{pair}] ⚠️ Нет активных уровней, создаем fallback уровень...")
                    # Ищем локальные экстремумы на последних 40 свечах (более свежие данные)
                    lookback = min(len(candles_15m), 40)
                    window = candles_15m[-lookback:]
                    
                    if trend_1h.startswith('DOWN'):
                        # Для нисходящего тренда нужен ближайший максимум СВЕРХУ от текущей цены
                        current_price = candles_15m[-1]['close']
                        # Берем максимумы выше текущей цены
                        maxima_above = [(i, c['high']) for i, c in enumerate(window) if c['high'] > current_price]
                        if maxima_above:
                            # Берем ближайший максимум сверху
                            idx, high = min(maxima_above, key=lambda x: abs(x[1] - current_price))
                            src = window[idx]
                        else:
                            # Если нет выше, берем максимальный high из окна
                            idx, high = max(enumerate([c['high'] for c in window]), key=lambda x: x[1])
                            src = window[idx]
                        
                        fallback = {
                            'pair': pair,
                            'type': 'resistance',
                            'price': float(high),
                            'timestamp': src['timestamp'],
                            'volume': src.get('volume', 0),
                            'candle_length': src.get('high', 0) - src.get('low', 0),
                            'test_count': 1,
                            'created_at': datetime.now(timezone.utc).isoformat(),
                            'source': 'fallback_resistance',
                            'signal_generated': False,
                            'trend_context': trend_1h
                        }
                        print(f"[{pair}] ✅ Fallback уровень создан: resistance @ {fallback['price']} (текущая цена: {current_price})")
                    else:
                        # Для восходящего тренда нужен ближайший минимум СНИЗУ от текущей цены
                        current_price = candles_15m[-1]['close']
                        # Берем минимумы ниже текущей цены
                        minima_below = [(i, c['low']) for i, c in enumerate(window) if c['low'] < current_price]
                        if minima_below:
                            # Берем ближайший минимум снизу
                            idx, low = min(minima_below, key=lambda x: abs(x[1] - current_price))
                            src = window[idx]
                        else:
                            # Если нет ниже, берем минимальный low из окна
                            idx, low = min(enumerate([c['low'] for c in window]), key=lambda x: x[1])
                            src = window[idx]
                        
                        fallback = {
                            'pair': pair,
                            'type': 'support',
                            'price': float(low),
                            'timestamp': src['timestamp'],
                            'volume': src.get('volume', 0),
                            'candle_length': src.get('high', 0) - src.get('low', 0),
                            'test_count': 1,
                            'created_at': datetime.now(timezone.utc).isoformat(),
                            'source': 'fallback_support',
                            'signal_generated': False,
                            'trend_context': trend_1h
                        }
                        print(f"[{pair}] ✅ Fallback уровень создан: support @ {fallback['price']} (текущая цена: {current_price})")
                    
                    pair_levels.append(fallback)
                    self._upsert_level_in_db(pair, fallback, timeframe='15m')
                    print(f"[{pair}] ✅ Fallback уровень добавлен и сохранен в БД: {fallback['type']} @ {fallback['price']}")
                except Exception as e:
                    print(f"[{pair}] ❌ Ошибка добавления fallback уровня: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Сохраняем исправленные уровни обратно в файл
            active_levels[pair] = pair_levels
            signal_manager.save_active_levels(active_levels)
            
            print(f"[{pair}] ИТОГО: активных уровней: {len(pair_levels)}, сигналов: {len(signals)}")
            print(f"=== КОНЕЦ АНАЛИЗА {pair} ===\n")
            
            return {
                'pair': pair,
                'status': 'success',
                'trend_1h': trend_1h,
                'current_price': current_price,
                'price_change_24h': price_change_24h,
                'volume_24h': volume_24h,
                'active_levels': len(pair_levels),
                'signals_generated': len(signals),
                'potential_levels': len(potential_levels)
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа {pair}: {e}")
            print(f"[{pair}] ОШИБКА: {e}")
            return {'pair': pair, 'status': 'error', 'message': str(e)}
    
    async def analyze_all_pairs(self) -> Dict[str, Any]:
        """Анализирует все торговые пары"""
        analysis_results = {}
        total_signals = 0
        successful_pairs = 0
        
        # ГЛОБАЛЬНАЯ ОЧИСТКА ВСЕХ МЕРТВЫХ УРОВНЕЙ
        print("\n=== ГЛОБАЛЬНАЯ ОЧИСТКА МЕРТВЫХ УРОВНЕЙ ===")
        active_levels = signal_manager.load_active_levels()
        total_levels_before = sum(len(levels) for levels in active_levels.values())
        
        for pair in TRADING_PAIRS:
            if pair in active_levels and active_levels[pair]:
                try:
                    # Получаем данные для проверки уровней
                    candles_15m = await self.fetch_ohlcv(pair, '15m', 50)
                    if candles_15m:
                        current_price = candles_15m[-1]['close']
                        pair_levels = active_levels[pair]
                        
                        # Очищаем пробитые уровни (каждые 5 минут)
                        cleaned_levels = self.clean_broken_levels(pair, pair_levels, candles_15m, current_price)
                        active_levels[pair] = cleaned_levels
                        
                        if len(pair_levels) != len(cleaned_levels):
                            print(f"[GLOBAL CLEAN] {pair}: удалено {len(pair_levels) - len(cleaned_levels)} пробитых уровней")
                except Exception as e:
                    print(f"[GLOBAL CLEAN] Ошибка очистки {pair}: {e}")
        
        # Сохраняем очищенные уровни
        signal_manager.save_active_levels(active_levels)
        total_levels_after = sum(len(levels) for levels in active_levels.values())
        print(f"[GLOBAL CLEAN] Итого удалено: {total_levels_before - total_levels_after} мертвых уровней")
        print("=== КОНЕЦ ГЛОБАЛЬНОЙ ОЧИСТКИ ===\n")
        
        # Анализируем пары
        for pair in TRADING_PAIRS:
            try:
                result = await self.analyze_pair(pair)
                analysis_results[pair] = result
                if result.get('status') == 'success':
                    successful_pairs += 1
                if result.get('signals_generated', 0) > 0:
                    total_signals += result['signals_generated']
            except Exception as e:
                logger.error(f"Ошибка анализа {pair}: {e}")
                analysis_results[pair] = {'status': 'error', 'message': str(e)}
        
        return {
            'timestamp': datetime.now().isoformat(),
            'pairs_analyzed': successful_pairs,
            'total_pairs': len(TRADING_PAIRS),
            'total_signals': total_signals,
            'results': analysis_results
        }

# Глобальный экземпляр движка анализа
analysis_engine = AnalysisEngine() 