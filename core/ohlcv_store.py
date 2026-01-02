"""
Модуль для работы с локальным хранилищем свечных данных (OHLCV)
С fallback на API биржи при отсутствии данных в БД
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Any
from sqlalchemy import and_, desc
from sqlalchemy.orm import Session
from core import database
from core.models import OHLCV, TradingPair
import ccxt

logger = logging.getLogger(__name__)


class OHLCVStore:
    """Класс для работы с локальным хранилищем свечных данных"""
    
    def __init__(self):
        self.exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'options': {
                'defaultType': 'future'
            }
        })
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Нормализует символ пары (BTC/USDT -> BTC)"""
        if '/' in symbol:
            return symbol.split('/')[0]
        return symbol
    
    def _timeframe_to_seconds(self, timeframe: str) -> int:
        """Конвертирует таймфрейм в секунды"""
        timeframe_map = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400
        }
        return timeframe_map.get(timeframe, 60)
    
    def _candle_to_dict(self, candle: OHLCV) -> Dict:
        """Конвертирует модель OHLCV в словарь"""
        return {
            'timestamp': int(candle.timestamp.timestamp() * 1000),
            'open': float(candle.open),
            'high': float(candle.high),
            'low': float(candle.low),
            'close': float(candle.close),
            'volume': float(candle.volume)
        }
    
    def get_ohlcv(
        self, 
        symbol: str, 
        timeframe: str, 
        limit: int = 200,
        since: Optional[int] = None
    ) -> List[Dict]:
        """
        Получает свечные данные из локального хранилища с fallback на API биржи
        
        Args:
            symbol: Торговая пара (например, 'BTC/USDT' или 'BTC')
            timeframe: Таймфрейм ('1m', '5m', '15m', '1h', '4h')
            limit: Количество свечей
            since: Timestamp начала (в миллисекундах), если None - берем последние N свечей
        
        Returns:
            Список свечей в формате [{'timestamp': ..., 'open': ..., 'high': ..., 'low': ..., 'close': ..., 'volume': ...}]
        """
        # Убеждаемся, что БД инициализирована
        if not database.init_database() or database.SessionLocal is None:
            logger.error("База данных не инициализирована")
            # Fallback на API биржи
            return self._fetch_from_exchange(symbol, timeframe, limit, since)
        
        db = database.SessionLocal()
        try:
            normalized_symbol = self._normalize_symbol(symbol)
            
            # Определяем временной диапазон
            if since:
                # Запрашиваем данные с определенного времени
                since_dt = datetime.fromtimestamp(since / 1000, tz=timezone.utc)
                end_dt = datetime.now(timezone.utc)
            else:
                # Запрашиваем последние N свечей
                tf_seconds = self._timeframe_to_seconds(timeframe)
                end_dt = datetime.now(timezone.utc)
                start_dt = end_dt - timedelta(seconds=tf_seconds * limit)
                since_dt = start_dt
            
            # Пытаемся получить данные из БД
            candles_db = db.query(OHLCV).filter(
                and_(
                    OHLCV.symbol == normalized_symbol,
                    OHLCV.timeframe == timeframe,
                    OHLCV.timestamp >= since_dt,
                    OHLCV.timestamp <= end_dt
                )
            ).order_by(OHLCV.timestamp.asc()).all()
            
            candles_list = [self._candle_to_dict(c) for c in candles_db]
            
            # ИСПРАВЛЕНИЕ: Проверяем наличие пропусков в данных и заполняем их
            if candles_list and len(candles_list) > 1:
                # Сортируем по timestamp для надежности
                candles_list.sort(key=lambda x: x['timestamp'])
                
                # Проверяем пропуски между свечами
                tf_seconds = self._timeframe_to_seconds(timeframe)
                tf_ms = tf_seconds * 1000
                gaps_found = []
                
                for i in range(len(candles_list) - 1):
                    current_ts = candles_list[i]['timestamp']
                    next_ts = candles_list[i + 1]['timestamp']
                    expected_next_ts = current_ts + tf_ms
                    
                    # Если разница больше чем 1.5 таймфрейма, считаем это пропуском
                    if next_ts - expected_next_ts > tf_ms * 1.5:
                        gap_start_ts = expected_next_ts
                        gap_end_ts = next_ts
                        gaps_found.append((gap_start_ts, gap_end_ts))
                
                # Заполняем обнаруженные пропуски
                if gaps_found:
                    logger.info(f"🔍 Обнаружено {len(gaps_found)} пропусков в данных для {symbol} {timeframe}, заполняем...")
                    for gap_start_ts, gap_end_ts in gaps_found:
                        try:
                            gap_start_dt = datetime.fromtimestamp(gap_start_ts / 1000, tz=timezone.utc)
                            gap_end_dt = datetime.fromtimestamp(gap_end_ts / 1000, tz=timezone.utc)
                            filled = self.fill_gaps(symbol, timeframe, gap_start_dt, gap_end_dt)
                            if filled > 0:
                                logger.info(f"✅ Заполнен пропуск: {filled} свечей")
                        except Exception as gap_error:
                            logger.warning(f"⚠️ Ошибка заполнения пропуска: {gap_error}")
                    
                    # После заполнения пропусков, перезапрашиваем данные из БД
                    candles_db = db.query(OHLCV).filter(
                        and_(
                            OHLCV.symbol == normalized_symbol,
                            OHLCV.timeframe == timeframe,
                            OHLCV.timestamp >= since_dt,
                            OHLCV.timestamp <= end_dt
                        )
                    ).order_by(OHLCV.timestamp.asc()).all()
                    candles_list = [self._candle_to_dict(c) for c in candles_db]
                    candles_list.sort(key=lambda x: x['timestamp'])
            
            # КРИТИЧНО: Всегда обновляем последнюю (текущую) свечу с биржи
            # Последняя свеча может быть еще не закрыта и должна обновляться в реальном времени
            if candles_list:
                try:
                    # Запрашиваем последнюю свечу с биржи для обновления текущей (незакрытой) свечи
                    latest_candles = self.exchange.fetch_ohlcv(symbol, timeframe, limit=1)
                    if latest_candles and len(latest_candles) > 0:
                        latest_candle = latest_candles[0]
                        latest_candle_dict = {
                            'timestamp': latest_candle[0],
                            'open': float(latest_candle[1]),
                            'high': float(latest_candle[2]),
                            'low': float(latest_candle[3]),
                            'close': float(latest_candle[4]),
                            'volume': float(latest_candle[5])
                        }
                        
                        # Обновляем последнюю свечу в БД
                        self._save_candles_to_db(db, normalized_symbol, timeframe, [latest_candle_dict])
                        
                        # Заменяем последнюю свечу в списке на свежую с биржи
                        last_candle_ts = candles_list[-1]['timestamp']
                        if latest_candle_dict['timestamp'] == last_candle_ts:
                            # Это та же свеча (еще не закрыта) - обновляем данные
                            candles_list[-1] = latest_candle_dict
                            logger.debug(f"🔄 Обновлена текущая свеча для {symbol} {timeframe}")
                        elif latest_candle_dict['timestamp'] > last_candle_ts:
                            # Появилась новая свеча - добавляем ее
                            candles_list.append(latest_candle_dict)
                            logger.debug(f"➕ Добавлена новая свеча для {symbol} {timeframe}")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось обновить последнюю свечу с биржи для {symbol} {timeframe}: {e}")
                    # Продолжаем с данными из БД, если не удалось обновить
            
            # Проверяем, достаточно ли данных
            if len(candles_list) >= limit:
                logger.debug(f"✅ Данные из БД для {symbol} {timeframe}: {len(candles_list)} свечей")
                # Возвращаем последние N свечей (самые свежие)
                return candles_list[-limit:]
            
            # Если данных недостаточно, дополняем из API биржи
            logger.info(f"⚠️ Недостаточно данных в БД для {symbol} {timeframe}: {len(candles_list)}/{limit}, запрашиваем с биржи")
            
            # Запрашиваем недостающие данные с биржи
            try:
                # ИСПРАВЛЕНИЕ: Если данных недостаточно, запрашиваем последние N свечей с биржи БЕЗ since
                # Это гарантирует получение исторических данных, а не только будущих
                # Если данных в БД мало (< 50% от запрошенного), запрашиваем полный набор с биржи
                if len(candles_list) < limit * 0.5:
                    # Запрашиваем полный набор свечей с биржи (последние N свечей)
                    logger.debug(f"📥 Запрашиваем {limit} свечей с биржи (данных в БД недостаточно)")
                    api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                else:
                    # Если данных достаточно много, дополняем только недостающие
                    if candles_list:
                        # Берем timestamp последней свечи из БД
                        last_ts = candles_list[-1]['timestamp']
                        api_since = last_ts + self._timeframe_to_seconds(timeframe) * 1000
                        # Запрашиваем недостающее количество
                        needed = limit - len(candles_list)
                        api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=api_since, limit=needed + 10)
                    else:
                        # Если данных нет вообще, запрашиваем с since_dt или без since
                        api_since = int(since_dt.timestamp() * 1000) if since else None
                        api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=api_since, limit=limit)
                
                if api_candles:
                    # Конвертируем формат биржи в наш формат
                    api_candles_dict = [
                        {
                            'timestamp': c[0],
                            'open': float(c[1]),
                            'high': float(c[2]),
                            'low': float(c[3]),
                            'close': float(c[4]),
                            'volume': float(c[5])
                        }
                        for c in api_candles
                    ]
                    
                    # Сохраняем новые свечи в БД
                    self._save_candles_to_db(db, normalized_symbol, timeframe, api_candles_dict)
                    
                    # Объединяем данные из БД и API
                    # Убираем дубликаты (если последняя свеча из БД совпадает с первой из API)
                    if candles_list and api_candles_dict:
                        if candles_list[-1]['timestamp'] == api_candles_dict[0]['timestamp']:
                            api_candles_dict = api_candles_dict[1:]
                    
                    candles_list.extend(api_candles_dict)
                    # Сортируем по timestamp после объединения
                    candles_list.sort(key=lambda x: x['timestamp'])
                    
                    logger.info(f"✅ Дополнено данными с биржи: {len(api_candles_dict)} свечей")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка запроса данных с биржи для {symbol} {timeframe}: {e}")
                # Возвращаем то, что есть в БД
                if candles_list:
                    return candles_list[:limit]
                # Если данных нет вообще, пробуем запросить напрямую с биржи без since
                try:
                    api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                    if api_candles:
                        api_candles_dict = [
                            {
                                'timestamp': c[0],
                                'open': float(c[1]),
                                'high': float(c[2]),
                                'low': float(c[3]),
                                'close': float(c[4]),
                                'volume': float(c[5])
                            }
                            for c in api_candles
                        ]
                        self._save_candles_to_db(db, normalized_symbol, timeframe, api_candles_dict)
                        return api_candles_dict[:limit]
                except Exception as e2:
                    logger.error(f"❌ Критическая ошибка получения данных для {symbol} {timeframe}: {e2}")
                    return []
            
            # КРИТИЧНО: Перед возвратом всегда обновляем последнюю свечу с биржи
            # Это гарантирует, что текущая (незакрытая) свеча всегда актуальна
            if candles_list:
                try:
                    latest_candles = self.exchange.fetch_ohlcv(symbol, timeframe, limit=1)
                    if latest_candles and len(latest_candles) > 0:
                        latest_candle = latest_candles[0]
                        latest_candle_dict = {
                            'timestamp': latest_candle[0],
                            'open': float(latest_candle[1]),
                            'high': float(latest_candle[2]),
                            'low': float(latest_candle[3]),
                            'close': float(latest_candle[4]),
                            'volume': float(latest_candle[5])
                        }
                        
                        # Обновляем последнюю свечу в БД
                        self._save_candles_to_db(db, normalized_symbol, timeframe, [latest_candle_dict])
                        
                        # Заменяем последнюю свечу в списке на свежую с биржи
                        last_candle_ts = candles_list[-1]['timestamp']
                        if latest_candle_dict['timestamp'] == last_candle_ts:
                            # Это та же свеча (еще не закрыта) - обновляем данные
                            candles_list[-1] = latest_candle_dict
                            logger.debug(f"🔄 Обновлена текущая свеча для {symbol} {timeframe} перед возвратом")
                        elif latest_candle_dict['timestamp'] > last_candle_ts:
                            # Появилась новая свеча - добавляем ее
                            candles_list.append(latest_candle_dict)
                            logger.debug(f"➕ Добавлена новая свеча для {symbol} {timeframe} перед возвратом")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось обновить последнюю свечу перед возвратом для {symbol} {timeframe}: {e}")
                    # Продолжаем с данными из БД, если не удалось обновить
            
            return candles_list[-limit:] if candles_list else []
        
        finally:
            if db:
                db.close()
                database.SessionLocal.remove()
    
    def _fetch_from_exchange(
        self, 
        symbol: str, 
        timeframe: str, 
        limit: int = 200,
        since: Optional[int] = None
    ) -> List[Dict]:
        """
        Fallback метод для получения данных напрямую с биржи
        Используется если БД недоступна
        """
        try:
            api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
            if api_candles:
                return [
                    {
                        'timestamp': c[0],
                        'open': float(c[1]),
                        'high': float(c[2]),
                        'low': float(c[3]),
                        'close': float(c[4]),
                        'volume': float(c[5])
                    }
                    for c in api_candles
                ]
        except Exception as e:
            logger.error(f"Ошибка получения данных с биржи для {symbol} {timeframe}: {e}")
        
        return []
    
    def _save_candles_to_db(
        self, 
        db: Session, 
        symbol: str, 
        timeframe: str, 
        candles: List[Dict]
    ) -> int:
        """
        Сохраняет свечи в БД (upsert - обновляет существующие, создает новые)
        
        КРИТИЧНО: Закрытые (исторические) свечи НЕ обновляются - они уже финальные.
        Обновляются только текущие (незакрытые) свечи.
        
        Returns:
            Количество сохраненных свечей
        """
        saved_count = 0
        now = datetime.now(timezone.utc)
        tf_seconds = self._timeframe_to_seconds(timeframe)
        
        try:
            for candle_dict in candles:
                timestamp_dt = datetime.fromtimestamp(
                    candle_dict['timestamp'] / 1000, 
                    tz=timezone.utc
                )
                
                # Определяем, закрыта ли свеча
                # Свеча закрыта, если прошло больше времени таймфрейма с момента ее начала
                candle_end_time = timestamp_dt + timedelta(seconds=tf_seconds)
                is_closed = now > candle_end_time
                
                # Проверяем, существует ли свеча
                existing = db.query(OHLCV).filter(
                    and_(
                        OHLCV.symbol == symbol,
                        OHLCV.timeframe == timeframe,
                        OHLCV.timestamp == timestamp_dt
                    )
                ).first()
                
                if existing:
                    # КРИТИЧНО: Обновляем только незакрытые свечи
                    # Закрытые свечи уже финальные и не должны изменяться
                    if not is_closed:
                        # Свеча еще не закрыта - обновляем данные
                        existing.open = candle_dict['open']
                        existing.high = candle_dict['high']
                        existing.low = candle_dict['low']
                        existing.close = candle_dict['close']
                        existing.volume = candle_dict['volume']
                        existing.updated_at = datetime.now(timezone.utc)
                        saved_count += 1
                    else:
                        # Свеча уже закрыта - НЕ обновляем, оставляем как есть
                        # Это гарантирует, что исторические данные остаются неизменными
                        logger.debug(f"⏸️ Свеча {symbol} {timeframe} {timestamp_dt} уже закрыта, не обновляем")
                else:
                    # Создаем новую свечу (независимо от того, закрыта она или нет)
                    new_candle = OHLCV(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp_dt,
                        open=candle_dict['open'],
                        high=candle_dict['high'],
                        low=candle_dict['low'],
                        close=candle_dict['close'],
                        volume=candle_dict['volume']
                    )
                    db.add(new_candle)
                    saved_count += 1
            
            db.commit()
            logger.debug(f"💾 Сохранено {saved_count} свечей для {symbol} {timeframe}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения свечей в БД для {symbol} {timeframe}: {e}")
            db.rollback()
            saved_count = 0
        
        return saved_count
    
    def update_current_candles(self, pairs: List[str], timeframes: List[str]) -> Dict[str, int]:
        """
        Обновляет текущие свечи для указанных пар и таймфреймов
        Вызывается периодически фоновым процессом
        
        Returns:
            Словарь с количеством обновленных свечей по парам
        """
        results = {}
        
        for pair in pairs:
            normalized_symbol = self._normalize_symbol(pair)
            pair_results = {}
            
            for timeframe in timeframes:
                try:
                    # Запрашиваем последнюю свечу с биржи
                    candles = self.exchange.fetch_ohlcv(pair, timeframe, limit=1)
                    
                    if candles and len(candles) > 0:
                        candle = candles[0]
                        candle_dict = {
                            'timestamp': candle[0],
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5])
                        }
                        
                        # Сохраняем в БД
                        if database.init_database() and database.SessionLocal is not None:
                            db = database.SessionLocal()
                            try:
                                saved = self._save_candles_to_db(db, normalized_symbol, timeframe, [candle_dict])
                                pair_results[timeframe] = saved
                            finally:
                                db.close()
                                database.SessionLocal.remove()
                        else:
                            pair_results[timeframe] = 0
                    else:
                        pair_results[timeframe] = 0
                        
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка обновления свечи для {pair} {timeframe}: {e}")
                    pair_results[timeframe] = 0
            
            results[pair] = pair_results
        
        return results
    
    def reload_historical_data_from_exchange(
        self,
        symbol: str,
        timeframe: str,
        days: int = 3,
        force_update_closed: bool = False
    ) -> Dict[str, Any]:
        """
        Перезагружает исторические данные с биржи и обновляет их в БД
        
        КРИТИЧНО: Эта функция используется для исправления неправильных данных в БД.
        По умолчанию обновляет только незакрытые свечи (как _save_candles_to_db).
        Если force_update_closed=True, то обновляет ВСЕ свечи, включая закрытые.
        
        Args:
            symbol: Торговая пара (например, 'BTC/USDT')
            timeframe: Таймфрейм ('1m', '5m', '15m', '1h', '4h')
            days: Количество дней истории для перезагрузки (по умолчанию 3 дня)
            force_update_closed: Если True, обновляет даже закрытые свечи (для исправления ошибок)
        
        Returns:
            Словарь с результатами: {'updated': int, 'created': int, 'skipped': int, 'errors': List}
        """
        if not database.init_database() or database.SessionLocal is None:
            return {'error': 'Database not initialized'}
        
        normalized_symbol = self._normalize_symbol(symbol)
        db = database.SessionLocal()
        results = {
            'updated': 0,
            'created': 0,
            'skipped': 0,
            'errors': []
        }
        
        try:
            # Вычисляем временной диапазон
            tf_seconds = self._timeframe_to_seconds(timeframe)
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=days)
            
            # Вычисляем количество свечей
            candles_per_day = {
                '1m': 1440,
                '5m': 288,
                '15m': 96,
                '1h': 24,
                '4h': 6
            }
            limit = candles_per_day.get(timeframe, 100) * days
            
            # Для больших объемов (30+ дней) разбиваем на батчи
            # Binance API имеет лимит ~1000 свечей за запрос
            MAX_CANDLES_PER_REQUEST = 1000
            use_batches = limit > MAX_CANDLES_PER_REQUEST
            
            logger.info(f"🔄 Перезагрузка исторических данных для {symbol} {timeframe}: {days} дней ({limit} свечей)")
            if use_batches:
                batches_count = (limit + MAX_CANDLES_PER_REQUEST - 1) // MAX_CANDLES_PER_REQUEST
                logger.info(f"   📦 Данные будут загружены батчами: {batches_count} запросов")
            
            # Запрашиваем данные с биржи
            since_ts = int(start_dt.timestamp() * 1000)
            
            if use_batches:
                # Загружаем батчами
                all_candles = []
                current_since = since_ts
                batch_num = 0
                
                while len(all_candles) < limit:
                    batch_num += 1
                    batch_limit = min(MAX_CANDLES_PER_REQUEST, limit - len(all_candles))
                    
                    try:
                        batch_candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=current_since, limit=batch_limit)
                        if not batch_candles:
                            break
                        
                        all_candles.extend(batch_candles)
                        
                        # Обновляем since для следующего батча
                        if len(batch_candles) > 0:
                            current_since = batch_candles[-1][0] + tf_seconds * 1000
                        else:
                            break
                        
                        # Небольшая задержка между батчами
                        import time
                        time.sleep(0.2)
                        
                        logger.debug(f"   📦 Батч {batch_num}: загружено {len(batch_candles)} свечей, всего {len(all_candles)}")
                        
                    except Exception as e:
                        logger.warning(f"   ⚠️ Ошибка загрузки батча {batch_num}: {e}")
                        break
                
                api_candles = all_candles[:limit]  # Ограничиваем до нужного количества
            else:
                # Загружаем одним запросом
                api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=since_ts, limit=limit)
            
            if not api_candles:
                logger.warning(f"⚠️ Не получены данные с биржи для {symbol} {timeframe}")
                return {'error': 'No data from exchange'}
            
            # Конвертируем в наш формат
            candles_dict = [
                {
                    'timestamp': c[0],
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                }
                for c in api_candles
            ]
            
            now = datetime.now(timezone.utc)
            
            # Сохраняем каждую свечу
            for candle_dict in candles_dict:
                try:
                    timestamp_dt = datetime.fromtimestamp(
                        candle_dict['timestamp'] / 1000,
                        tz=timezone.utc
                    )
                    
                    # Проверяем, закрыта ли свеча
                    candle_end_time = timestamp_dt + timedelta(seconds=tf_seconds)
                    is_closed = now > candle_end_time
                    
                    # Проверяем существование в БД
                    existing = db.query(OHLCV).filter(
                        and_(
                            OHLCV.symbol == normalized_symbol,
                            OHLCV.timeframe == timeframe,
                            OHLCV.timestamp == timestamp_dt
                        )
                    ).first()
                    
                    if existing:
                        # Обновляем только если:
                        # 1. Свеча не закрыта (обычное поведение)
                        # 2. ИЛИ force_update_closed=True (принудительное обновление)
                        if not is_closed or force_update_closed:
                            existing.open = candle_dict['open']
                            existing.high = candle_dict['high']
                            existing.low = candle_dict['low']
                            existing.close = candle_dict['close']
                            existing.volume = candle_dict['volume']
                            existing.updated_at = datetime.now(timezone.utc)
                            results['updated'] += 1
                        else:
                            results['skipped'] += 1
                    else:
                        # Создаем новую свечу
                        new_candle = OHLCV(
                            symbol=normalized_symbol,
                            timeframe=timeframe,
                            timestamp=timestamp_dt,
                            open=candle_dict['open'],
                            high=candle_dict['high'],
                            low=candle_dict['low'],
                            close=candle_dict['close'],
                            volume=candle_dict['volume']
                        )
                        db.add(new_candle)
                        results['created'] += 1
                        
                except Exception as e:
                    error_msg = f"Ошибка обработки свечи {candle_dict.get('timestamp')}: {e}"
                    logger.warning(f"⚠️ {error_msg}")
                    results['errors'].append(error_msg)
                    continue
            
            db.commit()
            logger.info(f"✅ Перезагрузка завершена: обновлено {results['updated']}, создано {results['created']}, пропущено {results['skipped']}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка перезагрузки данных для {symbol} {timeframe}: {e}", exc_info=True)
            db.rollback()
            results['error'] = str(e)
        finally:
            db.close()
            database.SessionLocal.remove()
        
        return results
    
    def detect_gaps(
        self,
        symbol: str,
        timeframe: str,
        max_gap_hours: int = 24
    ) -> List[Tuple[datetime, datetime]]:
        """
        Обнаруживает пропуски (gaps) в данных свечей
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            max_gap_hours: Максимальный размер пропуска для проверки (в часах)
        
        Returns:
            Список кортежей (start_time, end_time) для каждого пропуска
        """
        if not database.init_database() or database.SessionLocal is None:
            return []
        
        normalized_symbol = self._normalize_symbol(symbol)
        db = database.SessionLocal()
        gaps = []
        
        try:
            # Получаем все свечи, отсортированные по времени
            candles = db.query(OHLCV).filter(
                and_(
                    OHLCV.symbol == normalized_symbol,
                    OHLCV.timeframe == timeframe
                )
            ).order_by(OHLCV.timestamp.asc()).all()
            
            if len(candles) < 2:
                return []
            
            tf_seconds = self._timeframe_to_seconds(timeframe)
            expected_interval = timedelta(seconds=tf_seconds)
            
            # Проверяем промежутки между свечами
            for i in range(len(candles) - 1):
                current_ts = candles[i].timestamp
                next_ts = candles[i + 1].timestamp
                actual_interval = next_ts - current_ts
                
                # Если промежуток больше ожидаемого (с учетом небольшой погрешности)
                # и меньше max_gap_hours, это пропуск
                if actual_interval > expected_interval * 1.5:  # 50% запас на погрешность
                    gap_hours = actual_interval.total_seconds() / 3600
                    if gap_hours <= max_gap_hours:
                        gaps.append((current_ts, next_ts))
            
            logger.debug(f"🔍 Обнаружено {len(gaps)} пропусков для {symbol} {timeframe}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обнаружения пропусков для {symbol} {timeframe}: {e}")
        finally:
            db.close()
            database.SessionLocal.remove()
        
        return gaps
    
    def fill_gaps(
        self,
        symbol: str,
        timeframe: str,
        gap_start: datetime,
        gap_end: datetime
    ) -> int:
        """
        Заполняет пропуск в данных свечами с биржи
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            gap_start: Начало пропуска
            gap_end: Конец пропуска
        
        Returns:
            Количество загруженных свечей
        """
        try:
            # Вычисляем количество свечей в пропуске
            tf_seconds = self._timeframe_to_seconds(timeframe)
            gap_seconds = (gap_end - gap_start).total_seconds()
            expected_candles = int(gap_seconds / tf_seconds)
            
            # Запрашиваем данные с биржи для этого периода
            since_ts = int(gap_start.timestamp() * 1000)
            limit = min(expected_candles + 10, 1000)  # Небольшой запас, но не больше 1000
            
            api_candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=since_ts, limit=limit)
            
            if api_candles:
                # Фильтруем свечи, которые попадают в пропуск
                gap_candles = []
                gap_start_ts = int(gap_start.timestamp() * 1000)
                gap_end_ts = int(gap_end.timestamp() * 1000)
                
                for candle in api_candles:
                    candle_ts = candle[0]
                    if gap_start_ts <= candle_ts < gap_end_ts:
                        gap_candles.append({
                            'timestamp': candle_ts,
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5])
                        })
                
                if gap_candles:
                    # Сохраняем в БД
                    normalized_symbol = self._normalize_symbol(symbol)
                    if database.init_database() and database.SessionLocal is not None:
                        db = database.SessionLocal()
                        try:
                            saved = self._save_candles_to_db(db, normalized_symbol, timeframe, gap_candles)
                            logger.info(f"✅ Заполнен пропуск для {symbol} {timeframe}: {saved} свечей ({gap_start.strftime('%Y-%m-%d %H:%M')} - {gap_end.strftime('%Y-%m-%d %H:%M')})")
                            return saved
                        finally:
                            db.close()
                            database.SessionLocal.remove()
            
            return 0
            
        except Exception as e:
            logger.error(f"❌ Ошибка заполнения пропуска для {symbol} {timeframe}: {e}")
            return 0
    
    def check_and_fill_gaps(
        self,
        pairs: List[str],
        timeframes: List[str],
        max_gap_hours: int = 24
    ) -> Dict[str, int]:
        """
        Проверяет и заполняет пропуски для указанных пар и таймфреймов
        
        Returns:
            Словарь с количеством заполненных свечей по парам
        """
        results = {}
        total_filled = 0
        
        for pair in pairs:
            normalized_symbol = self._normalize_symbol(pair)
            pair_filled = 0
            
            for timeframe in timeframes:
                try:
                    # Обнаруживаем пропуски
                    gaps = self.detect_gaps(pair, timeframe, max_gap_hours)
                    
                    # Заполняем каждый пропуск
                    for gap_start, gap_end in gaps:
                        filled = self.fill_gaps(pair, timeframe, gap_start, gap_end)
                        pair_filled += filled
                        
                        # Небольшая задержка между запросами
                        import time
                        time.sleep(0.5)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка проверки пропусков для {pair} {timeframe}: {e}")
                    continue
            
            results[pair] = pair_filled
            total_filled += pair_filled
        
        logger.info(f"✅ Заполнение пропусков завершено: {total_filled} свечей")
        return results
    
    def ensure_historical_data(
        self,
        symbol: str,
        timeframe: str,
        days: int = 7
    ) -> int:
        """
        Обеспечивает наличие исторических данных для пары и таймфрейма
        
        Проверяет, есть ли достаточно данных в БД. Если нет - загружает с биржи.
        Также проверяет и заполняет пропуски.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            days: Количество дней истории, которое должно быть в БД
        
        Returns:
            Количество загруженных/дополненных свечей
        """
        if not database.init_database() or database.SessionLocal is None:
            return 0
        
        normalized_symbol = self._normalize_symbol(symbol)
        db = database.SessionLocal()
        total_loaded = 0
        
        try:
            # Проверяем, сколько свечей есть в БД
            candles_per_day = {
                '1m': 1440,
                '5m': 288,
                '15m': 96,
                '1h': 24,
                '4h': 6
            }
            expected_count = candles_per_day.get(timeframe, 100) * days
            
            current_count = db.query(OHLCV).filter(
                and_(
                    OHLCV.symbol == normalized_symbol,
                    OHLCV.timeframe == timeframe
                )
            ).count()
            
            # Если данных недостаточно, загружаем историю
            if current_count < expected_count * 0.8:  # 80% от ожидаемого
                logger.info(f"📥 Недостаточно данных для {symbol} {timeframe}: {current_count}/{expected_count}, загружаем историю...")
                
                limit = expected_count
                candles = self.get_ohlcv(symbol, timeframe, limit=limit)
                
                if candles:
                    total_loaded = len(candles) - current_count
                    logger.info(f"✅ Загружено {total_loaded} новых свечей для {symbol} {timeframe}")
            
            # Проверяем и заполняем пропуски
            gaps = self.detect_gaps(symbol, timeframe, max_gap_hours=days * 24)
            if gaps:
                logger.info(f"🔍 Обнаружено {len(gaps)} пропусков для {symbol} {timeframe}, заполняем...")
                for gap_start, gap_end in gaps:
                    filled = self.fill_gaps(symbol, timeframe, gap_start, gap_end)
                    total_loaded += filled
                    import time
                    time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"❌ Ошибка обеспечения данных для {symbol} {timeframe}: {e}")
        finally:
            db.close()
            database.SessionLocal.remove()
        
        return total_loaded


# Глобальный экземпляр хранилища
ohlcv_store = OHLCVStore()

