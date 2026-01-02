import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import logging

from core.config import settings

class SignalManager:
    def __init__(self):
        self.signals_dir = "signals"
        self.levels_dir = "levels"
        self._ensure_directories()
        self._setup_logging()
    
    def _ensure_directories(self):
        """Создает необходимые директории"""
        os.makedirs(self.signals_dir, exist_ok=True)
        os.makedirs(self.levels_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def _normalize_timestamp(self, value):
        """Приводит timestamp к ISO формату с таймзоной UTC."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.isoformat()
        if isinstance(value, str) and value:
            try:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
        return datetime.now(timezone.utc).isoformat()
    
    def _apply_result_labels(self, signal: Dict[str, Any]) -> None:
        """Обновляет текстовые поля результата исходя из calculated_result."""
        if signal.get('result') in ('profitable', 'losing'):
            return
        calc_result = signal.get('calculated_result')
        if calc_result is None:
            return
        if calc_result > 0:
            signal['result'] = 'profitable'
            signal.setdefault('status', 'CLOSED')
        elif calc_result < 0:
            signal['result'] = 'losing'
            signal.setdefault('status', 'CLOSED')
    
    def _price_is_close(self, a: float, b: float, tolerance: float = 0.0005) -> bool:
        if a is None or b is None:
            return False
        denominator = max(abs(a), abs(b), 1e-9)
        return abs(a - b) / denominator <= tolerance
    
    def _find_duplicate_signal_index(self, signals: List[Dict[str, Any]], new_signal: Dict[str, Any]) -> Optional[int]:
        pair = new_signal.get('pair')
        signal_type = new_signal.get('signal_type')
        level_price = new_signal.get('level_price')
        if not pair or level_price is None:
            return None
        for idx in range(len(signals) - 1, -1, -1):
            existing = signals[idx]
            if existing.get('pair') != pair:
                continue
            if existing.get('signal_type') != signal_type:
                continue
            status = (existing.get('status') or '').upper()
            if status not in ('OPEN', 'ACTIVE'):
                continue
            existing_level = existing.get('level_price')
            if self._price_is_close(existing_level, level_price):
                return idx
        return None
    
    def _prepare_signal_for_storage(self, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        signal = dict(signal_data)
        signal['timestamp'] = self._normalize_timestamp(signal.get('timestamp'))
        self._apply_result_labels(signal)
        if not signal.get('status'):
            signal['status'] = 'OPEN'
        return signal
    
    def _setup_logging(self):
        """Настраивает логирование после создания директорий с ротацией"""
        from core.logging_config import setup_analysis_logging
        setup_analysis_logging()
        global logger
        logger = logging.getLogger(__name__)
    
    def _ensure_directories(self):
        """Создает необходимые директории"""
        os.makedirs(self.signals_dir, exist_ok=True)
        os.makedirs(self.levels_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def save_signal(self, signal_data: Dict[str, Any]) -> bool:
        """Сохраняет новый сигнал в файл и в базу данных"""
        try:
            # Нормализуем данные сигнала
            normalized_signal = self._prepare_signal_for_storage(signal_data)
            
            # Сохраняем в JSON файл (для обратной совместимости)
            date_str = datetime.now().strftime("%Y_%m")
            filename = os.path.join(self.signals_dir, f"signals_{date_str}.json")
            
            # Загружаем существующие сигналы
            signals = self.load_signals_from_file(filename)
            duplicate_index = self._find_duplicate_signal_index(signals, normalized_signal)
            if duplicate_index is not None:
                logger.info(f"Обновляем существующий сигнал для {normalized_signal.get('pair')} @ {normalized_signal.get('level_price')}")
                signals[duplicate_index].update(normalized_signal)
            else:
                signals.append(normalized_signal)
            
            # Сохраняем обратно
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(signals, f, indent=2, default=str, ensure_ascii=False)
            
            # Обновляем сводную статистику
            self.update_signals_summary()
            
            # Сохраняем в базу данных СИНХРОННО (чтобы гарантировать сохранение)
            try:
                from core.database import init_database, SessionLocal
                from core.models import Signal, TradingPair
                from sqlalchemy import func
                from datetime import datetime as dt
                
                if init_database():
                    session = SessionLocal()
                    try:
                        # Находим пару
                        pair = session.query(TradingPair).filter_by(symbol=signal_data.get('pair')).first()
                        if not pair:
                            logger.error(f"Пара {signal_data.get('pair')} не найдена в БД")
                        else:
                            # КРИТИЧЕСКАЯ ПРОВЕРКА: проверяем, не существует ли уже АКТУАЛЬНЫЙ сигнал для этого уровня
                            level_price = float(signal_data.get('level_price', 0))
                            if level_price > 0:
                                # Используем строгую толерантность 0.1% для проверки дубликатов
                                price_tolerance = level_price * 0.001  # 0.1%
                                
                                # Максимальный возраст актуального сигнала (30 минут)
                                MAX_SIGNAL_AGE_SECONDS = 30 * 60
                                cutoff_time = dt.now(timezone.utc) - timedelta(seconds=MAX_SIGNAL_AGE_SECONDS)
                                
                                # Ищем только АКТУАЛЬНЫЕ сигналы (не старше 30 минут и не закрытые)
                                existing_signal = session.query(Signal).filter(
                                    Signal.pair_id == pair.id,
                                    func.abs(Signal.level_price - level_price) < price_tolerance,
                                    Signal.timestamp >= cutoff_time,  # Только свежие сигналы
                                    Signal.status == 'ACTIVE'  # Только активные сигналы
                                ).order_by(Signal.timestamp.desc()).first()
                                
                                if existing_signal:
                                    signal_age = (dt.now(timezone.utc) - existing_signal.timestamp.replace(tzinfo=timezone.utc)).total_seconds()
                                    logger.warning(f"⚠️ Актуальный сигнал для уровня {level_price} уже существует (ID: {existing_signal.id}, создан: {existing_signal.timestamp}, возраст: {signal_age/60:.1f} мин, статус: {existing_signal.status}). Пропускаем создание дубликата.")
                                    session.close()
                                    return True  # Возвращаем True, так как актуальный сигнал уже существует
                            
                            # Парсим timestamp
                            timestamp_str = signal_data.get('timestamp', dt.now().isoformat())
                            try:
                                if 'T' in timestamp_str:
                                    signal_timestamp = dt.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                else:
                                    signal_timestamp = dt.now()
                            except:
                                signal_timestamp = dt.now()
                            
                            # Парсим exit_timestamp если есть
                            exit_ts_value = signal_data.get('exit_timestamp')
                            if isinstance(exit_ts_value, str):
                                try:
                                    exit_ts_value = dt.fromisoformat(exit_ts_value.replace('Z', '+00:00'))
                                except Exception:
                                    exit_ts_value = None
                            
                            # Создаем сигнал только если дубликата нет
                            # Извлекаем данные Elder's Triple Screen System
                            elder_screens_metadata = signal_data.get('elder_screens_metadata', {})
                            
                            # Обрабатываем случаи, когда elder_screens_metadata отсутствует или пустой
                            if not elder_screens_metadata:
                                logger.warning(f"⚠️ Elder's Screens metadata отсутствует для сигнала {signal_data.get('pair')} @ {signal_data.get('level_price')}")
                                elder_screens_metadata = {
                                    'screen_1': {'passed': False, 'blocked_reason': 'Elder\'s Screens не были проверены при генерации сигнала'},
                                    'screen_2': {'passed': False, 'blocked_reason': 'Elder\'s Screens не были проверены при генерации сигнала'},
                                    'final_decision': 'NOT_CHECKED'
                                }
                            
                            screen_1 = elder_screens_metadata.get('screen_1', {})
                            screen_2 = elder_screens_metadata.get('screen_2', {})
                            
                            # Обеспечиваем, что passed всегда bool, а не None
                            screen_1_passed = screen_1.get('passed')
                            if screen_1_passed is None:
                                screen_1_passed = False
                                if not screen_1.get('blocked_reason'):
                                    screen_1['blocked_reason'] = 'Экран 1 не был проверен'
                            
                            screen_2_passed = screen_2.get('passed')
                            if screen_2_passed is None:
                                screen_2_passed = False
                                if not screen_2.get('blocked_reason'):
                                    screen_2['blocked_reason'] = 'Экран 2 не был проверен'
                            
                            signal = Signal(
                                pair_id=pair.id,
                                signal_type=signal_data.get('signal_type', 'LONG'),
                                level_price=float(signal_data.get('level_price', 0)),
                                entry_price=float(signal_data.get('entry_price', signal_data.get('level_price', 0))),
                                current_price=float(signal_data.get('current_price', 0)),
                                stop_loss=float(signal_data.get('stop_loss')) if signal_data.get('stop_loss') is not None else None,
                                timestamp=signal_timestamp,
                                trend_1h=signal_data.get('1h_trend'),
                                level_type=signal_data.get('level_type'),
                                test_count=int(signal_data.get('test_count', 1)),
                                status=signal_data.get('status', 'ACTIVE'),
                                level_timeframe=signal_data.get('timeframe'),
                                historical_touches=signal_data.get('historical_touches'),
                                live_test_count=signal_data.get('live_test_count'),
                                level_score=signal_data.get('level_score') or signal_data.get('score'),
                                distance_percent=signal_data.get('distance_percent'),
                                exit_price=signal_data.get('exit_price'),
                                exit_timestamp=exit_ts_value,
                                exit_reason=signal_data.get('exit_reason'),
                                notes=signal_data.get('notes'),
                                meta_data=signal_data,
                                # Elder's Triple Screen System
                                elder_screen_1_passed=screen_1_passed,
                                elder_screen_1_blocked_reason=screen_1.get('blocked_reason'),
                                elder_screen_2_passed=screen_2_passed,
                                elder_screen_2_blocked_reason=screen_2.get('blocked_reason'),
                                elder_screen_3_passed=None,  # Пока не используется
                                elder_screen_3_blocked_reason=None,
                                elder_screens_metadata=elder_screens_metadata
                            )
                            session.add(signal)
                            session.commit()
                            logger.info(f"✅ Сигнал сохранен в БД синхронно: {signal_data.get('pair')} {signal_data.get('signal_type')} @ {signal_data.get('level_price')} (ID: {signal.id})")
                            self._enqueue_demo_trade(signal.id)
                    except Exception as db_error:
                        session.rollback()
                        logger.error(f"❌ Ошибка синхронного сохранения сигнала в БД: {db_error}")
                        import traceback
                        traceback.print_exc()
                        # Пытаемся сохранить через Celery как fallback
                        try:
                            from tasks.signals_tasks import process_new_signal
                            process_new_signal.delay(signal_data)
                            logger.info(f"Сигнал отправлен в очередь Celery (fallback): {signal_data.get('pair')}")
                        except Exception as celery_error:
                            logger.warning(f"Не удалось отправить сигнал в Celery: {celery_error}")
                    finally:
                        session.close()
                else:
                    # Если БД недоступна, пытаемся через Celery
                    try:
                        from tasks.signals_tasks import process_new_signal
                        process_new_signal.delay(signal_data)
                        logger.info(f"Сигнал отправлен в очередь Celery (БД недоступна): {signal_data.get('pair')}")
                    except Exception as celery_error:
                        logger.warning(f"Не удалось отправить сигнал в Celery: {celery_error}")
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при сохранении сигнала в БД: {e}")
                import traceback
                traceback.print_exc()
            
            logger.info(f"СИГНАЛ: {signal_data.get('pair')} {signal_data.get('signal_type')} на уровне {signal_data.get('level_price')}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения сигнала: {e}")
            return False

    def _enqueue_demo_trade(self, signal_id: int) -> None:
        """Отправляет сигнал в Celery для автоматической live-торговли."""
        if not settings.DEMO_AUTO_TRADING_ENABLED:
            logger.debug("⏸️  Авто-торговля отключена, пропускаем signal_id=%s", signal_id)
            return
        try:
            from tasks.demo_trading_tasks import place_demo_order_for_signal
            task = place_demo_order_for_signal.delay(signal_id)
            logger.info("🚀 Ордер запланирован в Celery для live-торговли: signal_id=%s, task_id=%s", signal_id, task.id)
        except Exception as task_error:
            logger.exception("❌ Не удалось запланировать ордер для signal_id=%s: %s", signal_id, task_error)
    
    def save_signals_batch(self, signals: List[Dict[str, Any]]) -> bool:
        """Сохраняет пакет сигналов, группируя их по месяцам"""
        try:
            # Группируем сигналы по месяцам
            signals_by_month = {}
            
            for signal in signals:
                try:
                    normalized_signal = self._prepare_signal_for_storage(signal)
                    timestamp_str = normalized_signal.get('timestamp')
                    signal_date = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    date_str = signal_date.strftime("%Y_%m")
                    
                    if date_str not in signals_by_month:
                        signals_by_month[date_str] = []
                    
                    signals_by_month[date_str].append(normalized_signal)
                    
                except Exception as e:
                    logger.warning(f"Ошибка парсинга timestamp сигнала: {e}")
                    continue
            
            # Сохраняем каждый месяц в отдельный файл
            for date_str, month_signals in signals_by_month.items():
                filename = os.path.join(self.signals_dir, f"signals_{date_str}.json")
                
                # Загружаем существующие сигналы для этого месяца
                existing_signals = self.load_signals_from_file(filename)
                
                # Создаем словарь для быстрого поиска существующих сигналов
                existing_signals_dict = {}
                for existing_signal in existing_signals:
                    # Используем комбинацию полей как ключ
                    key = f"{existing_signal.get('pair')}_{existing_signal.get('timestamp')}_{existing_signal.get('signal_type')}"
                    existing_signals_dict[key] = existing_signal
                
                # Обновляем или добавляем сигналы
                for signal in month_signals:
                    key = f"{signal.get('pair')}_{signal.get('timestamp')}_{signal.get('signal_type')}"
                    existing_signals_dict[key] = signal
                
                # Преобразуем обратно в список
                updated_signals = list(existing_signals_dict.values())
                
                # Сохраняем файл
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(updated_signals, f, indent=2, default=str, ensure_ascii=False)
            
            # Обновляем сводную статистику
            self.update_signals_summary()
            
            logger.info(f"Пакетное обновление завершено: {len(signals)} сигналов")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка пакетного сохранения сигналов: {e}")
            return False
    
    def load_signals_from_file(self, filename: str) -> List[Dict]:
        """Загружает сигналы из файла"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            logger.error(f"Ошибка загрузки сигналов из {filename}: {e}")
            return []
    
    def load_recent_signals(self, limit: int = 50) -> List[Dict]:
        """Загружает последние сигналы"""
        all_signals = []
        
        try:
            # Получаем все файлы сигналов (исключаем summary файл)
            if not os.path.exists(self.signals_dir):
                return []
                
            signal_files = [f for f in os.listdir(self.signals_dir) 
                          if f.startswith("signals_") and f.endswith(".json") 
                          and f != "signals_summary.json"]
            signal_files.sort(reverse=True)  # Сначала новые
            
            for filename in signal_files:
                filepath = os.path.join(self.signals_dir, filename)
                signals = self.load_signals_from_file(filepath)
                
                # Проверяем, что signals это список
                if isinstance(signals, list):
                    all_signals.extend(signals)
                else:
                    logger.warning(f"Неверный формат данных в {filename}: {type(signals)}")
                
                if len(all_signals) >= limit:
                    break
            
            # Сортируем по времени и возвращаем последние
            all_signals.sort(key=lambda x: x.get("timestamp", "") if isinstance(x, dict) else "", reverse=True)
            return all_signals[:limit]
            
        except Exception as e:
            logger.error(f"Ошибка загрузки сигналов: {e}")
            return []
    
    def update_signals_summary(self):
        """Обновляет сводную статистику сигналов"""
        try:
            summary = {
                "total_signals": 0,
                "profitable_signals": 0,
                "losing_signals": 0,
                "winrate": 0.0,
                "last_updated": datetime.now().isoformat(),
                "signals_by_pair": {},
                "signals_by_month": {}
            }
            
            # Собираем статистику по всем файлам
            if not os.path.exists(self.signals_dir):
                return
                
            signal_files = [f for f in os.listdir(self.signals_dir) if f.startswith("signals_") and f.endswith(".json")]
            
            for filename in signal_files:
                filepath = os.path.join(self.signals_dir, filename)
                signals = self.load_signals_from_file(filepath)
                
                # Проверяем, что signals это список
                if not isinstance(signals, list):
                    continue
                
                for signal in signals:
                    # Проверяем, что signal это словарь
                    if not isinstance(signal, dict):
                        continue
                        
                    summary["total_signals"] += 1
                    
                    pair = signal.get("pair", "Unknown")
                    timestamp = signal.get("timestamp", "")
                    month = timestamp[:7] if timestamp else ""  # YYYY-MM
                    result = signal.get("result")
                    if not result:
                        calc = signal.get("calculated_result")
                        if isinstance(calc, (int, float)):
                            if calc > 0:
                                result = "profitable"
                            elif calc < 0:
                                result = "losing"
                    
                    # Статистика по парам
                    if pair not in summary["signals_by_pair"]:
                        summary["signals_by_pair"][pair] = {"total": 0, "profitable": 0, "losing": 0}
                    summary["signals_by_pair"][pair]["total"] += 1
                    
                    # Статистика по месяцам
                    if month and month not in summary["signals_by_month"]:
                        summary["signals_by_month"][month] = {"total": 0, "profitable": 0, "losing": 0}
                    if month:
                        summary["signals_by_month"][month]["total"] += 1
                    
                    # Подсчет результатов
                    if result == "profitable":
                        summary["profitable_signals"] += 1
                        summary["signals_by_pair"][pair]["profitable"] += 1
                        if month:
                            summary["signals_by_month"][month]["profitable"] += 1
                    elif result == "losing":
                        summary["losing_signals"] += 1
                        summary["signals_by_pair"][pair]["losing"] += 1
                        if month:
                            summary["signals_by_month"][month]["losing"] += 1
            
            # Вычисляем винрейт
            if summary["total_signals"] > 0:
                summary["winrate"] = round((summary["profitable_signals"] / summary["total_signals"]) * 100, 2)
            
            # Сохраняем сводку
            summary_file = os.path.join(self.signals_dir, "signals_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, default=str, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Ошибка обновления сводной статистики: {e}")
    
    def load_signals_summary(self) -> Dict:
        """Загружает сводную статистику сигналов"""
        try:
            summary_file = os.path.join(self.signals_dir, "signals_summary.json")
            if os.path.exists(summary_file):
                with open(summary_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"Ошибка загрузки сводной статистики: {e}")
            return {}
    
    def save_active_level(self, pair: str, level_data: Dict[str, Any]) -> bool:
        """Сохраняет активный уровень"""
        try:
            levels = self.load_active_levels()
            levels[pair] = level_data
            
            levels_file = os.path.join(self.levels_dir, "active_levels.json")
            with open(levels_file, 'w', encoding='utf-8') as f:
                json.dump(levels, f, indent=2, default=str, ensure_ascii=False)
            
            logger.info(f"УРОВЕНЬ: {pair} {level_data.get('type')} на {level_data.get('price')} (объем: {level_data.get('volume')})")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения уровня: {e}")
            return False
    
    def save_active_levels(self, levels_data: Dict[str, Any]) -> bool:
        """Сохраняет все активные уровни"""
        try:
            levels_file = os.path.join(self.levels_dir, "active_levels.json")
            with open(levels_file, 'w', encoding='utf-8') as f:
                json.dump(levels_data, f, indent=2, default=str, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения уровней: {e}")
            return False
    
    def load_active_levels(self) -> Dict:
        """Загружает активные уровни из БД (ОБЯЗАТЕЛЬНО, без fallback)"""
        try:
            from core.database import init_database, SessionLocal
            from core.models import TradingPair, Level
            from sqlalchemy.orm import joinedload
            
            if not init_database():
                logger.error("Не удалось инициализировать БД для загрузки уровней")
                return {}
            
            # Импортируем SessionLocal после инициализации
            from core.database import SessionLocal
            session = SessionLocal()
            
            try:
                # Получаем все активные уровни из БД
                levels = session.query(Level).options(
                    joinedload(Level.pair)
                ).filter(Level.is_active == True).all()
                
                # Группируем по парам в формате, ожидаемом analysis_engine
                levels_by_pair = {}
                for level in levels:
                    pair_symbol = level.pair.symbol if level.pair else 'UNKNOWN'
                    if pair_symbol not in levels_by_pair:
                        levels_by_pair[pair_symbol] = []
                    
                    meta = level.meta_data or {}
                    historical_touches = meta.get('historical_touches', level.test_count or 1)
                    live_tests = meta.get('live_test_count')
                    if live_tests is None:
                        live_tests = max((level.test_count or historical_touches) - historical_touches, 0)
                    effective_test_count = historical_touches + live_tests
                    
                    level_dict = {
                        'pair': pair_symbol,
                        'type': level.level_type,
                        'timeframe': meta.get('timeframe', level.timeframe),
                        'price': float(level.price),
                        'timestamp': int(level.first_touch.timestamp() * 1000) if level.first_touch else int(level.created_at.timestamp() * 1000),
                        'test_count': effective_test_count,
                        'historical_touches': historical_touches,
                        'live_test_count': live_tests,
                        'score': meta.get('score'),
                        'distance_percent': meta.get('distance_percent'),
                        'signal_generated': False,  # Будет обновляться при анализе
                        'created_at': level.created_at.isoformat() if level.created_at else datetime.now().isoformat(),
                        'last_test': int(level.last_touch.timestamp() * 1000) if level.last_touch else None,
                        'source': 'database'
                    }
                    # Добавляем остальные метаданные, не перезаписывая базовые поля
                    for key, value in meta.items():
                        if key not in level_dict:
                            level_dict[key] = value
                    levels_by_pair[pair_symbol].append(level_dict)
                
                logger.info(f"✅ Загружено {len(levels)} активных уровней из БД для {len(levels_by_pair)} пар")
                return levels_by_pair
                
            except Exception as db_error:
                logger.error(f"❌ Ошибка загрузки уровней из БД: {db_error}")
                import traceback
                traceback.print_exc()
                return {}
            finally:
                session.close()
                
        except ImportError as import_error:
            logger.error(f"❌ Ошибка импорта для загрузки уровней: {import_error}")
            return {}
        except Exception as e:
            logger.error(f"❌ Критическая ошибка загрузки активных уровней: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def remove_active_level(self, pair: str) -> bool:
        """Удаляет активный уровень для пары"""
        try:
            levels = self.load_active_levels()
            if pair in levels:
                del levels[pair]
                
                levels_file = os.path.join(self.levels_dir, "active_levels.json")
                with open(levels_file, 'w', encoding='utf-8') as f:
                    json.dump(levels, f, indent=2, default=str, ensure_ascii=False)
                
                logger.info(f"Удален активный уровень для {pair}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Ошибка удаления уровня: {e}")
            return False
    
    def check_level_validity(self, level_data: Dict[str, Any]) -> bool:
        """Проверяет, не устарел ли уровень"""
        try:
            current_time = datetime.now()
            level_time = datetime.fromisoformat(level_data["created_at"])
            
            # Уровень устарел только если прошло больше суток
            time_diff = current_time - level_time
            if time_diff.total_seconds() > 86400:  # 24 часа
                logger.info(f"Уровень {level_data.get('pair')} @ {level_data.get('price')} устарел (создан {time_diff.total_seconds()/3600:.1f} часов назад)")
                return False
            return True
            
        except Exception as e:
            logger.error(f"Ошибка проверки валидности уровня: {e}")
            return False
    
    def add_to_level_history(self, level_data: Dict[str, Any]) -> bool:
        """Добавляет уровень в историю"""
        try:
            history_file = os.path.join(self.levels_dir, "level_history.json")
            history = []
            
            if os.path.exists(history_file):
                with open(history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            
            history.append(level_data)
            
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, default=str, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления в историю уровней: {e}")
            return False

# Глобальный экземпляр менеджера сигналов
signal_manager = SignalManager() 