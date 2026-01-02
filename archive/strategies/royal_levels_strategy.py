"""
Торговая стратегия "Королевские уровни" v2.0
Автор: CryptoProject v0.01
Описание: Улучшенная реализация стратегии поиска и торговли на сильных уровнях поддержки/сопротивления
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

class RoyalLevelsStrategy:
    """
    Класс для реализации улучшенной стратегии "Королевские уровни"
    """
    
    def __init__(self, deposit: float = 10000, round_tolerance: float = 50, silent: bool = False,
                 rsi_short: float = 70, rsi_long: float = 30, atr_min: float = 0.0008, dist_min: float = 0.001,
                 tp_sl_ratio: float = 2.0, volume_window: int = 20, trend_filter: bool = True):
        # Параметры стратегии
        self.deposit = deposit
        self.risk_per_trade = 0.01  # 1% на сделку
        self.daily_stop_loss = 0.015  # 1.5% дневной стоп
        
        # Параметры индикаторов
        self.atr_period = 14
        self.rsi_period = 5
        self.supertrend_atr = 10
        self.supertrend_multiplier = 3
        self.volume_ma_period = 20
        self.ema_50_period = 50  # Изменено с 200 на 50 для более быстрой реакции
        
        # Параметры поиска уровней
        self.vp_window = 72  # 3 дня для 1H
        self.fractal_window = 5
        self.round_step_high = 100  # Шаг для "круглых" чисел > 10000
        self.round_step_low = 50    # Шаг для "круглых" чисел < 10000
        self.round_tolerance = round_tolerance  # Увеличен допуск округлости
        self.silent = silent
        
        # Состояние стратегии
        self.last_trade_time = None
        self.daily_pnl = 0.0
        self.trades_count = 0
        
        # Улучшенные параметры фильтров
        self.rsi_short = rsi_short      # Изменено с 65 на 70 - более строгий для SHORT
        self.rsi_long = rsi_long        # Изменено с 35 на 30 - более строгий для LONG
        self.atr_min = atr_min          # Изменено с 0.001 на 0.0008 - менее строгий
        self.dist_min = dist_min        # Изменено с 0.002 на 0.001 - менее строгий
        self.tp_sl_ratio = tp_sl_ratio  # Изменено с 1.7 на 2.0 - лучший риск/прибыль
        self.volume_window = volume_window
        self.trend_filter = trend_filter
        
        # Добавляем отслеживание касаний уровней
        self.level_touches = {}  # Словарь для отслеживания касаний каждого уровня
        self.touch_window = 20   # Окно для поиска касаний (в свечах)
        self.touch_tolerance = 0.002  # Допуск для определения касания (0.2%)
        
    def log(self, msg: str):
        # Убираем вывод в терминал - только в файл
        self.log_to_file(msg)
    
    def log_to_file(self, msg: str):
        """Логирование в файл (если доступно)"""
        # Этот метод будет переопределен в движке бэктеста
        pass
        
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Вычисление ATR (Average True Range)"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return true_range.rolling(window=period).mean()
    
    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Расчет RSI"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Расчет ADX (Average Directional Index)"""
        # True Range
        tr1 = df['high'] - df['low']
        tr2 = abs(df['high'] - df['close'].shift())
        tr3 = abs(df['low'] - df['close'].shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # Directional Movement
        dm_plus = df['high'] - df['high'].shift()
        dm_minus = df['low'].shift() - df['low']
        
        dm_plus = dm_plus.where((dm_plus > dm_minus) & (dm_plus > 0), 0)
        dm_minus = dm_minus.where((dm_minus > dm_plus) & (dm_minus > 0), 0)
        
        # Smoothed values
        tr_smooth = tr.rolling(window=period).mean()
        dm_plus_smooth = dm_plus.rolling(window=period).mean()
        dm_minus_smooth = dm_minus.rolling(window=period).mean()
        
        # Directional Indicators
        di_plus = 100 * (dm_plus_smooth / tr_smooth)
        di_minus = 100 * (dm_minus_smooth / tr_smooth)
        
        # Directional Index
        dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
        
        # ADX
        adx = dx.rolling(window=period).mean()
        return adx
    
    def calculate_supertrend(self, df: pd.DataFrame, atr_period: int = 10, multiplier: float = 3) -> Tuple[pd.Series, pd.Series]:
        """Вычисление SuperTrend индикатора"""
        atr = self.calculate_atr(df, atr_period)
        
        # Базовые линии
        basic_upper = (df['high'] + df['low']) / 2 + (multiplier * atr)
        basic_lower = (df['high'] + df['low']) / 2 - (multiplier * atr)
        
        # Инициализация
        final_upper = basic_upper.copy()
        final_lower = basic_lower.copy()
        supertrend = pd.Series(index=df.index, dtype=float)
        
        for i in range(1, len(df)):
            # Верхняя линия
            if basic_upper.iloc[i] < final_upper.iloc[i-1] or df['close'].iloc[i-1] > final_upper.iloc[i-1]:
                final_upper.iloc[i] = basic_upper.iloc[i]
            else:
                final_upper.iloc[i] = final_upper.iloc[i-1]
            
            # Нижняя линия
            if basic_lower.iloc[i] > final_lower.iloc[i-1] or df['low'].iloc[i-1] < final_lower.iloc[i-1]:
                final_lower.iloc[i] = basic_lower.iloc[i]
            else:
                final_lower.iloc[i] = final_lower.iloc[i-1]
            
            # SuperTrend
            if supertrend.iloc[i-1] == final_upper.iloc[i-1] and df['close'].iloc[i] <= final_upper.iloc[i]:
                supertrend.iloc[i] = final_upper.iloc[i]
            elif supertrend.iloc[i-1] == final_upper.iloc[i-1] and df['close'].iloc[i] > final_upper.iloc[i]:
                supertrend.iloc[i] = final_lower.iloc[i]
            elif supertrend.iloc[i-1] == final_lower.iloc[i-1] and df['close'].iloc[i] >= final_lower.iloc[i]:
                supertrend.iloc[i] = final_lower.iloc[i]
            elif supertrend.iloc[i-1] == final_lower.iloc[i-1] and df['close'].iloc[i] < final_lower.iloc[i]:
                supertrend.iloc[i] = final_upper.iloc[i]
        
        return supertrend, final_upper, final_lower
    
    def calculate_volume_profile(self, df: pd.DataFrame, window: int = 72) -> Dict:
        """Вычисление Volume Profile и поиск POC (Point of Control)"""
        if len(df) < window:
            return {'poc_level': None, 'poc_volume': 0, 'volume_profile': {}}
        
        # Берем последние window свечей
        recent_df = df.tail(window)
        
        # Создаем ценовые уровни (биннинг)
        price_range = recent_df['high'].max() - recent_df['low'].min()
        num_bins = 50
        bin_size = price_range / num_bins
        
        volume_profile = {}
        for i in range(num_bins):
            level = recent_df['low'].min() + i * bin_size
            volume_at_level = 0
            
            for _, row in recent_df.iterrows():
                if row['low'] <= level <= row['high']:
                    volume_at_level += row['volume']
            
            volume_profile[level] = volume_at_level
        
        # Находим POC (уровень с максимальным объемом)
        if volume_profile:
            poc_level = max(volume_profile, key=volume_profile.get)
            poc_volume = volume_profile[poc_level]
        else:
            poc_level = None
            poc_volume = 0
        
        return {
            'poc_level': poc_level,
            'poc_volume': poc_volume,
            'volume_profile': volume_profile
        }
    
    def detect_fractals(self, df: pd.DataFrame, window: int = 5) -> Tuple[pd.Series, pd.Series]:
        """Обнаружение фракталов (swing high/low)"""
        fractal_high = pd.Series(False, index=df.index)
        fractal_low = pd.Series(False, index=df.index)
        
        for i in range(window, len(df) - window):
            # Фрактал вверх
            if df['high'].iloc[i] == df['high'].iloc[i-window:i+window+1].max():
                fractal_high.iloc[i] = True
            
            # Фрактал вниз
            if df['low'].iloc[i] == df['low'].iloc[i-window:i+window+1].min():
                fractal_low.iloc[i] = True
        
        return fractal_high, fractal_low
    
    def is_round_number(self, price: float) -> bool:
        """Проверка, является ли цена 'круглым' числом с увеличенным допуском"""
        if price > 10000:
            nearest = round(price / self.round_step_high) * self.round_step_high
            return abs(price - nearest) < self.round_tolerance
        else:
            nearest = round(price / self.round_step_low) * self.round_step_low
            return abs(price - nearest) < self.round_tolerance
    
    def find_psychological_levels(self, current_price: float) -> List[float]:
        """Поиск психологических уровней"""
        levels = []
        
        # Уровни по 1000
        for i in range(int(current_price // 1000) - 2, int(current_price // 1000) + 3):
            level = i * 1000
            if level > 0:
                levels.append(level)
        
        # Уровни по 500
        for i in range(int(current_price // 500) - 2, int(current_price // 500) + 3):
            level = i * 500
            if level > 0:
                levels.append(level)
        
        # Уровни по 100
        for i in range(int(current_price // 100) - 2, int(current_price // 100) + 3):
            level = i * 100
            if level > 0:
                levels.append(level)
        
        return list(set(levels))  # Убираем дубликаты
    
    def find_fibonacci_levels(self, df: pd.DataFrame) -> List[float]:
        """Поиск уровней Фибоначчи"""
        if len(df) < 20:
            return []
        
        # Находим максимум и минимум за последние 20 свечей
        recent_high = df['high'].tail(20).max()
        recent_low = df['low'].tail(20).min()
        price_range = recent_high - recent_low
        
        # Уровни Фибоначчи
        fib_levels = [
            recent_low + price_range * 0.236,  # 23.6%
            recent_low + price_range * 0.382,  # 38.2%
            recent_low + price_range * 0.500,  # 50%
            recent_low + price_range * 0.618,  # 61.8%
            recent_low + price_range * 0.786,  # 78.6%
        ]
        
        return fib_levels
    
    def find_royal_levels(self, df_1h: pd.DataFrame) -> List[Dict]:
        """Улучшенный поиск королевских уровней с приоритетом фракталов"""
        if df_1h is None or df_1h.empty or len(df_1h) < self.vp_window:
            self.log(f'⚡ [find_royal_levels] Недостаточно данных для поиска уровней ({len(df_1h) if df_1h is not None else 0})')
            return []
        
        royal_levels = []
        current_price = df_1h['close'].iloc[-1]
        
        # 1. ФРАКТАЛЬНЫЕ УРОВНИ 1H (ВЫСШИЙ ПРИОРИТЕТ)
        fractal_levels = self.find_fractal_levels_1h(df_1h)
        royal_levels.extend(fractal_levels)
        
        # 2. Volume Profile POC
        vp_data = self.calculate_volume_profile(df_1h, self.vp_window)
        poc_level = vp_data['poc_level']
        
        if poc_level is not None:
            if self.is_round_number(poc_level):
                level_type = 'SUPPORT' if poc_level < current_price else 'RESISTANCE'
                royal_levels.append({
                    'level': poc_level,
                    'type': level_type,
                    'strength': 'HIGH',
                    'volume': vp_data['poc_volume'],
                    'source': 'POC',
                    'priority': 80
                })
        
        # 3. Психологические уровни
        psych_levels = self.find_psychological_levels(current_price)
        for level in psych_levels:
            if abs(level - current_price) / current_price < 0.05:  # В пределах 5%
                level_type = 'SUPPORT' if level < current_price else 'RESISTANCE'
                royal_levels.append({
                    'level': level,
                    'type': level_type,
                    'strength': 'MEDIUM',
                    'volume': 0,
                    'source': 'PSYCHOLOGICAL',
                    'priority': 60
                })
        
        # 4. Уровни Фибоначчи
        fib_levels = self.find_fibonacci_levels(df_1h)
        for level in fib_levels:
            if abs(level - current_price) / current_price < 0.03:  # В пределах 3%
                level_type = 'SUPPORT' if level < current_price else 'RESISTANCE'
                royal_levels.append({
                    'level': level,
                    'type': level_type,
                    'strength': 'MEDIUM',
                    'volume': 0,
                    'source': 'FIBONACCI',
                    'priority': 50
                })
        
        # Убираем дубликаты (близкие уровни)
        unique_levels = []
        for level in royal_levels:
            is_duplicate = False
            for existing in unique_levels:
                if abs(level['level'] - existing['level']) / existing['level'] < 0.001:  # 0.1%
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_levels.append(level)
        
        poc_display = f"{poc_level:.4f}" if poc_level else "0"
        fractal_count = len([l for l in unique_levels if 'FRACTAL' in l['source']])
        self.log(f'⚡ [find_royal_levels] На {df_1h.index[-1]} найдено уровней: {len(unique_levels)} (Фракталы: {fractal_count}, POC={poc_display})')
        
        return unique_levels
    
    def check_second_approach(self, df_5m: pd.DataFrame, royal_level: Dict, df_1h: pd.DataFrame = None) -> Dict:
        """Улучшенная проверка второго подхода к королевскому уровню с строгим фильтром тренда"""
        if df_5m is None or df_5m.empty or len(df_5m) < 30:
            self.log(f'⚡ [check_second_approach] Недостаточно данных для проверки входа')
            return {'signal': 'NO_SIGNAL', 'confidence': 0}
        
        # Определяем тренд (ОБЯЗАТЕЛЬНО)
        if df_1h is not None:
            trend = self.determine_trend(df_1h)
        else:
            trend = 'NEUTRAL'  # Если нет 1H данных
        
        # Отслеживаем касания уровня
        touch_data = self.track_level_touches(df_5m, royal_level)
        
        # Вычисляем индикаторы
        df_5m['atr'] = self.calculate_atr(df_5m, self.atr_period)
        df_5m['rsi'] = self.calculate_rsi(df_5m, self.rsi_period)
        df_5m['volume_ma'] = df_5m['volume'].rolling(window=self.volume_ma_period).mean()
        df_5m['volume_ratio'] = df_5m['volume'] / df_5m['volume_ma']
        
        latest = df_5m.iloc[-1]
        level = royal_level['level']
        level_type = royal_level['type']
        
        # Проверяем условия для входа (с учетом второго подхода)
        conditions_met = 0
        total_conditions = 5  # Увеличили количество условий
        
        # 1. Второй подход к уровню (ОБЯЗАТЕЛЬНОЕ условие)
        if touch_data['is_second_approach'] and touch_data['approach_quality'] >= 1.0:
            conditions_met += 1
            self.log(f'✅ Второй подход к уровню ${level:.2f} - {touch_data["touch_count"]} касаний, качество: {touch_data["approach_quality"]:.1f}')
        else:
            self.log(f'❌ Нет второго подхода к уровню ${level:.2f} - {touch_data["touch_count"]} касаний, качество: {touch_data["approach_quality"]:.1f}')
        
        # 2. ATR < 1.2% (смягчено)
        if latest['atr'] / latest['close'] < 0.012:
            conditions_met += 1
        
        # 3. RSI условия (смягчены)
        if level_type == 'SUPPORT' and latest['rsi'] < 40:  # Еще больше смягчили
            conditions_met += 1
        elif level_type == 'RESISTANCE' and latest['rsi'] > 60:  # Еще больше смягчили
            conditions_met += 1
        
        # 4. Объем > 110% среднего (еще больше смягчили)
        if latest['volume_ratio'] > 1.1:
            conditions_met += 1
        
        # 5. Цена приближается к уровню (ужесточено)
        price_distance = abs(latest['close'] - level) / level
        if price_distance < 0.008:  # В пределах 0.8% от уровня (ужесточили)
            conditions_met += 1
        
        confidence = (conditions_met / total_conditions) * 100
        
        # Добавляем бонус к уверенности за качество второго подхода
        if touch_data['is_second_approach']:
            confidence += touch_data['approach_quality'] * 20  # До 40% дополнительно
            confidence = min(confidence, 100)  # Максимум 100%
        
        self.log(f'⚡ [check_second_approach] {df_5m.index[-1]}: Условий выполнено {conditions_met}/5 (ATR={latest["atr"]/latest["close"]:.4f}, RSI={latest["rsi"]:.1f}, Объем={latest["volume_ratio"]:.2f}, dist={price_distance:.4f}, подходы={touch_data["touch_count"]})')
        
        # СТРОГАЯ ФИЛЬТРАЦИЯ ПО ТРЕНДУ
        signal = 'NO_SIGNAL'
        if touch_data['is_second_approach'] and conditions_met >= 3:
            # Только LONG в восходящем тренде
            if level_type == 'SUPPORT' and trend in ['STRONG_BULLISH', 'BULLISH']:
                signal = 'LONG'
                self.log(f'✅ [check_second_approach] LONG сигнал: поддержка в восходящем тренде ({trend})')
            # Только SHORT в нисходящем тренде
            elif level_type == 'RESISTANCE' and trend in ['STRONG_BEARISH', 'BEARISH']:
                signal = 'SHORT'
                self.log(f'✅ [check_second_approach] SHORT сигнал: сопротивление в нисходящем тренде ({trend})')
            else:
                self.log(f'⛔ [check_second_approach] Сигнал заблокирован: {level_type} в тренде {trend}')
                signal = 'NO_SIGNAL'
                confidence = 0
        
        # Применяем дополнительные фильтры только если сигнал прошел трендовую фильтрацию
        if signal != 'NO_SIGNAL':
            if not self.check_entry_filters(df_5m, -1, signal, latest['rsi'], 
                                          latest['atr'], latest['volume'], 
                                          price_distance, latest['close']):
                signal = 'NO_SIGNAL'
                confidence = 0
        
        return {
            'signal': signal,
            'confidence': confidence,
            'conditions_met': conditions_met,
            'price_distance': price_distance,
            'atr_ratio': latest['atr'] / latest['close'],
            'rsi': latest['rsi'],
            'volume_ratio': latest['volume_ratio'],
            'touch_data': touch_data  # Добавляем информацию о касаниях
        }
    
    def calculate_entry_exit(self, df_5m: pd.DataFrame, royal_level: Dict, signal: str) -> Dict:
        """Расчет точек входа и выхода"""
        if df_5m is None or df_5m.empty:
            return {}
        
        latest = df_5m.iloc[-1]
        level = royal_level['level']
        
        # ATR для расчета стоп-лосса
        atr = self.calculate_atr(df_5m, self.atr_period).iloc[-1]
        
        if signal == 'LONG':
            # Оптимизированная точка входа для LONG
            level_price = level
            current_price = latest['close']
            
            # Если цена близко к уровню (в пределах 0.2%), входим от текущей цены
            price_distance = abs(current_price - level_price) / level_price
            if price_distance <= 0.002:  # 0.2% (ужесточили)
                entry_price = current_price * 1.0005  # Небольшой отступ
            else:
                # Если цена далеко от уровня, входим от уровня с небольшим отступом
                entry_price = level_price * 1.0005
            
            # Улучшенный стоп-лосс - используем ATR * 1.5 для большего пространства
            stop_loss = entry_price - (atr * 1.5)
            # Используем улучшенное соотношение TP/SL
            take_profit_1 = entry_price + (abs(entry_price - stop_loss) * self.tp_sl_ratio)
            take_profit_2 = entry_price + (abs(entry_price - stop_loss) * self.tp_sl_ratio * 1.5)  # Уменьшаем второй TP
        else:  # SHORT
            # Оптимизированная точка входа для SHORT
            level_price = level
            current_price = latest['close']
            
            # Если цена близко к уровню (в пределах 0.2%), входим от текущей цены
            price_distance = abs(current_price - level_price) / level_price
            if price_distance <= 0.002:  # 0.2% (ужесточили)
                entry_price = current_price * 0.9995  # Небольшой отступ
            else:
                # Если цена далеко от уровня, входим от уровня с небольшим отступом
                entry_price = level_price * 0.9995
            
            # Улучшенный стоп-лосс - используем ATR * 1.5 для большего пространства
            stop_loss = entry_price + (atr * 1.5)
            # Используем улучшенное соотношение TP/SL
            take_profit_1 = entry_price - (abs(entry_price - stop_loss) * self.tp_sl_ratio)
            take_profit_2 = entry_price - (abs(entry_price - stop_loss) * self.tp_sl_ratio * 1.5)  # Уменьшаем второй TP
        
        # Размер позиции (1% от депозита)
        risk_amount = self.deposit * self.risk_per_trade
        position_size = risk_amount / abs(entry_price - stop_loss)
        
        return {
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'take_profit_1': take_profit_1,
            'take_profit_2': take_profit_2,
            'position_size': position_size,
            'risk_amount': risk_amount,
            'risk_reward_1': abs(take_profit_1 - entry_price) / abs(entry_price - stop_loss),
            'risk_reward_2': abs(take_profit_2 - entry_price) / abs(entry_price - stop_loss)
        }
    
    def check_trend_filter(self, df_1h: pd.DataFrame) -> str:
        """Улучшенная проверка фильтра тренда по 50 EMA на 1H"""
        if df_1h is None or df_1h.empty or len(df_1h) < self.ema_50_period:
            self.log(f'⚡ [check_trend_filter] Недостаточно данных для фильтра тренда')
            return 'NEUTRAL'  # Не блокируем сигналы
        
        # Вычисляем 50 EMA
        ema_50 = df_1h['close'].ewm(span=self.ema_50_period).mean().iloc[-1]
        current_price = df_1h['close'].iloc[-1]
        
        trend = 'BULLISH' if current_price > ema_50 else 'BEARISH' if current_price < ema_50 else 'NEUTRAL'
        self.log(f'⚡ [check_trend_filter] {df_1h.index[-1]}: Фильтр тренда = {trend} (Цена={current_price:.2f}, EMA50={ema_50:.2f})')
        return trend
    
    def analyze_strategy(self, data_dict: Dict) -> Dict:
        """Полный анализ улучшенной стратегии с строгим фильтром тренда"""
        results = {
            'royal_levels': [],
            'signals': [],
            'trend': 'NEUTRAL',
            'can_trade': False,
            'recommendations': []
        }
        
        # Проверяем наличие необходимых данных
        if '1h' not in data_dict or '5m' not in data_dict:
            results['recommendations'].append('Недостаточно данных для анализа')
            return results
        
        df_1h = data_dict['1h']
        df_5m = data_dict['5m']
        
        # 1. Определение тренда (КРИТИЧЕСКИ ВАЖНО)
        trend = self.determine_trend(df_1h)
        results['trend'] = trend
        
        # 2. Поиск королевских уровней с приоритизацией
        royal_levels = self.find_royal_levels(df_1h)
        royal_levels = self.prioritize_levels(royal_levels, trend)
        results['royal_levels'] = royal_levels
        
        if not royal_levels:
            results['recommendations'].append('Королевские уровни не найдены')
            return results
        
        # 3. Анализ сигналов для каждого уровня (строго по тренду)
        for level in royal_levels:
            signal_data = self.check_second_approach(df_5m, level, df_1h)
            
            if signal_data['signal'] != 'NO_SIGNAL':
                entry_exit = self.calculate_entry_exit(df_5m, level, signal_data['signal'])
                
                signal_info = {
                    'level': level,
                    'signal': signal_data,
                    'entry_exit': entry_exit,
                    'timestamp': df_5m.index[-1]
                }
                
                results['signals'].append(signal_info)
        
        # 4. Определяем возможность торговли
        if results['signals']:
            results['can_trade'] = True
            results['recommendations'].append(f'Найдено {len(results["signals"])} торговых сигналов в тренде {trend}')
        else:
            results['recommendations'].append(f'Торговые сигналы не найдены для тренда {trend}')
        
        return results
    
    def print_analysis(self, analysis: Dict):
        """Вывод результатов анализа"""
        self.log("\n" + "="*60)
        self.log("АНАЛИЗ СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ' v2.0")
        self.log("="*60)
        
        # Королевские уровни
        self.log(f"\n🔍 НАЙДЕННЫЕ КОРОЛЕВСКИЕ УРОВНИ: {len(analysis['royal_levels'])}")
        for i, level in enumerate(analysis['royal_levels'], 1):
            self.log(f"  {i}. Уровень: ${level['level']:,.2f}")
            self.log(f"     Тип: {level['type']}")
            self.log(f"     Сила: {level['strength']}")
            self.log(f"     Источник: {level['source']}")
            if level['volume'] > 0:
                self.log(f"     Объем: {level['volume']:,.0f}")
        
        # Тренд
        self.log(f"\n📈 ТРЕНД (EMA50+EMA200+ADX 1H): {analysis['trend']}")
        
        # Торговые сигналы
        self.log(f"\n🎯 ТОРГОВЫЕ СИГНАЛЫ: {len(analysis['signals'])}")
        for i, signal in enumerate(analysis['signals'], 1):
            level = signal['level']
            signal_data = signal['signal']
            entry_exit = signal['entry_exit']
            touch_data = signal_data.get('touch_data', {})
            
            self.log(f"  {i}. {signal_data['signal']} на уровне ${level['level']:,.2f}")
            self.log(f"     Уверенность: {signal_data['confidence']:.1f}%")
            self.log(f"     Касания уровня: {touch_data.get('touch_count', 0)} (качество: {touch_data.get('approach_quality', 0):.1f})")
            self.log(f"     Вход: ${entry_exit['entry_price']:,.2f}")
            self.log(f"     Стоп: ${entry_exit['stop_loss']:,.2f}")
            self.log(f"     TP1: ${entry_exit['take_profit_1']:,.2f} (R:R = {entry_exit['risk_reward_1']:.2f})")
            self.log(f"     TP2: ${entry_exit['take_profit_2']:,.2f} (R:R = {entry_exit['risk_reward_2']:.2f})")
            self.log(f"     Размер позиции: {entry_exit['position_size']:.4f}")
        
        # Рекомендации
        self.log(f"\n💡 РЕКОМЕНДАЦИИ:")
        for rec in analysis['recommendations']:
            self.log(f"  • {rec}")
        
        self.log("="*60) 

    def check_entry_filters(self, df, idx, direction, rsi, atr, volume, dist, close):
        """Проверка дополнительных фильтров для входа в позицию"""
        # Фильтр по RSI
        if direction == 'SHORT' and rsi <= self.rsi_short:
            filter_msg = f"⛔ Отклонено: RSI {rsi:.1f} <= {self.rsi_short} для SHORT"
            self.log_to_file(filter_msg)
            return False
        if direction == 'LONG' and rsi >= self.rsi_long:
            filter_msg = f"⛔ Отклонено: RSI {rsi:.1f} >= {self.rsi_long} для LONG"
            self.log_to_file(filter_msg)
            return False
        # Фильтр по объёму (смягченный)
        avg_volume = df['volume'].rolling(window=self.volume_window).mean().iloc[idx]
        volume_threshold = avg_volume * 0.8  # Снижаем требование до 80% от среднего
        if volume < volume_threshold:
            filter_msg = f"⛔ Отклонено: Объём {volume:.2f} < {volume_threshold:.2f} (80% от среднего {avg_volume:.2f})"
            self.log_to_file(filter_msg)
            return False
        # Фильтр по ATR
        if atr < self.atr_min:
            filter_msg = f"⛔ Отклонено: ATR {atr:.4f} < {self.atr_min}"
            self.log_to_file(filter_msg)
            return False
        # Фильтр по расстоянию до уровня
        if dist < self.dist_min:
            filter_msg = f"⛔ Отклонено: dist {dist:.4f} < {self.dist_min}"
            self.log_to_file(filter_msg)
            return False
        # Фильтр по тренду (EMA50 - более быстрая реакция)
        if self.trend_filter:
            ema50 = df['close'].ewm(span=50).mean().iloc[idx]
            # Смягченный фильтр - снижаем уверенность, но не блокируем полностью
            if direction == 'LONG' and close < ema50 * 0.995:  # Допускаем 0.5% ниже EMA
                filter_msg = f"⛔ Отклонено: LONG слишком далеко от EMA50 ({close:.2f} < {ema50*0.995:.2f})"
                self.log_to_file(filter_msg)
                return False
            if direction == 'SHORT' and close > ema50 * 1.005:  # Допускаем 0.5% выше EMA
                filter_msg = f"⛔ Отклонено: SHORT слишком далеко от EMA50 ({close:.2f} > {ema50*1.005:.2f})"
                self.log_to_file(filter_msg)
                return False
        return True 
    
    def track_level_touches(self, df_5m: pd.DataFrame, royal_level: Dict) -> Dict:
        """Отслеживание касаний уровня и определение второго подхода"""
        level = royal_level['level']
        level_type = royal_level['type']
        
        # Инициализируем отслеживание для нового уровня
        if level not in self.level_touches:
            self.level_touches[level] = {
                'touches': [],
                'last_touch_time': None,
                'touch_count': 0
            }
        
        # Проверяем последние свечи на касание уровня
        recent_data = df_5m.tail(self.touch_window)
        touches_found = []
        
        for i, (timestamp, row) in enumerate(recent_data.iterrows()):
            # Определяем касание уровня
            if level_type == 'SUPPORT':
                # Для поддержки - цена касается снизу
                if row['low'] <= level * (1 + self.touch_tolerance) and row['high'] >= level * (1 - self.touch_tolerance):
                    touches_found.append({
                        'time': timestamp,
                        'price': row['close'],
                        'type': 'support_touch',
                        'strength': abs(row['low'] - level) / level  # Чем ближе, тем сильнее
                    })
            else:  # RESISTANCE
                # Для сопротивления - цена касается сверху
                if row['high'] >= level * (1 - self.touch_tolerance) and row['low'] <= level * (1 + self.touch_tolerance):
                    touches_found.append({
                        'time': timestamp,
                        'price': row['close'],
                        'type': 'resistance_touch',
                        'strength': abs(row['high'] - level) / level  # Чем ближе, тем сильнее
                    })
        
        # Обновляем историю касаний
        current_touches = self.level_touches[level]['touches']
        
        # Добавляем новые касания (если их еще нет)
        for touch in touches_found:
            # Проверяем, не было ли уже такого касания
            is_new = True
            for existing in current_touches:
                if abs((touch['time'] - existing['time']).total_seconds()) < 300:  # 5 минут
                    is_new = False
                    break
            
            if is_new:
                current_touches.append(touch)
                self.level_touches[level]['touch_count'] += 1
                self.level_touches[level]['last_touch_time'] = touch['time']
        
        # Очищаем старые касания (старше 24 часов)
        current_time = df_5m.index[-1]
        current_touches = [t for t in current_touches 
                          if (current_time - t['time']).total_seconds() < 86400]  # 24 часа
        
        self.level_touches[level]['touches'] = current_touches
        
        # Определяем, является ли это вторым подходом
        is_second_approach = len(current_touches) >= 2
        
        # Валидируем время между касаниями
        timing_valid = self.validate_touch_timing(current_touches)
        
        # Анализируем качество второго подхода
        approach_quality = 0
        if is_second_approach and timing_valid:
            # Проверяем, что последнее касание произошло недавно (в последних 5 свечах)
            latest_touch = max(current_touches, key=lambda x: x['time'])
            time_since_touch = (current_time - latest_touch['time']).total_seconds() / 300  # в 5-минутных свечах
            
            if time_since_touch <= 5:  # Касание произошло в последние 25 минут
                approach_quality = 1
                
                # Дополнительные критерии качества
                if len(current_touches) >= 3:
                    approach_quality += 0.5  # Бонус за множественные касания
                
                # Анализируем качество отскока
                bounce_data = self.analyze_bounce_quality(df_5m, level, level_type)
                if bounce_data['quality'] in ['GOOD', 'EXCELLENT']:
                    approach_quality += 0.5  # Бонус за качественный отскок
                    if bounce_data['volume_confirmation']:
                        approach_quality += 0.3  # Дополнительный бонус за объем
        
        return {
            'is_second_approach': is_second_approach,
            'approach_quality': approach_quality,
            'touch_count': len(current_touches),
            'last_touch_time': self.level_touches[level]['last_touch_time'],
            'touches': current_touches
        } 

    def determine_trend(self, df_1h: pd.DataFrame) -> str:
        """Определение тренда с использованием EMA50, EMA200 и ADX"""
        if df_1h is None or df_1h.empty or len(df_1h) < 200:
            self.log(f'⚡ [determine_trend] Недостаточно данных для определения тренда')
            return 'NEUTRAL'
        
        # Вычисляем индикаторы
        ema_50 = df_1h['close'].ewm(span=50).mean().iloc[-1]
        ema_200 = df_1h['close'].ewm(span=200).mean().iloc[-1]
        current_price = df_1h['close'].iloc[-1]
        adx = self.calculate_adx(df_1h, period=14).iloc[-1]
        
        # Строгие условия с подтверждением силой тренда
        if current_price > ema_50 > ema_200 and adx > 25:
            trend = 'STRONG_BULLISH'
        elif current_price < ema_50 < ema_200 and adx > 25:
            trend = 'STRONG_BEARISH'
        elif current_price > ema_50 and ema_50 > ema_200:
            trend = 'BULLISH'
        elif current_price < ema_50 and ema_50 < ema_200:
            trend = 'BEARISH'
        else:
            trend = 'NEUTRAL'  # Боковик или слабый тренд
        
        self.log(f'⚡ [determine_trend] {df_1h.index[-1]}: Тренд = {trend} (Цена={current_price:.2f}, EMA50={ema_50:.2f}, EMA200={ema_200:.2f}, ADX={adx:.1f})')
        return trend 

    def find_fractal_levels_1h(self, df_1h: pd.DataFrame) -> List[Dict]:
        """Поиск фрактальных уровней на 1H (основные уровни для ретеста)"""
        if df_1h is None or df_1h.empty or len(df_1h) < 20:
            self.log(f'⚡ [find_fractal_levels_1h] Недостаточно данных для поиска фракталов')
            return []
        
        # Ищем фракталы на 1H
        fractal_high, fractal_low = self.detect_fractals(df_1h, window=5)
        
        # Берем только последние 5 фракталов
        recent_highs = df_1h[fractal_high].tail(5)['high'].tolist()
        recent_lows = df_1h[fractal_low].tail(5)['low'].tolist()
        
        levels = []
        current_price = df_1h['close'].iloc[-1]
        
        # Добавляем уровни сопротивления (фрактальные максимумы)
        for high in recent_highs:
            if abs(high - current_price) / current_price < 0.1:  # В пределах 10%
                levels.append({
                    'level': high,
                    'type': 'RESISTANCE',
                    'source': 'FRACTAL_HIGH_1H',
                    'strength': 'HIGH',
                    'volume': 0,
                    'priority': 100  # Высший приоритет
                })
        
        # Добавляем уровни поддержки (фрактальные минимумы)
        for low in recent_lows:
            if abs(low - current_price) / current_price < 0.1:  # В пределах 10%
                levels.append({
                    'level': low,
                    'type': 'SUPPORT',
                    'source': 'FRACTAL_LOW_1H', 
                    'strength': 'HIGH',
                    'volume': 0,
                    'priority': 100  # Высший приоритет
                })
        
        self.log(f'⚡ [find_fractal_levels_1h] Найдено фрактальных уровней: {len(levels)} (Highs: {len(recent_highs)}, Lows: {len(recent_lows)})')
        return levels 

    def analyze_bounce_quality(self, df_5m: pd.DataFrame, level: float, level_type: str) -> Dict:
        """Анализ качества отскока от уровня (минимум 0.5%)"""
        if df_5m is None or df_5m.empty or len(df_5m) < 5:
            return {'quality': 'POOR', 'strength': 0, 'volume_confirmation': False}
        
        # Анализируем последние 3 свечи после касания
        recent_candles = df_5m.tail(3)
        
        if level_type == 'SUPPORT':
            # Отскок вверх от поддержки
            bounce_high = recent_candles['high'].max()
            bounce_strength = (bounce_high - level) / level
            
            # Проверяем объем при отскоке
            avg_volume = df_5m['volume'].rolling(20).mean().iloc[-1]
            bounce_volume = recent_candles['volume'].mean()
            volume_confirmation = bounce_volume > avg_volume * 1.2
            
            if bounce_strength >= 0.005 and volume_confirmation:  # 0.5% + объем
                quality = 'EXCELLENT'
            elif bounce_strength >= 0.005:
                quality = 'GOOD'
            else:
                quality = 'POOR'
            
            return {
                'quality': quality,
                'strength': bounce_strength,
                'volume_confirmation': volume_confirmation,
                'bounce_high': bounce_high
            }
        
        elif level_type == 'RESISTANCE':
            # Отскок вниз от сопротивления
            bounce_low = recent_candles['low'].min()
            bounce_strength = (level - bounce_low) / level
            
            # Проверяем объем при отскоке
            avg_volume = df_5m['volume'].rolling(20).mean().iloc[-1]
            bounce_volume = recent_candles['volume'].mean()
            volume_confirmation = bounce_volume > avg_volume * 1.2
            
            if bounce_strength >= 0.005 and volume_confirmation:  # 0.5% + объем
                quality = 'EXCELLENT'
            elif bounce_strength >= 0.005:
                quality = 'GOOD'
            else:
                quality = 'POOR'
            
            return {
                'quality': quality,
                'strength': bounce_strength,
                'volume_confirmation': volume_confirmation,
                'bounce_low': bounce_low
            }
        
        return {'quality': 'POOR', 'strength': 0, 'volume_confirmation': False} 

    def validate_touch_timing(self, touches: List) -> bool:
        """Валидация времени между касаниями (30 мин - 24 часа)"""
        if len(touches) < 2:
            return False
        
        # Проверяем время между касаниями
        for i in range(1, len(touches)):
            time_diff = (touches[i]['time'] - touches[i-1]['time']).total_seconds()
            
            # Минимум 30 минут, максимум 24 часа
            if time_diff < 1800 or time_diff > 86400:  # 30 мин - 24 часа
                self.log(f'⛔ [validate_touch_timing] Некорректное время между касаниями: {time_diff/3600:.1f} часов')
                return False
        
        self.log(f'✅ [validate_touch_timing] Время между касаниями корректно: {len(touches)} касаний')
        return True
    
    def prioritize_levels(self, levels: List[Dict], trend: str) -> List[Dict]:
        """Приоритизация уровней с учетом тренда"""
        # Приоритет по источнику
        priority_map = {
            'FRACTAL_HIGH_1H': 100,
            'FRACTAL_LOW_1H': 100,
            'POC': 80,
            'PSYCHOLOGICAL': 60,
            'FIBONACCI': 50
        }
        
        # Бонус за соответствие тренду
        for level in levels:
            base_priority = priority_map.get(level['source'], 0)
            
            if trend in ['STRONG_BULLISH', 'BULLISH'] and level['type'] == 'SUPPORT':
                level['priority'] = base_priority + 20  # Бонус за LONG в восходящем
            elif trend in ['STRONG_BEARISH', 'BEARISH'] and level['type'] == 'RESISTANCE':
                level['priority'] = base_priority + 20  # Бонус за SHORT в нисходящем
            else:
                level['priority'] = base_priority
        
        # Сортируем по приоритету
        sorted_levels = sorted(levels, key=lambda x: x['priority'], reverse=True)
        
        top_3 = [f"{l['source']}({l['priority']})" for l in sorted_levels[:3]]
        self.log(f'⚡ [prioritize_levels] Приоритизировано {len(sorted_levels)} уровней. Топ-3: {top_3}')
        return sorted_levels 