"""
Детектор ценовых фигур (Chart Patterns)
Детектирует крупные формации на графике: флаги, треугольники, голова и плечи и т.д.
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class ChartPatternDetector:
    """Детектор ценовых фигур на основе уровней поддержки/сопротивления"""
    
    def __init__(self):
        self.min_pattern_candles = 20  # Минимум свечей для фигуры
        self.max_pattern_candles = 200  # Максимум свечей
        self.tolerance = 0.002  # 0.2% толерантность для уровней
        self.min_pattern_height_pct = 0.01  # Минимум 1% высоты фигуры
    
    def _safe_timestamp_to_datetime(self, timestamp: float) -> datetime:
        """
        Безопасная конвертация timestamp в datetime с валидацией
        
        Args:
            timestamp: Unix timestamp в секундах
        
        Returns:
            datetime объект в UTC
        
        Raises:
            ValueError: если timestamp некорректный (0, отрицательный, или очень старый)
        """
        # Валидация timestamp
        if timestamp is None:
            raise ValueError("Timestamp не может быть None")
        
        # Проверяем, что timestamp не равен 0 или очень маленькому значению (эпоха Unix)
        if timestamp <= 0:
            raise ValueError(f"Некорректный timestamp: {timestamp} (должен быть > 0)")
        
        # Проверяем, что timestamp не слишком старый (до 2000 года считается некорректным)
        MIN_VALID_TIMESTAMP = 946684800  # 2000-01-01 00:00:00 UTC
        if timestamp < MIN_VALID_TIMESTAMP:
            raise ValueError(f"Некорректный timestamp: {timestamp} (слишком старый, до 2000 года)")
        
        # Проверяем, что timestamp не в миллисекундах (если > 1e10, значит в миллисекундах)
        if timestamp > 1e10:
            timestamp = timestamp / 1000
        
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (ValueError, OSError) as e:
            raise ValueError(f"Ошибка конвертации timestamp {timestamp} в datetime: {e}")
        
    def detect_all_patterns(
        self, 
        candles: List[Dict], 
        symbol: str, 
        timeframe: str
    ) -> List[Dict]:
        """
        Детектирует все возможные фигуры на графике
        
        Args:
            candles: Список свечей в формате [{'time': timestamp, 'open': float, 'high': float, 'low': float, 'close': float}, ...]
            symbol: Символ пары (например, 'BTC/USDT')
            timeframe: Таймфрейм ('1h', '4h', etc.)
        
        Returns:
            Список обнаруженных фигур
        """
        if not candles or len(candles) < self.min_pattern_candles:
            return []
        
        patterns = []
        
        try:
            # 1. Разворотные фигуры
            patterns.extend(self.detect_head_and_shoulders(candles, symbol, timeframe))
            patterns.extend(self.detect_double_top(candles, symbol, timeframe))
            patterns.extend(self.detect_double_bottom(candles, symbol, timeframe))
            patterns.extend(self.detect_wedges(candles, symbol, timeframe))
            
            # 2. Продолжение тренда
            patterns.extend(self.detect_flags(candles, symbol, timeframe))
            patterns.extend(self.detect_pennants(candles, symbol, timeframe))
            patterns.extend(self.detect_channels(candles, symbol, timeframe))
            patterns.extend(self.detect_triangles(candles, symbol, timeframe))
            
            # 3. Консолидация
            patterns.extend(self.detect_rectangles(candles, symbol, timeframe))
            
        except Exception as e:
            logger.error(f"Ошибка детекции фигур для {symbol} {timeframe}: {e}", exc_info=True)
        
        # Убеждаемся, что каждая фигура имеет symbol и timeframe
        for pattern in patterns:
            if 'symbol' not in pattern:
                pattern['symbol'] = symbol
            if 'timeframe' not in pattern:
                pattern['timeframe'] = timeframe
        
        return patterns
    
    def find_local_extrema(self, candles: List[Dict], lookback: int = 5) -> Tuple[List[Dict], List[Dict]]:
        """
        Находит локальные максимумы и минимумы
        
        Args:
            candles: Список свечей
            lookback: Количество свечей для сравнения
        
        Returns:
            (peaks, troughs) - списки пиков и впадин
        """
        peaks = []
        troughs = []
        
        for i in range(lookback, len(candles) - lookback):
            # Проверка на локальный максимум
            is_peak = True
            for j in range(i - lookback, i + lookback + 1):
                if j != i and candles[j]['high'] >= candles[i]['high']:
                    is_peak = False
                    break
            
            if is_peak:
                peaks.append({
                    'index': i,
                    'time': candles[i]['time'],
                    'price': candles[i]['high'],
                    'candle': candles[i]
                })
            
            # Проверка на локальный минимум
            is_trough = True
            for j in range(i - lookback, i + lookback + 1):
                if j != i and candles[j]['low'] <= candles[i]['low']:
                    is_trough = False
                    break
            
            if is_trough:
                troughs.append({
                    'index': i,
                    'time': candles[i]['time'],
                    'price': candles[i]['low'],
                    'candle': candles[i]
                })
        
        return peaks, troughs
    
    def find_swing_extrema(self, candles: List[Dict], swing_period: int = 7) -> Tuple[List[Dict], List[Dict]]:
        """
        Находит swing-экстремумы (более глобальные, чем локальные)
        Swing-high: максимум, который выше всех соседей в окне swing_period
        Swing-low: минимум, который ниже всех соседей в окне swing_period
        
        Args:
            candles: Список свечей
            swing_period: Размер окна для поиска swing-экстремумов (по умолчанию 7)
        
        Returns:
            (swing_highs, swing_lows) - списки swing-пиков и swing-впадин
        """
        swing_highs = []
        swing_lows = []
        
        for i in range(swing_period, len(candles) - swing_period):
            # Проверка на swing-high
            is_swing_high = True
            for j in range(i - swing_period, i + swing_period + 1):
                if j != i and candles[j]['high'] >= candles[i]['high']:
                    is_swing_high = False
                    break
            
            if is_swing_high:
                swing_highs.append({
                    'index': i,
                    'time': candles[i]['time'],
                    'price': candles[i]['high'],
                    'candle': candles[i]
                })
            
            # Проверка на swing-low
            is_swing_low = True
            for j in range(i - swing_period, i + swing_period + 1):
                if j != i and candles[j]['low'] <= candles[i]['low']:
                    is_swing_low = False
                    break
            
            if is_swing_low:
                swing_lows.append({
                    'index': i,
                    'time': candles[i]['time'],
                    'price': candles[i]['low'],
                    'candle': candles[i]
                })
        
        return swing_highs, swing_lows
    
    def linear_regression(self, points: List[Dict]) -> Tuple[float, float]:
        """
        Выполняет линейную регрессию для набора точек
        Возвращает (slope, intercept) для линии y = slope * x + intercept
        
        Args:
            points: Список точек вида [{'index': int, 'price': float}, ...]
        
        Returns:
            (slope, intercept) - наклон и точка пересечения с осью Y
        """
        if len(points) < 2:
            return 0.0, 0.0
        
        n = len(points)
        sum_x = sum(p['index'] for p in points)
        sum_y = sum(p['price'] for p in points)
        sum_xy = sum(p['index'] * p['price'] for p in points)
        sum_x2 = sum(p['index'] ** 2 for p in points)
        
        denominator = n * sum_x2 - sum_x ** 2
        if abs(denominator) < 1e-10:
            return 0.0, sum_y / n if n > 0 else 0.0
        
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        intercept = (sum_y - slope * sum_x) / n
        
        return slope, intercept
    
    def detect_head_and_shoulders(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует фигуру 'Голова и плечи'
        
        Алгоритм:
        1. Найти локальные максимумы
        2. Найти три пика: левое плечо, голова, правое плечо
        3. Проверить: голова > оба плеча
        4. Найти линию шеи (минимумы между пиками)
        5. Вычислить целевую цену
        """
        patterns = []
        
        try:
            peaks, troughs = self.find_local_extrema(candles, lookback=5)
            
            if len(peaks) < 3:
                return patterns
            
            # Ищем три последовательных пика
            for i in range(len(peaks) - 2):
                left_shoulder = peaks[i]
                head = peaks[i + 1]
                right_shoulder = peaks[i + 2]
                
                # Проверка условий Head and Shoulders
                head_price = head['price']
                left_price = left_shoulder['price']
                right_price = right_shoulder['price']
                
                # Голова должна быть выше обоих плеч
                if head_price <= left_price or head_price <= right_price:
                    continue
                
                # Плечи должны быть примерно на одном уровне (разница < 2%)
                shoulder_diff = abs(left_price - right_price) / head_price
                if shoulder_diff > 0.02:
                    continue
                
                # Найти линию шеи (минимумы между пиками)
                neckline_candles = candles[left_shoulder['index']:right_shoulder['index'] + 1]
                if not neckline_candles:
                    continue
                
                neckline_low = min(c['low'] for c in neckline_candles)
                neckline_high = max(c['low'] for c in neckline_candles)
                
                # Линия шеи должна быть относительно горизонтальной
                if (neckline_high - neckline_low) / neckline_low > 0.01:
                    continue
                
                neckline = (neckline_low + neckline_high) / 2
                
                # Вычислить целевую цену
                pattern_height = head_price - neckline
                target_price = neckline - pattern_height
                
                # Проверка минимальной высоты фигуры
                pattern_height_pct = pattern_height / head_price
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                # Вычислить надежность
                reliability = self._calculate_reliability(
                    pattern_height_pct,
                    shoulder_diff,
                    len(candles[left_shoulder['index']:right_shoulder['index']])
                )
                
                patterns.append({
                    'pattern_type': 'head_and_shoulders',
                    'pattern_category': 'reversal',
                    'direction': 'bearish',
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(left_shoulder['time']),
                    'end_time': self._safe_timestamp_to_datetime(right_shoulder['time']),
                    'support_level': neckline,
                    'resistance_level': head_price,
                    'neckline': neckline,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': right_shoulder['index'] - left_shoulder['index'],
                    'candles_count': right_shoulder['index'] - left_shoulder['index'] + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'left_shoulder': left_shoulder,
                        'head': head,
                        'right_shoulder': right_shoulder,
                        'neckline': neckline
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Head and Shoulders для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_double_top(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """Детектирует фигуру 'Двойная вершина'"""
        patterns = []
        
        try:
            peaks, troughs = self.find_local_extrema(candles, lookback=5)
            
            if len(peaks) < 2:
                return patterns
            
            # Ищем две вершины примерно на одном уровне
            for i in range(len(peaks) - 1):
                peak1 = peaks[i]
                peak2 = peaks[i + 1]
                
                # Проверка уровня вершин (разница < 1%)
                price_diff = abs(peak1['price'] - peak2['price']) / peak1['price']
                if price_diff > 0.01:
                    continue
                
                # Найти минимум между вершинами (линия шеи)
                trough_between = None
                for trough in troughs:
                    if peak1['index'] < trough['index'] < peak2['index']:
                        if trough_between is None or trough['price'] < trough_between['price']:
                            trough_between = trough
                
                if not trough_between:
                    continue
                
                neckline = trough_between['price']
                
                # Вычислить целевую цену
                pattern_height = peak1['price'] - neckline
                target_price = neckline - pattern_height
                
                # Проверка минимальной высоты
                pattern_height_pct = pattern_height / peak1['price']
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                reliability = self._calculate_reliability(pattern_height_pct, price_diff, peak2['index'] - peak1['index'])
                
                patterns.append({
                    'pattern_type': 'double_top',
                    'pattern_category': 'reversal',
                    'direction': 'bearish',
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(peak1['time']),
                    'end_time': self._safe_timestamp_to_datetime(peak2['time']),
                    'support_level': neckline,
                    'resistance_level': peak1['price'],
                    'neckline': neckline,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': peak2['index'] - peak1['index'],
                    'candles_count': peak2['index'] - peak1['index'] + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'peak1': peak1,
                        'peak2': peak2,
                        'trough': trough_between
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Double Top для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_double_bottom(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """Детектирует фигуру 'Двойное дно'"""
        patterns = []
        
        try:
            peaks, troughs = self.find_local_extrema(candles, lookback=5)
            
            if len(troughs) < 2:
                return patterns
            
            # Ищем два минимума примерно на одном уровне
            for i in range(len(troughs) - 1):
                trough1 = troughs[i]
                trough2 = troughs[i + 1]
                
                # Проверка уровня минимумов (разница < 1%)
                price_diff = abs(trough1['price'] - trough2['price']) / trough1['price']
                if price_diff > 0.01:
                    continue
                
                # Найти максимум между минимумами (линия шеи)
                peak_between = None
                for peak in peaks:
                    if trough1['index'] < peak['index'] < trough2['index']:
                        if peak_between is None or peak['price'] > peak_between['price']:
                            peak_between = peak
                
                if not peak_between:
                    continue
                
                neckline = peak_between['price']
                
                # Вычислить целевую цену
                pattern_height = neckline - trough1['price']
                target_price = neckline + pattern_height
                
                # Проверка минимальной высоты
                pattern_height_pct = pattern_height / trough1['price']
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                reliability = self._calculate_reliability(pattern_height_pct, price_diff, trough2['index'] - trough1['index'])
                
                patterns.append({
                    'pattern_type': 'double_bottom',
                    'pattern_category': 'reversal',
                    'direction': 'bullish',
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(trough1['time']),
                    'end_time': self._safe_timestamp_to_datetime(trough2['time']),
                    'support_level': trough1['price'],
                    'resistance_level': neckline,
                    'neckline': neckline,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': trough2['index'] - trough1['index'],
                    'candles_count': trough2['index'] - trough1['index'] + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'trough1': trough1,
                        'trough2': trough2,
                        'peak': peak_between
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Double Bottom для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_flags(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует фигуру 'Флаг'
        
        Алгоритм:
        1. Найти резкое движение (флагшток) - минимум 2% за 5-20 свечей
        2. Найти консолидацию после движения (5-30 свечей)
        3. Проверить параллельность линий консолидации
        4. Определить направление: если флагшток вверх → пробой вверх
        """
        patterns = []
        
        try:
            if len(candles) < 30:
                return patterns
            
            # Ищем резкие движения (флагштоки)
            for i in range(10, len(candles) - 20):
                # Проверяем движение вверх (бычий флаг)
                flagpole_start = i
                flagpole_end = min(i + 20, len(candles) - 1)
                
                # Вычисляем изменение цены за флагшток
                start_price = candles[flagpole_start]['close']
                end_price = candles[flagpole_end]['close']
                price_change = (end_price - start_price) / start_price
                
                # Флагшток должен быть минимум 2%
                if abs(price_change) < 0.02:
                    continue
                
                direction = 'bullish' if price_change > 0 else 'bearish'
                
                # Ищем консолидацию после флагштока (флаг)
                consolidation_start = flagpole_end + 1
                consolidation_end = min(consolidation_start + 30, len(candles) - 1)
                
                if consolidation_end - consolidation_start < 5:
                    continue
                
                # Получаем свечи консолидации
                consolidation_candles = candles[consolidation_start:consolidation_end + 1]
                
                # Находим локальные максимумы и минимумы в консолидации
                consolidation_peaks = []
                consolidation_troughs = []
                
                for j in range(1, len(consolidation_candles) - 1):
                    if consolidation_candles[j]['high'] > consolidation_candles[j-1]['high'] and \
                       consolidation_candles[j]['high'] > consolidation_candles[j+1]['high']:
                        consolidation_peaks.append({
                            'index': consolidation_start + j,
                            'price': consolidation_candles[j]['high']
                        })
                    if consolidation_candles[j]['low'] < consolidation_candles[j-1]['low'] and \
                       consolidation_candles[j]['low'] < consolidation_candles[j+1]['low']:
                        consolidation_troughs.append({
                            'index': consolidation_start + j,
                            'price': consolidation_candles[j]['low']
                        })
                
                # Нужно минимум 2 пика и 2 впадины для параллельных линий
                if len(consolidation_peaks) < 2 or len(consolidation_troughs) < 2:
                    continue
                
                # Проверяем параллельность линий (разница наклона < 0.5%)
                # Вычисляем средние уровни поддержки и сопротивления
                avg_resistance = sum(p['price'] for p in consolidation_peaks) / len(consolidation_peaks)
                avg_support = sum(t['price'] for t in consolidation_troughs) / len(consolidation_troughs)
                
                # Проверяем, что линии примерно параллельны (вариация < 1%)
                resistance_variance = max(p['price'] for p in consolidation_peaks) - min(p['price'] for p in consolidation_peaks)
                support_variance = max(t['price'] for t in consolidation_troughs) - min(t['price'] for t in consolidation_troughs)
                
                if resistance_variance / avg_resistance > 0.01 or support_variance / avg_support > 0.01:
                    continue
                
                # Вычисляем целевую цену (высота флагштока)
                flagpole_height = abs(end_price - start_price)
                target_price = avg_resistance + flagpole_height if direction == 'bullish' else avg_support - flagpole_height
                
                pattern_height_pct = (avg_resistance - avg_support) / avg_support
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                reliability = self._calculate_reliability(
                    pattern_height_pct,
                    max(resistance_variance / avg_resistance, support_variance / avg_support),
                    consolidation_end - consolidation_start
                )
                
                patterns.append({
                    'pattern_type': 'flag',
                    'pattern_category': 'continuation',
                    'direction': direction,
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(candles[flagpole_start]['time']),
                    'end_time': self._safe_timestamp_to_datetime(candles[consolidation_end]['time']),
                    'support_level': avg_support,
                    'resistance_level': avg_resistance,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': consolidation_end - consolidation_start,
                    'candles_count': consolidation_end - flagpole_start + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'flagpole_start': flagpole_start,
                        'flagpole_end': flagpole_end,
                        'consolidation_start': consolidation_start,
                        'consolidation_end': consolidation_end,
                        'peaks': consolidation_peaks,
                        'troughs': consolidation_troughs
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Flag для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_pennants(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует фигуру 'Вымпел'
        
        Алгоритм:
        1. Найти резкое движение (флагшток) - минимум 2% за 5-20 свечей
        2. Найти симметричный треугольник после движения (5-30 свечей)
        3. Проверить сходимость линий треугольника
        4. Определить направление: если флагшток вверх → пробой вверх
        """
        patterns = []
        
        try:
            if len(candles) < 30:
                return patterns
            
            # Ищем резкие движения (флагштоки)
            for i in range(10, len(candles) - 20):
                # Проверяем движение вверх (бычий вымпел)
                flagpole_start = i
                flagpole_end = min(i + 20, len(candles) - 1)
                
                # Вычисляем изменение цены за флагшток
                start_price = candles[flagpole_start]['close']
                end_price = candles[flagpole_end]['close']
                price_change = (end_price - start_price) / start_price
                
                # Флагшток должен быть минимум 2%
                if abs(price_change) < 0.02:
                    continue
                
                direction = 'bullish' if price_change > 0 else 'bearish'
                
                # Ищем симметричный треугольник после флагштока (вымпел)
                pennant_start = flagpole_end + 1
                pennant_end = min(pennant_start + 30, len(candles) - 1)
                
                if pennant_end - pennant_start < 5:
                    continue
                
                # Получаем свечи вымпела
                pennant_candles = candles[pennant_start:pennant_end + 1]
                
                # Находим локальные максимумы и минимумы в вымпеле
                pennant_peaks = []
                pennant_troughs = []
                
                for j in range(1, len(pennant_candles) - 1):
                    if pennant_candles[j]['high'] > pennant_candles[j-1]['high'] and \
                       pennant_candles[j]['high'] > pennant_candles[j+1]['high']:
                        pennant_peaks.append({
                            'index': pennant_start + j,
                            'price': pennant_candles[j]['high']
                        })
                    if pennant_candles[j]['low'] < pennant_candles[j-1]['low'] and \
                       pennant_candles[j]['low'] < pennant_candles[j+1]['low']:
                        pennant_troughs.append({
                            'index': pennant_start + j,
                            'price': pennant_candles[j]['low']
                        })
                
                # Нужно минимум 2 пика и 2 впадины для треугольника
                if len(pennant_peaks) < 2 or len(pennant_troughs) < 2:
                    continue
                
                # Проверяем сходимость линий (треугольник)
                # Первый и последний пики/впадины должны сходиться
                first_peak = pennant_peaks[0]['price']
                last_peak = pennant_peaks[-1]['price']
                first_trough = pennant_troughs[0]['price']
                last_trough = pennant_troughs[-1]['price']
                
                # Линии должны сходиться (первый пик > последнего, первый минимум < последнего)
                if first_peak <= last_peak or first_trough >= last_trough:
                    continue
                
                # Вычисляем средние уровни
                avg_resistance = (first_peak + last_peak) / 2
                avg_support = (first_trough + last_trough) / 2
                
                # Вычисляем целевую цену (высота флагштока)
                flagpole_height = abs(end_price - start_price)
                target_price = avg_resistance + flagpole_height if direction == 'bullish' else avg_support - flagpole_height
                
                pattern_height_pct = (avg_resistance - avg_support) / avg_support
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                # Симметричность треугольника (разница сходимости)
                convergence_diff = abs((first_peak - last_peak) - (last_trough - first_trough)) / avg_support
                reliability = self._calculate_reliability(
                    pattern_height_pct,
                    convergence_diff,
                    pennant_end - pennant_start
                )
                
                patterns.append({
                    'pattern_type': 'pennant',
                    'pattern_category': 'continuation',
                    'direction': direction,
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(candles[flagpole_start]['time']),
                    'end_time': self._safe_timestamp_to_datetime(candles[pennant_end]['time']),
                    'support_level': avg_support,
                    'resistance_level': avg_resistance,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': pennant_end - pennant_start,
                    'candles_count': pennant_end - flagpole_start + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'flagpole_start': flagpole_start,
                        'flagpole_end': flagpole_end,
                        'pennant_start': pennant_start,
                        'pennant_end': pennant_end,
                        'peaks': pennant_peaks,
                        'troughs': pennant_troughs
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Pennant для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_channels(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует фигуру 'Канал'
        
        Алгоритм:
        1. Найти параллельные линии поддержки и сопротивления
        2. Проверить, что цена движется внутри канала
        3. Определить направление канала (восходящий, нисходящий, горизонтальный)
        """
        patterns = []
        
        try:
            if len(candles) < 30:
                return patterns
            
            peaks, troughs = self.find_local_extrema(candles, lookback=5)
            
            if len(peaks) < 3 or len(troughs) < 3:
                return patterns
            
            # Ищем последовательные пики и впадины для построения канала
            for i in range(len(peaks) - 2):
                peak1 = peaks[i]
                peak2 = peaks[i + 1]
                peak3 = peaks[i + 2]
                
                # Находим соответствующие впадины между пиками
                trough1 = None
                trough2 = None
                
                for trough in troughs:
                    if peak1['index'] < trough['index'] < peak2['index']:
                        if trough1 is None or trough['price'] < trough1['price']:
                            trough1 = trough
                    if peak2['index'] < trough['index'] < peak3['index']:
                        if trough2 is None or trough['price'] < trough2['price']:
                            trough2 = trough
                
                if not trough1 or not trough2:
                    continue
                
                # Вычисляем наклоны линий поддержки и сопротивления
                # Линия сопротивления: через peak1 и peak2
                resistance_slope = (peak2['price'] - peak1['price']) / (peak2['index'] - peak1['index'])
                # Линия поддержки: через trough1 и trough2
                support_slope = (trough2['price'] - trough1['price']) / (trough2['index'] - trough1['index'])
                
                # Проверяем параллельность (разница наклонов < 0.1% от цены)
                slope_diff = abs(resistance_slope - support_slope)
                avg_price = (peak1['price'] + trough1['price']) / 2
                
                if slope_diff / avg_price > 0.001:
                    continue
                
                # Определяем направление канала
                if resistance_slope > 0.0001:
                    direction = 'bullish'
                elif resistance_slope < -0.0001:
                    direction = 'bearish'
                else:
                    direction = 'neutral'
                
                # Вычисляем средние уровни
                avg_resistance = (peak1['price'] + peak2['price'] + peak3['price']) / 3
                avg_support = (trough1['price'] + trough2['price']) / 2
                
                # Вычисляем целевую цену (высота канала)
                channel_height = avg_resistance - avg_support
                target_price = avg_resistance + channel_height if direction == 'bullish' else avg_support - channel_height
                
                pattern_height_pct = channel_height / avg_support
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                reliability = self._calculate_reliability(
                    pattern_height_pct,
                    slope_diff / avg_price,
                    peak3['index'] - peak1['index']
                )
                
                patterns.append({
                    'pattern_type': 'channel',
                    'pattern_category': 'continuation',
                    'direction': direction,
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(candles[peak1['index']]['time']),
                    'end_time': self._safe_timestamp_to_datetime(candles[peak3['index']]['time']),
                    'support_level': avg_support,
                    'resistance_level': avg_resistance,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': peak3['index'] - peak1['index'],
                    'candles_count': peak3['index'] - peak1['index'] + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'peaks': [peak1, peak2, peak3],
                        'troughs': [trough1, trough2],
                        'resistance_slope': resistance_slope,
                        'support_slope': support_slope
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Channel для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_triangles(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует треугольники (восходящий, нисходящий, симметричный)
        
        НОВЫЙ АЛГОРИТМ:
        1. Использует swing-экстремумы (более глобальные, чем локальные)
        2. Строит линии тренда через линейную регрессию (минимум 3-4 точки)
        3. Проверяет сходимость линий (расстояние между ними уменьшается)
        4. Выбирает одну лучшую фигуру на основе скоринга
        
        Args:
            candles: Список свечей
            symbol: Символ пары
            timeframe: Таймфрейм
        
        Returns:
            Список треугольников (обычно 0-1, максимум 1 лучший)
        """
        patterns = []
        
        try:
            last_index = len(candles) - 1
            if last_index < 0:
                return patterns
            
            last_close = float(candles[last_index]['close'])
            
            # Минимальные требования для треугольника
            min_candles_map = {
                '15m': 200,  # ~2 дня
                '1h': 200,   # ~8 дней
                '4h': 150    # ~25 дней
            }
            min_candles = min_candles_map.get(timeframe, 200)
            
            if len(candles) < min_candles:
                return patterns
            
            # Используем swing-экстремумы вместо локальных
            swing_period_map = {
                '15m': 7,
                '1h': 5,
                '4h': 4
            }
            swing_period = swing_period_map.get(timeframe, 7)
            
            swing_highs, swing_lows = self.find_swing_extrema(candles, swing_period=swing_period)
            
            # Нужно минимум 4 swing-high и 4 swing-low для построения треугольника
            if len(swing_highs) < 4 or len(swing_lows) < 4:
                return patterns
            
            # Минимальная ширина треугольника (в свечах)
            # Согласно классической литературе по техническому анализу:
            # - Треугольники должны формироваться минимум на 20-30 свечах
            # - Для более надежных треугольников рекомендуется 50-100 свечей
            # - Увеличиваем минимальную ширину для более значимых фигур
            min_width_map = {
                '15m': 60,   # ~1.5 дня (увеличено с 40)
                '1h': 50,    # ~2 дня (увеличено с 30)
                '4h': 30     # ~5 дней (увеличено с 20)
            }
            min_width = min_width_map.get(timeframe, 50)
            
            # Ищем лучший треугольник: пробуем разные комбинации swing-экстремумов
            candidates = []
            
            # Берем последние N swing-high и swing-low для анализа
            # Увеличиваем количество проверяемых экстремумов для более широких треугольников
            # Начинаем с самых последних и идем назад
            max_swings_to_check = min(12, len(swing_highs), len(swing_lows))  # Увеличено с 8 до 12
            
            for num_res_points in range(3, max_swings_to_check + 1):
                for num_sup_points in range(3, max_swings_to_check + 1):
                    # Берем последние num_res_points swing-high для линии сопротивления
                    res_points = swing_highs[-num_res_points:]
                    # Берем последние num_sup_points swing-low для линии поддержки
                    sup_points = swing_lows[-num_sup_points:]
                    
                    if len(res_points) < 3 or len(sup_points) < 3:
                        continue
                    
                    # Строим линии тренда через линейную регрессию
                    res_slope, res_intercept = self.linear_regression(res_points)
                    sup_slope, sup_intercept = self.linear_regression(sup_points)
                    
                    # Определяем границы треугольника
                    start_idx = min(res_points[0]['index'], sup_points[0]['index'])
                    end_idx = max(res_points[-1]['index'], sup_points[-1]['index'])
                    
                    # Проверяем минимальную ширину
                    if end_idx - start_idx < min_width:
                        continue
                    
                    # Вычисляем значения линий на границах
                    res_start = res_slope * start_idx + res_intercept
                    res_end = res_slope * end_idx + res_intercept
                    sup_start = sup_slope * start_idx + sup_intercept
                    sup_end = sup_slope * end_idx + sup_intercept
                    
                    # Проверяем сходимость: расстояние между линиями должно уменьшаться
                    distance_start = res_start - sup_start
                    distance_end = res_end - sup_end
                    
                    if distance_start <= 0 or distance_end <= 0:
                        continue
                    
                    # Линии должны сходиться (расстояние уменьшается минимум на 20%)
                    convergence_ratio = distance_end / distance_start
                    if convergence_ratio >= 0.8:  # Недостаточная сходимость
                        continue
                    
                    # Определяем тип треугольника
                    avg_price = (res_start + sup_start) / 2
                    price_tolerance = avg_price * 0.001  # 0.1% толерантность для горизонтальности
                    
                    pattern_type = None
                    direction = None
                    pattern_category = None
                    
                    # Восходящий треугольник: горизонтальное сопротивление, восходящая поддержка
                    if abs(res_slope) < price_tolerance and sup_slope > 0:
                        pattern_type = 'ascending_triangle'
                        direction = 'bullish'
                        pattern_category = 'continuation'
                    # Нисходящий треугольник: горизонтальная поддержка, нисходящее сопротивление
                    elif abs(sup_slope) < price_tolerance and res_slope < 0:
                        pattern_type = 'descending_triangle'
                        direction = 'bearish'
                        pattern_category = 'continuation'
                    # Симметричный треугольник: обе линии сходятся
                    elif res_slope < 0 and sup_slope > 0:
                        pattern_type = 'symmetrical_triangle'
                        direction = 'neutral'
                        pattern_category = 'consolidation'
                    else:
                        continue
                    
                    # Вычисляем характеристики треугольника
                    triangle_height = distance_start
                    pattern_height_pct = triangle_height / avg_price
                    
                    if pattern_height_pct < self.min_pattern_height_pct:
                        continue
                    
                    # Вычисляем целевую цену
                    if pattern_type == 'ascending_triangle':
                        target_price = res_end + triangle_height
                    elif pattern_type == 'descending_triangle':
                        target_price = sup_end - triangle_height
                    else:  # симметричный
                        # По умолчанию вверх (можно улучшить на основе тренда)
                        target_price = res_end + triangle_height
                    
                    # Подсчитываем количество касаний линий (для скоринга)
                    touches_resistance = 0
                    touches_support = 0
                    touch_tolerance = avg_price * 0.005  # 0.5% толерантность для касания
                    
                    for sh in swing_highs:
                        if start_idx <= sh['index'] <= end_idx:
                            line_value = res_slope * sh['index'] + res_intercept
                            if abs(sh['price'] - line_value) < touch_tolerance:
                                touches_resistance += 1
                    
                    for sl in swing_lows:
                        if start_idx <= sl['index'] <= end_idx:
                            line_value = sup_slope * sl['index'] + sup_intercept
                            if abs(sl['price'] - line_value) < touch_tolerance:
                                touches_support += 1
                    
                    # Вычисляем score для выбора лучшего треугольника
                    # Больше касаний = лучше, больше высота = лучше, оптимальная ширина = лучше
                    score = 0.0
                    score += touches_resistance * 0.15
                    score += touches_support * 0.15
                    score += min(pattern_height_pct * 10, 0.3)  # Максимум 0.3 за высоту
                    
                    # Оптимальная ширина (обновлено под новые минимальные требования)
                    width = end_idx - start_idx
                    # Предпочитаем треугольники шириной 50-150 свечей
                    if 50 <= width <= 150:
                        score += 0.2
                    elif 40 <= width < 50 or 150 < width <= 200:
                        score += 0.1
                    
                    # Бонус за хорошую сходимость
                    if convergence_ratio < 0.5:  # Сходимость более чем в 2 раза
                        score += 0.2
                    
                    # Проверяем статус фигуры относительно текущей цены
                    current_resistance = res_slope * last_index + res_intercept
                    current_support = sup_slope * last_index + sup_intercept
                    
                    price_tolerance_pct = 0.003  # 0.3%
                    
                    is_active = False
                    is_confirmed = False
                    confirmation_time = None
                    
                    # Фигура активна, если текущая цена внутри треугольника
                    if current_support < last_close < current_resistance and last_index >= start_idx:
                        is_active = True
                    # Фигура пробита, если цена вышла за пределы
                    elif last_close > current_resistance * (1 + price_tolerance_pct):
                        is_confirmed = True
                        # В candles['time'] уже лежит timestamp в СЕКУНДАХ
                        confirmation_time = datetime.fromtimestamp(
                            candles[last_index]['time'], tz=timezone.utc
                        )
                    elif last_close < current_support * (1 - price_tolerance_pct):
                        is_confirmed = True
                        confirmation_time = datetime.fromtimestamp(
                            candles[last_index]['time'], tz=timezone.utc
                        )
                    
                    # Для активных фигур конец считаем по последней свече
                    if is_active:
                        end_time_dt = self._safe_timestamp_to_datetime(candles[last_index]['time'])
                    else:
                        end_time_dt = self._safe_timestamp_to_datetime(candles[end_idx]['time'])
                    
                    # Вычисляем надежность
                    symmetry = abs(1.0 - convergence_ratio)
                    reliability = self._calculate_reliability(
                        pattern_height_pct,
                        symmetry,
                        width
                    )
                    
                    # Добавляем score в reliability для финального выбора
                    reliability = min(reliability + score, 1.0)
                    
                    candidates.append({
                        'pattern_type': pattern_type,
                        'pattern_category': pattern_category,
                        'direction': direction,
                        'reliability': reliability,
                        'score': score,
                        # В candles['time'] уже секунды, поэтому не делим на 1000
                        'start_time': self._safe_timestamp_to_datetime(candles[start_idx]['time']),
                        'end_time': end_time_dt,
                        'confirmation_time': confirmation_time,
                        'support_level': (sup_start + sup_end) / 2,
                        'resistance_level': (res_start + res_end) / 2,
                        'target_price': target_price,
                        'pattern_height': pattern_height_pct,
                        'pattern_width': width,
                        'candles_count': width + 1,
                        'is_active': is_active,
                        'is_confirmed': is_confirmed,
                        'pattern_data': {
                            'resistance_points': res_points,
                            'support_points': sup_points,
                            'resistance_slope': res_slope,
                            'resistance_intercept': res_intercept,
                            'support_slope': sup_slope,
                            'support_intercept': sup_intercept,
                            'start_index': start_idx,
                            'end_index': end_idx,
                            'current_support': current_support,
                            'current_resistance': current_resistance,
                            'last_close': last_close,
                            'touches_resistance': touches_resistance,
                            'touches_support': touches_support,
                            'convergence_ratio': convergence_ratio
                        }
                    })
            
            # Выбираем лучший треугольник (с максимальным score)
            if candidates:
                best_candidate = max(candidates, key=lambda x: x['score'])
                patterns.append(best_candidate)
                logger.debug(
                    f"✅ Найден треугольник {best_candidate['pattern_type']} для {symbol} {timeframe}: "
                    f"score={best_candidate['score']:.2f}, reliability={best_candidate['reliability']:.2f}, "
                    f"width={best_candidate['pattern_width']}, touches={best_candidate['pattern_data']['touches_resistance']}/{best_candidate['pattern_data']['touches_support']}"
                )
        
        except Exception as e:
            logger.error(f"Ошибка детекции Triangles для {symbol} {timeframe}: {e}", exc_info=True)
        
        return patterns
    
    def detect_wedges(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует клинья (восходящий, нисходящий)
        
        Алгоритм:
        1. Найти сходящиеся линии, обе направлены в одну сторону
        2. Восходящий клин: обе линии восходящие, но сходятся (разворот вниз)
        3. Нисходящий клин: обе линии нисходящие, но сходятся (разворот вверх)
        """
        patterns = []
        
        try:
            if len(candles) < 30:
                return patterns
            
            peaks, troughs = self.find_local_extrema(candles, lookback=5)
            
            if len(peaks) < 3 or len(troughs) < 3:
                return patterns
            
            # Ищем последовательные пики и впадины для построения клина
            for i in range(len(peaks) - 2):
                peak1 = peaks[i]
                peak2 = peaks[i + 1]
                peak3 = peaks[i + 2]
                
                # Находим соответствующие впадины
                trough1 = None
                trough2 = None
                trough3 = None
                
                for trough in troughs:
                    if trough['index'] < peak1['index']:
                        if trough1 is None or trough['price'] < trough1['price']:
                            trough1 = trough
                    elif peak1['index'] < trough['index'] < peak2['index']:
                        if trough2 is None or trough['price'] < trough2['price']:
                            trough2 = trough
                    elif peak2['index'] < trough['index'] < peak3['index']:
                        if trough3 is None or trough['price'] < trough3['price']:
                            trough3 = trough
                
                if not trough1 or not trough2 or not trough3:
                    continue
                
                # Вычисляем наклоны линий
                # Линия сопротивления: через peak1 и peak2
                resistance_slope = (peak2['price'] - peak1['price']) / (peak2['index'] - peak1['index'])
                # Линия поддержки: через trough1 и trough2
                support_slope = (trough2['price'] - trough1['price']) / (trough2['index'] - trough1['index'])
                
                # Проверяем сходимость (линии должны сходиться)
                if peak2['price'] >= peak1['price'] or trough2['price'] <= trough1['price']:
                    continue
                
                # Определяем тип клина
                # Восходящий клин: обе линии восходящие, но сходятся (медвежий)
                if resistance_slope > 0.0001 and support_slope > 0.0001 and resistance_slope < support_slope:
                    pattern_type = 'rising_wedge'
                    direction = 'bearish'
                    pattern_category = 'reversal'
                # Нисходящий клин: обе линии нисходящие, но сходятся (бычий)
                elif resistance_slope < -0.0001 and support_slope < -0.0001 and resistance_slope > support_slope:
                    pattern_type = 'falling_wedge'
                    direction = 'bullish'
                    pattern_category = 'reversal'
                else:
                    continue
                
                # Вычисляем средние уровни
                avg_resistance = (peak1['price'] + peak2['price']) / 2
                avg_support = (trough1['price'] + trough2['price']) / 2
                
                # Вычисляем целевую цену (высота клина в начале)
                wedge_height = peak1['price'] - trough1['price']
                if pattern_type == 'rising_wedge':
                    target_price = avg_support - wedge_height  # Разворот вниз
                else:  # falling_wedge
                    target_price = avg_resistance + wedge_height  # Разворот вверх
                
                pattern_height_pct = wedge_height / avg_support
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                # Симметричность клина
                convergence_ratio = abs((peak2['price'] - peak1['price']) / (trough2['price'] - trough1['price']))
                symmetry = abs(1.0 - convergence_ratio) if convergence_ratio > 0 else 1.0
                
                reliability = self._calculate_reliability(
                    pattern_height_pct,
                    symmetry,
                    peak3['index'] - peak1['index']
                )
                
                patterns.append({
                    'pattern_type': pattern_type,
                    'pattern_category': pattern_category,
                    'direction': direction,
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(candles[peak1['index']]['time']),
                    'end_time': self._safe_timestamp_to_datetime(candles[peak3['index']]['time']),
                    'support_level': avg_support,
                    'resistance_level': avg_resistance,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': peak3['index'] - peak1['index'],
                    'candles_count': peak3['index'] - peak1['index'] + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'peaks': [peak1, peak2, peak3],
                        'troughs': [trough1, trough2, trough3],
                        'resistance_slope': resistance_slope,
                        'support_slope': support_slope
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Wedges для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def detect_rectangles(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует фигуру 'Прямоугольник'
        
        Алгоритм:
        1. Найти горизонтальные параллельные линии поддержки и сопротивления
        2. Проверить, что цена движется внутри прямоугольника
        3. Определить направление пробоя (на основе тренда до прямоугольника)
        """
        patterns = []
        
        try:
            if len(candles) < 30:
                return patterns
            
            peaks, troughs = self.find_local_extrema(candles, lookback=5)
            
            if len(peaks) < 3 or len(troughs) < 3:
                return patterns
            
            # Ищем последовательные пики и впадины на примерно одинаковых уровнях
            for i in range(len(peaks) - 2):
                peak1 = peaks[i]
                peak2 = peaks[i + 1]
                peak3 = peaks[i + 2]
                
                # Проверяем, что пики примерно на одном уровне (разница < 1%)
                peak_diff_12 = abs(peak1['price'] - peak2['price']) / peak1['price']
                peak_diff_23 = abs(peak2['price'] - peak3['price']) / peak2['price']
                
                if peak_diff_12 > 0.01 or peak_diff_23 > 0.01:
                    continue
                
                # Находим соответствующие впадины
                trough1 = None
                trough2 = None
                trough3 = None
                
                for trough in troughs:
                    if trough['index'] < peak1['index']:
                        if trough1 is None or trough['price'] < trough1['price']:
                            trough1 = trough
                    elif peak1['index'] < trough['index'] < peak2['index']:
                        if trough2 is None or trough['price'] < trough2['price']:
                            trough2 = trough
                    elif peak2['index'] < trough['index'] < peak3['index']:
                        if trough3 is None or trough['price'] < trough3['price']:
                            trough3 = trough
                
                if not trough1 or not trough2 or not trough3:
                    continue
                
                # Проверяем, что впадины тоже примерно на одном уровне
                trough_diff_12 = abs(trough1['price'] - trough2['price']) / trough1['price']
                trough_diff_23 = abs(trough2['price'] - trough3['price']) / trough2['price']
                
                if trough_diff_12 > 0.01 or trough_diff_23 > 0.01:
                    continue
                
                # Вычисляем средние уровни
                avg_resistance = (peak1['price'] + peak2['price'] + peak3['price']) / 3
                avg_support = (trough1['price'] + trough2['price'] + trough3['price']) / 3
                
                # Проверяем, что уровни горизонтальные (наклоны близки к нулю)
                resistance_slope = (peak3['price'] - peak1['price']) / (peak3['index'] - peak1['index'])
                support_slope = (trough3['price'] - trough1['price']) / (trough3['index'] - trough1['index'])
                
                avg_price = (avg_resistance + avg_support) / 2
                
                if abs(resistance_slope) / avg_price > 0.0005 or abs(support_slope) / avg_price > 0.0005:
                    continue
                
                # Определяем направление на основе тренда до прямоугольника
                # Смотрим на движение цены перед первым пиком
                trend_start = max(0, peak1['index'] - 20)
                trend_price = candles[trend_start]['close']
                current_price = candles[peak1['index']]['close']
                
                if current_price > trend_price:
                    direction = 'bullish'  # Восходящий тренд до прямоугольника
                elif current_price < trend_price:
                    direction = 'bearish'  # Нисходящий тренд до прямоугольника
                else:
                    direction = 'neutral'
                
                # Вычисляем целевую цену (высота прямоугольника)
                rectangle_height = avg_resistance - avg_support
                target_price = avg_resistance + rectangle_height if direction == 'bullish' else avg_support - rectangle_height
                
                pattern_height_pct = rectangle_height / avg_support
                if pattern_height_pct < self.min_pattern_height_pct:
                    continue
                
                # Симметричность прямоугольника
                symmetry = max(peak_diff_12, peak_diff_23, trough_diff_12, trough_diff_23)
                
                reliability = self._calculate_reliability(
                    pattern_height_pct,
                    symmetry,
                    peak3['index'] - peak1['index']
                )
                
                patterns.append({
                    'pattern_type': 'rectangle',
                    'pattern_category': 'consolidation',
                    'direction': direction,
                    'reliability': reliability,
                    'start_time': self._safe_timestamp_to_datetime(candles[peak1['index']]['time']),
                    'end_time': self._safe_timestamp_to_datetime(candles[peak3['index']]['time']),
                    'support_level': avg_support,
                    'resistance_level': avg_resistance,
                    'target_price': target_price,
                    'pattern_height': pattern_height_pct,
                    'pattern_width': peak3['index'] - peak1['index'],
                    'candles_count': peak3['index'] - peak1['index'] + 1,
                    'is_confirmed': False,
                    'pattern_data': {
                        'peaks': [peak1, peak2, peak3],
                        'troughs': [trough1, trough2, trough3],
                        'resistance_slope': resistance_slope,
                        'support_slope': support_slope
                    }
                })
        
        except Exception as e:
            logger.error(f"Ошибка детекции Rectangle для {symbol}: {e}", exc_info=True)
        
        return patterns
    
    def _calculate_reliability(
        self, 
        pattern_height_pct: float, 
        symmetry: float, 
        width: int
    ) -> float:
        """
        Вычисляет надежность фигуры на основе ее характеристик
        
        Args:
            pattern_height_pct: Высота фигуры в процентах
            symmetry: Симметричность фигуры (0.0 = идеальная симметрия)
            width: Ширина фигуры в свечах
        
        Returns:
            Надежность от 0.0 до 1.0
        """
        # Базовая надежность
        reliability = 0.5
        
        # Бонус за высоту (больше высота = выше надежность)
        if pattern_height_pct > 0.03:  # > 3%
            reliability += 0.2
        elif pattern_height_pct > 0.02:  # > 2%
            reliability += 0.1
        
        # Бонус за симметрию
        if symmetry < 0.005:  # Очень симметричная
            reliability += 0.15
        elif symmetry < 0.01:  # Симметричная
            reliability += 0.1
        
        # Бонус за ширину (оптимальная ширина 30-100 свечей)
        if 30 <= width <= 100:
            reliability += 0.15
        elif 20 <= width < 30 or 100 < width <= 150:
            reliability += 0.1
        
        return min(reliability, 1.0)

