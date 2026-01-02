"""
Модуль для детекции паттернов свечного анализа (японские свечи)
Реализует детекцию основных паттернов: разворотные, продолжение тренда, нейтральные
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from core.models import CandlestickPattern

logger = logging.getLogger(__name__)


class PatternDetector:
    """Класс для детекции паттернов свечного анализа"""
    
    # Минимальное количество свечей для анализа
    MIN_CANDLES_FOR_PATTERN = 3
    
    def __init__(self):
        pass
    
    def detect_patterns(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """
        Детектирует все паттерны в массиве свечей
        
        Args:
            candles: Список свечей в формате [{'timestamp': int, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float}, ...]
            symbol: Символ пары (например, 'BTC/USDT')
            timeframe: Таймфрейм ('1m', '5m', '15m', '1h', '4h')
        
        Returns:
            Список обнаруженных паттернов в формате:
            [{
                'pattern_type': str,
                'direction': str,
                'reliability': float,
                'candles_indices': List[int],
                'timestamp': datetime,
                'price': float,
                'pattern_zone': str
            }, ...]
        """
        if len(candles) < self.MIN_CANDLES_FOR_PATTERN:
            return []
        
        patterns = []
        
        # Проходим по свечам, начиная с индекса 2 (для 3-свечных паттернов)
        for i in range(2, len(candles)):
            # 1-свечные паттерны
            patterns.extend(self._detect_single_candle_patterns(candles, i, symbol, timeframe))
            
            # 2-свечные паттерны
            if i >= 1:
                patterns.extend(self._detect_two_candle_patterns(candles, i, symbol, timeframe))
            
            # 3-свечные паттерны
            if i >= 2:
                patterns.extend(self._detect_three_candle_patterns(candles, i, symbol, timeframe))
        
        return patterns
    
    def _detect_single_candle_patterns(self, candles: List[Dict], idx: int, symbol: str, timeframe: str) -> List[Dict]:
        """Детектирует 1-свечные паттерны"""
        patterns = []
        candle = candles[idx]
        
        # Доджи
        doji = self._is_doji(candle)
        if doji:
            patterns.append({
                'pattern_type': 'doji',
                'direction': 'neutral',
                'reliability': 0.6,
                'candles_indices': [idx],
                'timestamp': datetime.fromtimestamp(candle['timestamp'] / 1000, tz=timezone.utc),
                'price': candle['close'],
                'pattern_zone': 'reversal' if idx > 0 else 'neutral'
            })
        
        # Молот (Hammer) - бычий разворот
        hammer = self._is_hammer(candle)
        if hammer:
            patterns.append({
                'pattern_type': 'hammer',
                'direction': 'bullish',
                'reliability': 0.7,
                'candles_indices': [idx],
                'timestamp': datetime.fromtimestamp(candle['timestamp'] / 1000, tz=timezone.utc),
                'price': candle['close'],
                'pattern_zone': 'support'
            })
        
        # Перевернутый молот (Inverted Hammer) - бычий разворот
        inverted_hammer = self._is_inverted_hammer(candle)
        if inverted_hammer:
            patterns.append({
                'pattern_type': 'inverted_hammer',
                'direction': 'bullish',
                'reliability': 0.65,
                'candles_indices': [idx],
                'timestamp': datetime.fromtimestamp(candle['timestamp'] / 1000, tz=timezone.utc),
                'price': candle['close'],
                'pattern_zone': 'support'
            })
        
        # Падающая звезда (Shooting Star) - медвежий разворот
        shooting_star = self._is_shooting_star(candle)
        if shooting_star:
            patterns.append({
                'pattern_type': 'shooting_star',
                'direction': 'bearish',
                'reliability': 0.7,
                'candles_indices': [idx],
                'timestamp': datetime.fromtimestamp(candle['timestamp'] / 1000, tz=timezone.utc),
                'price': candle['close'],
                'pattern_zone': 'resistance'
            })
        
        # Висельник (Hanging Man) - медвежий разворот
        hanging_man = self._is_hanging_man(candle)
        if hanging_man:
            patterns.append({
                'pattern_type': 'hanging_man',
                'direction': 'bearish',
                'reliability': 0.65,
                'candles_indices': [idx],
                'timestamp': datetime.fromtimestamp(candle['timestamp'] / 1000, tz=timezone.utc),
                'price': candle['close'],
                'pattern_zone': 'resistance'
            })
        
        return patterns
    
    def _detect_two_candle_patterns(self, candles: List[Dict], idx: int, symbol: str, timeframe: str) -> List[Dict]:
        """Детектирует 2-свечные паттерны"""
        patterns = []
        prev_candle = candles[idx - 1]
        curr_candle = candles[idx]
        
        # Бычье поглощение (Bullish Engulfing)
        bullish_engulfing = self._is_bullish_engulfing(prev_candle, curr_candle)
        if bullish_engulfing:
            patterns.append({
                'pattern_type': 'bullish_engulfing',
                'direction': 'bullish',
                'reliability': 0.75,
                'candles_indices': [idx - 1, idx],
                'timestamp': datetime.fromtimestamp(curr_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': curr_candle['close'],
                'pattern_zone': 'support'
            })
        
        # Медвежье поглощение (Bearish Engulfing)
        bearish_engulfing = self._is_bearish_engulfing(prev_candle, curr_candle)
        if bearish_engulfing:
            patterns.append({
                'pattern_type': 'bearish_engulfing',
                'direction': 'bearish',
                'reliability': 0.75,
                'candles_indices': [idx - 1, idx],
                'timestamp': datetime.fromtimestamp(curr_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': curr_candle['close'],
                'pattern_zone': 'resistance'
            })
        
        # Проникающая линия (Piercing Pattern) - бычий разворот
        piercing = self._is_piercing_pattern(prev_candle, curr_candle)
        if piercing:
            patterns.append({
                'pattern_type': 'piercing_pattern',
                'direction': 'bullish',
                'reliability': 0.7,
                'candles_indices': [idx - 1, idx],
                'timestamp': datetime.fromtimestamp(curr_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': curr_candle['close'],
                'pattern_zone': 'support'
            })
        
        # Темная накрывающая туча (Dark Cloud Cover) - медвежий разворот
        dark_cloud = self._is_dark_cloud_cover(prev_candle, curr_candle)
        if dark_cloud:
            patterns.append({
                'pattern_type': 'dark_cloud_cover',
                'direction': 'bearish',
                'reliability': 0.7,
                'candles_indices': [idx - 1, idx],
                'timestamp': datetime.fromtimestamp(curr_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': curr_candle['close'],
                'pattern_zone': 'resistance'
            })
        
        # Бычья харами (Bullish Harami)
        bullish_harami = self._is_bullish_harami(prev_candle, curr_candle)
        if bullish_harami:
            patterns.append({
                'pattern_type': 'bullish_harami',
                'direction': 'bullish',
                'reliability': 0.6,
                'candles_indices': [idx - 1, idx],
                'timestamp': datetime.fromtimestamp(curr_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': curr_candle['close'],
                'pattern_zone': 'support'
            })
        
        # Медвежья харами (Bearish Harami)
        bearish_harami = self._is_bearish_harami(prev_candle, curr_candle)
        if bearish_harami:
            patterns.append({
                'pattern_type': 'bearish_harami',
                'direction': 'bearish',
                'reliability': 0.6,
                'candles_indices': [idx - 1, idx],
                'timestamp': datetime.fromtimestamp(curr_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': curr_candle['close'],
                'pattern_zone': 'resistance'
            })
        
        return patterns
    
    def _detect_three_candle_patterns(self, candles: List[Dict], idx: int, symbol: str, timeframe: str) -> List[Dict]:
        """Детектирует 3-свечные паттерны"""
        patterns = []
        first_candle = candles[idx - 2]
        second_candle = candles[idx - 1]
        third_candle = candles[idx]
        
        # Утренняя звезда (Morning Star) - бычий разворот
        morning_star = self._is_morning_star(first_candle, second_candle, third_candle)
        if morning_star:
            patterns.append({
                'pattern_type': 'morning_star',
                'direction': 'bullish',
                'reliability': 0.8,
                'candles_indices': [idx - 2, idx - 1, idx],
                'timestamp': datetime.fromtimestamp(third_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': third_candle['close'],
                'pattern_zone': 'support'
            })
        
        # Вечерняя звезда (Evening Star) - медвежий разворот
        evening_star = self._is_evening_star(first_candle, second_candle, third_candle)
        if evening_star:
            patterns.append({
                'pattern_type': 'evening_star',
                'direction': 'bearish',
                'reliability': 0.8,
                'candles_indices': [idx - 2, idx - 1, idx],
                'timestamp': datetime.fromtimestamp(third_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': third_candle['close'],
                'pattern_zone': 'resistance'
            })
        
        # Три белых солдата (Three White Soldiers) - продолжение бычьего тренда
        three_white_soldiers = self._is_three_white_soldiers(first_candle, second_candle, third_candle)
        if three_white_soldiers:
            patterns.append({
                'pattern_type': 'three_white_soldiers',
                'direction': 'bullish',
                'reliability': 0.75,
                'candles_indices': [idx - 2, idx - 1, idx],
                'timestamp': datetime.fromtimestamp(third_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': third_candle['close'],
                'pattern_zone': 'trend_continuation'
            })
        
        # Три ворона (Three Black Crows) - продолжение медвежьего тренда
        three_black_crows = self._is_three_black_crows(first_candle, second_candle, third_candle)
        if three_black_crows:
            patterns.append({
                'pattern_type': 'three_black_crows',
                'direction': 'bearish',
                'reliability': 0.75,
                'candles_indices': [idx - 2, idx - 1, idx],
                'timestamp': datetime.fromtimestamp(third_candle['timestamp'] / 1000, tz=timezone.utc),
                'price': third_candle['close'],
                'pattern_zone': 'trend_continuation'
            })
        
        return patterns
    
    # ========== Вспомогательные методы для определения паттернов ==========
    
    def _get_body_size(self, candle: Dict) -> float:
        """Возвращает размер тела свечи (open - close)"""
        return abs(candle['close'] - candle['open'])
    
    def _get_upper_shadow(self, candle: Dict) -> float:
        """Возвращает размер верхней тени"""
        return candle['high'] - max(candle['open'], candle['close'])
    
    def _get_lower_shadow(self, candle: Dict) -> float:
        """Возвращает размер нижней тени"""
        return min(candle['open'], candle['close']) - candle['low']
    
    def _get_total_range(self, candle: Dict) -> float:
        """Возвращает общий диапазон свечи (high - low)"""
        return candle['high'] - candle['low']
    
    def _is_bullish(self, candle: Dict) -> bool:
        """Проверяет, является ли свеча бычьей"""
        return candle['close'] > candle['open']
    
    def _is_bearish(self, candle: Dict) -> bool:
        """Проверяет, является ли свеча медвежьей"""
        return candle['close'] < candle['open']
    
    # ========== 1-свечные паттерны ==========
    
    def _is_doji(self, candle: Dict) -> bool:
        """Доджи: очень маленькое тело, тени примерно равны"""
        body_size = self._get_body_size(candle)
        total_range = self._get_total_range(candle)
        if total_range == 0:
            return False
        
        # Тело должно быть меньше 5% от общего диапазона
        body_ratio = body_size / total_range
        return body_ratio < 0.05
    
    def _is_hammer(self, candle: Dict) -> bool:
        """Молот: маленькое тело вверху, длинная нижняя тень, маленькая верхняя тень"""
        body_size = self._get_body_size(candle)
        lower_shadow = self._get_lower_shadow(candle)
        upper_shadow = self._get_upper_shadow(candle)
        total_range = self._get_total_range(candle)
        
        if total_range == 0:
            return False
        
        # Нижняя тень должна быть минимум в 2 раза больше тела
        # Верхняя тень должна быть маленькой (меньше тела)
        # Тело должно быть в верхней половине диапазона
        return (lower_shadow >= body_size * 2 and 
                upper_shadow <= body_size * 0.5 and
                candle['close'] > candle['low'] + total_range * 0.3)
    
    def _is_inverted_hammer(self, candle: Dict) -> bool:
        """Перевернутый молот: маленькое тело внизу, длинная верхняя тень, маленькая нижняя тень"""
        body_size = self._get_body_size(candle)
        lower_shadow = self._get_lower_shadow(candle)
        upper_shadow = self._get_upper_shadow(candle)
        total_range = self._get_total_range(candle)
        
        if total_range == 0:
            return False
        
        # Верхняя тень должна быть минимум в 2 раза больше тела
        # Нижняя тень должна быть маленькой
        # Тело должно быть в нижней половине диапазона
        return (upper_shadow >= body_size * 2 and 
                lower_shadow <= body_size * 0.5 and
                candle['close'] < candle['high'] - total_range * 0.3)
    
    def _is_shooting_star(self, candle: Dict) -> bool:
        """Падающая звезда: маленькое тело внизу, длинная верхняя тень, маленькая нижняя тень"""
        body_size = self._get_body_size(candle)
        lower_shadow = self._get_lower_shadow(candle)
        upper_shadow = self._get_upper_shadow(candle)
        total_range = self._get_total_range(candle)
        
        if total_range == 0:
            return False
        
        # Верхняя тень должна быть минимум в 2 раза больше тела
        # Нижняя тень должна быть маленькой
        # Тело должно быть в нижней половине диапазона
        # Свеча должна быть медвежьей или нейтральной
        return (upper_shadow >= body_size * 2 and 
                lower_shadow <= body_size * 0.5 and
                candle['close'] < candle['high'] - total_range * 0.3)
    
    def _is_hanging_man(self, candle: Dict) -> bool:
        """Висельник: похож на молот, но появляется после восходящего тренда"""
        # Похож на молот, но контекст важен (нужно проверять предыдущие свечи)
        body_size = self._get_body_size(candle)
        lower_shadow = self._get_lower_shadow(candle)
        upper_shadow = self._get_upper_shadow(candle)
        total_range = self._get_total_range(candle)
        
        if total_range == 0:
            return False
        
        # Те же условия, что и у молота
        return (lower_shadow >= body_size * 2 and 
                upper_shadow <= body_size * 0.5 and
                candle['close'] > candle['low'] + total_range * 0.3)
    
    # ========== 2-свечные паттерны ==========
    
    def _is_bullish_engulfing(self, prev_candle: Dict, curr_candle: Dict) -> bool:
        """Бычье поглощение: первая свеча медвежья, вторая бычья и полностью поглощает первую"""
        if not (self._is_bearish(prev_candle) and self._is_bullish(curr_candle)):
            return False
        
        # Вторая свеча должна полностью поглотить первую
        return (curr_candle['open'] < prev_candle['close'] and
                curr_candle['close'] > prev_candle['open'])
    
    def _is_bearish_engulfing(self, prev_candle: Dict, curr_candle: Dict) -> bool:
        """Медвежье поглощение: первая свеча бычья, вторая медвежья и полностью поглощает первую"""
        if not (self._is_bullish(prev_candle) and self._is_bearish(curr_candle)):
            return False
        
        # Вторая свеча должна полностью поглотить первую
        return (curr_candle['open'] > prev_candle['close'] and
                curr_candle['close'] < prev_candle['open'])
    
    def _is_piercing_pattern(self, prev_candle: Dict, curr_candle: Dict) -> bool:
        """Проникающая линия: первая медвежья, вторая бычья, открывается ниже минимума первой, закрывается выше середины первой"""
        if not (self._is_bearish(prev_candle) and self._is_bullish(curr_candle)):
            return False
        
        prev_midpoint = (prev_candle['open'] + prev_candle['close']) / 2
        
        return (curr_candle['open'] < prev_candle['low'] and
                curr_candle['close'] > prev_midpoint and
                curr_candle['close'] < prev_candle['open'])
    
    def _is_dark_cloud_cover(self, prev_candle: Dict, curr_candle: Dict) -> bool:
        """Темная накрывающая туча: первая бычья, вторая медвежья, открывается выше максимума первой, закрывается ниже середины первой"""
        if not (self._is_bullish(prev_candle) and self._is_bearish(curr_candle)):
            return False
        
        prev_midpoint = (prev_candle['open'] + prev_candle['close']) / 2
        
        return (curr_candle['open'] > prev_candle['high'] and
                curr_candle['close'] < prev_midpoint and
                curr_candle['close'] > prev_candle['close'])
    
    def _is_bullish_harami(self, prev_candle: Dict, curr_candle: Dict) -> bool:
        """Бычья харами: первая большая медвежья, вторая маленькая бычья внутри первой"""
        if not (self._is_bearish(prev_candle) and self._is_bullish(curr_candle)):
            return False
        
        # Вторая свеча должна быть внутри первой
        return (curr_candle['open'] > prev_candle['close'] and
                curr_candle['close'] < prev_candle['open'] and
                curr_candle['high'] < prev_candle['high'] and
                curr_candle['low'] > prev_candle['low'])
    
    def _is_bearish_harami(self, prev_candle: Dict, curr_candle: Dict) -> bool:
        """Медвежья харами: первая большая бычья, вторая маленькая медвежья внутри первой"""
        if not (self._is_bullish(prev_candle) and self._is_bearish(curr_candle)):
            return False
        
        # Вторая свеча должна быть внутри первой
        return (curr_candle['open'] < prev_candle['close'] and
                curr_candle['close'] > prev_candle['open'] and
                curr_candle['high'] < prev_candle['high'] and
                curr_candle['low'] > prev_candle['low'])
    
    # ========== 3-свечные паттерны ==========
    
    def _is_morning_star(self, first: Dict, second: Dict, third: Dict) -> bool:
        """Утренняя звезда: медвежья свеча, маленькая свеча с гэпом вниз, бычья свеча"""
        if not (self._is_bearish(first) and self._is_bullish(third)):
            return False
        
        # Вторая свеча должна быть маленькой (звезда)
        second_body = self._get_body_size(second)
        first_body = self._get_body_size(first)
        
        # Вторая свеча должна быть ниже первой (гэп вниз)
        # Третья свеча должна закрыться выше середины первой
        first_midpoint = (first['open'] + first['close']) / 2
        
        return (second_body < first_body * 0.3 and
                second['high'] < first['close'] and
                third['close'] > first_midpoint)
    
    def _is_evening_star(self, first: Dict, second: Dict, third: Dict) -> bool:
        """Вечерняя звезда: бычья свеча, маленькая свеча с гэпом вверх, медвежья свеча"""
        if not (self._is_bullish(first) and self._is_bearish(third)):
            return False
        
        # Вторая свеча должна быть маленькой (звезда)
        second_body = self._get_body_size(second)
        first_body = self._get_body_size(first)
        
        # Вторая свеча должна быть выше первой (гэп вверх)
        # Третья свеча должна закрыться ниже середины первой
        first_midpoint = (first['open'] + first['close']) / 2
        
        return (second_body < first_body * 0.3 and
                second['low'] > first['close'] and
                third['close'] < first_midpoint)
    
    def _is_three_white_soldiers(self, first: Dict, second: Dict, third: Dict) -> bool:
        """Три белых солдата: три последовательные бычьи свечи с растущими закрытиями"""
        if not (self._is_bullish(first) and self._is_bullish(second) and self._is_bullish(third)):
            return False
        
        # Каждая следующая свеча должна закрываться выше предыдущей
        # Каждая следующая должна открываться внутри тела предыдущей или выше
        return (second['close'] > first['close'] and
                third['close'] > second['close'] and
                second['open'] >= first['open'] and
                third['open'] >= second['open'])
    
    def _is_three_black_crows(self, first: Dict, second: Dict, third: Dict) -> bool:
        """Три ворона: три последовательные медвежьи свечи с падающими закрытиями"""
        if not (self._is_bearish(first) and self._is_bearish(second) and self._is_bearish(third)):
            return False
        
        # Каждая следующая свеча должна закрываться ниже предыдущей
        # Каждая следующая должна открываться внутри тела предыдущей или ниже
        return (second['close'] < first['close'] and
                third['close'] < second['close'] and
                second['open'] <= first['open'] and
                third['open'] <= second['open'])


# Глобальный экземпляр детектора
pattern_detector = PatternDetector()

