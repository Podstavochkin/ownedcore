"""
Технический анализ криптовалют с использованием индикаторов
Автор: CryptoProject v0.01
Описание: Определение тренда и торговых сигналов на основе технических индикаторов
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, List
import warnings
warnings.filterwarnings('ignore')

class TechnicalAnalyzer:
    """
    Класс для технического анализа криптовалютных данных
    """
    
    def __init__(self):
        # Параметры индикаторов
        self.ma_short = 9      # Короткая скользящая средняя
        self.ma_long = 21      # Длинная скользящая средняя
        self.rsi_period = 14   # Период RSI
        self.macd_fast = 12    # Быстрая EMA для MACD
        self.macd_slow = 26    # Медленная EMA для MACD
        self.macd_signal = 9   # Сигнальная линия MACD
        self.adx_period = 14   # Период ADX
        self.volume_ma = 20    # Скользящая средняя объема
        
    def calculate_ma(self, df: pd.DataFrame, period: int) -> pd.Series:
        """Вычисление скользящей средней"""
        return df['close'].rolling(window=period).mean()
    
    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        """Вычисление экспоненциальной скользящей средней"""
        return df['close'].ewm(span=period).mean()
    
    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Вычисление RSI (Relative Strength Index)"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Вычисление MACD (Moving Average Convergence Divergence)"""
        ema_fast = self.calculate_ema(df, self.macd_fast)
        ema_slow = self.calculate_ema(df, self.macd_slow)
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.macd_signal).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    
    def calculate_adx(self, df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Вычисление ADX (Average Directional Index)"""
        # True Range
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        
        # Directional Movement
        up_move = df['high'] - df['high'].shift()
        down_move = df['low'].shift() - df['low']
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
        
        # Smoothed values
        tr_smooth = true_range.rolling(window=period).mean()
        plus_di = 100 * pd.Series(plus_dm).rolling(window=period).mean() / tr_smooth
        minus_di = 100 * pd.Series(minus_dm).rolling(window=period).mean() / tr_smooth
        
        # ADX
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = pd.Series(dx).rolling(window=period).mean()
        
        return adx, plus_di, minus_di
    
    def calculate_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Вычисление полос Боллинджера"""
        ma = self.calculate_ma(df, period)
        std = df['close'].rolling(window=period).std()
        upper_band = ma + (std * std_dev)
        lower_band = ma - (std * std_dev)
        return upper_band, ma, lower_band
    
    def calculate_volume_indicators(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """Вычисление индикаторов объема"""
        volume_ma = df['volume'].rolling(window=self.volume_ma).mean()
        volume_ratio = df['volume'] / volume_ma
        return volume_ma, volume_ratio
    
    def analyze_trend(self, df: pd.DataFrame) -> Dict:
        """
        Комплексный анализ тренда на основе нескольких индикаторов
        """
        if df is None or df.empty or len(df) < 30:
            return {
                'trend': 'Недостаточно данных',
                'strength': 0,
                'signal': 'НЕТ СИГНАЛА',
                'confidence': 0,
                'score': 0,
                'signals': [],
                'indicators': {}
            }
        
        # Вычисляем все индикаторы
        df_analysis = df.copy()
        
        # Скользящие средние
        df_analysis['ma_short'] = self.calculate_ma(df_analysis, self.ma_short)
        df_analysis['ma_long'] = self.calculate_ma(df_analysis, self.ma_long)
        
        # RSI
        df_analysis['rsi'] = self.calculate_rsi(df_analysis, self.rsi_period)
        
        # MACD
        df_analysis['macd'], df_analysis['macd_signal'], df_analysis['macd_histogram'] = self.calculate_macd(df_analysis)
        
        # ADX
        df_analysis['adx'], df_analysis['plus_di'], df_analysis['minus_di'] = self.calculate_adx(df_analysis, self.adx_period)
        
        # Полосы Боллинджера
        df_analysis['bb_upper'], df_analysis['bb_middle'], df_analysis['bb_lower'] = self.calculate_bollinger_bands(df_analysis)
        
        # Объем
        df_analysis['volume_ma'], df_analysis['volume_ratio'] = self.calculate_volume_indicators(df_analysis)
        
        # Получаем последние значения
        latest = df_analysis.iloc[-1]
        prev = df_analysis.iloc[-2] if len(df_analysis) > 1 else latest
        
        # Анализ тренда по скользящим средним
        ma_trend = 'ВОСХОДЯЩИЙ' if latest['ma_short'] > latest['ma_long'] else 'НИСХОДЯЩИЙ'
        ma_strength = abs(latest['ma_short'] - latest['ma_long']) / latest['close'] * 100
        
        # Анализ RSI
        rsi_signal = 'ПЕРЕПРОДАНО' if latest['rsi'] < 30 else 'ПЕРЕКУПЛЕНО' if latest['rsi'] > 70 else 'НЕЙТРАЛЬНО'
        rsi_trend = 'ВОСХОДЯЩИЙ' if latest['rsi'] > prev['rsi'] else 'НИСХОДЯЩИЙ'
        
        # Анализ MACD
        macd_trend = 'ВОСХОДЯЩИЙ' if latest['macd'] > latest['macd_signal'] else 'НИСХОДЯЩИЙ'
        macd_signal = 'ПОКУПКА' if (latest['macd'] > latest['macd_signal']) and (prev['macd'] <= prev['macd_signal']) else \
                      'ПРОДАЖА' if (latest['macd'] < latest['macd_signal']) and (prev['macd'] >= prev['macd_signal']) else 'НЕТ СИГНАЛА'
        
        # Анализ ADX
        adx_strength = 'СИЛЬНЫЙ' if latest['adx'] > 25 else 'СЛАБЫЙ'
        adx_trend = 'ВОСХОДЯЩИЙ' if latest['plus_di'] > latest['minus_di'] else 'НИСХОДЯЩИЙ'
        
        # Анализ полос Боллинджера
        bb_position = 'ВЕРХНЯЯ' if latest['close'] > latest['bb_upper'] else \
                     'НИЖНЯЯ' if latest['close'] < latest['bb_lower'] else 'СРЕДНЯЯ'
        
        # Анализ объема
        volume_signal = 'ВЫСОКИЙ' if latest['volume_ratio'] > 1.5 else 'НИЗКИЙ' if latest['volume_ratio'] < 0.5 else 'НОРМАЛЬНЫЙ'
        
        # Комплексный анализ тренда
        trend_signals = []
        trend_score = 0
        
        # MA анализ (вес: 30%)
        if ma_trend == 'ВОСХОДЯЩИЙ':
            trend_score += 30
            trend_signals.append('MA: ВОСХОДЯЩИЙ')
        else:
            trend_score -= 30
            trend_signals.append('MA: НИСХОДЯЩИЙ')
        
        # RSI анализ (вес: 20%)
        if rsi_trend == 'ВОСХОДЯЩИЙ' and latest['rsi'] < 70:
            trend_score += 20
            trend_signals.append('RSI: ВОСХОДЯЩИЙ')
        elif rsi_trend == 'НИСХОДЯЩИЙ' and latest['rsi'] > 30:
            trend_score -= 20
            trend_signals.append('RSI: НИСХОДЯЩИЙ')
        
        # MACD анализ (вес: 25%)
        if macd_trend == 'ВОСХОДЯЩИЙ':
            trend_score += 25
            trend_signals.append('MACD: ВОСХОДЯЩИЙ')
        else:
            trend_score -= 25
            trend_signals.append('MACD: НИСХОДЯЩИЙ')
        
        # ADX анализ (вес: 15%)
        if adx_trend == 'ВОСХОДЯЩИЙ':
            trend_score += 15
            trend_signals.append('ADX: ВОСХОДЯЩИЙ')
        else:
            trend_score -= 15
            trend_signals.append('ADX: НИСХОДЯЩИЙ')
        
        # Объем анализ (вес: 10%)
        if volume_signal == 'ВЫСОКИЙ' and trend_score > 0:
            trend_score += 10
            trend_signals.append('ОБЪЕМ: ПОДТВЕРЖДАЕТ')
        elif volume_signal == 'ВЫСОКИЙ' and trend_score < 0:
            trend_score -= 10
            trend_signals.append('ОБЪЕМ: ПОДТВЕРЖДАЕТ')
        
        # Определение финального тренда
        if trend_score >= 30:
            final_trend = 'ВОСХОДЯЩИЙ'
            signal = 'ПОКУПКА'
        elif trend_score <= -30:
            final_trend = 'НИСХОДЯЩИЙ'
            signal = 'ПРОДАЖА'
        else:
            final_trend = 'БОКОВОЙ'
            signal = 'ОЖИДАНИЕ'
        
        # Уровень уверенности
        confidence = min(abs(trend_score), 100)
        
        return {
            'trend': final_trend,
            'strength': confidence,
            'signal': signal,
            'confidence': confidence,
            'score': trend_score,
            'signals': trend_signals,
            'indicators': {
                'ma_trend': ma_trend,
                'ma_strength': ma_strength,
                'rsi_value': latest['rsi'],
                'rsi_signal': rsi_signal,
                'rsi_trend': rsi_trend,
                'macd_trend': macd_trend,
                'macd_signal': macd_signal,
                'adx_value': latest['adx'],
                'adx_strength': adx_strength,
                'adx_trend': adx_trend,
                'bb_position': bb_position,
                'volume_signal': volume_signal,
                'volume_ratio': latest['volume_ratio']
            },
            'data': df_analysis
        }
    
    def get_trading_recommendation(self, analysis: Dict) -> str:
        """Получение торговой рекомендации"""
        trend = analysis['trend']
        signal = analysis['signal']
        confidence = analysis['confidence']
        
        if trend == 'Недостаточно данных':
            return f"⚠️  НЕДОСТАТОЧНО ДАННЫХ"
        elif signal == 'ПОКУПКА' and confidence > 60:
            return f"🟢 СИЛЬНАЯ ПОКУПКА (уверенность: {confidence:.0f}%)"
        elif signal == 'ПОКУПКА' and confidence > 40:
            return f"🟡 СЛАБАЯ ПОКУПКА (уверенность: {confidence:.0f}%)"
        elif signal == 'ПРОДАЖА' and confidence > 60:
            return f"🔴 СИЛЬНАЯ ПРОДАЖА (уверенность: {confidence:.0f}%)"
        elif signal == 'ПРОДАЖА' and confidence > 40:
            return f"🟠 СЛАБАЯ ПРОДАЖА (уверенность: {confidence:.0f}%)"
        else:
            return f"⚪ ОЖИДАНИЕ (уверенность: {confidence:.0f}%)"
    
    def print_analysis(self, analysis: Dict, timeframe: str, symbol: str):
        """Вывод результатов анализа в консоль"""
        print(f"\n{'='*60}")
        print(f"ТЕХНИЧЕСКИЙ АНАЛИЗ {symbol} - {timeframe.upper()}")
        print(f"{'='*60}")
        
        print(f"📈 ТРЕНД: {analysis['trend']}")
        print(f"🎯 СИГНАЛ: {analysis['signal']}")
        print(f"💪 УВЕРЕННОСТЬ: {analysis['confidence']:.0f}%")
        print(f"📊 СЧЕТ: {analysis['score']:.0f}")
        
        print(f"\n📋 РЕКОМЕНДАЦИЯ:")
        print(f"   {self.get_trading_recommendation(analysis)}")
        
        # Проверяем, есть ли данные индикаторов
        if analysis['trend'] == 'Недостаточно данных':
            print(f"\n⚠️  ПРЕДУПРЕЖДЕНИЕ: Недостаточно данных для анализа {timeframe}")
            print(f"   Требуется минимум 30 свечей, получено: {len(analysis.get('data', pd.DataFrame()))}")
            return
        
        print(f"\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ:")
        indicators = analysis['indicators']
        print(f"   MA: {indicators['ma_trend']} (сила: {indicators['ma_strength']:.2f}%)")
        print(f"   RSI: {indicators['rsi_value']:.1f} - {indicators['rsi_signal']} ({indicators['rsi_trend']})")
        print(f"   MACD: {indicators['macd_trend']} - {indicators['macd_signal']}")
        print(f"   ADX: {indicators['adx_value']:.1f} - {indicators['adx_strength']} ({indicators['adx_trend']})")
        print(f"   BB: {indicators['bb_position']}")
        print(f"   Объем: {indicators['volume_signal']} (x{indicators['volume_ratio']:.2f})")
        
        print(f"\n📝 СИГНАЛЫ:")
        for signal in analysis['signals']:
            print(f"   • {signal}")

def analyze_all_timeframes(data_dict: Dict, symbol: str = 'BTC/USDT') -> Dict:
    """
    Анализ всех таймфреймов
    """
    analyzer = TechnicalAnalyzer()
    results = {}
    
    for timeframe, df in data_dict.items():
        if df is not None and not df.empty:
            analysis = analyzer.analyze_trend(df)
            results[timeframe] = analysis
            analyzer.print_analysis(analysis, timeframe, symbol)
    
    return results 