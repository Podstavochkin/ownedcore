"""
Интерактивные графики для анализа криптовалют с использованием Lightweight Charts от TradingView
Автор: CryptoProject v0.01
Описание: Создание профессиональных графиков в стиле TradingView с использованием Lightweight Charts
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import ccxt
from technical_analysis import TechnicalAnalyzer
import json
import webbrowser
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

class LightweightCryptoCharts:
    """
    Класс для создания профессиональных графиков криптовалют с использованием Lightweight Charts
    """
    
    def __init__(self):
        # Инициализация подключения к бирже
        self.exchange = ccxt.binance({
            'apiKey': '',
            'secret': '',
            'sandbox': False,
            'enableRateLimit': True,
        })
        
        # Инициализация Jinja2
        self.env = Environment(loader=FileSystemLoader('templates'))
        
        # Цвета для разных таймфреймов
        self.colors = {
            '5m': '#00ff00',
            '15m': '#ff8800', 
            '1h': '#ff0088'
        }
        
        # Создаем папку для HTML файлов
        self.output_dir = Path("charts_output")
        self.output_dir.mkdir(exist_ok=True)
    
    def get_current_price(self, symbol='BTC/USDT'):
        """Получение текущей цены"""
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker['last']
        except Exception as e:
            print(f"Ошибка получения цены: {e}")
            return None
    
    def get_candles(self, symbol='BTC/USDT', timeframe='5m', limit=100):
        """Получение свечей для указанного таймфрейма"""
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            print(f"Ошибка получения свечей: {e}")
            return None
    
    def get_all_timeframes(self, symbol='BTC/USDT'):
        """Получение данных для всех таймфреймов"""
        timeframes = ['5m', '15m', '1h']
        data_dict = {}
        
        for tf in timeframes:
            df = self.get_candles(symbol, tf, 100)
            if df is not None and not df.empty:
                data_dict[tf] = df
        
        return data_dict
    
    def convert_data_for_lightweight_charts(self, df):
        """Конвертация данных для Lightweight Charts"""
        if df is None or df.empty:
            print("[DEBUG] DataFrame пустой, ничего не конвертируем")
            return []
        chart_data = []
        for _, row in df.iterrows():
            if hasattr(row['timestamp'], 'timestamp'):
                time_value = int(row['timestamp'].timestamp())
            else:
                time_value = int(row['timestamp'] / 1000)
            chart_data.append({
                'time': time_value,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': float(row['volume'])
            })
        chart_data.sort(key=lambda x: x['time'])
        print(f"[DEBUG] Передаём в шаблон {len(chart_data)} свечей (период: {df.iloc[0]['timestamp']} — {df.iloc[-1]['timestamp']})")
        return chart_data
    
    def create_single_chart_html(self, df, symbol='BTC/USDT', timeframe='5m', current_price=None):
        """Создание HTML для единого графика (устаревший метод)"""
        print("Этот метод устарел. Используйте create_dashboard()")
        return None
    
    def create_multi_timeframe_dashboard(self, data_dict, symbol='BTC/USDT', current_price=None, analysis_results=None):
        """Создание дашборда с несколькими таймфреймами"""
        if not data_dict:
            print("Нет данных для создания дашборда")
            return None
        
        # Конвертируем данные для каждого таймфрейма
        chart_data = {}
        for timeframe, df in data_dict.items():
            if df is not None and not df.empty:
                chart_data[timeframe] = self.convert_data_for_lightweight_charts(df)
        
        # Рендерим шаблон
        template = self.env.get_template('multi_timeframe_dashboard.html')
        html_content = template.render(
            symbol=symbol,
            chart_data=json.dumps(chart_data),
            current_price=current_price,
            analysis_results=analysis_results
        )
        
        # Сохраняем в файл
        filename = f"multi_timeframe_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Дашборд сохранен в файл: {filename}")
        webbrowser.open(f'file://{os.path.abspath(filename)}')
        
        return html_content
    
    def create_single_chart_with_timeframe_switcher(self, data_dict, symbol='BTC/USDT', current_price=None, analysis_results=None):
        """Создание единого графика с переключателем таймфреймов"""
        if not data_dict:
            print("Нет данных для создания дашборда")
            return None
        
        # Конвертируем данные для каждого таймфрейма
        chart_data = {}
        for timeframe, df in data_dict.items():
            if df is not None and not df.empty:
                chart_data[timeframe] = self.convert_data_for_lightweight_charts(df)
        
        # Подготавливаем данные для шаблона
        signals = {}
        price_change_24h = 0
        volume_24h = 0
        
        if analysis_results and '1h' in analysis_results:
            signals = analysis_results['1h']
        
        # Вычисляем изменение цены за 24 часа
        if '5m' in chart_data and len(chart_data['5m']) > 0:
            first_price = chart_data['5m'][0]['close']
            last_price = chart_data['5m'][-1]['close']
            price_change_24h = ((last_price - first_price) / first_price) * 100
            
            # Суммируем объем за 24 часа
            volume_24h = sum(candle['volume'] for candle in chart_data['5m'])
        
        # Рендерим шаблон
        template = self.env.get_template('single_chart_dashboard.html')
        html_content = template.render(
            symbol=symbol,
            chart_data=json.dumps(chart_data),
            current_price=current_price,
            analysis_results=analysis_results,
            signals=signals,
            price_change_24h=price_change_24h,
            volume_24h=volume_24h,
            data_5m=chart_data.get('5m', []),
            data_15m=chart_data.get('15m', []),
            data_1h=chart_data.get('1h', []),
            data_4h=chart_data.get('4h', [])
        )
        
        # Сохраняем в файл
        filename = f"single_chart_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Дашборд сохранен в файл: {filename}")
        webbrowser.open(f'file://{os.path.abspath(filename)}')
        
        return html_content
    
    def create_dashboard(self, symbol='BTC/USDT', analysis_results=None, data_dict=None):
        """Создание полного дашборда с единым графиком и переключателем таймфреймов"""
        print(f"Получение данных для {symbol}...")
        
        # Получаем текущую цену
        current_price = self.get_current_price(symbol)
        
        # Если данные не переданы, получаем их
        if data_dict is None:
            print("⚠️ Данные не переданы, получаем базовые данные...")
            data_dict = self.get_all_timeframes(symbol)
        else:
            print("✅ Используем переданные исторические данные")
            # Проверяем качество данных
            for timeframe, df in data_dict.items():
                if df is not None and not df.empty:
                    print(f"  {timeframe}: {len(df)} свечей ({df.iloc[0]['timestamp'].strftime('%H:%M')} - {df.iloc[-1]['timestamp'].strftime('%H:%M')})")
        
        if not data_dict:
            print("Не удалось получить данные")
            return
        
        print("Создание дашборда с единым графиком и переключателем таймфреймов...")
        
        # Если результаты анализа не переданы, создаем их
        if analysis_results is None:
            try:
                from technical_analysis import TechnicalAnalyzer
                analyzer = TechnicalAnalyzer()
                analysis_results = {}
                for timeframe in ['5m', '15m', '1h']:
                    if timeframe in data_dict:
                        df = data_dict[timeframe]
                        if df is not None and not df.empty:
                            analysis = analyzer.analyze_dataframe(df)
                            analysis_results[timeframe] = analysis
            except ImportError:
                print("Модуль technical_analysis не найден, анализ не выполнен")
                analysis_results = None
        
        # Создаем дашборд
        html_content = self.create_single_chart_with_timeframe_switcher(data_dict, symbol, current_price, analysis_results)
        
        return html_content

# Функция для быстрого запуска
def run_lightweight_analysis(symbol='BTC/USDT', analysis_results=None, data_dict=None):
    """
    Быстрый запуск анализа с Lightweight Charts
    """
    analyzer = LightweightCryptoCharts()
    return analyzer.create_dashboard(symbol, analysis_results, data_dict) 