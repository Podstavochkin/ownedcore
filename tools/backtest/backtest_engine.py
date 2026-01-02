"""
Модуль бэктестинга стратегии "Королевские уровни"
Автор: CryptoProject v0.01
Описание: Система для тестирования стратегии на исторических данных
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

from royal_levels_strategy import RoyalLevelsStrategy

class BacktestEngine:
    """
    Движок для бэктестинга стратегии
    """
    
    def __init__(self, initial_deposit: float = 10000, commission: float = 0.001):
        self.initial_deposit = initial_deposit
        self.commission = commission  # 0.1% комиссия
        self.strategy = RoyalLevelsStrategy(deposit=initial_deposit)
        
        # Результаты бэктеста
        self.trades = []
        self.equity_curve = []
        self.current_balance = initial_deposit
        self.max_balance = initial_deposit
        self.max_drawdown = 0
        self.max_drawdown_pct = 0
        
        # Статистика
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0
        self.total_loss = 0
        
        # Параметры для проверки истощения депозита
        self.deposit_depletion_threshold = 0.5  # 50% от начального депозита
        self.min_balance_threshold = initial_deposit * (1 - self.deposit_depletion_threshold)
        
        # Параметры логирования
        self.log_every_n_trades = 10  # Логируем каждую 10-ю сделку
        self.log_every_n_steps = 1000  # Логируем прогресс каждые 1000 шагов
        self.verbose_logging = False  # Подробное логирование
        self.log_file = None  # Файл для логирования
        
    def download_historical_data(self, symbol: str = 'BTC/USDT', 
                                start_date: str = '2022-01-01', 
                                end_date: str = '2024-12-31') -> Dict:
        """
        Скачивание исторических данных с биржи
        """
        import ccxt
        
        print(f"📥 Скачивание исторических данных для {symbol}...")
        print(f"Период: {start_date} - {end_date}")
        
        # Инициализация биржи
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        # Конвертируем даты в timestamp
        start_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_timestamp = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        
        data = {}
        
        # Скачиваем данные для разных таймфреймов
        timeframes = {
            '5m': '5m',
            '15m': '15m', 
            '1h': '1h',
            '4h': '4h'
        }
        
        for tf_name, tf in timeframes.items():
            print(f"  Скачивание {tf_name} данных...")
            
            all_candles = []
            current_timestamp = start_timestamp
            
            while current_timestamp < end_timestamp:
                try:
                    # Получаем свечи (максимум 1000 за раз)
                    candles = exchange.fetch_ohlcv(
                        symbol, 
                        tf, 
                        since=current_timestamp, 
                        limit=1000
                    )
                    
                    if not candles:
                        break
                    
                    all_candles.extend(candles)
                    
                    # Обновляем timestamp для следующего запроса
                    current_timestamp = candles[-1][0] + 1
                    
                    # Небольшая задержка для соблюдения лимитов API
                    import time
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Ошибка при скачивании {tf_name}: {e}")
                    break
            
            if all_candles:
                # Преобразуем в DataFrame
                df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.set_index('timestamp')
                
                # Убираем дубликаты и сортируем
                df = df.drop_duplicates().sort_index()
                
                # Фильтруем по датам
                df = df[(df.index >= start_date) & (df.index <= end_date)]
                
                data[tf_name] = df
                print(f"    ✅ Загружено {len(df)} свечей")
            else:
                print(f"    ❌ Не удалось загрузить данные для {tf_name}")
        
        return data
    
    def save_data_to_csv(self, data: Dict, symbol: str = 'BTCUSDT'):
        """
        Сохранение данных в CSV файлы
        """
        print(f"💾 Сохранение данных в CSV файлы...")
        
        for timeframe, df in data.items():
            filename = f"historical_data_{symbol}_{timeframe}_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename)
            print(f"  ✅ {filename} - {len(df)} записей")
    
    def load_data_from_csv(self, symbol: str = 'BTCUSDT', date: str = None) -> Dict:
        """
        Загрузка данных из CSV файлов
        """
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        print(f"📂 Загрузка данных из CSV файлов...")
        
        data = {}
        timeframes = ['5m', '15m', '1h', '4h']
        
        for timeframe in timeframes:
            filename = f"historical_data_{symbol}_{timeframe}_{date}.csv"
            try:
                df = pd.read_csv(filename, index_col=0, parse_dates=True)
                data[timeframe] = df
                print(f"  ✅ {filename} - {len(df)} записей")
            except FileNotFoundError:
                print(f"  ❌ Файл {filename} не найден")
        
        return data
    
    def setup_logging(self, log_filename: str):
        """Настройка логирования в файл"""
        self.log_file = open(log_filename, 'w', encoding='utf-8')
        self.log_file.write(f"ЛОГ БЭКТЕСТА СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'\n")
        self.log_file.write(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.log_file.write("="*80 + "\n\n")
        
        # Связываем логирование стратегии с движком
        self.strategy.log_to_file = self.log_to_file
    
    def log_to_file(self, message: str):
        """Запись сообщения в лог-файл"""
        if self.log_file:
            self.log_file.write(message + "\n")
            self.log_file.flush()
    
    def close_logging(self):
        """Закрытие лог-файла"""
        if self.log_file:
            self.log_file.write("\n" + "="*80 + "\n")
            self.log_file.write(f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            self.log_file.close()
            self.log_file = None
    
    def run_backtest(self, data: Dict, symbol: str = 'BTC/USDT') -> Dict:
        """
        Запуск бэктеста на исторических данных
        """
        print(f"🚀 Запуск бэктеста стратегии 'Королевские уровни'...")
        print(f"Символ: {symbol}")
        print(f"Начальный депозит: ${self.initial_deposit:,.2f}")
        
        # Проверяем наличие необходимых данных
        required_timeframes = ['5m', '1h', '4h']
        for tf in required_timeframes:
            if tf not in data or data[tf] is None or data[tf].empty:
                print(f"❌ Отсутствуют данные для таймфрейма {tf}")
                return {}
        
        # Получаем 5-минутные данные для основного цикла
        df_5m = data['5m'].copy()
        df_1h = data['1h'].copy()
        df_4h = data['4h'].copy()
        
        print(f"📊 Данные для бэктеста:")
        print(f"  5M: {len(df_5m)} свечей ({df_5m.index[0]} - {df_5m.index[-1]})")
        print(f"  1H: {len(df_1h)} свечей ({df_1h.index[0]} - {df_1h.index[-1]})")
        print(f"  4H: {len(df_4h)} свечей ({df_4h.index[0]} - {df_4h.index[-1]})")
        
        # Инициализация
        self.reset_backtest()
        current_position = None
        total_steps = len(df_5m) - 100
        print(f"🔄 Всего шагов для анализа: {total_steps}")
        
        # Основной цикл бэктеста
        for i in range(100, len(df_5m)):
            current_time = df_5m.index[i]
            current_price = df_5m.iloc[i]['close']
            
            # Проверяем истощение депозита
            if self.current_balance <= self.min_balance_threshold:
                depletion_msg = f"⚠️  ДЕПОЗИТ ИСТОЩЕН НА 50%!"
                print(depletion_msg)
                self.log_to_file(depletion_msg)
                
                details = [
                    f"   Начальный депозит: ${self.initial_deposit:,.2f}",
                    f"   Текущий баланс: ${self.current_balance:,.2f}",
                    f"   Минимальный порог: ${self.min_balance_threshold:,.2f}",
                    f"   Бэктест остановлен на {current_time}"
                ]
                
                for detail in details:
                    print(detail)
                    self.log_to_file(detail)
                
                break
            
            # Прогресс-бар/print (только в терминал)
            if (i-100) % self.log_every_n_steps == 0 or i == len(df_5m)-1:
                progress_pct = (i-99)/total_steps*100
                print(f"  ▶️ Прогресс: {i-99}/{total_steps} ({progress_pct:.1f}%) | {current_time} | Баланс: ${self.current_balance:,.2f}")
                
                # Логируем прогресс в файл
                progress_log = f"🔄 Прогресс: {i-99}/{total_steps} ({progress_pct:.1f}%) | {current_time} | Баланс: ${self.current_balance:,.2f}"
                self.log_to_file(progress_log)
            
            # Обновляем equity curve
            self.update_equity_curve(current_time, current_price, current_position)
            
            # Если есть открытая позиция, проверяем стоп/тейк
            if current_position:
                result = self.check_position_exit(df_5m.iloc[i], current_position)
                if result:
                    self.close_position(current_time, current_price, result, current_position)
                    current_position = None
            
            # Если нет открытой позиции, ищем новые сигналы
            if not current_position:
                # Создаем "скользящее окно" данных (только прошлое)
                window_data = {
                    '5m': df_5m.iloc[:i+1],
                    '1h': df_1h[df_1h.index <= current_time],
                    '4h': df_4h[df_4h.index <= current_time]
                }
                
                # Анализируем стратегию
                analysis = self.strategy.analyze_strategy(window_data)
                
                # Если есть сигнал, открываем позицию
                if analysis['signals']:
                    signal = analysis['signals'][0]  # Берем первый сигнал
                    
                    # Логируем сигнал в файл (не в терминал)
                    signal_log = f"🚩 Сигнал #{self.total_trades + 1} на {current_time}: {signal['signal']['signal']} | Уровень: {signal['level']['level']:.2f} | Уверенность: {signal['signal']['confidence']:.1f}%"
                    self.log_to_file(signal_log)
                    
                    current_position = self.open_position(
                        current_time, 
                        current_price, 
                        signal
                    )
        
        # Закрываем последнюю позицию, если она открыта
        if current_position:
            self.close_position(
                df_5m.index[-1], 
                df_5m.iloc[-1]['close'], 
                'END_OF_DATA', 
                current_position
            )
        
        # Вычисляем финальную статистику
        results = self.calculate_statistics()
        
        # Определяем причину завершения бэктеста
        if self.current_balance <= self.min_balance_threshold:
            print(f"\n⚠️  БЭКТЕСТ ОСТАНОВЛЕН ИЗ-ЗА ИСТОЩЕНИЯ ДЕПОЗИТА!")
            print(f"   Начальный депозит: ${self.initial_deposit:,.2f}")
            print(f"   Конечный баланс: ${self.current_balance:,.2f}")
            print(f"   Потеря: ${self.initial_deposit - self.current_balance:,.2f} ({(self.initial_deposit - self.current_balance) / self.initial_deposit * 100:.1f}%)")
            print(f"   Всего сделок до остановки: {results.get('total_trades', 0)}")
        elif not results or results.get('total_trades', 0) == 0:
            print("❗️ Бэктест завершен: не найдено ни одной сделки на этом участке истории.")
        else:
            print(f"✅ Бэктест завершен успешно: всего сделок {results['total_trades']}")
        
        # Закрываем логирование
        self.close_logging()
        
        return results
    
    def reset_backtest(self):
        """Сброс результатов бэктеста"""
        self.trades = []
        self.equity_curve = []
        self.current_balance = self.initial_deposit
        self.max_balance = self.initial_deposit
        self.max_drawdown = 0
        self.max_drawdown_pct = 0
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0
        self.total_loss = 0
    
    def open_position(self, timestamp, current_price, signal) -> Dict:
        """Открытие позиции"""
        entry_exit = signal['entry_exit']
        
        position = {
            'entry_time': timestamp,
            'entry_price': entry_exit['entry_price'],
            'stop_loss': entry_exit['stop_loss'],
            'take_profit_1': entry_exit['take_profit_1'],
            'take_profit_2': entry_exit['take_profit_2'],
            'position_size': entry_exit['position_size'],
            'signal_type': signal['signal']['signal'],
            'level': signal['level']['level'],
            'confidence': signal['signal']['confidence']
        }
        
        # Вычисляем комиссию за вход
        entry_commission = entry_exit['entry_price'] * entry_exit['position_size'] * self.commission
        self.current_balance -= entry_commission
        
        # Логируем открытие позиции в файл
        open_log = f"📈 ОТКРЫТА ПОЗИЦИЯ #{self.total_trades + 1}: {signal['signal']['signal']} по ${entry_exit['entry_price']:,.2f} | SL: ${entry_exit['stop_loss']:,.2f} | TP1: ${entry_exit['take_profit_1']:,.2f} | TP2: ${entry_exit['take_profit_2']:,.2f} | Баланс: ${self.current_balance:,.2f}"
        self.log_to_file(open_log)
        
        return position
    
    def check_position_exit(self, current_candle, position) -> Optional[str]:
        """Проверка выхода из позиции"""
        current_price = current_candle['close']
        high = current_candle['high']
        low = current_candle['low']
        
        if position['signal_type'] == 'LONG':
            # Проверяем стоп-лосс
            if low <= position['stop_loss']:
                return 'STOP_LOSS'
            
            # Проверяем тейк-профит 1
            if high >= position['take_profit_1']:
                return 'TAKE_PROFIT_1'
            
            # Проверяем тейк-профит 2
            if high >= position['take_profit_2']:
                return 'TAKE_PROFIT_2'
        
        else:  # SHORT
            # Проверяем стоп-лосс
            if high >= position['stop_loss']:
                return 'STOP_LOSS'
            
            # Проверяем тейк-профит 1
            if low <= position['take_profit_1']:
                return 'TAKE_PROFIT_1'
            
            # Проверяем тейк-профит 2
            if low <= position['take_profit_2']:
                return 'TAKE_PROFIT_2'
        
        return None
    
    def close_position(self, timestamp, current_price, exit_reason, position):
        """Закрытие позиции"""
        # Вычисляем P&L
        if position['signal_type'] == 'LONG':
            pnl = (current_price - position['entry_price']) * position['position_size']
        else:  # SHORT
            pnl = (position['entry_price'] - current_price) * position['position_size']
        
        # Комиссия за выход
        exit_commission = current_price * position['position_size'] * self.commission
        pnl -= exit_commission
        
        # Обновляем баланс
        self.current_balance += pnl
        
        # Записываем сделку
        trade = {
            'entry_time': position['entry_time'],
            'exit_time': timestamp,
            'entry_price': position['entry_price'],
            'exit_price': current_price,
            'signal_type': position['signal_type'],
            'exit_reason': exit_reason,
            'pnl': pnl,
            'pnl_pct': (pnl / self.initial_deposit) * 100,
            'position_size': position['position_size'],
            'level': position['level'],
            'confidence': position['confidence']
        }
        
        self.trades.append(trade)
        
        # Обновляем статистику
        self.total_trades += 1
        if pnl > 0:
            self.winning_trades += 1
            self.total_profit += pnl
        else:
            self.losing_trades += 1
            self.total_loss += abs(pnl)
        
        # Вычисляем просадку
        drawdown = self.max_balance - self.current_balance
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown
            self.max_drawdown_pct = (drawdown / self.max_balance) * 100
        
        # Логируем закрытие позиции в файл
        close_log = f"📉 ЗАКРЫТА ПОЗИЦИЯ #{self.total_trades}: {exit_reason} по ${current_price:,.2f}, P&L: ${pnl:+,.2f} | Баланс: ${self.current_balance:,.2f}"
        self.log_to_file(close_log)
        
        # Логируем детали сделки в файл
        trade_details = f"💼 Сделка #{self.total_trades}: {position['signal_type']} | Вход: ${position['entry_price']:,.2f} | Выход: ${current_price:,.2f} | {exit_reason} | P&L: ${pnl:+,.2f} | Уровень: ${position['level']:,.2f} | Уверенность: {position['confidence']:.1f}%"
        self.log_to_file(trade_details)
    
    def update_equity_curve(self, timestamp, current_price, current_position):
        """Обновление кривой доходности"""
        balance = self.current_balance
        
        # Если есть открытая позиция, добавляем unrealized P&L
        if current_position:
            if current_position['signal_type'] == 'LONG':
                unrealized_pnl = (current_price - current_position['entry_price']) * current_position['position_size']
            else:  # SHORT
                unrealized_pnl = (current_position['entry_price'] - current_price) * current_position['position_size']
            balance += unrealized_pnl
        
        self.equity_curve.append({
            'timestamp': timestamp,
            'balance': balance
        })
        
        # Обновляем максимальный баланс и просадку
        if balance > self.max_balance:
            self.max_balance = balance
        
        drawdown = self.max_balance - balance
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown
            self.max_drawdown_pct = (drawdown / self.max_balance) * 100
    
    def calculate_statistics(self) -> Dict:
        """Вычисление статистики бэктеста"""
        if not self.trades:
            return {}
        
        # Базовая статистика
        total_return = self.current_balance - self.initial_deposit
        total_return_pct = (total_return / self.initial_deposit) * 100
        
        winrate = (self.winning_trades / self.total_trades) * 100 if self.total_trades > 0 else 0
        
        avg_win = self.total_profit / self.winning_trades if self.winning_trades > 0 else 0
        avg_loss = self.total_loss / self.losing_trades if self.losing_trades > 0 else 0
        
        profit_factor = self.total_profit / self.total_loss if self.total_loss > 0 else float('inf')
        
        # Создаем DataFrame для дополнительной статистики
        trades_df = pd.DataFrame(self.trades)
        
        # Статистика по типам сигналов
        long_trades = trades_df[trades_df['signal_type'] == 'LONG']
        short_trades = trades_df[trades_df['signal_type'] == 'SHORT']
        
        long_winrate = (len(long_trades[long_trades['pnl'] > 0]) / len(long_trades) * 100) if len(long_trades) > 0 else 0
        short_winrate = (len(short_trades[short_trades['pnl'] > 0]) / len(short_trades) * 100) if len(short_trades) > 0 else 0
        
        # Статистика по причинам выхода
        exit_stats = trades_df['exit_reason'].value_counts()
        
        return {
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'winrate': winrate,
            'total_return': total_return,
            'total_return_pct': total_return_pct,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'max_drawdown': self.max_drawdown,
            'max_drawdown_pct': self.max_drawdown_pct,
            'final_balance': self.current_balance,
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'long_winrate': long_winrate,
            'short_winrate': short_winrate,
            'exit_stats': exit_stats.to_dict(),
            'trades': self.trades,
            'equity_curve': self.equity_curve
        }
    
    def calculate_strategy_summary(self) -> Dict:
        """Вычисление итоговой статистики по стратегии"""
        summary = {
            'total_levels_found': 0,
            'total_signals_generated': 0,
            'signals_executed': 0,
            'signals_skipped': 0,
            'level_effectiveness': {},
            'signal_quality': {
                'high_confidence': 0,  # > 75%
                'medium_confidence': 0,  # 50-75%
                'low_confidence': 0,    # < 50%
            },
            'trend_filter_stats': {
                'bullish_periods': 0,
                'bearish_periods': 0,
                'neutral_periods': 0
            }
        }
        
        # Анализируем сделки для статистики по уровням
        if self.trades:
            for trade in self.trades:
                level = trade.get('level', 0)
                if level > 0:
                    if level not in summary['level_effectiveness']:
                        summary['level_effectiveness'][level] = {
                            'trades': 0,
                            'wins': 0,
                            'losses': 0,
                            'total_pnl': 0
                        }
                    
                    summary['level_effectiveness'][level]['trades'] += 1
                    summary['level_effectiveness'][level]['total_pnl'] += trade['pnl']
                    
                    if trade['pnl'] > 0:
                        summary['level_effectiveness'][level]['wins'] += 1
                    else:
                        summary['level_effectiveness'][level]['losses'] += 1
                
                # Статистика по уверенности сигналов
                confidence = trade.get('confidence', 0)
                if confidence > 75:
                    summary['signal_quality']['high_confidence'] += 1
                elif confidence > 50:
                    summary['signal_quality']['medium_confidence'] += 1
                else:
                    summary['signal_quality']['low_confidence'] += 1
        
        return summary

    def print_strategy_summary(self, summary: Dict):
        """Вывод итоговой статистики по стратегии"""
        print("\n" + "="*80)
        print("📋 ИТОГОВАЯ СТАТИСТИКА СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'")
        print("="*80)
        
        print(f"\n🔍 АНАЛИЗ УРОВНЕЙ:")
        print(f"  Всего уникальных уровней: {len(summary['level_effectiveness'])}")
        
        if summary['level_effectiveness']:
            print(f"  Топ-5 самых эффективных уровней:")
            level_stats = []
            for level, stats in summary['level_effectiveness'].items():
                winrate = (stats['wins'] / stats['trades'] * 100) if stats['trades'] > 0 else 0
                level_stats.append({
                    'level': level,
                    'trades': stats['trades'],
                    'winrate': winrate,
                    'total_pnl': stats['total_pnl']
                })
            
            # Сортируем по общему P&L
            level_stats.sort(key=lambda x: x['total_pnl'], reverse=True)
            
            for i, stat in enumerate(level_stats[:5], 1):
                print(f"    {i}. ${stat['level']:,.2f}: {stat['trades']} сделок, винрейт {stat['winrate']:.1f}%, P&L ${stat['total_pnl']:+,.2f}")
        
        print(f"\n🎯 КАЧЕСТВО СИГНАЛОВ:")
        total_signals = (summary['signal_quality']['high_confidence'] + 
                        summary['signal_quality']['medium_confidence'] + 
                        summary['signal_quality']['low_confidence'])
        
        if total_signals > 0:
            print(f"  Высокая уверенность (>75%): {summary['signal_quality']['high_confidence']} ({summary['signal_quality']['high_confidence']/total_signals*100:.1f}%)")
            print(f"  Средняя уверенность (50-75%): {summary['signal_quality']['medium_confidence']} ({summary['signal_quality']['medium_confidence']/total_signals*100:.1f}%)")
            print(f"  Низкая уверенность (<50%): {summary['signal_quality']['low_confidence']} ({summary['signal_quality']['low_confidence']/total_signals*100:.1f}%)")
        
        print(f"\n💡 РЕКОМЕНДАЦИИ:")
        if summary['level_effectiveness']:
            best_level = max(summary['level_effectiveness'].items(), key=lambda x: x[1]['total_pnl'])
            worst_level = min(summary['level_effectiveness'].items(), key=lambda x: x[1]['total_pnl'])
            
            print(f"  ✅ Лучший уровень: ${best_level[0]:,.2f} (P&L: ${best_level[1]['total_pnl']:+,.2f})")
            print(f"  ❌ Худший уровень: ${worst_level[0]:,.2f} (P&L: ${worst_level[1]['total_pnl']:+,.2f})")
        
        if total_signals > 0:
            high_conf_ratio = summary['signal_quality']['high_confidence'] / total_signals
            if high_conf_ratio > 0.6:
                print(f"  ✅ Высокое качество сигналов ({high_conf_ratio*100:.1f}% высокой уверенности)")
            elif high_conf_ratio < 0.3:
                print(f"  ⚠️ Низкое качество сигналов ({high_conf_ratio*100:.1f}% высокой уверенности)")
            else:
                print(f"  📊 Среднее качество сигналов ({high_conf_ratio*100:.1f}% высокой уверенности)")
        
        print("="*80)
    
    def print_results(self, results: Dict):
        """Вывод результатов бэктеста"""
        if not results:
            print("❌ Нет результатов для отображения")
            return
        
        print("\n" + "="*80)
        print("📊 РЕЗУЛЬТАТЫ БЭКТЕСТА СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'")
        print("="*80)
        
        # Информация о причине завершения
        if self.current_balance <= self.min_balance_threshold:
            print(f"\n⚠️  ПРИЧИНА ЗАВЕРШЕНИЯ: ДЕПОЗИТ ИСТОЩЕН НА 50%")
            print(f"   Начальный депозит: ${self.initial_deposit:,.2f}")
            print(f"   Конечный баланс: ${self.current_balance:,.2f}")
            print(f"   Потеря: ${self.initial_deposit - self.current_balance:,.2f} ({(self.initial_deposit - self.current_balance) / self.initial_deposit * 100:.1f}%)")
            print(f"   Минимальный порог: ${self.min_balance_threshold:,.2f}")
        else:
            print(f"\n✅ БЭКТЕСТ ЗАВЕРШЕН УСПЕШНО")
            print(f"   Начальный депозит: ${self.initial_deposit:,.2f}")
            print(f"   Конечный баланс: ${self.current_balance:,.2f}")
            print(f"   Прибыль/убыток: ${self.current_balance - self.initial_deposit:+,.2f} ({(self.current_balance - self.initial_deposit) / self.initial_deposit * 100:+.1f}%)")
        
        print(f"\n💰 ОБЩАЯ СТАТИСТИКА:")
        print(f"  Начальный депозит: ${self.initial_deposit:,.2f}")
        print(f"  Конечный баланс: ${results['final_balance']:,.2f}")
        print(f"  Общая доходность: ${results['total_return']:+,.2f} ({results['total_return_pct']:+.2f}%)")
        print(f"  Максимальная просадка: ${results['max_drawdown']:,.2f} ({results['max_drawdown_pct']:.2f}%)")
        
        print(f"\n📈 ТОРГОВАЯ СТАТИСТИКА:")
        print(f"  Всего сделок: {results['total_trades']}")
        print(f"  Прибыльных: {results['winning_trades']}")
        print(f"  Убыточных: {results['losing_trades']}")
        print(f"  Винрейт: {results['winrate']:.2f}%")
        print(f"  Profit Factor: {results['profit_factor']:.2f}")
        
        print(f"\n📊 СРЕДНИЕ ПОКАЗАТЕЛИ:")
        print(f"  Средняя прибыль: ${results['avg_win']:,.2f}")
        print(f"  Средний убыток: ${results['avg_loss']:,.2f}")
        print(f"  Соотношение: {results['avg_win']/results['avg_loss']:.2f}:1" if results['avg_loss'] > 0 else "  Соотношение: ∞:1")
        
        print(f"\n🎯 СТАТИСТИКА ПО ТИПАМ СДЕЛОК:")
        print(f"  LONG сделок: {results['long_trades']} (винрейт: {results['long_winrate']:.2f}%)")
        print(f"  SHORT сделок: {results['short_trades']} (винрейт: {results['short_winrate']:.2f}%)")
        
        print(f"\n🚪 СТАТИСТИКА ПО ВЫХОДАМ:")
        for reason, count in results['exit_stats'].items():
            print(f"  {reason}: {count} сделок")
        
        # Добавляем вывод итоговой статистики по стратегии
        strategy_summary = self.calculate_strategy_summary()
        self.print_strategy_summary(strategy_summary)
        
        print("="*80) 