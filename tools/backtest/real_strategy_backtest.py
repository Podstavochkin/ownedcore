"""
Бэктест реальной стратегии с использованием той же логики, что и в production.
Использует analysis_engine для генерации сигналов и симулирует исполнение.
"""

import sys
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import asyncio
import time
import ccxt
import pandas as pd
import numpy as np

# Добавляем путь к проекту
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from core.analysis_engine import AnalysisEngine
from core.signal_manager import signal_manager
from core.trading.bybit_demo_client import bybit_demo_client

# Параметры стратегии (из config/demo_trading_settings.json)
TAKE_PROFIT_PCT = 1.5
STOP_LOSS_PCT = 0.5
COMMISSION_RATE = 0.035  # 0.035% Taker
ORDER_SIZE_USDT = 50
LEVERAGE = 2

# Параметры безубытка
BREAKEVEN_MINUTES = 40
BREAKEVEN_FAST_MINUTES = 15
BREAKEVEN_MIN_MOVE_PCT = 0.4
BREAKEVEN_SL_PCT = 0.1  # -0.1% от entry


class RealStrategyBacktest:
    """Бэктест с использованием реальной логики analysis_engine"""
    
    def __init__(self, initial_deposit: float = 10000):
        self.initial_deposit = initial_deposit
        self.current_balance = initial_deposit
        self.max_balance = initial_deposit
        
        # Статистика
        self.trades = []
        self.open_positions = {}  # {signal_id: position_dict}
        self.equity_curve = []
        
        # Анализ
        self.analysis_engine = AnalysisEngine()
        
        # Исторические данные (загружаются заранее)
        self.historical_data = {}  # {pair: {timeframe: DataFrame}}
        
    def load_historical_data(self, pairs: List[str], start_date: str, end_date: str, 
                            timeframes: List[str] = ['15m', '1h', '4h']) -> Dict:
        """
        Загружает исторические данные для всех пар и таймфреймов.
        
        Args:
            pairs: Список торговых пар (например, ['BTC/USDT', 'ETH/USDT'])
            start_date: Начальная дата (формат 'YYYY-MM-DD')
            end_date: Конечная дата (формат 'YYYY-MM-DD')
            timeframes: Список таймфреймов
            
        Returns:
            Словарь {pair: {timeframe: DataFrame}}
        """
        print(f"📥 Загрузка исторических данных...")
        print(f"   Период: {start_date} - {end_date}")
        print(f"   Пар: {len(pairs)}, таймфреймов: {len(timeframes)}")
        
        exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        data = {}
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        
        for pair in pairs:
            data[pair] = {}
            print(f"\n  📊 {pair}:")
            
            for tf in timeframes:
                print(f"    {tf}...", end=' ', flush=True)
                all_candles = []
                current_ts = start_ts
                
                while current_ts < end_ts:
                    try:
                        candles = exchange.fetch_ohlcv(pair, tf, since=current_ts, limit=1000)
                        if not candles:
                            break
                        all_candles.extend(candles)
                        current_ts = candles[-1][0] + 1
                        time.sleep(0.1)  # Rate limit (синхронный sleep)
                    except Exception as e:
                        print(f"❌ Ошибка: {e}")
                        break
                
                if all_candles:
                    df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df = df.set_index('timestamp')
                    df = df.drop_duplicates().sort_index()
                    df = df[(df.index >= start_date) & (df.index <= end_date)]
                    data[pair][tf] = df
                    print(f"✅ {len(df)} свечей")
                else:
                    print(f"❌ Нет данных")
        
        self.historical_data = data
        return data
    
    def simulate_order_execution(self, signal_type: str, level_price: float, 
                                 current_price: float, timestamp: datetime) -> Tuple[float, float]:
        """
        Симулирует исполнение ордера с учетом slippage и комиссий.
        
        Returns:
            (entry_price, commission)
        """
        # Slippage: 0.05% для market, 0% для limit (если цена в пределах 0.15%)
        price_deviation = abs(current_price - level_price) / level_price * 100
        
        if price_deviation <= 0.15:
            # Market order (очень близко к уровню)
            slippage = 0.0005  # 0.05%
            entry_price = current_price * (1 + slippage if signal_type == 'LONG' else 1 - slippage)
        else:
            # Limit order (исполняется по level_price, но может быть небольшой slippage)
            slippage = 0.0002  # 0.02%
            entry_price = level_price * (1 + slippage if signal_type == 'LONG' else 1 - slippage)
        
        # Комиссия
        quantity = ORDER_SIZE_USDT / entry_price
        commission = entry_price * quantity * (COMMISSION_RATE / 100)
        
        return entry_price, commission
    
    def simulate_tp_sl(self, signal_type: str, entry_price: float) -> Tuple[float, float]:
        """Рассчитывает TP и SL цены"""
        if signal_type == 'LONG':
            tp_price = entry_price * (1 + TAKE_PROFIT_PCT / 100)
            sl_price = entry_price * (1 - STOP_LOSS_PCT / 100)
        else:  # SHORT
            tp_price = entry_price * (1 - TAKE_PROFIT_PCT / 100)
            sl_price = entry_price * (1 + STOP_LOSS_PCT / 100)
        
        return tp_price, sl_price
    
    def check_position_exit(self, position: Dict, candle: Dict, timestamp: datetime) -> Optional[Dict]:
        """
        Проверяет, нужно ли закрыть позицию (TP/SL).
        
        Returns:
            None если позиция остается открытой, иначе dict с exit_info
        """
        signal_type = position['signal_type']
        entry_price = position['entry_price']
        tp_price = position['tp_price']
        sl_price = position['sl_price']
        
        high = candle['high']
        low = candle['low']
        close = candle['close']
        
        # Проверяем TP/SL
        if signal_type == 'LONG':
            if low <= sl_price:
                exit_price = sl_price
                exit_reason = 'STOP_LOSS'
            elif high >= tp_price:
                exit_price = tp_price
                exit_reason = 'TAKE_PROFIT'
            else:
                return None
        else:  # SHORT
            if high >= sl_price:
                exit_price = sl_price
                exit_reason = 'STOP_LOSS'
            elif low <= tp_price:
                exit_price = tp_price
                exit_reason = 'TAKE_PROFIT'
            else:
                return None
        
        # Проверяем безубыток
        time_in_position = (timestamp - position['entry_time']).total_seconds() / 60
        move_pct = ((close - entry_price) / entry_price * 100) if signal_type == 'LONG' else ((entry_price - close) / entry_price * 100)
        
        # Если безубыток еще не применен и условия выполнены
        if not position.get('breakeven_applied', False):
            timeframe = position.get('timeframe', '15m')
            if timeframe == '15m':
                if move_pct >= BREAKEVEN_MIN_MOVE_PCT:
                    breakeven_minutes = BREAKEVEN_FAST_MINUTES
                else:
                    breakeven_minutes = BREAKEVEN_MINUTES
                
                if time_in_position >= breakeven_minutes and move_pct >= BREAKEVEN_MIN_MOVE_PCT:
                    # Применяем безубыток
                    if signal_type == 'LONG':
                        new_sl = entry_price * (1 - BREAKEVEN_SL_PCT / 100)
                    else:
                        new_sl = entry_price * (1 + BREAKEVEN_SL_PCT / 100)
                    
                    # Проверяем, не сработал ли новый SL
                    if signal_type == 'LONG' and low <= new_sl:
                        exit_price = new_sl
                        exit_reason = 'STOP_LOSS_BREAKEVEN'
                    elif signal_type == 'SHORT' and high >= new_sl:
                        exit_price = new_sl
                        exit_reason = 'STOP_LOSS_BREAKEVEN'
                    else:
                        position['sl_price'] = new_sl
                        position['breakeven_applied'] = True
                        return None  # Позиция остается открытой, но SL обновлен
        
        return {
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'exit_time': timestamp
        }
    
    async def run_backtest(self, start_date: str, end_date: str, 
                          pairs: Optional[List[str]] = None) -> Dict:
        """
        Запускает бэктест на исторических данных.
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            pairs: Список пар для тестирования (если None, использует все из TRADING_PAIRS)
        """
        if pairs is None:
            from core.analysis_engine import TRADING_PAIRS
            pairs = TRADING_PAIRS[:5]  # Для начала тестируем на 5 парах
        
        print(f"\n🚀 ЗАПУСК БЭКТЕСТА РЕАЛЬНОЙ СТРАТЕГИИ")
        print(f"=" * 80)
        print(f"Период: {start_date} - {end_date}")
        print(f"Пар: {len(pairs)}")
        print(f"Начальный депозит: ${self.initial_deposit:,.2f}")
        print(f"=" * 80)
        
        # Загружаем исторические данные
        if not self.historical_data:
            self.load_historical_data(pairs, start_date, end_date)
        
        # Создаем единый временной ряд из всех свечей 15m
        all_timestamps = set()
        for pair in pairs:
            if '15m' in self.historical_data.get(pair, {}):
                df = self.historical_data[pair]['15m']
                all_timestamps.update(df.index)
        
        sorted_timestamps = sorted(all_timestamps)
        print(f"\n📅 Всего временных точек: {len(sorted_timestamps)}")
        print(f"   От: {sorted_timestamps[0]}")
        print(f"   До: {sorted_timestamps[-1]}")
        
        # Основной цикл бэктеста
        signals_generated = 0
        signals_executed = 0
        
        for i, current_time in enumerate(sorted_timestamps[100:], start=100):  # Пропускаем первые 100 свечей для "прогрева"
            if i % 1000 == 0:
                progress = (i - 100) / (len(sorted_timestamps) - 100) * 100
                print(f"  ⏳ Прогресс: {i-100}/{len(sorted_timestamps)-100} ({progress:.1f}%) | {current_time} | Баланс: ${self.current_balance:,.2f}")
            
            # Для каждой пары проверяем сигналы и позиции
            for pair in pairs:
                if pair not in self.historical_data:
                    continue
                
                # Получаем данные до текущего момента (как в реальном времени)
                candles_15m = self.historical_data[pair]['15m']
                candles_1h = self.historical_data[pair].get('1h', pd.DataFrame())
                candles_4h = self.historical_data[pair].get('4h', pd.DataFrame())
                
                # Фильтруем только прошлые данные
                past_15m = candles_15m[candles_15m.index <= current_time]
                past_1h = candles_1h[candles_1h.index <= current_time] if not candles_1h.empty else pd.DataFrame()
                past_4h = candles_4h[candles_4h.index <= current_time] if not candles_4h.empty else pd.DataFrame()
                
                if len(past_15m) < 50:  # Нужно минимум 50 свечей для анализа
                    continue
                
                current_candle = past_15m.iloc[-1]
                current_price = current_candle['close']
                
                # Проверяем открытые позиции на закрытие
                for signal_id, position in list(self.open_positions.items()):
                    if position['pair'] == pair:
                        exit_info = self.check_position_exit(position, current_candle.to_dict(), current_time)
                        if exit_info:
                            self.close_position(signal_id, exit_info)
                
                # Генерируем новые сигналы (только если нет открытой позиции по этой паре)
                has_open_position = any(pos['pair'] == pair for pos in self.open_positions.values())
                if not has_open_position:
                    # Конвертируем DataFrame в формат, который ожидает analysis_engine
                    candles_15m_list = [{
                        'timestamp': int(ts.timestamp() * 1000),
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    } for ts, row in past_15m.iterrows()]
                    
                    # ВАЖНО: Здесь нужно вызвать analysis_engine.analyze_pair()
                    # Но это асинхронная функция, которая требует реального exchange
                    # Для упрощения, мы можем симулировать генерацию сигналов
                    # или использовать упрощенную логику
                    
                    # TODO: Интегрировать реальный вызов analysis_engine.analyze_pair()
                    # Это требует модификации для работы с историческими данными
        
        # Вычисляем финальную статистику
        results = self.calculate_statistics()
        return results
    
    def close_position(self, signal_id: int, exit_info: Dict):
        """Закрывает позицию и записывает сделку"""
        position = self.open_positions.pop(signal_id)
        
        entry_price = position['entry_price']
        exit_price = exit_info['exit_price']
        signal_type = position['signal_type']
        quantity = ORDER_SIZE_USDT / entry_price
        
        # Рассчитываем P&L
        if signal_type == 'LONG':
            pnl = (exit_price - entry_price) * quantity
        else:  # SHORT
            pnl = (entry_price - exit_price) * quantity
        
        # Комиссия за выход
        exit_commission = exit_price * quantity * (COMMISSION_RATE / 100)
        pnl -= exit_commission
        
        # Обновляем баланс
        self.current_balance += pnl
        
        # Записываем сделку
        trade = {
            'entry_time': position['entry_time'],
            'exit_time': exit_info['exit_time'],
            'pair': position['pair'],
            'signal_type': signal_type,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'exit_reason': exit_info['exit_reason'],
            'pnl': pnl,
            'pnl_pct': (pnl / self.initial_deposit) * 100,
            'quantity': quantity
        }
        
        self.trades.append(trade)
        
        # Обновляем максимальный баланс
        if self.current_balance > self.max_balance:
            self.max_balance = self.current_balance
    
    def calculate_statistics(self) -> Dict:
        """Вычисляет статистику бэктеста"""
        if not self.trades:
            return {'error': 'Нет сделок для анализа'}
        
        total_return = self.current_balance - self.initial_deposit
        total_return_pct = (total_return / self.initial_deposit) * 100
        
        winning_trades = [t for t in self.trades if t['pnl'] > 0]
        losing_trades = [t for t in self.trades if t['pnl'] < 0]
        
        winrate = (len(winning_trades) / len(self.trades)) * 100 if self.trades else 0
        
        total_profit = sum(t['pnl'] for t in winning_trades)
        total_loss = abs(sum(t['pnl'] for t in losing_trades))
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        
        avg_win = total_profit / len(winning_trades) if winning_trades else 0
        avg_loss = total_loss / len(losing_trades) if losing_trades else 0
        
        # Максимальная просадка
        max_drawdown = 0
        max_drawdown_pct = 0
        peak = self.initial_deposit
        running_balance = self.initial_deposit
        
        for trade in self.trades:
            # Симулируем баланс после каждой сделки
            running_balance += trade['pnl']
            if running_balance > peak:
                peak = running_balance
            
            drawdown = peak - running_balance
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_drawdown_pct = (drawdown / peak) * 100 if peak > 0 else 0
        
        return {
            'total_trades': len(self.trades),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'winrate': winrate,
            'total_return': total_return,
            'total_return_pct': total_return_pct,
            'total_profit': total_profit,
            'total_loss': total_loss,
            'profit_factor': profit_factor,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'final_balance': self.current_balance,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown_pct,
            'trades': self.trades
        }
    
    def print_results(self, results: Dict):
        """Выводит результаты бэктеста"""
        print("\n" + "=" * 80)
        print("📊 РЕЗУЛЬТАТЫ БЭКТЕСТА")
        print("=" * 80)
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
        
        print(f"\n💰 ОБЩАЯ СТАТИСТИКА:")
        print(f"  Начальный депозит: ${self.initial_deposit:,.2f}")
        print(f"  Конечный баланс: ${results['final_balance']:,.2f}")
        print(f"  Общая доходность: ${results['total_return']:+,.2f} ({results['total_return_pct']:+.2f}%)")
        
        print(f"\n📈 ТОРГОВАЯ СТАТИСТИКА:")
        print(f"  Всего сделок: {results['total_trades']}")
        print(f"  Прибыльных: {results['winning_trades']}")
        print(f"  Убыточных: {results['losing_trades']}")
        print(f"  Винрейт: {results['winrate']:.2f}%")
        print(f"  Profit Factor: {results['profit_factor']:.2f}")
        
        print(f"\n📊 СРЕДНИЕ ПОКАЗАТЕЛИ:")
        print(f"  Средняя прибыль: ${results['avg_win']:,.2f}")
        print(f"  Средний убыток: ${results['avg_loss']:,.2f}")
        if results['avg_loss'] > 0:
            print(f"  Соотношение: {results['avg_win']/results['avg_loss']:.2f}:1")
        
        print("=" * 80)


async def main():
    """Основная функция"""
    print("🚀 БЭКТЕСТ РЕАЛЬНОЙ СТРАТЕГИИ")
    print("=" * 80)
    
    # Параметры
    start_date = '2024-11-01'
    end_date = '2024-11-30'
    initial_deposit = 10000
    
    # Создаем бэктест
    backtest = RealStrategyBacktest(initial_deposit=initial_deposit)
    
    # Запускаем
    results = await backtest.run_backtest(start_date, end_date)
    
    # Выводим результаты
    backtest.print_results(results)
    
    return results


if __name__ == "__main__":
    asyncio.run(main())

