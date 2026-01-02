"""
Скрипт для запуска бэктеста стратегии "Королевские уровни"
Автор: CryptoProject v0.01
Описание: Полный бэктест стратегии на исторических данных BTC/USDT
"""

import sys
import os
from datetime import datetime

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest_engine import BacktestEngine
from royal_levels_strategy import RoyalLevelsStrategy

def main():
    """Основная функция запуска бэктеста"""
    print("🚀 ЗАПУСК БЭКТЕСТА СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'")
    print("="*80)
    
    # Параметры бэктеста
    symbol = 'BTC/USDT'
    start_date = '2022-01-01'
    end_date = '2024-12-31'
    initial_deposit = 10000
    commission = 0.001  # 0.1%
    
    print(f"📊 Параметры бэктеста:")
    print(f"  Символ: {symbol}")
    print(f"  Период: {start_date} - {end_date}")
    print(f"  Начальный депозит: ${initial_deposit:,.2f}")
    print(f"  Комиссия: {commission*100:.1f}%")
    print()
    
    # Параметры стратегии
    round_tolerance = 10  # Допуск округлости для уровней
    silent_mode = False   # Тихий режим логирования
    
    print(f"⚙️ Параметры стратегии:")
    print(f"  Допуск округлости: {round_tolerance}")
    print(f"  Тихий режим: {'Да' if silent_mode else 'Нет'}")
    print()
    
    # Создаем движок бэктеста
    engine = BacktestEngine(initial_deposit=initial_deposit, commission=commission)
    
    # Обновляем стратегию с новыми параметрами
    engine.strategy = RoyalLevelsStrategy(
        deposit=initial_deposit,
        round_tolerance=round_tolerance,
        silent=silent_mode
    )
    
    try:
        # Проверяем, есть ли уже сохраненные данные
        print("🔍 Проверка наличия исторических данных...")
        saved_data = engine.load_data_from_csv('BTCUSDT')
        
        if not saved_data or len(saved_data) < 4:
            print("📥 Исторические данные не найдены. Скачиваем с биржи...")
            
            # Скачиваем исторические данные
            data = engine.download_historical_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            if not data or len(data) < 4:
                print("❌ Не удалось скачать данные")
                return
            
            # Сохраняем данные в CSV
            engine.save_data_to_csv(data, 'BTCUSDT')
        else:
            print("✅ Найдены сохраненные исторические данные")
            data = saved_data
        
        # Запускаем бэктест
        print("\n🎯 Запуск бэктеста...")
        results = engine.run_backtest(data, symbol)
        
        if results:
            # Выводим результаты
            engine.print_results(results)
            
            # Сохраняем результаты в файл
            save_results_to_file(results, symbol, start_date, end_date)
            
            print(f"\n✅ Бэктест завершен успешно!")
            print(f"📈 Итоговая доходность: {results['total_return_pct']:+.2f}%")
            print(f"🎯 Винрейт: {results['winrate']:.2f}%")
            print(f"💰 Конечный баланс: ${results['final_balance']:,.2f}")
            
        else:
            print("❌ Бэктест не дал результатов")
            
    except Exception as e:
        print(f"❌ ОШИБКА ПРИ ВЫПОЛНЕНИИ БЭКТЕСТА: {e}")
        import traceback
        traceback.print_exc()

def save_results_to_file(results: dict, symbol: str, start_date: str, end_date: str):
    """Сохранение результатов бэктеста в файл"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"backtest_results_{symbol.replace('/', '')}_{start_date}_{end_date}_{timestamp}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("РЕЗУЛЬТАТЫ БЭКТЕСТА СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Параметры бэктеста:\n")
        f.write(f"  Символ: {symbol}\n")
        f.write(f"  Период: {start_date} - {end_date}\n")
        f.write(f"  Начальный депозит: $10,000\n")
        f.write(f"  Комиссия: 0.1%\n")
        f.write(f"  Порог истощения депозита: 50%\n\n")
        
        # Информация о причине завершения
        if results.get('final_balance', 10000) <= 5000:  # 50% от 10000
            f.write("ПРИЧИНА ЗАВЕРШЕНИЯ: ДЕПОЗИТ ИСТОЩЕН НА 50%\n")
            f.write(f"  Начальный депозит: $10,000\n")
            f.write(f"  Конечный баланс: ${results.get('final_balance', 0):,.2f}\n")
            f.write(f"  Потеря: ${10000 - results.get('final_balance', 0):,.2f} ({(10000 - results.get('final_balance', 0)) / 10000 * 100:.1f}%)\n")
            f.write(f"  Минимальный порог: $5,000\n\n")
        else:
            f.write("БЭКТЕСТ ЗАВЕРШЕН УСПЕШНО\n")
            f.write(f"  Начальный депозит: $10,000\n")
            f.write(f"  Конечный баланс: ${results.get('final_balance', 0):,.2f}\n")
            f.write(f"  Прибыль/убыток: ${results.get('final_balance', 0) - 10000:+,.2f} ({(results.get('final_balance', 0) - 10000) / 10000 * 100:+.1f}%)\n\n")
        
        f.write(f"ОБЩАЯ СТАТИСТИКА:\n")
        f.write(f"  Конечный баланс: ${results['final_balance']:,.2f}\n")
        f.write(f"  Общая доходность: ${results['total_return']:+,.2f} ({results['total_return_pct']:+.2f}%)\n")
        f.write(f"  Максимальная просадка: ${results['max_drawdown']:,.2f} ({results['max_drawdown_pct']:.2f}%)\n\n")
        
        f.write(f"ТОРГОВАЯ СТАТИСТИКА:\n")
        f.write(f"  Всего сделок: {results['total_trades']}\n")
        f.write(f"  Прибыльных: {results['winning_trades']}\n")
        f.write(f"  Убыточных: {results['losing_trades']}\n")
        f.write(f"  Винрейт: {results['winrate']:.2f}%\n")
        f.write(f"  Profit Factor: {results['profit_factor']:.2f}\n\n")
        
        f.write(f"СРЕДНИЕ ПОКАЗАТЕЛИ:\n")
        f.write(f"  Средняя прибыль: ${results['avg_win']:,.2f}\n")
        f.write(f"  Средний убыток: ${results['avg_loss']:,.2f}\n")
        if results['avg_loss'] > 0:
            f.write(f"  Соотношение: {results['avg_win']/results['avg_loss']:.2f}:1\n\n")
        else:
            f.write(f"  Соотношение: ∞:1\n\n")
        
        f.write(f"СТАТИСТИКА ПО ТИПАМ СДЕЛОК:\n")
        f.write(f"  LONG сделок: {results['long_trades']} (винрейт: {results['long_winrate']:.2f}%)\n")
        f.write(f"  SHORT сделок: {results['short_trades']} (винрейт: {results['short_winrate']:.2f}%)\n\n")
        
        f.write(f"СТАТИСТИКА ПО ВЫХОДАМ:\n")
        for reason, count in results['exit_stats'].items():
            f.write(f"  {reason}: {count} сделок\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write("ДЕТАЛЬНАЯ ИСТОРИЯ СДЕЛОК:\n")
        f.write("="*80 + "\n\n")
        
        for i, trade in enumerate(results['trades'], 1):
            f.write(f"Сделка #{i}:\n")
            f.write(f"  Тип: {trade['signal_type']}\n")
            f.write(f"  Вход: {trade['entry_time']} по ${trade['entry_price']:,.2f}\n")
            f.write(f"  Выход: {trade['exit_time']} по ${trade['exit_price']:,.2f}\n")
            f.write(f"  Причина выхода: {trade['exit_reason']}\n")
            f.write(f"  P&L: ${trade['pnl']:+,.2f} ({trade['pnl_pct']:+.2f}%)\n")
            f.write(f"  Уровень: ${trade['level']:,.2f}\n")
            f.write(f"  Уверенность: {trade['confidence']:.1f}%\n\n")
    
    print(f"💾 Результаты сохранены в файл: {filename}")

if __name__ == "__main__":
    main() 