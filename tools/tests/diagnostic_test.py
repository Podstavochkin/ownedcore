"""
Диагностический тест стратегии "Королевские уровни"
Автор: CryptoProject v0.01
Описание: Максимально мягкие фильтры для диагностики проблем
"""

import sys
import os
from datetime import datetime, timedelta

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest_engine import BacktestEngine
from royal_levels_strategy import RoyalLevelsStrategy

class LogCapture:
    """Класс для захвата и сохранения логов"""
    def __init__(self, filename):
        self.filename = filename
        self.original_stdout = sys.stdout
        self.log_file = open(filename, 'w', encoding='utf-8')
        
    def write(self, text):
        # Записываем ТОЛЬКО в файл
        self.log_file.write(text)
        self.log_file.flush()
        
    def flush(self):
        self.log_file.flush()
        
    def close(self):
        self.log_file.close()
        sys.stdout = self.original_stdout

def diagnostic_test():
    """Диагностический тест с максимально мягкими фильтрами"""
    # Создаем файл для логов
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"diagnostic_test_logs_{timestamp}.txt"
    
    print("🔍 ДИАГНОСТИЧЕСКИЙ ТЕСТ СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'")
    print("="*80)
    print(f"📝 Подробные логи сохраняются в файл: {log_filename}")
    print("🎯 Цель: найти проблемы с фильтрами стратегии")
    print()
    
    # Параметры для диагностики (последние 7 дней)
    symbol = 'BTC/USDT'
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    initial_deposit = 10000
    commission = 0.001
    
    print(f"📊 Параметры диагностики:")
    print(f"  Символ: {symbol}")
    print(f"  Период: {start_date} - {end_date} (7 дней)")
    print(f"  Начальный депозит: ${initial_deposit:,.2f}")
    print()
    
    # МАКСИМАЛЬНО МЯГКИЕ ПАРАМЕТРЫ СТРАТЕГИИ
    round_tolerance = 50  # Очень мягкий допуск
    silent_mode = True    # Тихий режим для консоли
    
    print(f"⚙️ ДИАГНОСТИЧЕСКИЕ ПАРАМЕТРЫ СТРАТЕГИИ:")
    print(f"  Допуск округлости: {round_tolerance} (ОЧЕНЬ МЯГКИЙ)")
    print(f"  Тихий режим: {'Да' if silent_mode else 'Нет'}")
    print(f"  Цель: найти ЛЮБЫЕ уровни и сигналы")
    print()
    
    # Создаем движок бэктеста
    engine = BacktestEngine(initial_deposit=initial_deposit, commission=commission)
    
    # Создаем стратегию с диагностическими параметрами
    strategy = RoyalLevelsStrategy(
        deposit=initial_deposit,
        round_tolerance=round_tolerance,
        silent=silent_mode
    )
    
    # Временно ослабляем фильтры для диагностики
    print("🔧 ВРЕМЕННО ОСЛАБЛЯЕМ ФИЛЬТРЫ ДЛЯ ДИАГНОСТИКИ:")
    print("  - Увеличиваем допуск округлости до 50")
    print("  - Включаем все логи в файл")
    print("  - Анализируем каждый шаг")
    print()
    
    engine.strategy = strategy
    
    print("📥 Скачивание данных за последние 7 дней...")
    
    # Скачиваем данные
    data = engine.download_historical_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date
    )
    
    if not data or len(data) < 4:
        print("❌ Не удалось скачать данные")
        return
    
    print(f"✅ Загружено данных:")
    for tf, df in data.items():
        print(f"  {tf}: {len(df)} свечей")
    
    # Запускаем диагностику с полными логами в файл
    print("\n🔍 Запуск диагностики...")
    print("📋 Подробные логи записываются в файл...")
    
    # Создаем отдельный поток для индикатора прогресса в консоли
    import threading
    import time
    
    progress_stop = threading.Event()
    progress_thread = None
    
    def progress_indicator():
        """Индикатор прогресса в консоли"""
        steps = 0
        while not progress_stop.is_set():
            steps += 1
            # Примерный прогресс на основе времени
            estimated_progress = min(95, steps * 3)  # Быстрее для диагностики
            print(f"\r🔍 Прогресс диагностики: {estimated_progress}%", end="", flush=True)
            time.sleep(1.5)  # Обновляем каждые 1.5 секунды
    
    # Запускаем индикатор прогресса в отдельном потоке
    progress_thread = threading.Thread(target=progress_indicator)
    progress_thread.daemon = True
    progress_thread.start()
    
    # Временно перенаправляем вывод в файл для полных логов
    log_capture = LogCapture(log_filename)
    sys.stdout = log_capture
    
    try:
        # Записываем в файл полную информацию
        print("🔍 ДИАГНОСТИЧЕСКИЙ ТЕСТ СТРАТЕГИИ 'КОРОЛЕВСКИЕ УРОВНИ'")
        print("="*80)
        print(f"📝 Полные диагностические логи")
        print()
        
        # Временно включаем логи стратегии для файла
        engine.strategy.silent = False
        
        # ДИАГНОСТИКА: Проверяем каждый компонент отдельно
        print("\n🔍 ДИАГНОСТИКА КОМПОНЕНТОВ СТРАТЕГИИ:")
        print("="*60)
        
        # 1. Проверяем поиск уровней
        print("\n1️⃣ ПРОВЕРКА ПОИСКА УРОВНЕЙ:")
        df_1h = data['1h']
        royal_levels = strategy.find_royal_levels(df_1h)
        print(f"   Найдено королевских уровней: {len(royal_levels)}")
        
        if royal_levels:
            for i, level in enumerate(royal_levels, 1):
                print(f"   Уровень {i}: ${level['level']:,.2f} ({level['type']})")
        else:
            print("   ❌ Уровни не найдены - проблема в фильтрах уровней")
        
        # 2. Проверяем фильтр тренда
        print("\n2️⃣ ПРОВЕРКА ФИЛЬТРА ТРЕНДА:")
        df_4h = data['4h']
        trend = strategy.check_trend_filter(df_4h)
        print(f"   Тренд: {trend}")
        
        # 3. Проверяем сигналы (если есть уровни)
        if royal_levels:
            print("\n3️⃣ ПРОВЕРКА СИГНАЛОВ:")
            df_5m = data['5m']
            
            for i, level in enumerate(royal_levels[:3], 1):  # Проверяем первые 3 уровня
                print(f"   Анализ уровня {i}: ${level['level']:,.2f}")
                signal_data = strategy.check_second_approach(df_5m, level)
                print(f"     Сигнал: {signal_data['signal']}")
                print(f"     Уверенность: {signal_data['confidence']:.1f}%")
                print(f"     Условий выполнено: {signal_data['conditions_met']}/4")
        
        # Запускаем бэктест
        print("\n🎯 ЗАПУСК ДИАГНОСТИЧЕСКОГО БЭКТЕСТА...")
        results = engine.run_backtest(data, symbol)
        
        # Записываем результаты в файл
        if results:
            engine.print_results(results)
        else:
            print("\n❌ ДИАГНОСТИКА: Бэктест не дал результатов")
            print("🔍 Это означает, что даже с очень мягкими фильтрами стратегия не находит сигналов")
            print("💡 Возможные причины:")
            print("   - Проблема в логике поиска уровней")
            print("   - Проблема в логике генерации сигналов")
            print("   - Неподходящий период истории")
            print("   - Ошибка в коде стратегии")
            
    except Exception as e:
        print(f"❌ ОШИБКА ДИАГНОСТИКИ: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Останавливаем индикатор прогресса
        progress_stop.set()
        if progress_thread:
            progress_thread.join(timeout=1)
        
        # Закрываем захват логов
        log_capture.close()
    
    # Показываем 100% завершения
    print("\r✅ Диагностика завершена: 100%")
    
    # Возвращаем вывод в консоль для кратких результатов
    print("\n" + "="*60)
    print("📊 КРАТКИЕ РЕЗУЛЬТАТЫ ДИАГНОСТИКИ")
    print("="*60)
    
    if results:
        print(f"💰 Доходность: {results['total_return_pct']:+.2f}%")
        print(f"🎯 Винрейт: {results['winrate']:.2f}%")
        print(f"📈 Сделок: {results['total_trades']}")
        print(f"📉 Макс. просадка: {results['max_drawdown_pct']:.2f}%")
        print(f"💵 Конечный баланс: ${results['final_balance']:,.2f}")
        
        if results['total_trades'] > 0:
            print(f"\n📊 Детализация:")
            print(f"  Прибыльных: {results['winning_trades']}")
            print(f"  Убыточных: {results['losing_trades']}")
            print(f"  LONG: {results['long_trades']} (винрейт: {results['long_winrate']:.1f}%)")
            print(f"  SHORT: {results['short_trades']} (винрейт: {results['short_winrate']:.1f}%)")
            
            print(f"\n🚪 Выходы:")
            for reason, count in results['exit_stats'].items():
                print(f"  {reason}: {count}")
        
        print("="*60)
        
    else:
        print("❌ Диагностика: Бэктест не дал результатов")
        print("📋 Проверьте файл логов для детального анализа")
    
    print(f"\n💾 Все диагностические логи сохранены в файл: {log_filename}")
    print("📋 Откройте этот файл для полного анализа")

if __name__ == "__main__":
    diagnostic_test() 