# План реализации детекции ценовых фигур (Chart Patterns)

## 📊 Обзор

Ценовые фигуры (chart patterns) — это крупные формации, которые формируются на графике из множества свечей и указывают на возможное продолжение или разворот тренда. В отличие от свечных паттернов (1-3 свечи), ценовые фигуры формируются на протяжении десятков или сотен свечей.

**Источник:** [Скальпинг стратегии криптовалюты](https://livetouring.org/skalping-strategii-kriptovaljuty-s-primerami/)

---

## 🎯 12 основных ценовых фигур

### Разворотные фигуры (Reversal Patterns):

1. **Голова и плечи (Head and Shoulders)** 🔴
   - Три пика: средний выше двух боковых
   - Разворот вниз после пробоя линии шеи
   - Надежность: высокая

2. **Двойная вершина (Double Top)** 🔴
   - Два пика на одном уровне
   - Разворот вниз после пробоя
   - Надежность: высокая

3. **Двойное дно (Double Bottom)** 🟢
   - Два минимума на одном уровне
   - Разворот вверх после пробоя
   - Надежность: высокая

4. **Восходящий клин (Rising Wedge)** 🔴
   - Сходящиеся линии, обе восходящие
   - Разворот вниз
   - Надежность: средняя

5. **Нисходящий клин (Falling Wedge)** 🟢
   - Сходящиеся линии, обе нисходящие
   - Разворот вверх
   - Надежность: средняя

### Продолжение тренда (Continuation Patterns):

6. **Флаг (Flag)** 📈📉
   - Резкое движение (флагшток) + консолидация
   - Продолжение в направлении флагштока
   - Надежность: высокая

7. **Вымпел (Pennant)** 📈📉
   - Резкое движение + симметричный треугольник
   - Продолжение в направлении движения
   - Надежность: высокая

8. **Канал (Channel)** 📈📉
   - Параллельные линии поддержки/сопротивления
   - Продолжение тренда внутри канала
   - Надежность: средняя

9. **Восходящий треугольник (Ascending Triangle)** 🟢
   - Горизонтальное сопротивление + восходящая поддержка
   - Пробой вверх
   - Надежность: высокая

10. **Нисходящий треугольник (Descending Triangle)** 🔴
    - Горизонтальная поддержка + нисходящее сопротивление
    - Пробой вниз
    - Надежность: высокая

### Консолидация (Consolidation Patterns):

11. **Симметричный треугольник (Symmetrical Triangle)** ⚪
    - Сходящиеся линии поддержки и сопротивления
    - Неопределенность направления
    - Надежность: низкая (требует подтверждения)

12. **Прямоугольник (Rectangle)** ⚪
    - Горизонтальные параллельные линии
    - Консолидация перед пробоем
    - Надежность: средняя

---

## 🏗️ Архитектура реализации

### 1. Модель данных (ChartPattern)

```python
class ChartPattern(Base):
    """Модель для хранения обнаруженных ценовых фигур"""
    __tablename__ = 'chart_patterns'
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False, index=True)
    pattern_type = Column(String(50), nullable=False)  # head_and_shoulders, flag, etc.
    pattern_category = Column(String(20), nullable=False)  # reversal, continuation, consolidation
    direction = Column(String(10), nullable=False)  # bullish, bearish, neutral
    reliability = Column(Float, default=0.5)  # 0.0-1.0
    
    # Геометрия фигуры
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    confirmation_time = Column(DateTime(timezone=True))  # Время подтверждения (пробой)
    
    # Ключевые уровни фигуры
    support_level = Column(Float)  # Уровень поддержки/нижняя граница
    resistance_level = Column(Float)  # Уровень сопротивления/верхняя граница
    neckline = Column(Float)  # Для Head and Shoulders, Double Top/Bottom
    target_price = Column(Float)  # Целевая цена после пробоя
    
    # Характеристики фигуры
    pattern_height = Column(Float)  # Высота фигуры в процентах
    pattern_width = Column(Integer)  # Ширина в свечах
    volume_confirmation = Column(Boolean, default=False)  # Подтверждение объемом
    
    # Статус
    is_active = Column(Boolean, default=True, index=True)
    is_confirmed = Column(Boolean, default=False)  # Пробой произошел?
    
    # Метаданные
    candles_count = Column(Integer)  # Количество свечей в фигуре
    pattern_data = Column(JSON)  # Детальные данные (точки, линии и т.д.)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
```

### 2. Детектор ценовых фигур (ChartPatternDetector)

```python
class ChartPatternDetector:
    """Детектор ценовых фигур на основе уровней поддержки/сопротивления"""
    
    def __init__(self):
        self.min_pattern_candles = 20  # Минимум свечей для фигуры
        self.max_pattern_candles = 200  # Максимум свечей
        self.tolerance = 0.002  # 0.2% толерантность для уровней
        
    def detect_all_patterns(self, candles: List[Dict], symbol: str, timeframe: str) -> List[Dict]:
        """Детектирует все возможные фигуры"""
        patterns = []
        
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
        
        return patterns
    
    def detect_head_and_shoulders(self, candles, symbol, timeframe):
        """Детектирует фигуру 'Голова и плечи'"""
        # Алгоритм:
        # 1. Найти локальные максимумы
        # 2. Найти три пика: средний выше двух боковых
        # 3. Проверить симметрию
        # 4. Определить линию шеи
        # 5. Вычислить целевую цену
        pass
    
    def detect_flags(self, candles, symbol, timeframe):
        """Детектирует фигуру 'Флаг'"""
        # Алгоритм:
        # 1. Найти резкое движение (флагшток)
        # 2. Найти консолидацию после движения
        # 3. Проверить параллельность линий флага
        # 4. Определить направление пробоя
        pass
    
    # ... остальные методы детекции
```

### 3. Алгоритмы детекции

#### 3.1. Голова и плечи (Head and Shoulders)

```python
def detect_head_and_shoulders(candles):
    """
    Алгоритм:
    1. Найти локальные максимумы (пики)
    2. Отфильтровать по высоте (минимум 1% от цены)
    3. Найти три пика: левое плечо, голова, правое плечо
    4. Проверить: голова > оба плеча
    5. Найти линию шеи (минимумы между пиками)
    6. Вычислить целевую цену: neckline - (head - neckline)
    """
    peaks = find_local_maxima(candles, min_height_pct=0.01)
    
    for i in range(len(peaks) - 2):
        left_shoulder = peaks[i]
        head = peaks[i + 1]
        right_shoulder = peaks[i + 2]
        
        # Проверка условий
        if head['price'] > left_shoulder['price'] and \
           head['price'] > right_shoulder['price'] and \
           abs(left_shoulder['price'] - right_shoulder['price']) / head['price'] < 0.01:
            
            # Найти линию шеи
            neckline = find_neckline(candles, left_shoulder, head, right_shoulder)
            
            # Вычислить целевую цену
            pattern_height = head['price'] - neckline
            target_price = neckline - pattern_height
            
            return {
                'pattern_type': 'head_and_shoulders',
                'category': 'reversal',
                'direction': 'bearish',
                'reliability': calculate_reliability(...),
                'support_level': neckline,
                'resistance_level': head['price'],
                'neckline': neckline,
                'target_price': target_price,
                'start_time': left_shoulder['time'],
                'end_time': right_shoulder['time']
            }
```

#### 3.2. Флаг (Flag)

```python
def detect_flags(candles):
    """
    Алгоритм:
    1. Найти резкое движение (флагшток) - минимум 2% за 5-20 свечей
    2. Найти консолидацию после движения (5-30 свечей)
    3. Проверить параллельность линий консолидации
    4. Определить направление: если флагшток вверх → пробой вверх
    """
    # Найти резкие движения
    strong_moves = find_strong_moves(candles, min_pct=0.02, min_candles=5, max_candles=20)
    
    for move in strong_moves:
        # Проверить консолидацию после движения
        consolidation = find_consolidation(candles, after=move['end_index'], max_candles=30)
        
        if consolidation and is_parallel(consolidation['support'], consolidation['resistance']):
            return {
                'pattern_type': 'flag',
                'category': 'continuation',
                'direction': 'bullish' if move['direction'] == 'up' else 'bearish',
                'reliability': 0.75,
                'support_level': consolidation['support'],
                'resistance_level': consolidation['resistance'],
                'target_price': calculate_flag_target(move, consolidation),
                'start_time': move['start_time'],
                'end_time': consolidation['end_time']
            }
```

---

## 🔗 Интеграция в стратегию

### 1. Использование в Elder Screens

```python
async def check_elder_screens(..., chart_patterns: List[ChartPattern] = None):
    """Проверка Elder Screens с учетом ценовых фигур"""
    
    # Получить релевантные фигуры
    relevant_patterns = get_patterns_near_level(chart_patterns, level_price, current_price)
    
    # Проверка противоречивых фигур
    if has_contradictory_pattern(relevant_patterns, signal_type):
        return False, {"reason": "Ценовая фигура противоречит сигналу"}
    
    # Бонус к score для фигур продолжения тренда
    continuation_bonus = calculate_continuation_bonus(relevant_patterns, signal_type)
    adjusted_score = level_score + continuation_bonus
    
    # Смягчение требований Screen 2 при подтвержденной фигуре
    confirmed_pattern = get_confirmed_pattern(relevant_patterns, signal_type)
    if confirmed_pattern and confirmed_pattern.reliability > 0.7:
        # Смягчаем требования к осцилляторам
        rsi_threshold = 75 if signal_type == 'LONG' else 25
        macd_tolerance = 0.01
```

### 2. Динамический Take Profit

```python
def calculate_tp_with_pattern(signal, chart_pattern):
    """Использует целевую цену фигуры для TP"""
    fixed_tp = signal.entry_price * (1 + 0.015)  # 1.5% фиксированный
    
    if chart_pattern and chart_pattern.target_price:
        pattern_tp = chart_pattern.target_price
        
        # Используем паттерн TP если он больше фиксированного и < 5%
        if pattern_tp > fixed_tp:
            tp_percent = (pattern_tp - signal.entry_price) / signal.entry_price
            if tp_percent < 0.05:  # Не больше 5%
                return pattern_tp
    
    return fixed_tp
```

### 3. Блокировка противоречивых сигналов

```python
def has_contradictory_pattern(patterns, signal_type):
    """Проверяет наличие противоречивых фигур"""
    for pattern in patterns:
        if pattern.is_confirmed:
            # Разворотные фигуры
            if pattern.pattern_category == 'reversal':
                if signal_type == 'LONG' and pattern.direction == 'bearish':
                    return True
                if signal_type == 'SHORT' and pattern.direction == 'bullish':
                    return True
            
            # Продолжение тренда
            if pattern.pattern_category == 'continuation':
                if signal_type == 'LONG' and pattern.direction == 'bearish':
                    return True
                if signal_type == 'SHORT' and pattern.direction == 'bullish':
                    return True
    
    return False
```

---

## 📊 Визуализация на графиках

### 1. Отображение фигур

- **Линии фигуры**: поддержка, сопротивление, линия шеи
- **Закрашенная область**: зона фигуры
- **Маркеры**: точки входа/выхода, целевая цена
- **Аннотации**: тип фигуры, надежность, направление

### 2. Интеграция в charts.html

```javascript
// Отображение фигур на графике
function renderChartPatterns(patterns) {
    patterns.forEach(pattern => {
        // Рисуем линии фигуры
        if (pattern.support_level) {
            drawSupportLine(pattern.support_level, pattern.start_time, pattern.end_time);
        }
        if (pattern.resistance_level) {
            drawResistanceLine(pattern.resistance_level, pattern.start_time, pattern.end_time);
        }
        if (pattern.neckline) {
            drawNeckline(pattern.neckline, pattern.start_time, pattern.end_time);
        }
        
        // Закрашиваем область фигуры
        fillPatternArea(pattern);
        
        // Добавляем маркеры
        addPatternMarkers(pattern);
        
        // Добавляем аннотацию
        addPatternAnnotation(pattern);
    });
}
```

---

## 🚀 План реализации

### Этап 1: Инфраструктура (1-2 дня)
- [ ] Создать модель `ChartPattern` в БД
- [ ] Создать Alembic миграцию
- [ ] Создать базовый класс `ChartPatternDetector`

### Этап 2: Детекция фигур (3-5 дней)
- [ ] Реализовать детекцию разворотных фигур (Head and Shoulders, Double Top/Bottom, Wedges)
- [ ] Реализовать детекцию фигур продолжения (Flag, Pennant, Channel, Triangles)
- [ ] Реализовать детекцию консолидации (Rectangle, Symmetrical Triangle)
- [ ] Добавить проверку надежности фигур

### Этап 3: Celery задачи (1 день)
- [ ] Создать задачу `detect_chart_patterns_periodic`
- [ ] Добавить в `celery_app.py`
- [ ] Настроить периодичность (каждые 15 минут для 1h, каждый час для 4h)

### Этап 4: API и визуализация (2-3 дня)
- [ ] Добавить API endpoint `/api/chart-patterns`
- [ ] Интегрировать в `charts.html`
- [ ] Добавить визуализацию фигур на графике

### Этап 5: Интеграция в стратегию (2-3 дня)
- [ ] Интегрировать в `check_elder_screens()`
- [ ] Реализовать блокировку противоречивых сигналов
- [ ] Добавить динамический TP на основе фигур
- [ ] Добавить бонус к score

### Этап 6: Тестирование и оптимизация (2-3 дня)
- [ ] Тестирование на исторических данных
- [ ] Оптимизация параметров детекции
- [ ] Анализ эффективности

---

## 📈 Ожидаемые результаты

### Улучшение качества сигналов:
- ✅ **Меньше ложных входов** — фигуры фильтруют слабые сигналы
- ✅ **Лучшие точки входа** — фигуры указывают на оптимальные моменты
- ✅ **Более точный TP** — целевые цены фигур
- ✅ **Предсказание разворотов** — разворотные фигуры предупреждают о смене тренда

### Метрики для отслеживания:
- **Win Rate с фигурами** vs **без фигур**
- **Average P&L** с фигурами vs без фигур
- **Количество заблокированных сигналов** из-за противоречивых фигур
- **Точность предсказаний** разворотных фигур

---

## ⚠️ Важные замечания

1. **Ценовые фигуры требуют больше данных** — минимум 20-50 свечей для формирования
2. **Подтверждение важно** — фигура считается активной только после пробоя
3. **Контекст имеет значение** — фигура на фоне сильного тренда надежнее
4. **Таймфрейм важен** — фигуры на 1h/4h надежнее, чем на 1m/5m
5. **Не перегружаем логику** — используем только подтвержденные фигуры (is_confirmed=True)

---

## 📚 Дополнительные ресурсы

- [Скальпинг стратегии криптовалюты](https://livetouring.org/skalping-strategii-kriptovaljuty-s-primerami/)
- [Chart Patterns Guide](https://www.investopedia.com/trading/chart-patterns/)
- [Technical Analysis Patterns](https://www.tradingview.com/ideas/chartpattern/)

