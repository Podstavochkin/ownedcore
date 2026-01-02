#!/bin/bash
# Скрипт для загрузки данных в систему

echo "🚀 Загрузка данных в систему"
echo ""

# Шаг 1: Инициализация торговых пар
echo "📊 Шаг 1: Инициализация торговых пар..."
python3 scripts/init_data.py

if [ $? -ne 0 ]; then
    echo "❌ Ошибка инициализации данных"
    exit 1
fi

echo ""
echo "📈 Шаг 2: Запуск анализа всех пар..."
echo "   Это может занять 1-2 минуты..."
echo ""

# Запускаем анализ
TASK_RESPONSE=$(curl -s -X POST http://localhost:8000/api/force-analysis)
TASK_ID=$(echo $TASK_RESPONSE | python3 -c "import sys, json; print(json.load(sys.stdin).get('task_id', ''))" 2>/dev/null)

if [ -z "$TASK_ID" ]; then
    echo "❌ Не удалось запустить анализ"
    exit 1
fi

echo "✅ Анализ запущен (Task ID: $TASK_ID)"
echo ""
echo "⏳ Ожидание результатов анализа..."
echo "   (Это может занять 1-2 минуты)"
echo ""

# Ждем результатов
for i in {1..30}; do
    sleep 2
    STATUS=$(curl -s http://localhost:8000/api/pairs-status | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('status', 'unknown'))" 2>/dev/null)
    
    if [ "$STATUS" = "success" ] || [ "$STATUS" != "processing" ]; then
        echo "✅ Анализ завершен!"
        break
    fi
    
    echo -n "."
done

echo ""
echo ""
echo "📊 Проверка данных:"
echo ""

# Проверяем сигналы
SIGNALS_COUNT=$(curl -s http://localhost:8000/api/signals | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('signals', [])))" 2>/dev/null)
echo "   Сигналов в базе: $SIGNALS_COUNT"

# Проверяем пары
PAIRS_DATA=$(curl -s http://localhost:8000/api/pairs-status)
PAIRS_COUNT=$(echo $PAIRS_DATA | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('results', {})))" 2>/dev/null)
echo "   Пар проанализировано: $PAIRS_COUNT"

echo ""
echo "✅ Готово! Откройте в браузере:"
echo "   http://localhost:8000/dashboard"
echo ""

