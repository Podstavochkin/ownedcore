#!/bin/bash
# Скрипт для перезагрузки исторических данных OHLCV с биржи

cd "$(dirname "$0")"
PROJECT_ROOT=$(pwd)

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Параметры по умолчанию
PAIR=""
TIMEFRAME=""
DAYS=3
FORCE_UPDATE_CLOSED=false
FULL_HISTORY=false
API_URL="http://localhost:8000"

# Функция помощи
show_help() {
    echo "🔄 Перезагрузка исторических данных OHLCV с биржи"
    echo ""
    echo "Использование:"
    echo "  ./RELOAD_HISTORICAL_OHLCV.sh [опции]"
    echo ""
    echo "Опции:"
    echo "  -p, --pair PAIR           Торговая пара (например, BTC/USDT)"
    echo "                           Если не указано - для всех пар"
    echo "  -t, --timeframe TF       Таймфрейм (1m, 5m, 15m, 1h, 4h)"
    echo "                           Если не указано - для всех таймфреймов (15m, 1h, 4h)"
    echo "  -d, --days DAYS          Количество дней истории (по умолчанию: 3)"
    echo "  -f, --force              Обновить даже закрытые свечи (для исправления ошибок)"
    echo "  --full-history           Полная перезагрузка истории (30 дней) для всех пар"
    echo "                           ВНИМАНИЕ: Это может занять 10-20 минут!"
    echo "  -h, --help               Показать эту справку"
    echo ""
    echo "Примеры:"
    echo "  # Перезагрузить последние 3 дня для конкретной пары и таймфрейма"
    echo "  ./RELOAD_HISTORICAL_OHLCV.sh -p BTC/USDT -t 1h -d 3"
    echo ""
    echo "  # Перезагрузить последний день для всех пар (быстро)"
    echo "  ./RELOAD_HISTORICAL_OHLCV.sh -d 1"
    echo ""
    echo "  # Перезагрузить с принудительным обновлением закрытых свечей (для исправления ошибок)"
    echo "  ./RELOAD_HISTORICAL_OHLCV.sh -p AAVE/USDT -t 1h -d 3 -f"
    echo ""
    echo "  # Полная перезагрузка истории (30 дней) для всех пар и таймфреймов"
    echo "  ./RELOAD_HISTORICAL_OHLCV.sh --full-history"
    echo ""
    echo "⚠️  ВНИМАНИЕ:"
    echo "  - Используйте -f только при обнаружении ошибок в закрытых свечах"
    echo "  - Для больших объемов (все пары, много дней) процесс может занять 5-10 минут"
    echo "  - Убедитесь, что API Gateway запущен: ./START_ALL.sh"
}

# Парсинг аргументов
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--pair)
            PAIR="$2"
            shift 2
            ;;
        -t|--timeframe)
            TIMEFRAME="$2"
            shift 2
            ;;
        -d|--days)
            DAYS="$2"
            shift 2
            ;;
        -f|--force)
            FORCE_UPDATE_CLOSED=true
            shift
            ;;
        --full-history)
            FULL_HISTORY=true
            DAYS=30
            PAIR=""
            TIMEFRAME=""
            FORCE_UPDATE_CLOSED=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Неизвестный параметр: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

# Проверка доступности API
echo -e "${BLUE}🔍 Проверка доступности API Gateway...${NC}"
if ! curl -s --max-time 3 --connect-timeout 3 "$API_URL/health" > /dev/null 2>&1; then
    echo -e "${RED}❌ API Gateway не доступен на $API_URL${NC}"
    echo -e "${YELLOW}💡 Убедитесь, что API Gateway запущен: ./START_ALL.sh${NC}"
    exit 1
fi
echo -e "${GREEN}   ✅ API Gateway доступен${NC}"

# Проверка доступности endpoint
echo -e "${BLUE}🔍 Проверка доступности endpoint...${NC}"
ENDPOINT_CHECK=$(curl -s -X POST "$API_URL/api/reload-historical-ohlcv?days=1" 2>&1)
if echo "$ENDPOINT_CHECK" | grep -q "Not Found\|404"; then
    echo -e "${RED}❌ Endpoint /api/reload-historical-ohlcv не найден${NC}"
    echo -e "${YELLOW}💡 API Gateway нужно перезапустить для применения изменений:${NC}"
    echo -e "${YELLOW}   1. pkill -f 'services/api_gateway/main.py'${NC}"
    echo -e "${YELLOW}   2. ./START_API.sh${NC}"
    echo -e "${YELLOW}   Или перезапустите весь проект: ./START_ALL.sh${NC}"
    exit 1
fi
echo -e "${GREEN}   ✅ Endpoint доступен${NC}"

# Формирование URL запроса
URL="$API_URL/api/reload-historical-ohlcv"
PARAMS="days=$DAYS&force_update_closed=$FORCE_UPDATE_CLOSED"

if [ -n "$PAIR" ]; then
    PARAMS="$PARAMS&pair=$(echo "$PAIR" | sed 's|/|%2F|g')"
fi

if [ -n "$TIMEFRAME" ]; then
    PARAMS="$PARAMS&timeframe=$TIMEFRAME"
fi

FULL_URL="$URL?$PARAMS"

# Показываем параметры запроса
echo ""
echo -e "${BLUE}📋 Параметры перезагрузки:${NC}"
if [ "$FULL_HISTORY" = true ]; then
    echo -e "${YELLOW}   ⚠️  РЕЖИМ ПОЛНОЙ ПЕРЕЗАГРУЗКИ${NC}"
    echo "   Пара: все пары (30)"
    echo "   Таймфрейм: все (15m, 1h, 4h)"
    echo "   Дней: 30 (полная история)"
    echo "   Обновить закрытые: да (принудительно)"
else
    echo "   Пара: ${PAIR:-все пары}"
    echo "   Таймфрейм: ${TIMEFRAME:-все (15m, 1h, 4h)}"
    echo "   Дней: $DAYS"
    echo "   Обновить закрытые: $FORCE_UPDATE_CLOSED"
fi
echo ""

if [ "$FULL_HISTORY" = true ]; then
    echo -e "${RED}⚠️  ВНИМАНИЕ: ПОЛНАЯ ПЕРЕЗАГРУЗКА ИСТОРИИ!${NC}"
    echo -e "${YELLOW}   Это обновит 30 дней истории для ВСЕХ 30 пар и всех таймфреймов.${NC}"
    echo -e "${YELLOW}   Процесс может занять 10-20 минут.${NC}"
    echo -e "${YELLOW}   Будет создана большая нагрузка на API биржи.${NC}"
    echo ""
    read -p "Вы уверены? Введите 'YES' для подтверждения: " -r
    echo
    if [ "$REPLY" != "YES" ]; then
        echo -e "${YELLOW}Отменено пользователем${NC}"
        exit 0
    fi
elif [ "$FORCE_UPDATE_CLOSED" = true ]; then
    echo -e "${YELLOW}⚠️  ВНИМАНИЕ: Будет обновлено даже закрытые свечи!${NC}"
    echo -e "${YELLOW}   Это может занять больше времени.${NC}"
    echo ""
    read -p "Продолжить? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Отменено пользователем${NC}"
        exit 0
    fi
fi

# Отправка запроса
echo -e "${BLUE}🔄 Запуск перезагрузки исторических данных...${NC}"
RESPONSE=$(curl -s -X POST "$FULL_URL" 2>&1)

# Проверка ответа
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ошибка при отправке запроса${NC}"
    echo "$RESPONSE"
    exit 1
fi

# Парсинг ответа
TASK_ID=$(echo "$RESPONSE" | grep -o '"task_id":"[^"]*' | cut -d'"' -f4)
SUCCESS=$(echo "$RESPONSE" | grep -o '"success":[^,}]*' | cut -d':' -f2)
MESSAGE=$(echo "$RESPONSE" | grep -o '"message":"[^"]*' | cut -d'"' -f4)

if [ "$SUCCESS" != "true" ] || [ -z "$TASK_ID" ]; then
    echo -e "${RED}❌ Ошибка запуска задачи${NC}"
    echo "$RESPONSE"
    exit 1
fi

echo -e "${GREEN}   ✅ Задача запущена${NC}"
echo -e "${BLUE}   📝 ID задачи: $TASK_ID${NC}"
if [ -n "$MESSAGE" ]; then
    echo -e "${BLUE}   💬 $MESSAGE${NC}"
fi

echo ""
echo -e "${BLUE}⏳ Ожидание завершения задачи...${NC}"
echo -e "${YELLOW}   (Это может занять несколько минут)${NC}"
echo ""

# Проверка Celery worker
if ! pgrep -f "celery.*worker" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  ВНИМАНИЕ: Celery worker не запущен!${NC}"
    echo -e "${YELLOW}   Задача не сможет выполниться без worker.${NC}"
    echo -e "${YELLOW}   Запустите: ./START_CELERY.sh или ./START_ALL.sh${NC}"
    echo ""
    read -p "Продолжить ожидание? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Отменено пользователем${NC}"
        exit 0
    fi
fi
echo ""

# Мониторинг статуса задачи
# Для полной истории увеличиваем время ожидания
if [ "$FULL_HISTORY" = true ]; then
    MAX_WAIT=1800  # Максимальное время ожидания (30 минут для полной истории)
    echo -e "${YELLOW}   ⏳ Ожидание может занять до 30 минут...${NC}"
else
    MAX_WAIT=600  # Максимальное время ожидания (10 минут)
fi
WAIT_INTERVAL=5  # Интервал проверки (5 секунд)
ELAPSED=0

LAST_PROGRESS=""
while [ $ELAPSED -lt $MAX_WAIT ]; do
    sleep $WAIT_INTERVAL
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
    
    # Получаем статус задачи
    TASK_STATUS=$(curl -s "$API_URL/api/task/$TASK_ID" 2>/dev/null)
    
    if [ $? -eq 0 ] && echo "$TASK_STATUS" | grep -q '"status"'; then
        STATUS=$(echo "$TASK_STATUS" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
        
        # Показываем статус задачи
        if [ "$STATUS" = "PENDING" ]; then
            # Задача еще не началась - показываем каждые 5 секунд
            if [ $((ELAPSED % 5)) -eq 0 ]; then
                echo -ne "\r\033[K"
                echo -ne "${YELLOW}⏳ Ожидание запуска задачи... (${ELAPSED} сек)${NC}"
            fi
        elif [ "$STATUS" = "STARTED" ]; then
            # Задача запущена, но прогресс еще не обновлен
            if [ $((ELAPSED % 5)) -eq 0 ]; then
                echo -ne "\r\033[K"
                echo -ne "${BLUE}🔄 Задача запущена, ожидание прогресса... (${ELAPSED} сек)${NC}"
            fi
        fi
        
        # Показываем прогресс, если доступен
        if [ "$STATUS" = "PROGRESS" ] || echo "$TASK_STATUS" | grep -q '"progress"'; then
            if command -v python3 > /dev/null 2>&1; then
                PROGRESS_INFO=$(echo "$TASK_STATUS" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    progress = data.get('progress', {})
    if isinstance(progress, dict):
        current = progress.get('current', 0)
        total = progress.get('total', 0)
        percent = progress.get('percent', 0)
        status_msg = progress.get('status', '')
        current_pair = progress.get('current_pair', '')
        current_tf = progress.get('current_timeframe', '')
        if current > 0 and total > 0:
            print(f'{current}|{total}|{percent}|{status_msg}|{current_pair}|{current_tf}')
except:
    pass
" 2>/dev/null)
                
                if [ -n "$PROGRESS_INFO" ] && [ "$PROGRESS_INFO" != "$LAST_PROGRESS" ]; then
                    CURRENT=$(echo "$PROGRESS_INFO" | cut -d'|' -f1)
                    TOTAL=$(echo "$PROGRESS_INFO" | cut -d'|' -f2)
                    PERCENT=$(echo "$PROGRESS_INFO" | cut -d'|' -f3)
                    STATUS_MSG=$(echo "$PROGRESS_INFO" | cut -d'|' -f4)
                    CURRENT_PAIR=$(echo "$PROGRESS_INFO" | cut -d'|' -f5)
                    CURRENT_TF=$(echo "$PROGRESS_INFO" | cut -d'|' -f6)
                    
                    # Очищаем предыдущую строку и выводим новую
                    echo -ne "\r\033[K"
                    echo -ne "${BLUE}⏳ Прогресс: ${PERCENT}% (${CURRENT}/${TOTAL}) - ${CURRENT_PAIR} ${CURRENT_TF}${NC}"
                    LAST_PROGRESS="$PROGRESS_INFO"
                fi
            fi
        fi
        
        if [ "$STATUS" = "SUCCESS" ]; then
            # Очищаем строку прогресса
            echo -ne "\r\033[K"
            echo ""
            echo ""
            echo -e "${GREEN}✅ Задача завершена успешно!${NC}"
            
            # Показываем результаты (используем python для парсинга JSON)
            if command -v python3 > /dev/null 2>&1; then
                RESULTS_INFO=$(echo "$TASK_STATUS" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    result = data.get('result', {})
    if isinstance(result, dict):
        results = result.get('results', {})
        if isinstance(results, dict):
            updated = results.get('updated', 0)
            created = results.get('created', 0)
            skipped = results.get('skipped', 0)
            if updated > 0 or created > 0 or skipped > 0:
                print(f'{updated}|{created}|{skipped}')
except:
    pass
" 2>/dev/null)
                
                if [ -n "$RESULTS_INFO" ]; then
                    UPDATED=$(echo "$RESULTS_INFO" | cut -d'|' -f1)
                    CREATED=$(echo "$RESULTS_INFO" | cut -d'|' -f2)
                    SKIPPED=$(echo "$RESULTS_INFO" | cut -d'|' -f3)
                    
                    echo ""
                    echo -e "${BLUE}📊 Результаты:${NC}"
                    echo "   Обновлено свечей: $UPDATED"
                    echo "   Создано свечей: $CREATED"
                    echo "   Пропущено свечей: $SKIPPED"
                fi
            fi
            
            echo ""
            echo -e "${GREEN}✅ Перезагрузка завершена!${NC}"
            exit 0
        elif [ "$STATUS" = "FAILURE" ] || [ "$STATUS" = "REVOKED" ]; then
            # Очищаем строку прогресса
            echo -ne "\r\033[K"
            echo ""
            echo -e "${RED}❌ Задача завершилась с ошибкой${NC}"
            ERROR_INFO=$(echo "$TASK_STATUS" | grep -o '"error":"[^"]*' | cut -d'"' -f4 || echo "")
            if [ -n "$ERROR_INFO" ]; then
                echo -e "${RED}   Ошибка: $ERROR_INFO${NC}"
            fi
            echo "$TASK_STATUS"
            exit 1
        fi
    fi
    
    # Показываем таймер только если нет прогресса и статус не PENDING/STARTED
    if [ -z "$LAST_PROGRESS" ] && [ "$STATUS" != "PENDING" ] && [ "$STATUS" != "STARTED" ] && [ $((ELAPSED % 30)) -eq 0 ]; then
        echo -e "${YELLOW}   ⏳ Ожидание... ($ELAPSED сек)${NC}"
    fi
done

# Очищаем строку прогресса перед финальным сообщением
echo -ne "\r\033[K"

echo ""
echo -e "${YELLOW}⚠️  Превышено время ожидания ($MAX_WAIT сек)${NC}"
echo -e "${BLUE}💡 Задача продолжает выполняться в фоне${NC}"
echo -e "${BLUE}   Проверьте статус: curl $API_URL/api/task/$TASK_ID${NC}"
echo -e "${BLUE}   Или проверьте логи: tail -f /tmp/celery_worker.log${NC}"
exit 0

