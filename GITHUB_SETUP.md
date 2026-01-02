# Инструкция по настройке GitHub репозитория

## Шаги для выгрузки проекта в GitHub

### 1. Добавление remote (если еще не добавлен)
```bash
git remote add origin https://github.com/Podstavochkin/ownedcore.git
```

Если remote уже существует, обновите его:
```bash
git remote set-url origin https://github.com/Podstavochkin/ownedcore.git
```

### 2. Проверка текущего состояния
```bash
git status
```

### 3. Добавление всех файлов
```bash
# Добавить все новые и измененные файлы
git add .

# Или добавить файлы по категориям:
git add core/ tasks/ services/ web/ scripts/ docs/ migrations/
git add *.md *.sh *.ini *.json
git add config/ docker/
```

### 4. Создание первого коммита
```bash
git commit -m "Initial commit: OwnedCore trading system

- Core trading system with level detection
- Signal generation and filtering
- Chart patterns detection (triangles)
- Demo trading execution
- Web interface with charts
- Celery tasks for background processing
- Documentation and analysis scripts"
```

### 5. Переименование ветки в main (если нужно)
```bash
git branch -M main
```

### 6. Отправка в GitHub
```bash
git push -u origin main
```

Если возникнут проблемы с правами доступа, используйте:
```bash
git push -u origin main --force
```

## Важные файлы, которые НЕ должны попасть в репозиторий

Убедитесь, что в `.gitignore` есть:
- `.env` - файл с секретами
- `celerybeat-schedule*` - файлы расписания Celery
- `logs/` - логи
- `__pycache__/` - кэш Python
- `*.pyc` - скомпилированные файлы Python

## После успешной загрузки

1. Проверьте репозиторий на GitHub: https://github.com/Podstavochkin/ownedcore
2. Убедитесь, что все файлы загружены
3. Проверьте, что `.env` файл НЕ попал в репозиторий (это критично для безопасности!)

## Дальнейшая работа

После настройки репозитория, для каждого изменения:
```bash
git add .
git commit -m "Описание изменений"
git push
```

