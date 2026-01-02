#!/usr/bin/env python3
"""
Монитор Celery worker'а.

Скрипт выполняет две проверки:
1. Запущен ли вообще celery worker (`pgrep -f "celery -A tasks.celery_app worker"`).
2. Есть ли в последних строках /tmp/celery_worker.log ошибки вида
   "Received unregistered task..." или KeyError по watcher-у.

Если любая проверка не проходит, воркер перезапускается автоматически:
 - Завершается текущий процесс (`pkill -f "celery -A tasks.celery_app worker"`).
 - Запускается новый python3 -m celery ... (как в START_ALL.sh), вывод в /tmp/celery_worker.log.

Скрипт можно добавить в cron/systemd-timer и вызывать, например, каждые 5 минут:
    */5 * * * * /usr/bin/python3 /path/to/project/scripts/celery_watchdog.py >> /tmp/celery_watchdog.log 2>&1
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = Path("/tmp/celery_worker.log")
ERROR_PATTERNS = [
    "Received unregistered task of type 'tasks.demo_trading_tasks.watch_waiting_signals'",
    "KeyError: 'tasks.demo_trading_tasks.watch_waiting_signals'",
]
LOG_LINES_TO_CHECK = 400
ERROR_TIME_WINDOW = timedelta(minutes=5)
CELERY_CMD = [
    "python3",
    "-m",
    "celery",
    "-A",
    "tasks.celery_app",
    "worker",
    "--loglevel=info",
    "--queues=analysis,signals",
]


def run_cmd(cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
    """Выполняет команду и возвращает CompletedProcess."""
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **kwargs,
    )


def tail_log(path: Path, max_lines: int) -> deque[str]:
    if not path.exists():
        return deque(maxlen=max_lines)
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        return deque(fh, maxlen=max_lines)


def parse_timestamp(line: str) -> datetime | None:
    line = line.strip()
    if not line.startswith("["):
        return None
    try:
        chunk = line[1:25]  # "2025-11-27 17:35:11,824"
        dt = datetime.strptime(chunk, "%Y-%m-%d %H:%M:%S,%f")
        return dt
    except Exception:
        return None


def has_recent_errors(lines: Iterable[str]) -> bool:
    now = datetime.now()
    for raw in reversed(list(lines)):
        line = raw.strip()
        if not line:
            continue

        if any(pattern in line for pattern in ERROR_PATTERNS):
            ts = parse_timestamp(line)
            if ts and now - ts > ERROR_TIME_WINDOW:
                continue  # старое сообщение
            return True
    return False


def is_worker_running() -> bool:
    result = subprocess.run(
        ["pgrep", "-f", "celery.+tasks\\.celery_app.+worker"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.returncode == 0


def stop_worker():
    subprocess.run(
        ["pkill", "-f", "celery -A tasks.celery_app worker"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    time.sleep(2)


def start_worker():
    env = os.environ.copy()
    existing_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{PROJECT_ROOT}:{existing_path}" if existing_path else str(PROJECT_ROOT)
    )
    concurrency = env.get("CELERY_CONCURRENCY", "4")

    log_file = LOG_PATH.open("a", encoding="utf-8")
    cmd = CELERY_CMD + [f"--concurrency={concurrency}"]
    subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )
    time.sleep(5)


def ensure_worker():
    needs_restart = False
    reasons: List[str] = []

    if not is_worker_running():
        needs_restart = True
        reasons.append("worker_not_running")

    log_tail = tail_log(LOG_PATH, LOG_LINES_TO_CHECK)
    if has_recent_errors(log_tail):
        needs_restart = True
        reasons.append("error_in_log")

    if not needs_restart:
        print("✅ Celery worker в порядке.")
        return

    print(f"⚠️  Требуется перезапуск Celery worker ({', '.join(reasons)})")
    stop_worker()
    start_worker()

    if is_worker_running():
        print("✅ Celery worker перезапущен успешно.")
    else:
        print("❌ Не удалось перезапустить Celery worker. Проверьте /tmp/celery_worker.log")
        sys.exit(1)


if __name__ == "__main__":
    ensure_worker()

