"""Управление режимом live-торговли."""

from __future__ import annotations

import threading
from typing import Optional

from core.cache import cache

_CACHE_KEY = "trading:live_enabled"
_DEFAULT_STATE = True
_FALLBACK_STATE = {"value": _DEFAULT_STATE}
_LOCK = threading.Lock()
_LONG_TTL = 60 * 60 * 24 * 30  # 30 дней


def _normalize(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    if isinstance(value, dict) and "enabled" in value:
        return _normalize(value.get("enabled"))
    return None


def _read_from_cache() -> Optional[bool]:
    try:
        cached = cache.get(_CACHE_KEY)
    except Exception:
        return None
    return _normalize(cached)


def is_live_trading_enabled() -> bool:
    """Возвращает текущее состояние live-торговли (по умолчанию включено)."""
    cached = _read_from_cache()
    if cached is not None:
        with _LOCK:
            _FALLBACK_STATE["value"] = cached
        return cached

    with _LOCK:
        return _FALLBACK_STATE["value"]


def set_live_trading_enabled(enabled: bool) -> bool:
    """Меняет состояние live-торговли (значение сохраняется в Redis и в памяти)."""
    try:
        cache.set(_CACHE_KEY, "true" if enabled else "false", ttl=_LONG_TTL)
    except Exception:
        pass

    with _LOCK:
        _FALLBACK_STATE["value"] = enabled

    return enabled


