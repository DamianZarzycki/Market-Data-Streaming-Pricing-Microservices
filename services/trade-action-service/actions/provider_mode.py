import logging
import os
import threading

_provider_mode_lock = threading.Lock()


def _env_use_provider_data() -> bool:
    raw = os.getenv("USE_PROVIDER_DATA")
    if raw is None or not raw.strip():
        return False
    normalized = raw.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    logging.warning("Invalid USE_PROVIDER_DATA=%r, falling back to false", raw)
    return False


_use_provider_data = _env_use_provider_data()


def use_provider_data() -> bool:
    with _provider_mode_lock:
        return _use_provider_data


def set_use_provider_data(enabled: bool) -> bool:
    global _use_provider_data
    if not isinstance(enabled, bool):
        raise ValueError("use_provider_data must be a boolean")
    with _provider_mode_lock:
        _use_provider_data = enabled
        return _use_provider_data
