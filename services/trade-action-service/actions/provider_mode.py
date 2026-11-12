import threading

from shared.trading_shared.env import env_bool

_provider_mode_lock = threading.Lock()
_use_provider_data = env_bool("USE_PROVIDER_DATA", False)


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
