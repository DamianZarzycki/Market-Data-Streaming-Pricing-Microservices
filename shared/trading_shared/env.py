import logging
import os

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


def env(name: str) -> str:
    """Return the stripped value of `name`. Raise if it is missing or blank."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        raise RuntimeError(f"Missing environment variable {name}")
    return raw.strip()


def env_optional(name: str) -> str | None:
    """Stripped value, or None when the variable is missing or blank."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return raw.strip()


def env_str(name: str, default: str) -> str:
    value = env_optional(name)
    return default if value is None else value


def _invalid(name: str, raw: str, default) -> None:
    logging.warning("Invalid %s=%r, falling back to %s", name, raw, default)


def env_int(name: str, default: int, *, minimum: int | None = 1) -> int:
    """Positive integer from the environment. A blank, invalid, or too-small value uses `default`."""
    raw = env_optional(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        _invalid(name, raw, default)
        return default
    if minimum is not None and value < minimum:
        _invalid(name, raw, default)
        return default
    return value


def env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    """Float from the environment. A blank or invalid value uses `default`."""
    raw = env_optional(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        _invalid(name, raw, default)
        return default
    if minimum is not None and value < minimum:
        _invalid(name, raw, default)
        return default
    return value


def env_bool(name: str, default: bool) -> bool:
    """Bool from 1/true/yes/on and 0/false/no/off. Anything else uses `default`."""
    raw = env_optional(name)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    _invalid(name, raw, default)
    return default
