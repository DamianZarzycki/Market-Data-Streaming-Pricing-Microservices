import logging
import os
from datetime import datetime, timezone

DEFAULT_POLL_INTERVAL_SECONDS = 30.0
DEFAULT_POLL_SYMBOL = "AAPL"
DEFAULT_CURVE_QUEUE_MAXSIZE = 1000
DEFAULT_CURVE_PERSIST_BATCH_SIZE = 20
DEFAULT_CURVE_PERSIST_FLUSH_SECONDS = 2.0


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_poll_interval_seconds() -> float:
    interval_seconds_raw = os.getenv(
        "MARKET_DATA_POLL_INTERVAL_SECONDS",
        str(DEFAULT_POLL_INTERVAL_SECONDS),
    )
    try:
        return max(float(interval_seconds_raw), 1.0)
    except ValueError:
        logging.warning(
            "Invalid MARKET_DATA_POLL_INTERVAL_SECONDS=%r, falling back to %s",
            interval_seconds_raw,
            DEFAULT_POLL_INTERVAL_SECONDS,
        )
        return DEFAULT_POLL_INTERVAL_SECONDS


def read_positive_int_env(variable_name: str, default_value: int) -> int:
    env_value_raw = os.getenv(variable_name, str(default_value))
    try:
        parsed_value = int(env_value_raw)
        if parsed_value <= 0:
            raise ValueError
        return parsed_value
    except ValueError:
        logging.warning(
            "Invalid %s=%r, falling back to %s",
            variable_name,
            env_value_raw,
            default_value,
        )
        return default_value


def read_positive_float_env(variable_name: str, default_value: float) -> float:
    env_value_raw = os.getenv(variable_name, str(default_value))
    try:
        parsed_value = float(env_value_raw)
        if parsed_value <= 0:
            raise ValueError
        return parsed_value
    except ValueError:
        logging.warning(
            "Invalid %s=%r, falling back to %s",
            variable_name,
            env_value_raw,
            default_value,
        )
        return default_value


def read_poll_symbols() -> list[str]:
    symbols_raw = os.getenv("MARKET_DATA_SYMBOLS", "")
    seen_symbols: set[str] = set()
    poll_symbols: list[str] = []
    for symbol_token in symbols_raw.split(","):
        normalized_symbol = symbol_token.strip().upper()
        if not normalized_symbol or normalized_symbol in seen_symbols:
            continue
        seen_symbols.add(normalized_symbol)
        poll_symbols.append(normalized_symbol)
    return poll_symbols or [DEFAULT_POLL_SYMBOL]


def parse_curve_as_of_time(as_of_time_value: object) -> datetime | None:
    if as_of_time_value is None:
        return None
    if isinstance(as_of_time_value, datetime):
        if as_of_time_value.tzinfo is None:
            return as_of_time_value.replace(tzinfo=timezone.utc)
        return as_of_time_value
    if not isinstance(as_of_time_value, str) or not as_of_time_value.strip():
        return None

    timestamp_text = as_of_time_value.strip()
    try:
        if timestamp_text.endswith("Z"):
            return datetime.strptime(timestamp_text, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        parsed_datetime = datetime.fromisoformat(timestamp_text)
        if parsed_datetime.tzinfo is None:
            return parsed_datetime.replace(tzinfo=timezone.utc)
        return parsed_datetime
    except ValueError:
        logging.warning(
            "Invalid curve as_of_time=%r; using current UTC time",
            as_of_time_value,
        )
        return None
