import os


_DEFAULT_CONFIG = {
    "interval_ms": 4000,
    "batch_size": 10,
    "open_weight_pct": 70,
    "close_weight_pct": 30,
    "qty_min": 10,
    "qty_max": 1000,
    "price_min": 50.0,
    "price_max": 150.0,
    "use_provider_data": False,
}

CONFIG_KEYS = frozenset(_DEFAULT_CONFIG.keys())

_ENV_BY_KEY = {
    "interval_ms": "TRADE_GENERATION_INTERVAL_MS",
    "batch_size": "TRADE_GENERATION_BATCH_SIZE",
    "open_weight_pct": "TRADE_GENERATION_OPEN_WEIGHT_PCT",
    "close_weight_pct": "TRADE_GENERATION_CLOSE_WEIGHT_PCT",
    "qty_min": "TRADE_GENERATION_QTY_MIN",
    "qty_max": "TRADE_GENERATION_QTY_MAX",
    "price_min": "TRADE_GENERATION_PRICE_MIN",
    "price_max": "TRADE_GENERATION_PRICE_MAX",
    "use_provider_data": "USE_PROVIDER_DATA",
}


def _env_raw(name):
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _as_int(value, field):
    try:
        if isinstance(value, bool):
            raise ValueError
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be an integer")


def _as_bool(value, field):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on"):
            return True
        if normalized in ("0", "false", "no", "off", ""):
            return False
    raise ValueError(f"{field} must be a boolean")


def _as_float(value, field):
    try:
        if isinstance(value, bool):
            raise ValueError
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be a number")


def _patch_interval_ms(config, payload):
    if "interval_ms" not in payload:
        return
    interval_ms = _as_int(payload["interval_ms"], "interval_ms")
    if interval_ms < 50:
        raise ValueError("interval_ms must be >= 50")
    config["interval_ms"] = interval_ms


def _patch_batch_size(config, payload):
    if "batch_size" not in payload:
        return
    batch_size = _as_int(payload["batch_size"], "batch_size")
    if batch_size < 1 or batch_size > 100:
        raise ValueError("batch_size must be between 1 and 100")
    config["batch_size"] = batch_size


def _parse_weight_pct(payload, field):
    weight = _as_int(payload[field], field)
    if weight < 0 or weight > 100:
        raise ValueError(f"{field} must be between 0 and 100")
    return weight


def _patch_open_close_weights(config, payload):
    has_open = "open_weight_pct" in payload
    has_close = "close_weight_pct" in payload
    if not has_open and not has_close:
        return

    open_weight = (
        _parse_weight_pct(payload, "open_weight_pct")
        if has_open
        else config["open_weight_pct"]
    )
    close_weight = (
        _parse_weight_pct(payload, "close_weight_pct")
        if has_close
        else config["close_weight_pct"]
    )

    if has_open and not has_close:
        close_weight = 100 - open_weight
    elif has_close and not has_open:
        open_weight = 100 - close_weight
    elif open_weight + close_weight != 100:
        raise ValueError("open_weight_pct and close_weight_pct must sum to 100")

    config["open_weight_pct"] = open_weight
    config["close_weight_pct"] = close_weight


def _patch_qty_range(config, payload):
    if "qty_min" in payload:
        config["qty_min"] = _as_int(payload["qty_min"], "qty_min")
    if "qty_max" in payload:
        config["qty_max"] = _as_int(payload["qty_max"], "qty_max")

    qty_min = config["qty_min"]
    qty_max = config["qty_max"]
    if qty_min < 1 or qty_max > 1_000_000:
        raise ValueError("qty_min/qty_max must be between 1 and 1000000")
    if qty_min > qty_max:
        raise ValueError("qty_min must be <= qty_max")


def _patch_use_provider_data(config, payload):
    if "use_provider_data" not in payload:
        return
    config["use_provider_data"] = _as_bool(
        payload["use_provider_data"], "use_provider_data"
    )


def _patch_price_range(config, payload):
    if "price_min" in payload:
        config["price_min"] = _as_float(payload["price_min"], "price_min")
    if "price_max" in payload:
        config["price_max"] = _as_float(payload["price_max"], "price_max")

    price_min = config["price_min"]
    price_max = config["price_max"]
    if price_min <= 0 or price_max <= 0:
        raise ValueError("price_min and price_max must be > 0")
    if price_min >= price_max:
        raise ValueError("price_min must be < price_max")


def _apply_config_patch(base, payload):
    """Return a validated config dict. `payload` is a partial mapping of raw values."""
    next_config = dict(base)
    _patch_interval_ms(next_config, payload)
    _patch_batch_size(next_config, payload)
    _patch_open_close_weights(next_config, payload)
    _patch_qty_range(next_config, payload)
    _patch_price_range(next_config, payload)
    _patch_use_provider_data(next_config, payload)
    return next_config


def _config_from_env():
    payload = {}
    for key, env_name in _ENV_BY_KEY.items():
        raw = _env_raw(env_name)
        if raw is not None:
            payload[key] = raw
    if "interval_ms" not in payload:
        tick = _env_raw("TICK_INTERVAL_MS")
        if tick is not None:
            payload["interval_ms"] = tick
    try:
        return _apply_config_patch(_DEFAULT_CONFIG, payload)
    except ValueError as exc:
        raise ValueError(f"Invalid TRADE_GENERATION_* env: {exc}") from exc


generator_config = _config_from_env()


def get_config():
    return dict(generator_config)


def update_config(payload):
    """Apply a partial config update. Returns the new config or raises ValueError."""
    if not isinstance(payload, dict) or not payload:
        raise ValueError("Request body must be a non-empty JSON object")

    unknown = set(payload.keys()) - CONFIG_KEYS
    if unknown:
        raise ValueError(f"Unknown config keys: {', '.join(sorted(unknown))}")

    next_config = _apply_config_patch(generator_config, payload)
    generator_config.update(next_config)
    return get_config()
