import datetime
import json
import logging
import os
import random
import threading
import urllib.request
import uuid
from typing import TypedDict

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.decorators import retry
from shared.trading_shared.enums import ActionType, AssetClass, TradeSide, TradeStatus


class GeneratorState(TypedDict):
    is_running: bool
    total_generated: int
    thread: threading.Thread | None


# TODO threading event moze? (wait)
generator_state: GeneratorState = {
    "is_running": False,
    "total_generated": 0,
    "thread": None,
}

_DEFAULT_CONFIG = {
    "interval_ms": 4000,
    "batch_size": 10,
    "open_weight_pct": 70,
    "close_weight_pct": 30,
    "qty_min": 10,
    "qty_max": 1000,
    "price_min": 50.0,
    "price_max": 150.0,
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


def expected_rate_per_sec():
    if not generator_state["is_running"]:
        return 0.0
    interval_ms = generator_config["interval_ms"]
    if interval_ms <= 0:
        return 0.0
    return round(1000.0 / interval_ms, 4)


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


# jak constrainy moze wplynac na transakcje DB -> duze tabele i sprawdzanie id w trakcie insertow
# profiling

@retry(max_attempts=3)
def fetch_json(url, method="GET", headers=None, data=None):
    if headers is None:
        headers = {}

    req = urllib.request.Request(url, method=method, headers=headers, data=data)
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))

def send_to_trade_action_service(payload):
    if isinstance(payload, list):
        url = "http://trade-action-service:8080/trade-actions/batch"
        message = f"batch of {len(payload)} orders"
    else:
        url = "http://trade-action-service:8080/trade-actions"
        message = "single order"

    try:
        fetch_json(
            url,
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"),
        )
        logging.info(f"Successfully sent {message} to trade-action-service.")
    except Exception:
        logging.error(f"Failed to send {message} to trade-action-service.")


def generate_open_or_close_trade():
    return random.choices(
        [ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value],
        weights=[
            generator_config["open_weight_pct"],
            generator_config["close_weight_pct"],
        ],
    )[0]

def generate_and_send_to_trade_action_service_batch(count=None):
    if count is None:
        count = generator_config["batch_size"]

    batch_payload = []

    for _ in range(count):
        intention = generate_random_intention()
        if intention:
            batch_payload.append(intention)
            generator_state["total_generated"] += 1

    if batch_payload:
        send_to_trade_action_service(batch_payload)
        return batch_payload
        
    logging.info("Couldn't generate any intentions for batch.")
    return None


def generate_and_send_to_trade_action_service():
    intention = generate_random_intention()

    if intention:
        generator_state["total_generated"] += 1
        send_to_trade_action_service(intention)
        logging.info(
            f"[Trade Generation] Generated intention {generator_state['total_generated']}."
        )

        return intention
    else:
        logging.info("No intention generated.")
        return None


def generate_request_id():
    return f"req-{datetime.datetime.now().strftime('%Y%m%d')}-{uuid.uuid4()}"


IRS_OPEN_FIELDS = (
    "notional",
    "fixed_rate",
    "maturity_years",
    "payments_per_year",
    "direction",
)
IRS_REQUIRED_FIELDS = ("notional", "fixed_rate", "maturity_years", "direction")
OPTION_OPEN_FIELDS = (
    "spot",
    "strike",
    "volatility",
    "maturity_years",
    "option_right_type",
    "option_type",
    "option_price",
)


def _tick_for_symbol(snapshot, symbol, asset_class):
    if not isinstance(snapshot, dict):
        return None
    for tick in snapshot.values():
        if not isinstance(tick, dict):
            continue
        if tick.get("symbol") == symbol and tick.get("asset_type") == asset_class:
            return tick
    return None


def _copy_tick_fields(tick, fields):
    return {key: tick[key] for key in fields if tick.get(key) is not None}


def _enrich_open_payload(payload, asset_class, tick):
    """Copy contract params from the live market-data tick onto an OPEN intent.

    IRS/OPTION pricing reads these from trade.metadata_payload — a generic
    equity-style OPEN (qty + random price) leaves them empty and gets skipped.
    """
    if not tick:
        return payload
    if asset_class == AssetClass.IRS.value:
        payload.update(_copy_tick_fields(tick, IRS_OPEN_FIELDS))
        if tick.get("currency"):
            payload["currency"] = tick["currency"]
        payload["trade_price"] = 0
    elif asset_class == AssetClass.OPTION.value:
        payload.update(_copy_tick_fields(tick, OPTION_OPEN_FIELDS))
        if tick.get("currency"):
            payload["currency"] = tick["currency"]
    return payload


def generate_random_intention():
    with DBSessionManager() as db:
        try:
            action = generate_open_or_close_trade()
            logging.info(f"Generated action: {action} == {ActionType.CLOSE_TRADE.value}")

            if action == ActionType.CLOSE_TRADE.value:
                logging.info("Checking for active trades to close...")
                active_trades = db.trades.get_trades(None, status=TradeStatus.ACTIVE.value, symbol=None)

                if not active_trades:
                    logging.info(
                        "No active trades found. Defaulting to opening a new trade."
                    )
                    action = ActionType.OPEN_TRADE.value
                else:
                    trade_to_close = random.choice(active_trades)
                    logging.info(f"Found trade. Selected: {trade_to_close}")
                    logging.info(f"Found trade. Selected ID: {trade_to_close.trade_id}")

                    payload = {
                        "action_type": ActionType.CLOSE_TRADE.value,
                        "client_request_id": generate_request_id(),
                        "trade_id": str(trade_to_close.trade_id),
                        "close_price": round(
                            float(trade_to_close.trade_price) * random.uniform(0.98, 1.02),
                            4,
                        ),
                        # TODO what other reasons could it be?
                        "symbol": trade_to_close.symbol,
                        "close_reason": "RANDOM_TRADE_OUT",
                    }
                    logging.info(f"Success! Generated JSON for closing: {payload}")
                    return payload

            if action == ActionType.OPEN_TRADE.value:
                logging.info(
                    "ASKING BOOKS-SERVICE for available books and market-data-service for symbols..."
                )

                response = fetch_json("http://books-service:8004/books")
                symbols_response = fetch_json("http://market-data-service:8001/symbols")
                snapshot = fetch_json("http://market-data-service:8001/snapshot")

                books_data = response.get("books", []) if response else []
                symbols_data = symbols_response if symbols_response else {}

                logging.info(
                    f"Fetched {len(books_data)} books and {len(symbols_data)} asset classes."
                )

                if not books_data:
                    logging.error(
                        "Operation aborted: API returned 0 books. Ensure books are created!"
                    )
                    return None

                book = random.choice(books_data)
                book_asset_class = book.get("expected_asset_class")
                logging.info(
                    f"Fetched book {book.get('book_id')} with asset class {book_asset_class}."
                )

                # Books may use legacy/alias names (e.g. EUROPEAN_OPTION) while
                # market-data catalogs under AssetClass enum values (OPTION).
                _ASSET_CLASS_ALIASES = {
                    "EUROPEAN_OPTION": AssetClass.OPTION.value,
                    "EUROPEAN_OPTIONS": AssetClass.OPTION.value,
                }
                lookup_class = _ASSET_CLASS_ALIASES.get(
                    book_asset_class, book_asset_class
                )
                available_symbols = symbols_data.get(lookup_class, [])
                if not available_symbols and lookup_class != book_asset_class:
                    available_symbols = symbols_data.get(book_asset_class, [])

                if not available_symbols:
                    logging.error(
                        f"Operation aborted: Market Data has no symbols for asset class {book_asset_class}."
                    )
                    return None

                symbol = random.choice(available_symbols)
                logging.info(f"Generated symbol: {symbol}")

                payload = {
                    "action_type": ActionType.OPEN_TRADE.value,
                    "client_request_id": generate_request_id(),
                    "book_id": book.get("book_id"),
                    "asset_class": book_asset_class,
                    "symbol": symbol,
                    "side": random.choice([TradeSide.BUY.value, TradeSide.SELL.value]),
                    "quantity": random.randint(
                        generator_config["qty_min"],
                        generator_config["qty_max"],
                    ),
                    "trade_price": round(
                        random.uniform(
                            generator_config["price_min"],
                            generator_config["price_max"],
                        ),
                        4,
                    ),
                    "currency": "USD",
                }
                tick = _tick_for_symbol(snapshot, symbol, lookup_class)
                payload = _enrich_open_payload(payload, lookup_class, tick)
                if lookup_class == AssetClass.IRS.value:
                    missing = [
                        key
                        for key in IRS_REQUIRED_FIELDS
                        if payload.get(key) is None
                    ]
                    if missing:
                        logging.error(
                            f"Operation aborted: IRS tick missing {missing} for {symbol}."
                        )
                        return None
                logging.info(f"Success! Generated JSON for opening: {payload}")
                return payload

        except Exception as e:
            logging.error(f"CRITICAL ERROR in intention generator: {e}", exc_info=True)
            return None
