import datetime
import logging
import random
import uuid

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ActionType, AssetClass, TradeSide, TradeStatus

from .config import generator_config
from .http import (
    _BOOKS_URL,
    _HTTP_POOL,
    _SNAPSHOT_URL,
    _SYMBOLS_URL,
    fetch_json_once,
    send_to_trade_action_service,
)
from .lifecycle import generator_state

_NO_SHARED_CONTEXT = object()


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

    open_context = _fetch_open_market_context()
    batch_payload = []

    for _ in range(count):
        intention = generate_random_intention(open_context=open_context)
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


def generate_random_intention(open_context=_NO_SHARED_CONTEXT):
    try:
        action = generate_open_or_close_trade()
        logging.info(f"Generated action: {action} == {ActionType.CLOSE_TRADE.value}")

        if action == ActionType.CLOSE_TRADE.value:
            close_payload = _close_intention_from_db()
            if close_payload is not None:
                return close_payload
            logging.info(
                "No active trades found. Defaulting to opening a new trade."
            )
            action = ActionType.OPEN_TRADE.value

        if action == ActionType.OPEN_TRADE.value:
            return _open_intention(open_context)

        return None

    except Exception as e:
        logging.error(f"CRITICAL ERROR in intention generator: {e}", exc_info=True)
        return None


def _open_intention(open_context):
    """OPEN from a shared batch context, or fetch a fresh one for a single call."""
    if open_context is _NO_SHARED_CONTEXT:
        return _open_intention_from_http()
    if open_context is None:
        return None
    return _open_intention_from_context(*open_context)


# jak constrainy moze wplynac na transakcje DB -> duze tabele i sprawdzanie id w trakcie insertow
# profiling

def _close_intention_from_db():
    """Read active trades in a short session, then return. No HTTP in here (TX-2)."""
    with DBSessionManager() as db:
        logging.info("Checking for active trades to close...")
        active_trades = db.trades.get_trades(
            None, status=TradeStatus.ACTIVE.value, symbol=None
        )
        if not active_trades:
            return None

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
            "symbol": trade_to_close.symbol,
            "close_reason": "RANDOM_TRADE_OUT",
        }
        logging.info(f"Success! Generated JSON for closing: {payload}")
        return payload


def _fetch_open_market_context():
    """Fetch books, symbols, and snapshot in parallel. Returns a tuple or None."""
    logging.info(
        "ASKING BOOKS-SERVICE for available books and market-data-service for symbols..."
    )
    books_future = _HTTP_POOL.submit(fetch_json_once, _BOOKS_URL)
    symbols_future = _HTTP_POOL.submit(fetch_json_once, _SYMBOLS_URL)
    snapshot_future = _HTTP_POOL.submit(fetch_json_once, _SNAPSHOT_URL)
    try:
        books_response = books_future.result()
        symbols_response = symbols_future.result()
        snapshot = snapshot_future.result()
    except Exception as e:
        logging.error(f"Failed to fetch OPEN market context: {e}", exc_info=True)
        return None

    books_data = books_response.get("books", []) if books_response else []
    symbols_data = symbols_response if symbols_response else {}
    logging.info(
        f"Fetched {len(books_data)} books and {len(symbols_data)} asset classes."
    )
    return books_data, symbols_data, snapshot


def _open_intention_from_http():
    context = _fetch_open_market_context()
    if context is None:
        return None
    return _open_intention_from_context(*context)


def _open_intention_from_context(books_data, symbols_data, snapshot):
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
