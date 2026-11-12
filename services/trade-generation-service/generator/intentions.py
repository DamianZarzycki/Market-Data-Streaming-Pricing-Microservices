import asyncio
import datetime
import logging
import random
import uuid

from sqlalchemy import func, select

from shared.trading_shared.http_errors import ApiError
from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.enums import ActionType, AssetClass, TradeSide, TradeSource, TradeStatus
from shared.trading_shared.models import Trade

from .config import generator_config
from .http import (
    _BOOKS_URL,
    _INTEGRATION_SNAPSHOT_URL,
    _INTEGRATION_SYMBOLS_URL,
    _SNAPSHOT_URL,
    _SYMBOLS_URL,
    fetch_json_once,
    forward_supplied_trade_action,
    send_to_trade_action_service,
)
from .lifecycle import record_generated

_SEND_FAILED = "Failed to send trade intention to trade-action-service"
_BATCH_SEND_FAILED = "Failed to send trade intentions to trade-action-service"


def generate_open_or_close_trade():
    return random.choices(
        [ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value],
        weights=[
            generator_config["open_weight_pct"],
            generator_config["close_weight_pct"],
        ],
    )[0]


def _close_source():
    if generator_config.get("use_provider_data"):
        return TradeSource.TRADING_TICKET.value
    return None


async def generate_and_send_to_trade_action_service_batch(count=None, *, wake_on_stop: bool = False):
    """Build up to `count` intentions, then one batch POST after the DB session is closed.

    Callers hold the generation lock. Close candidates come from one SQL read,
    so the same trade is not chosen twice. Rolls that cannot be closed become
    opens, and the books/symbols/snapshot fetch runs once for those opens.
    """
    if count is None:
        count = generator_config["batch_size"]

    actions = [generate_open_or_close_trade() for _ in range(count)]
    close_slots = sum(action == ActionType.CLOSE_TRADE.value for action in actions)
    closes = await _select_closes(close_slots, _close_source()) if close_slots else []
    batch_payload = [_close_payload(row) for row in closes]

    open_slots = count - len(closes)
    if open_slots:
        open_context = await _fetch_open_market_context()
        if open_context is not None:
            for _ in range(open_slots):
                intention = _open_intention_from_context(*open_context)
                if intention:
                    batch_payload.append(intention)

    if not batch_payload:
        logging.info("Couldn't generate any intentions for batch.")
        return None

    if not await send_to_trade_action_service(batch_payload, wake_on_stop=wake_on_stop):
        raise ApiError(502, _BATCH_SEND_FAILED, intentions=batch_payload)

    record_generated(len(batch_payload))
    return batch_payload


async def generate_and_send_to_trade_action_service(payload=None, *, wake_on_stop: bool = False):
    """Send one intention. Callers hold the generation lock.

    The counter moves only after trade-action accepts the POST. A failed send
    raises 502 and keeps the original `client_request_id` on the error.
    """
    if payload is not None:
        if not isinstance(payload, dict) or not payload:
            raise ValueError("Payload must be a JSON object")
        result = await forward_supplied_trade_action(payload)
        record_generated(1)
        logging.info(
            "[Trade Generation] Forwarded supplied intention %s.",
            payload.get("client_request_id"),
        )
        return result if result is not None else payload

    intention = await generate_random_intention()
    if not intention:
        logging.info("No intention generated.")
        return None

    if not await send_to_trade_action_service(intention, wake_on_stop=wake_on_stop):
        raise ApiError(502, _SEND_FAILED, intention=intention)

    record_generated(1)
    logging.info(
        "[Trade Generation] Generated intention %s.",
        intention.get("client_request_id"),
    )
    return intention


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


async def generate_random_intention():
    action = generate_open_or_close_trade()
    logging.info("Generated action: %s == %s", action, ActionType.CLOSE_TRADE.value)

    if action == ActionType.CLOSE_TRADE.value:
        # Provider mode only unwinds trades the ticket opened. Simulator
        # mode still picks any active trade.
        closes = await _select_closes(1, _close_source())
        if closes:
            return _close_payload(closes[0])
        logging.info("No active trades found. Defaulting to opening a new trade.")
        action = ActionType.OPEN_TRADE.value

    if action == ActionType.OPEN_TRADE.value:
        return await _open_intention_from_http()

    return None


async def _select_closes(limit, source):
    """One short read of `limit` distinct active trades. The session is closed
    before the caller builds payloads or calls trade-action.

    `ORDER BY random()` scans the active set in Postgres and returns only
    `limit` rows, so older trades stay eligible and Python never holds the book.
    """
    if limit < 1:
        return []
    logging.info("Checking for active trades to close (source=%s)...", source or "any")
    statement = select(Trade.trade_id, Trade.trade_price, Trade.symbol).where(
        Trade.status == TradeStatus.ACTIVE.value
    )
    if source:
        statement = statement.where(Trade.source == source)
    statement = statement.order_by(func.random()).limit(limit)
    async with AsyncDBSessionManager() as db:
        result = await db.session.execute(statement)
        selected = [
            (row.trade_id, row.trade_price, row.symbol) for row in result.all()
        ]
    if selected:
        logging.info("Selected %s active trade(s) to close.", len(selected))
    return selected


def _close_payload(row):
    trade_id, trade_price, symbol = row
    payload = {
        "action_type": ActionType.CLOSE_TRADE.value,
        "client_request_id": generate_request_id(),
        "trade_id": str(trade_id),
        "close_price": round(float(trade_price) * random.uniform(0.98, 1.02), 4),
        "symbol": symbol,
        "close_reason": "RANDOM_TRADE_OUT",
    }
    logging.info("Success! Generated JSON for closing: %s", payload)
    return payload


async def _fetch_open_market_context():
    """Fetch books, symbols, and snapshot together. Returns a tuple or None."""
    use_provider = bool(generator_config.get("use_provider_data"))
    symbols_url = _INTEGRATION_SYMBOLS_URL if use_provider else _SYMBOLS_URL
    snapshot_url = _INTEGRATION_SNAPSHOT_URL if use_provider else _SNAPSHOT_URL
    source_name = (
        "market-data-service-integration" if use_provider else "market-data-service"
    )
    logging.info(
        "ASKING BOOKS-SERVICE for available books and %s for symbols...",
        source_name,
    )
    try:
        books_response, symbols_response, snapshot = await asyncio.gather(
            fetch_json_once(_BOOKS_URL),
            fetch_json_once(symbols_url),
            fetch_json_once(snapshot_url),
        )
    except Exception as e:
        logging.error("Failed to fetch OPEN market context: %s", e, exc_info=True)
        return None

    books_data = books_response.get("books", []) if books_response else []
    symbols_data = symbols_response if symbols_response else {}
    if use_provider:
        snapshot = snapshot.get("snapshot") if isinstance(snapshot, dict) else []
        if not isinstance(snapshot, list):
            snapshot = []
    logging.info(
        f"Fetched {len(books_data)} books and {len(symbols_data)} asset classes."
    )
    return books_data, symbols_data, snapshot, use_provider


async def _open_intention_from_http():
    context = await _fetch_open_market_context()
    if context is None:
        return None
    return _open_intention_from_context(*context)


def _quote_price(quote):
    """Last traded price, or bid/ask when the provider did not send last."""
    last = quote.get("last")
    if last is not None:
        return float(last)
    bid = quote.get("bid")
    ask = quote.get("ask")
    if bid is not None and ask is not None:
        return (float(bid) + float(ask)) / 2.0
    if bid is not None:
        return float(bid)
    if ask is not None:
        return float(ask)
    return None


def _reference_timestamp(quote):
    raw = quote.get("provider_timestamp") or quote.get("received_at")
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    else:
        parsed = parsed.astimezone(datetime.timezone.utc)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def _open_from_provider_quote(book, book_asset_class, lookup_class, symbols_data, quotes):
    """OPEN priced off a persisted integration quote.

    OPTION and IRS quotes do not carry strike, volatility, or swap terms, so
    those books are skipped the same way a missing simulator tick is.
    """
    if lookup_class in (AssetClass.OPTION.value, AssetClass.IRS.value):
        logging.error(
            "Operation aborted: provider mode has no contract params for %s.",
            lookup_class,
        )
        return None

    available_symbols = symbols_data.get(lookup_class, [])
    if not available_symbols and lookup_class != book_asset_class:
        available_symbols = symbols_data.get(book_asset_class, [])
    if not available_symbols:
        logging.error(
            f"Operation aborted: Market Data has no symbols for asset class {book_asset_class}."
        )
        return None

    symbol_set = {str(symbol).upper() for symbol in available_symbols}
    matching = [
        quote
        for quote in quotes
        if isinstance(quote, dict)
        and str(quote.get("asset_class") or "").upper() == lookup_class
        and str(quote.get("symbol") or "").upper() in symbol_set
    ]
    if not matching:
        logging.error(
            "Operation aborted: no provider quote for asset class %s.",
            lookup_class,
        )
        return None

    quote = random.choice(matching)
    price = _quote_price(quote)
    timestamp = _reference_timestamp(quote)
    provider = quote.get("provider")
    if price is None or price <= 0 or not timestamp or not provider:
        logging.error(
            "Operation aborted: provider quote for %s is missing price, provider, or timestamp.",
            quote.get("symbol"),
        )
        return None

    side = random.choice([TradeSide.BUY.value, TradeSide.SELL.value])
    payload = {
        "action_type": ActionType.OPEN_TRADE.value,
        "client_request_id": generate_request_id(),
        "book_id": book.get("book_id"),
        "asset_class": book_asset_class,
        "symbol": quote.get("symbol"),
        "side": side,
        "quantity": random.randint(
            generator_config["qty_min"],
            generator_config["qty_max"],
        ),
        "trade_price": round(price, 4),
        "currency": quote.get("currency") or "USD",
        "market_data_provider": provider,
        "reference_price": round(price, 6),
        "reference_price_timestamp": timestamp,
        "source": "GENERATED",
    }
    if lookup_class == AssetClass.FUTURES.value:
        raw = quote.get("raw_payload") if isinstance(quote.get("raw_payload"), dict) else {}
        multiplier = raw.get("contract_multiplier")
        try:
            multiplier = float(multiplier)
        except (TypeError, ValueError):
            multiplier = None
        if multiplier is None or multiplier <= 0:
            logging.error(
                "Operation aborted: futures quote %s has no contract_multiplier.",
                quote.get("symbol"),
            )
            return None
        payload["contract_multiplier"] = multiplier
        if raw.get("underlying_symbol"):
            payload["underlying_symbol"] = raw.get("underlying_symbol")

    logging.info(f"Success! Generated JSON for opening: {payload}")
    return payload


def _open_intention_from_context(books_data, symbols_data, snapshot, use_provider=False):
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
    if use_provider:
        return _open_from_provider_quote(
            book,
            book_asset_class,
            lookup_class,
            symbols_data,
            snapshot if isinstance(snapshot, list) else [],
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
