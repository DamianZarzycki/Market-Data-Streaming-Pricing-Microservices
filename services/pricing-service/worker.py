import asyncio
import json
import logging
import threading
from urllib.parse import quote

import httpx2

import cache_service
import calculation_service
import valuation_service
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.enums import AssetClass, ConnectionStatus, EventType
from shared.trading_shared.env import env, env_float, env_int

stats_lock = threading.Lock()
audit_logger = AuditLogger("pricing-service")

connection_status = ConnectionStatus.DISCONNECTED.value
events_counter = 0
last_market_event_time = None
last_pricing_time = None
last_valuation_symbol = None
last_valuation_asset_class = None

BASE_RECONNECT_DELAY = env_int("PRICING_BASE_RECONNECT_DELAY_SECONDS", 1, minimum=0)
MAX_RECONNECT_DELAY = env_int("PRICING_MAX_RECONNECT_DELAY_SECONDS", 30)
_MARKET_DATA_SERVICE_URL = env("MARKET_DATA_SERVICE_URL").rstrip("/")
MARKET_DATA_STREAM_URL = f"{_MARKET_DATA_SERVICE_URL}/stream"
MARKET_DATA_INTEGRATION_URL = env("MARKET_DATA_INTEGRATION_URL").rstrip("/")
QUOTE_CONCURRENCY = env_int("PRICING_QUOTE_CONCURRENCY", 8)
QUOTE_TIMEOUT_SECONDS = env_float("PRICING_QUOTE_TIMEOUT_SECONDS", 5.0, minimum=0.1)
STREAM_CONNECT_TIMEOUT_SECONDS = env_float("PRICING_STREAM_CONNECT_TIMEOUT_SECONDS", 5.0, minimum=0.1)
STREAM_READ_TIMEOUT_SECONDS = env_float("PRICING_STREAM_READ_TIMEOUT_SECONDS", 30.0, minimum=0.1)
VALUATION_BATCH_CAP = env_int("PRICING_VALUATION_BATCH_CAP", 200)
VALUATION_FLUSH_ATTEMPTS = env_int("PRICING_VALUATION_FLUSH_ATTEMPTS", 3)
FLUSH_RETRY_SECONDS = env_float("PRICING_FLUSH_RETRY_SECONDS", 0.5, minimum=0.0)
POLL_INTERVAL_SECONDS = env_float("PRICING_POLL_INTERVAL_SECONDS", 5.0, minimum=0.1)

http_client: httpx2.AsyncClient | None = None


def open_http() -> None:
    """Build the shared client. The first request is the await, not this call."""
    global http_client
    http_client = httpx2.AsyncClient(
        timeout=httpx2.Timeout(
            STREAM_CONNECT_TIMEOUT_SECONDS,
            read=STREAM_READ_TIMEOUT_SECONDS,
        )
    )


# Async: awaits AsyncClient.aclose.
async def close_http() -> None:
    global http_client
    client = http_client
    http_client = None
    if client is not None:
        await client.aclose()


def health_view():
    with stats_lock:
        return {
            "service": "pricing-service",
            "status": "UP",
            "market_data_connection": connection_status,
            "received_events": events_counter,
            "last_market_event_time": last_market_event_time,
            "last_pricing_time": last_pricing_time,
            "last_valuation_symbol": last_valuation_symbol,
            "last_valuation_asset_class": last_valuation_asset_class,
        }


def parse_market_data_line(line: str):
    decoded = line.strip()
    if not decoded.startswith("data:"):
        return None
    json_string = decoded[5:].strip()
    if not json_string:
        return None
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        logging.warning("Received invalid data format.")
        return None


def update_market_state(tick):
    logging.debug(f"Received tick for: {tick.get('symbol')}")
    calculation_service.valuation_state.metrics_queue.put_nowait(
        {"type": "TICK_RECEIVED", "timestamp": tick.get("timestamp")}
    )


# Async generator: awaits AsyncClient.stream and aiter_lines on the market-data SSE body.
async def iter_market_ticks():
    global connection_status
    if http_client is None:
        with stats_lock:
            connection_status = ConnectionStatus.DISCONNECTED.value
        raise RuntimeError("HTTP client is not open")

    with stats_lock:
        connection_status = ConnectionStatus.RECONNECTING.value
    logging.info("Attempting to connect to the stream...")
    try:
        async with http_client.stream("GET", MARKET_DATA_STREAM_URL) as response:
            response.raise_for_status()
            with stats_lock:
                connection_status = ConnectionStatus.CONNECTED.value
            logging.info("Connected to the stream successfully.")
            await audit_logger.ainfo(
                EventType.STREAM_CONNECTED,
                "Connected to market-data-service stream",
            )
            async for line in response.aiter_lines():
                tick = parse_market_data_line(line)
                if tick is None:
                    continue
                logging.debug(f"Read tick: {tick}")
                yield tick
    except asyncio.CancelledError:
        with stats_lock:
            connection_status = ConnectionStatus.DISCONNECTED.value
        raise
    except Exception:
        with stats_lock:
            connection_status = ConnectionStatus.DISCONNECTED.value
        raise


# Async task: awaits the market-data SSE body and asyncio.sleep between reconnects.
async def pricing_worker():
    global connection_status
    delay = BASE_RECONNECT_DELAY
    logging.info("Pricing worker started")
    while True:
        try:
            async for tick in iter_market_ticks():
                delay = BASE_RECONNECT_DELAY
                update_market_state(tick)
                try:
                    if tick.get("curve_id"):
                        await calculation_service.irs_curve_pricing.update_from_tick(tick)
                    else:
                        calculation_service.simulator_pricing.recalculate(tick)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logging.error(f"Error recalculating valuations: {exc}")
            delay = BASE_RECONNECT_DELAY
        except asyncio.CancelledError:
            with stats_lock:
                connection_status = ConnectionStatus.DISCONNECTED.value
            raise
        except Exception as exc:
            with stats_lock:
                connection_status = ConnectionStatus.DISCONNECTED.value
            logging.error(f"Connection lost: {exc}")
            await audit_logger.aerror(
                EventType.STREAM_DISCONNECTED,
                f"Lost connection to market-data-service: {exc}",
            )
            logging.info("Attempting to reconnect...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)


# Async task: awaits metrics_queue.get, which suspends this task and holds no thread.
async def metrics_worker():
    global last_pricing_time, events_counter, last_market_event_time
    global last_valuation_symbol, last_valuation_asset_class

    while True:
        message = await calculation_service.valuation_state.metrics_queue.get()
        try:
            if message["type"] == "TICK_RECEIVED":
                with stats_lock:
                    events_counter += 1
                    last_market_event_time = message["timestamp"]
                    logged_time = last_market_event_time
                    logged_count = events_counter
                logging.debug(
                    f"Tick received at {logged_time}. Total events: {logged_count}"
                )
            elif message["type"] == "PRICING_DONE":
                logging.debug(
                    f"Pricing done for {message['instrument']} at {message['timestamp']}. "
                    f"Value: {message['value']}"
                )
                with stats_lock:
                    last_pricing_time = message["timestamp"]
                    last_valuation_symbol = message.get("symbol") or last_valuation_symbol
                    last_valuation_asset_class = (
                        message.get("asset_class") or last_valuation_asset_class
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error(f"Error in metrics worker: {exc}")
        finally:
            calculation_service.valuation_state.metrics_queue.task_done()


# Async task: awaits the ACTIVE-trade read and asyncio.sleep between reloads.
async def cache_refresh_worker():
    while True:
        active_ids = await cache_service.reload_active_trades_cache()
        if active_ids is not None:
            calculation_service.valuation_feed.evict_inactive(active_ids)
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# Async: awaits AsyncClient.get for one provider quote.
async def _fetch_provider_quote(provider, symbol):
    # /market-data/quotes/<provider>/<symbol> validates <provider> against
    # lowercase provider keys ("finnhub"), while trade.market_data_provider
    # is stored uppercase ("FINNHUB") — lowercase it for the URL only; the
    # route's own DB lookup uppercases again internally.
    url = (
        f"{MARKET_DATA_INTEGRATION_URL}/market-data/quotes/"
        f"{provider.lower()}/{quote(symbol, safe='')}"
    )
    if http_client is None:
        logging.warning(f"Failed to fetch quote for {provider}/{symbol}: HTTP client is not open")
        return None
    try:
        response = await http_client.get(url, timeout=QUOTE_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logging.warning(f"Failed to fetch quote for {provider}/{symbol}: {exc}")
        return None


# Async task: awaits provider quote GETs, then asyncio.sleep until the next pass.
async def provider_pricing_worker():
    """Reprice active trades opened against a real provider.

    IRS is left to curve_pricing_worker. OPTION is marked with Black-Scholes
    using this quote as spot. FUTURES uses the quote times contract_multiplier.
    Everything else is side-aware bid/ask/last times quantity.
    """
    while True:
        try:
            provider_trades = [
                trade
                for trade in cache_service.all_trades()
                if trade.market_data_provider and trade.asset_class != AssetClass.IRS.value
            ]
            pairs = {(trade.market_data_provider, trade.symbol) for trade in provider_trades}
            semaphore = asyncio.Semaphore(QUOTE_CONCURRENCY)

            async def fetch(pair):
                async with semaphore:
                    provider, symbol = pair
                    return pair, await _fetch_provider_quote(provider, symbol)

            fetched = await asyncio.gather(*(fetch(pair) for pair in pairs))
            quotes_by_pair = {}
            for pair, quote_body in fetched:
                if quote_body is not None and not quote_body.get("error"):
                    quotes_by_pair[pair] = quote_body

            for trade in provider_trades:
                quote_body = quotes_by_pair.get((trade.market_data_provider, trade.symbol))
                if quote_body is None:
                    continue
                try:
                    calculation_service.quote_pricing.price(trade, quote_body)
                except Exception as exc:
                    logging.error(f"Error pricing provider trade {trade.trade_id}: {exc}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error(f"Error in provider_pricing_worker: {exc}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# Async task: awaits the curve read and asyncio.sleep between passes.
async def curve_pricing_worker():
    """Reprice IRS from persisted FRED/ECB yield curves.

    Reads MarketDataCurve (latest YIELD_CURVE per currency, excluding the
    simulator). Does not call /curves on the integration service — that GET
    is a DB snapshot, and live fetches stay on POST /curves/refresh and the
    curve poll worker.
    """
    while True:
        try:
            await calculation_service.irs_curve_pricing.reprice_persisted()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error(f"Error in curve_pricing_worker: {exc}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# Async: awaits AsyncSession.commit. Builds a fresh row on every attempt.
async def _flush_valuations(batch) -> int | None:
    latest = {}
    for item in batch:
        trade_id = item.get("trade_id")
        if trade_id is None:
            continue
        latest[trade_id] = item
    if not latest:
        return 0

    delay = FLUSH_RETRY_SECONDS
    for attempt in range(VALUATION_FLUSH_ATTEMPTS):
        try:
            rows = [valuation_service.to_valuation(payload) for payload in latest.values()]
            async with AsyncDBSessionManager() as db:
                db.valuations.add_all(rows)
                await db.commit()
            return len(rows)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error(f"Error flushing valuations: {exc}")
            if attempt + 1 == VALUATION_FLUSH_ATTEMPTS:
                return None
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5)
    return None


# Async task: awaits trade_valuation_queue.get, then AsyncSession.commit.
async def valuation_worker():
    while True:
        first = await calculation_service.valuation_state.trade_valuation_queue.get()
        batch = [first]
        while len(batch) < VALUATION_BATCH_CAP:
            try:
                batch.append(calculation_service.valuation_state.trade_valuation_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        try:
            saved = await _flush_valuations(batch)
        except asyncio.CancelledError:
            raise
        for _item in batch:
            calculation_service.valuation_state.trade_valuation_queue.task_done()
        if saved:
            logging.info(f"Saved {saved} valuations to DB")
