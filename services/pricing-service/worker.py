import queue
import json
import logging
import threading
import time
from urllib import request
import cache_service
import calculation_service
import valuation_service
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import AssetClass, ConnectionStatus, EventType
from shared.trading_shared.db import DBSessionManager

stats_lock = threading.Lock()
audit_logger = AuditLogger("pricing-service")

connection_status = ConnectionStatus.DISCONNECTED.value
events_counter = 0
last_market_event_time = None
last_pricing_time = None
last_valuation_symbol = None
last_valuation_asset_class = None

def connect_to_market_data_stream():
    global connection_status

    with stats_lock:
        connection_status = ConnectionStatus.RECONNECTING.value

    logging.info("Attempting to connect to the stream...")

    url = "http://market-data-service:8001/stream"
    try:
        response = request.urlopen(url)
    except Exception as e:
        with stats_lock:
            connection_status = ConnectionStatus.DISCONNECTED.value
        audit_logger.error(EventType.STREAM_DISCONNECTED, f"Failed to connect to market-data-service: {e}")
        raise

    with stats_lock:
        connection_status = ConnectionStatus.CONNECTED.value

    logging.info("Connected to the stream successfully.")
    audit_logger.info(EventType.STREAM_CONNECTED, "Connected to market-data-service stream")
    return response


def read_events(stream_response):
    for line in stream_response:
        decoded_line = line.decode("utf-8").strip()

        if decoded_line.startswith("data: "):
            json_string = decoded_line[6:]
            try:
                yield json.loads(json_string)
            except json.JSONDecodeError:
                logging.warning("Received invalid data format.")


def update_market_state(tick):
    logging.info(f"Received tick for: {tick.get('symbol')}")
    calculation_service.metrics_queue.put({"type": "TICK_RECEIVED", "timestamp": tick.get("timestamp")})


def pricing_worker():
    global connection_status
    audit_logger.info(EventType.WORKER_STARTED, "Pricing worker started")
    while True:
        try:
            stream_response = connect_to_market_data_stream()

            for tick in read_events(stream_response):
                logging.info(f"Read tick: {tick}")
                update_market_state(tick)
                calculation_service.recalculate_valuations(tick)

        except Exception as e:
            with stats_lock:
                connection_status = ConnectionStatus.DISCONNECTED.value

            logging.error(f"Connection lost: {e}")
            audit_logger.error(EventType.STREAM_DISCONNECTED, f"Lost connection to market-data-service: {e}")
            time.sleep(1)
            logging.info("Attempting to reconnect...")


def metrics_worker():
    global last_pricing_time, events_counter, last_market_event_time
    global last_valuation_symbol, last_valuation_asset_class

    while True:
        try:
            message = calculation_service.metrics_queue.get()

            if message["type"] == "TICK_RECEIVED":
                with stats_lock:
                    events_counter += 1
                    last_market_event_time = message["timestamp"]
                    logging.info(
                        f"Tick received at {last_market_event_time}. Total events: {events_counter}"
                    )

            elif message["type"] == "PRICING_DONE":
                logging.info(
                    f"Pricing done for {message['instrument']} at {message['timestamp']}. Value: {message['value']}"
                )

                with stats_lock:
                    last_pricing_time = message["timestamp"]
                    last_valuation_symbol = message.get("symbol") or last_valuation_symbol
                    last_valuation_asset_class = (
                        message.get("asset_class") or last_valuation_asset_class
                    )

            calculation_service.metrics_queue.task_done()

        except Exception as e:
            logging.error(f"Error in metrics worker: {e}")


def cache_refresh_worker():
    while True:
        cache_service.reload_active_trades_cache()
        time.sleep(5)


MARKET_DATA_INTEGRATION_URL = "http://market-data-service-integration:8005"


def _fetch_provider_quote(provider, symbol):
    # /market-data/quotes/<provider>/<symbol> validates <provider> against
    # lowercase provider keys ("finnhub"), while trade.market_data_provider
    # is stored uppercase ("FINNHUB") — lowercase it for the URL only; the
    # route's own DB lookup uppercases again internally.
    url = f"{MARKET_DATA_INTEGRATION_URL}/market-data/quotes/{provider.lower()}/{symbol}"
    try:
        with request.urlopen(url, timeout=5) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        logging.warning(f"Failed to fetch quote for {provider}/{symbol}: {e}")
        return None


def provider_pricing_worker():
    """Reprice active trades opened against a real provider.

    IRS is left to curve_pricing_worker. OPTION is marked with Black-Scholes
    using this quote as spot. FUTURES uses the quote times contract_multiplier.
    Everything else is side-aware bid/ask/last times quantity.
    """
    while True:
        try:
            with cache_service.cache_lock:
                provider_trades = [
                    t
                    for t in cache_service.active_trades_cache.values()
                    if t.market_data_provider and t.asset_class != AssetClass.IRS.value
                ]

            pairs = {(t.market_data_provider, t.symbol) for t in provider_trades}
            quotes_by_pair = {}
            for provider, symbol in pairs:
                quote = _fetch_provider_quote(provider, symbol)
                if quote is not None and not quote.get("error"):
                    quotes_by_pair[(provider, symbol)] = quote

            for trade in provider_trades:
                quote = quotes_by_pair.get((trade.market_data_provider, trade.symbol))
                if quote is None:
                    continue
                try:
                    calculation_service.price_trade_from_provider_quote(trade, quote)
                except Exception as e:
                    logging.error(f"Error pricing provider trade {trade.trade_id}: {e}")
        except Exception as e:
            logging.error(f"Error in provider_pricing_worker: {e}")
        time.sleep(5)


def curve_pricing_worker():
    """Reprice IRS from persisted FRED/ECB yield curves.

    Reads MarketDataCurve (latest YIELD_CURVE per currency, excluding the
    simulator). Does not call /curves on the integration service — that GET
    is a DB snapshot, and live fetches stay on POST /curves/refresh and the
    curve poll worker.
    """
    while True:
        try:
            calculation_service.reprice_irs_from_persisted_curves()
        except Exception as e:
            logging.error(f"Error in curve_pricing_worker: {e}")
        time.sleep(5)


def valuation_worker():
    while True:
        time.sleep(0.2)
        batch = []
        while True:
            try:
                item = calculation_service.trade_valuation_queue.get_nowait()
            except queue.Empty:
                break
            batch.append(valuation_service.to_valuation(item))
            calculation_service.trade_valuation_queue.task_done()
        if not batch:
            continue
        try:
            with DBSessionManager() as db:
                db.valuations.add_all(batch)
                db.commit()
            logging.info(f"Saved {len(batch)} valuations to DB")
        except Exception as e:
            logging.error(f"Error flushing valuations: {e}")
