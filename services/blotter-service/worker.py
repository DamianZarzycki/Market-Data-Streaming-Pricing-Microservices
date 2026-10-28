import json
import logging
import threading
from urllib import request

from cache.valuation_cache_service import ValuationCacheService
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType

MAX_RECONNECT_DELAY = 30
BASE_RECONNECT_DELAY = 1
STREAM_URL = "http://pricing-service:8002/valuation-stream"
STREAM_TIMEOUT_SECONDS = 30

cache_service = ValuationCacheService()
audit_logger = AuditLogger("blotter-service")
stop_event = threading.Event()
_response_lock = threading.Lock()
_open_response = None


def sleep(delay: float) -> None:
    """Wait out the reconnect backoff. Returns early when shutdown is requested."""
    stop_event.wait(delay)


def hold_stream(response) -> None:
    global _open_response
    with _response_lock:
        _open_response = response


def close_stream() -> None:
    """Close the response the worker is blocked on so shutdown does not wait for the read timeout."""
    global _open_response
    with _response_lock:
        response = _open_response
        _open_response = None
    if response is None:
        return
    close = getattr(response, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception:
        logging.exception("Failed to close valuation stream")


def valuation_worker():
    logging.info("Valuation worker started.")
    reconnect_delay = BASE_RECONNECT_DELAY
    while not stop_event.is_set():
        try:
            stream_response = connect_to_valuation_stream()
            hold_stream(stream_response)
            logging.info("Connected to valuation stream, starting to read events...")
            reconnect_delay = BASE_RECONNECT_DELAY

            for raw_line in stream_response:
                if stop_event.is_set():
                    break
                line = raw_line.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                data_str = line[len("data:"):].strip()
                try:
                    valuation_data = json.loads(data_str)
                except json.JSONDecodeError as exc:
                    logging.error("Failed to parse valuation data: %s", exc)
                    continue
                if not isinstance(valuation_data, dict):
                    continue
                trade_id = valuation_data.get("trade_id")
                if not trade_id:
                    continue
                cache_service.update_cache(trade_id, valuation_data)
                logging.info("Updated valuation cache for trade %s", trade_id)

        except Exception as exc:
            if stop_event.is_set():
                break
            logging.error(
                "Connection lost: %s. Reconnecting in %ss...",
                exc,
                reconnect_delay,
            )
            audit_logger.error(
                EventType.STREAM_DISCONNECTED,
                "Lost connection to valuation stream: %s" % exc,
            )
            sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)
        finally:
            close_stream()


def connect_to_valuation_stream():
    try:
        response = request.urlopen(STREAM_URL, timeout=STREAM_TIMEOUT_SECONDS)
    except Exception as exc:
        audit_logger.error(
            EventType.STREAM_DISCONNECTED,
            "Failed to connect to valuation stream: %s" % exc,
        )
        raise
    logging.info("Connected to the stream successfully.")
    audit_logger.info(
        EventType.STREAM_CONNECTED,
        "Connected to pricing-service valuation stream",
    )
    return response
