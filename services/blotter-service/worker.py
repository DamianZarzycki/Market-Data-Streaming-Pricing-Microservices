import json
import logging
from time import sleep

from urllib import request
from cache.valuation_cache_service import ValuationCacheService
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType

MAX_RECONNECT_DELAY = 30
BASE_RECONNECT_DELAY = 1

cache_service = ValuationCacheService()
audit_logger = AuditLogger("blotter-service")

def valuation_worker():
    logging.info("Valuation worker started.")
    audit_logger.info(EventType.WORKER_STARTED, "Valuation worker started")
    reconnect_delay = BASE_RECONNECT_DELAY
    try:
        while True:
            try:
                stream_response = connect_to_valuation_stream()
                logging.info("Connected to valuation stream, starting to read events...")
                reconnect_delay = BASE_RECONNECT_DELAY

                for raw_line in stream_response:
                    line = raw_line.decode("utf-8").strip()
                    if line.startswith("data:"):
                        data_str = line[len("data:"):].strip()
                        logging.info(f"Received valuation data: {data_str}")
                        try:
                            valuation_data = json.loads(data_str)
                            trade_id = valuation_data.get("trade_id")
                            if trade_id:
                                cache_service.update_cache(trade_id, valuation_data)
                                logging.info(f"Updated valuation cache for trade {trade_id}")
                        except json.JSONDecodeError as e:
                            logging.error(f"Failed to parse valuation data: {e}")

            except Exception as e:
                logging.error(f"Connection lost: {e}. Reconnecting in {reconnect_delay}s...")
                audit_logger.error(EventType.STREAM_DISCONNECTED, f"Lost connection to valuation stream: {e}")
                sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)
    except Exception as e:
        logging.critical(f"Valuation worker crashed unexpectedly: {e}")
        audit_logger.error(EventType.WORKER_STOPPED, f"Valuation worker crashed: {e}")
        raise


def connect_to_valuation_stream():
    url = "http://pricing-service:8002/valuation-stream"
    try:
        response = request.urlopen(url, timeout=30)
    except Exception as e:
        audit_logger.error(EventType.STREAM_DISCONNECTED, f"Failed to connect to valuation stream: {e}")
        raise
    logging.info("Connected to the stream successfully.")
    audit_logger.info(EventType.STREAM_CONNECTED, "Connected to pricing-service valuation stream")
    return response
