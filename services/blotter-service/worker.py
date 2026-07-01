import json
import logging
from time import sleep

from urllib import request
from cache.valuation_cache_service import ValuationCacheService


cache_service = ValuationCacheService()
def valuation_worker():
    while True:
        try:
            stream_response = connect_to_valuation_stream()
            logging.info("Connected to valuation stream, starting to read events...")

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
            logging.error(f"Connection lost: {e}")
            logging.info("Attempting to reconnect...")

        sleep(1)


def connect_to_valuation_stream():
    url = "http://pricing-service:8002/valuation-stream"
    response = request.urlopen(url)
    logging.info("Connected to the stream successfully.")
    return response
