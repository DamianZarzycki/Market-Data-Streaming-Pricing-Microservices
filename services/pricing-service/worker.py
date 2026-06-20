import json
import logging
import threading
import time
from urllib import request
from models import ConnectionStatus
import cache_service
import calculation_service

stats_lock = threading.Lock()

connection_status = ConnectionStatus.DISCONNECTED.value
events_counter = 0
last_market_event_time = None
last_pricing_time = None



def connect_to_market_data_stream():
    global connection_status

    with stats_lock:
        connection_status = ConnectionStatus.RECONNECTING.value

    logging.info("Attempting to connect to the stream...")

    url = "http://market-data-service:8001/stream"
    response = request.urlopen(url)

    with stats_lock:
        connection_status = ConnectionStatus.CONNECTED.value

    logging.info("Connected to the stream successfully.")

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
    # received for 'None' sometimes
    logging.info(f"Received tick for: {tick.get('symbol')}")
    calculation_service.metrics_queue.put({"type": "TICK_RECEIVED", "timestamp": tick.get("timestamp")})


def pricing_worker():
    global connection_status

    while True:
        try:
            stream_response = connect_to_market_data_stream()

            for tick in read_events(stream_response):
                update_market_state(tick)
                calculation_service.recalculate_valuations(tick)

        except Exception as e:
            with stats_lock:
                connection_status = ConnectionStatus.DISCONNECTED.value

            logging.error(f"Connection lost: {e}")
            time.sleep(1)
            logging.info("Attempting to reconnect...")


def metrics_worker():
    global last_pricing_time, events_counter, last_market_event_time

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

            calculation_service.metrics_queue.task_done()

        except Exception as e:
            logging.error(f"Error in metrics worker: {e}")


def cache_refresh_worker():
    while True:
        cache_service.reload_active_trades_cache()
        time.sleep(5)