import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from urllib import request
from models import ConnectionStatus

stats_lock = threading.Lock()
pricing_lock = threading.Lock()

connection_status = ConnectionStatus.DISCONNECTED.value
events_counter = 0
last_market_event_time = None
last_pricing_time = None
valuations_store = {}

metrics_queue = queue.Queue()


def calculate_bond_pv(bond_yield):
    """Present Value"""
    face_value = 1000
    coupon_rate = 0.05
    maturity_years = 5
    coupon = face_value * coupon_rate
    pv = 0.0
    for year in range(1, maturity_years + 1):
        if year < maturity_years:
            cash_flow = coupon
        else:
            cash_flow = coupon + face_value

        pv += cash_flow / ((1 + bond_yield) ** year)

    return pv


def current_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


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
    logging.info(f"Received tick for: {tick.get('symbol')}")
    metrics_queue.put({"type": "TICK_RECEIVED", "timestamp": tick.get("timestamp")})


def calculate_bond_valuations(tick, symbol):
    bond_yield = tick.get("yield")

    if bond_yield is not None:
        pv = round(calculate_bond_pv(bond_yield), 4)

        instrument_key = f"BOND_{symbol}"

        with pricing_lock:
            last_pricing_time = current_timestamp()
            valuations_store[instrument_key] = {
                "type": "BOND",
                "fair_value": pv,
                "currency": "USD",
                "market_symbol": symbol,
                "last_updated": last_pricing_time,
            }
        logging.info(f"Recalculated valuation for {instrument_key}: {pv}")
        metrics_queue.put(
            {
                "type": "PRICING_DONE",
                "timestamp": current_timestamp(),
                "instrument": instrument_key,
                "value": pv,
            }
        )


def calculate_equity_valuations(tick, symbol):
    bid = tick.get("bid")
    ask = tick.get("ask")

    if bid is not None and ask is not None:
        mid = round((bid + ask) / 2, 4)
        instrument_key = f"EQ_{symbol}"

        with pricing_lock:
            last_pricing_time = current_timestamp()
            valuations_store[instrument_key] = {
                "type": "EQUITY",
                "fair_value": mid,
                "currency": "USD",
                "market_symbol": symbol,
                "last_updated": last_pricing_time,
            }
        logging.info(f"Recalculated valuation for {instrument_key}: {mid}")
        metrics_queue.put(
            {
                "type": "PRICING_DONE",
                "timestamp": current_timestamp(),
                "instrument": instrument_key,
                "value": mid,
            }
        )


def calculate_fx_valuations(tick, symbol):
    tenor_years = tick.get("tenor_years", 1)

    forward = (
        tick.get("spot")
        * (1 + tick.get("domestic_rate") * tenor_years)
        / (1 + tick.get("foreign_rate") * tenor_years)
    )
    forward = round(forward, 4)

    instrument_key = f"FX_{symbol}_{int(tenor_years)}Y"

    with pricing_lock:
        last_pricing_time = current_timestamp()
        valuations_store[instrument_key] = {
            "type": "FX_FORWARD",
            "fair_value": forward,
            "currency": "USD",
            "market_symbol": symbol,
            "last_updated": last_pricing_time,
        }
        metrics_queue.put(
            {
                "type": "PRICING_DONE",
                "timestamp": current_timestamp(),
                "instrument": instrument_key,
                "value": forward,
            }
        )
    logging.info(f"Recalculated valuation for {instrument_key}: {forward}")


def recalculate_valuations(tick):
    global last_pricing_time

    asset_type = tick.get("asset_type")
    symbol = tick.get("symbol")

    if asset_type == "EQUITY":
        calculate_equity_valuations(tick, symbol)

    elif asset_type == "BOND":
        calculate_bond_valuations(tick, symbol)

    elif asset_type == "FX":
        calculate_fx_valuations(tick, symbol)


def pricing_worker():
    global connection_status

    while True:
        try:
            stream_response = connect_to_market_data_stream()

            for tick in read_events(stream_response):
                update_market_state(tick)
                recalculate_valuations(tick)

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
            message = metrics_queue.get()

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

            metrics_queue.task_done()

        except Exception as e:
            logging.error(f"Error in metrics worker: {e}")
