import json
import logging
import random
import threading
import time
from datetime import datetime, timezone

from equity_data_simulator import EquityDataSimulator
from models import AssetType

subscribers = []
data_lock = threading.Lock()

stats = {"generated_events": 0, "last_event_time": None}
global_event_id = 0
instruments_state = {}


def current_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_yield():
    return round(random.uniform(0.03, 0.06), 4)


def generate_market_tick():
    global global_event_id
    
    simulator = EquityDataSimulator(initial_price=100.0)
    tick = simulator.generate_tick()
    current_tmsp = current_timestamp()
    
    global_event_id += 3
    
    new_instruments = {
        "ACME": {
            "event_id": global_event_id - 2,
            "asset_type": AssetType.EQUITY.value,
            "timestamp": current_tmsp,
            "symbol": "ACME",
            "bid": tick["bid"],
            "ask": tick["ask"],
            "last": tick["last"],
        },
        "GOVT_5Y": {
            "event_id": global_event_id - 1,
            "asset_type": AssetType.BOND.value,
            "timestamp": current_tmsp,
            "symbol": "GOVT_5Y",
            "yield": random_yield(),
        },
        "EURUSD": {
            "event_id": global_event_id,
            "asset_type": AssetType.FX.value,
            "symbol": "EURUSD",
            "timestamp": current_tmsp,
            "spot": round(1.0875 + random.normalvariate(0, 0.0001), 4),
            "domestic_rate": 0.045,
            "foreign_rate": 0.032,
            "tenor_years": 1,
        },
    }
    return new_instruments


def update_snapshot(new_instruments):
    global instruments_state
    
    with data_lock:
        instruments_state = new_instruments
        stats["generated_events"] += len(new_instruments)
        stats["last_event_time"] = current_timestamp()
        
        logging.info(f"Generated {len(new_instruments)} market ticks. Total events: {stats['generated_events']}")


def publish_tick_to_stream(new_instruments):
    with data_lock:
        for _, instrument_data in new_instruments.items():
            msg = f"data: {json.dumps(instrument_data)}\n\n"
            for subscriber_queue in subscribers:
                subscriber_queue.put(msg)


def market_worker():
    while True:
        new_ticks = generate_market_tick()
        update_snapshot(new_ticks)
        publish_tick_to_stream(new_ticks)
        time.sleep(0.1)