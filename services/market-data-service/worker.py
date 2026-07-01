from shared.trading_shared.db import DBSessionManager, SessionLocal
from shared.trading_shared.enums import CurveType
from shared.trading_shared.models import MarketDataCurve, MarketDataSpotPrice
from shared.trading_shared.enums import AssetClass, ServiceStatus
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType, Severity, EntityType

import json
import logging
import os
import queue
import random
import threading
import time
from datetime import datetime, timezone

from market_data_simulator import MarketDataSimulator

subscribers = []
data_lock = threading.Lock()

stats = {"generated_events": 0, "last_event_time": None}
global_event_id = 0
market_tick_data_state = {}

metrics_queue = queue.Queue()
db_queue = queue.Queue()
health_stats = {
    "service": "market-data-service",
    "status": ServiceStatus.UP.value,
    "generated_events": 0,
    "last_event_time": None,
}
batch_size = 30
market_simulator = MarketDataSimulator()


def metric_worker():
    global health_stats

    local_events_count = 0
    local_last_time = None

    while True:
        try:
            message = metrics_queue.get()
            if message["type"] == "EVENT_GENERATED":
                logging.info(f"Processing metric: {message['type']} at {message['timestamp']}")
                local_events_count += 1
                local_last_time = message["timestamp"]

            metrics_queue.task_done()

            health_stats = {
                "service": "market-data-service",
                "status": ServiceStatus.UP.value,
                "generated_events": local_events_count,
                "last_event_time": local_last_time,
            }

        except Exception as e:
            logging.error(f"Error occurred while processing metric: {e}")
        finally:
            pass


def current_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_yield():
    return round(random.uniform(0.03, 0.06), 4)


def generate_market_tick():
    global global_event_id

    current_tmsp = current_timestamp()
    global_event_id += 5

    eq_tick = market_simulator.generate_equity_tick()
    bond_yield = market_simulator.generate_bond_tick()
    fx_spot = market_simulator.generate_fx_tick()

    usd_rates = market_simulator.generate_usd_curve_tick()
    eur_rates = market_simulator.generate_eur_curve_tick()

    market_tick_data = {
        "ACME": {
            "event_id": global_event_id - 4,
            "asset_type": AssetClass.EQUITY.value,
            "timestamp": current_tmsp,
            "symbol": "ACME",
            "bid": eq_tick["bid"],
            "ask": eq_tick["ask"],
            "last": eq_tick["last"],
        },
        "GOVT_5Y": {
            "event_id": global_event_id - 3,
            "asset_type": AssetClass.BOND.value,
            "timestamp": current_tmsp,
            "symbol": "GOVT_5Y",
            "yield": bond_yield,
        },
        "EURUSD": {
            "event_id": global_event_id - 2,
            "asset_type": AssetClass.FX.value,
            "symbol": "EURUSD",
            "timestamp": current_tmsp,
            "spot": fx_spot,
            # "domestic_rate": 0.045,
            # "foreign_rate": 0.032,
            # "tenor_years": 1,
            # DO PRICING SERVICE
        },
        "USD_YIELD_CURVE": {
            "event_id": global_event_id - 1,
            "curve_name": "USD_YIELD_CURVE",
            "curve_type": CurveType.YIELD_CURVE.value,
            "currency": "USD",
            "tenors": ["1M", "3M", "1Y", "5Y"],
            "rates": usd_rates,
            "timestamp": current_tmsp,
        },
        "EUR_YIELD_CURVE": {
            "event_id": global_event_id,
            "curve_name": "EUR_YIELD_CURVE",
            "curve_type": CurveType.YIELD_CURVE.value,
            "currency": "EUR",
            "tenors": ["1M", "3M", "1Y", "5Y"],
            "rates": eur_rates,
            "timestamp": current_tmsp,
        },
    }

    metrics_queue.put({"type": "EVENT_GENERATED", "timestamp": current_tmsp})

    return market_tick_data


def update_snapshot(market_tick_data):
    global market_tick_data_state

    with data_lock:
        market_tick_data_state = market_tick_data
        stats["generated_events"] += len(market_tick_data)
        stats["last_event_time"] = current_timestamp()

        logging.info(
            f"Generated {len(market_tick_data)} market ticks. Total events: {stats['generated_events']}"
        )


def publish_tick_to_stream(market_tick_data):
    with data_lock:
        for _, instrument_data in market_tick_data.items():
            msg = f"data: {json.dumps(instrument_data)}\n\n"
            for subscriber_queue in subscribers:
                subscriber_queue.put(msg)


def market_worker():
    interval_ms_str = os.getenv("TICK_INTERVAL_MS", "100")
    interval_ms = int(interval_ms_str)
    sleep_seconds = interval_ms / 1000.0

    while True:
        new_ticks = generate_market_tick()
        update_snapshot(new_ticks)
        publish_tick_to_stream(new_ticks)
        db_queue.put(new_ticks)
        time.sleep(sleep_seconds)


def symbols():
    db = SessionLocal()
    try:
        data = db.query(MarketDataSpotPrice).all()
        grouped_symbols = {}
        logging.info(f"Fetchedddd {len(data)} market data records from DB for symbol extraction")
        for item in data:
            if item.asset_class:
                if item.asset_class not in grouped_symbols:
                    grouped_symbols[item.asset_class] = []

                if item.symbol not in grouped_symbols[item.asset_class]:
                    grouped_symbols[item.asset_class].append(item.symbol)
        logging.info(f"Grouped symbols by asset class: {grouped_symbols}")
        return grouped_symbols
    finally:
        db.close()


def db_worker():
    buffer = []
    logging.info("DB worker started")
    with DBSessionManager() as db:
        while True:
            try:
                new_ticks = db_queue.get()
                try:
                    for _, data in new_ticks.items():
                        if "curve_type" in data:
                            logging.info(f"Processing market data for DB: {data}")
                            curve_record = MarketDataCurve(
                                event_id=data.get("event_id"),
                                curve_name=data.get("curve_name"),  # np. "USD_YIELD_CURVE"
                                curve_type=data.get("curve_type"),  # np. "YIELD_CURVE"
                                currency=data.get("currency"),  # np. "USD"
                                tenors=data.get("tenors"),  # np. ["1M", "3M", "1Y", "5Y"]
                                rates=data.get("rates"),  # np. [0.0412, 0.0415, 0.0421, 0.0450]
                                event_time=datetime.now(timezone.utc),  # czas wystąpienia ticku
                                raw_payload=data,  # cały wygenerowany słownik dla pewności audytowej
                            )
                            buffer.append(curve_record)
                        else:
                            record = MarketDataSpotPrice(
                                event_id=data.get("event_id"),
                                symbol=data["symbol"],
                                asset_class=data["asset_type"],
                                source="GENERATED",
                                event_time=datetime.now(timezone.utc),
                                raw_payload=data,
                            )

                            if data["asset_type"] == AssetClass.EQUITY.value:
                                record.bid = data.get("bid")
                                record.ask = data.get("ask")
                                record.last = data.get("last")
                            elif data["asset_type"] == AssetClass.FX.value:
                                record.spot = data.get("spot")
                            elif data["asset_type"] == AssetClass.BOND.value:
                                record.last = data.get("yield")

                            buffer.append(record)

                    if len(buffer) >= batch_size or db_queue.empty() and len(buffer) > 0:
                        try:
                            db.market_data.add_all(buffer)
                            db.commit()
                            logging.info(f"Saved {len(buffer)} market data records to DB")
                            buffer.clear()
                        except Exception as e:
                            db.rollback()
                            logging.error(f"Error occurred while saving market data: {e}")
                            db = SessionLocal()

                except Exception as e:
                    db.rollback()
                    logging.error(f"Error occurred while saving market data: {e}")
                    db = SessionLocal()
                finally:
                    db_queue.task_done()
            except Exception as e:
                logging.error(f"Error in DB worker: {e}")
