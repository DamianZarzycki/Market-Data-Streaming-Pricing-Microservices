from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import (
    AssetClass,
    CurveType,
    EventType,
    IRSDirection,
    ServiceStatus,
    SnapshotType,
)
from shared.trading_shared.models import MarketDataCurve, MarketDataSnapshot, MarketDataSpotPrice
from shared.trading_shared.audit import AuditLogger

import json
import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timezone

from market_data_simulator import MarketDataSimulator

subscribers = []
data_lock = threading.Lock()
audit_logger = AuditLogger("market-data-service")

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
    "last_symbol": None,
    "last_asset_type": None,
}
batch_size = 30

market_simulator = MarketDataSimulator()

def metric_worker():
    global health_stats

    local_events_count = 0
    local_last_time = None
    local_last_symbol = None
    local_last_asset_type = None

    while True:
        try:
            message = metrics_queue.get()
            if message["type"] == "EVENT_GENERATED":
                logging.info(f"Processing metric: {message['type']} at {message['timestamp']}")
                local_events_count += 1
                local_last_time = message["timestamp"]
                local_last_symbol = message.get("symbol") or local_last_symbol
                local_last_asset_type = message.get("asset_type") or local_last_asset_type

            metrics_queue.task_done()

            health_stats = {
                "service": "market-data-service",
                "status": ServiceStatus.UP.value,
                "generated_events": local_events_count,
                "last_event_time": local_last_time,
                "last_symbol": local_last_symbol,
                "last_asset_type": local_last_asset_type,
            }

        except Exception as e:
            logging.error(f"Error occurred while processing metric: {e}")


def current_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def generate_market_tick():
    """Generuje wyłącznie dane rynkowe (spot + krzywe). Bez parametrów transakcji."""
    global global_event_id

    current_tmsp = current_timestamp()
    global_event_id += 6

    eq_tick = market_simulator.generate_equity_tick()
    bond_yield = market_simulator.generate_bond_tick()
    fx_spot = market_simulator.generate_fx_tick()
    usd_rates = market_simulator.generate_usd_curve_tick()
    eur_rates = market_simulator.generate_eur_curve_tick()
    option_details = market_simulator.generate_option_details()
    irs_details = market_simulator.generate_irs_details()
    benchmark_level = market_simulator.generate_benchmark_tick()

    market_tick_data = {
        "ACME_OPT": {
            "event_id": global_event_id - 7,
            "asset_type": AssetClass.OPTION.value,
            "timestamp": current_tmsp,
            "symbol": "ACME",
            "currency": "USD",
            "spot": option_details["spot"],
            "strike": option_details["strike"],
            "option_right_type": option_details["option_right_type"],
            "maturity_years": option_details["maturity_years"],
            "volatility": option_details["volatility"],
        },
        "IRS": {
            "event_id": global_event_id - 6,
            "asset_type": AssetClass.IRS.value,
            "timestamp": current_tmsp,
            "currency": irs_details["currency"],
            "notional": irs_details["notional"],
            "fixed_rate": irs_details["fixed_rate"],
            "maturity_years": irs_details["maturity_years"],
            "payments_per_year": irs_details["payments_per_year"],
            "direction": irs_details["direction"],
        },
        "MARKET_INDEX": {
            "event_id": global_event_id - 5,
            "asset_type": AssetClass.BENCHMARK.value,
            "timestamp": current_tmsp,
            "symbol": "MARKET_INDEX",
            "currency": "USD",
            "last": benchmark_level,
        },
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
        },
        "USD_YIELD_CURVE": {
            "event_id": global_event_id - 1,
            "curve_id": str(uuid.uuid4()),
            "symbol": "USD_YIELD_CURVE",
            "curve_name": "USD_YIELD_CURVE",
            "curve_type": CurveType.YIELD_CURVE.value,
            "currency": "USD",
            "tenors": ["1M", "3M", "1Y", "5Y"],
            "rates": usd_rates,
            "timestamp": current_tmsp,
        },
        "EUR_YIELD_CURVE": {
            "event_id": global_event_id,
            "curve_id": str(uuid.uuid4()),
            "symbol": "EUR_YIELD_CURVE",
            "curve_name": "EUR_YIELD_CURVE",
            "curve_type": CurveType.YIELD_CURVE.value,
            "currency": "EUR",
            "tenors": ["1M", "3M", "1Y", "5Y"],
            "rates": eur_rates,
            "timestamp": current_tmsp,
        },
    }

    # Prefer a liquid cash instrument for monitoring "last tick" detail.
    representative = market_tick_data.get("ACME") or next(
        (tick for tick in market_tick_data.values() if tick.get("symbol")),
        {},
    )
    metrics_queue.put(
        {
            "type": "EVENT_GENERATED",
            "timestamp": current_tmsp,
            "symbol": representative.get("symbol"),
            "asset_type": representative.get("asset_type"),
        }
    )
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
            logging.info(f"Publishing tick to stream: {instrument_data}")
            msg = f"data: {json.dumps(instrument_data)}\n\n"
            for subscriber_queue in subscribers:
                subscriber_queue.put(msg)


def persist_curves(market_tick_data):
    """Zapisuje krzywe synchronicznie przed publish — pricing może od razu zrobić get_curve(curve_id)."""
    curve_records = []
    for _, data in market_tick_data.items():
        if "curve_type" not in data:
            continue
        curve_records.append(
            MarketDataCurve(
                curve_id=data.get("curve_id"),
                event_id=data.get("event_id"),
                curve_name=data.get("curve_name"),
                curve_type=data.get("curve_type"),
                currency=data.get("currency"),
                tenors=data.get("tenors"),
                rates=data.get("rates"),
                event_time=datetime.now(timezone.utc),
                raw_payload=data,
            )
        )

    if not curve_records:
        return

    try:
        with DBSessionManager() as db:
            db.market_data.add_all(curve_records)
            db.commit()
        logging.info(
            f"Flushed {len(curve_records)} curve(s) to MarketDataCurve before stream publish"
        )
    except Exception as e:
        logging.error(f"Failed to flush curves before publish: {e}")
        audit_logger.error(
            EventType.DB_ERROR,
            f"Failed to flush curves before publish: {e}",
            payload={"error": str(e), "curve_count": len(curve_records)},
        )


def persist_full_snapshot(market_tick_data):
    """Okresowy FULL snapshot lokalnego stanu rynku → MarketDataSnapshots."""
    snapshot = MarketDataSnapshot(
        event_id=global_event_id,
        snapshot_type=SnapshotType.FULL.value,
        snapshot_time=datetime.now(timezone.utc),
        payload=market_tick_data,
    )
    try:
        with DBSessionManager() as db:
            db.session.add(snapshot)
            db.commit()
        logging.info(f"Saved FULL market data snapshot (event_id={global_event_id})")
        audit_logger.info(
            EventType.SNAPSHOT_GENERATED,
            "FULL market data snapshot persisted",
            payload={"event_id": global_event_id, "instruments": list(market_tick_data.keys())},
        )
    except Exception as e:
        logging.error(f"Failed to persist market data snapshot: {e}")
        audit_logger.error(
            EventType.DB_ERROR,
            f"Failed to persist market data snapshot: {e}",
            payload={"error": str(e)},
        )


def market_worker():
    global _ticks_since_snapshot

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
    with DBSessionManager() as db:
        try:
            data = db.session.query(MarketDataSpotPrice).all()
            grouped_symbols = {}
            logging.info(
                f"Fetched {len(data)} market data records from DB for symbol extraction"
            )
            for item in data:
                if item.asset_class:
                    if item.asset_class not in grouped_symbols:
                        grouped_symbols[item.asset_class] = []

                    if item.symbol not in grouped_symbols[item.asset_class]:
                        grouped_symbols[item.asset_class].append(item.symbol)
            logging.info(f"Grouped symbols by asset class: {grouped_symbols}")
            return grouped_symbols
        finally:
            pass


def db_worker():
    """Batchuje tylko spoty. Krzywe są flushowane synchronicznie w persist_curves."""
    buffer = []
    logging.info("DB worker started")
    while True:
        try:
            new_ticks = db_queue.get()
            try:
                for _, data in new_ticks.items():
                    if "curve_type" in data:
                        continue

                    record = MarketDataSpotPrice(
                        event_id=data.get("event_id"),
                        symbol=data["symbol"],
                        asset_class=data["asset_type"],
                        source="GENERATED",
                        event_time=datetime.now(timezone.utc),
                        raw_payload=data,
                    )

                    asset_type = data["asset_type"]
                    if asset_type == AssetClass.EQUITY.value:
                        record.bid = data.get("bid")
                        record.ask = data.get("ask")
                        record.last = data.get("last")
                    elif asset_type == AssetClass.FX.value:
                        record.spot = data.get("spot")
                    elif asset_type == AssetClass.BOND.value:
                        record.last = data.get("yield")
                    elif asset_type == AssetClass.OPTION.value:
                        record.spot = data.get("spot")
                        # volatility siedzi w raw_payload; kolumna spot/last nie ma pola vol
                    elif asset_type == AssetClass.BENCHMARK.value:
                        record.spot = data.get("spot")
                        record.last = data.get("last")

                    buffer.append(record)

                if len(buffer) >= batch_size or (db_queue.empty() and len(buffer) > 0):
                    try:
                        batch_count = len(buffer)
                        with DBSessionManager() as db:
                            db.market_data.add_all(buffer)
                            db.commit()
                        logging.info(f"Saved {batch_count} spot market data records to DB")
                        buffer.clear()
                        audit_logger.info(
                            EventType.DB_CREATE,
                            f"Market data batch saved: {batch_count} records",
                            payload={"record_count": batch_count},
                        )
                    except Exception as e:
                        logging.error(f"Error occurred while saving market data: {e}")
                        audit_logger.error(
                            EventType.DB_ERROR,
                            f"Failed to save market data batch: {e}",
                            payload={"error": str(e), "batch_size": len(buffer)},
                        )
                        buffer.clear()

            except Exception as e:
                logging.error(f"Error occurred while processing market data: {e}")
                buffer.clear()
            finally:
                db_queue.task_done()
        except Exception as e:
            logging.error(f"Error in DB worker: {e}")
