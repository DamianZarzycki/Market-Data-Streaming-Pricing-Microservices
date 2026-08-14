import logging
import queue
import threading
import time
import uuid

from bottle import Bottle, request, response
from custom_server import ThreadedServer

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging
import worker

configure_logging("market-data-service")

app = Bottle()


@app.route("/health")
def health():
    with worker.data_lock:
        subscriber_count = len(worker.subscribers)
    return {**worker.health_stats, "active_subscribers": subscriber_count}


@app.route("/symbols")
def symbols():
    return worker.symbols()


@app.route("/snapshot")
def snapshot():
    with worker.data_lock:
        market_tick_data_state = dict(worker.market_tick_data_state)

    audit_logger = AuditLogger("market-data-service")
    audit_logger.info(
        EventType.SNAPSHOT_GENERATED,
        "Market Data service snapshot generated",
        entity_type="Service",
        correlation_id=None,
    )
    return market_tick_data_state


@app.route("/stream")
def stream():
    response.content_type = "text/event-stream"
    client_queue = queue.Queue()
    subscriber = {"id": str(uuid.uuid4())[:8], "queue": client_queue}

    with worker.data_lock:
        worker.subscribers.append(subscriber)
        subscriber_count = len(worker.subscribers)

    logging.info(
        f"Client {subscriber['id']} connected to stream. "
        f"Active subscribers: {subscriber_count}"
    )

    def event_generator():
        try:
            while True:
                message = client_queue.get()
                yield message
        except Exception as e:
            logging.error(f"Error while generating event: {e}")
        finally:
            with worker.data_lock:
                if subscriber in worker.subscribers:
                    worker.subscribers.remove(subscriber)
                subscriber_count = len(worker.subscribers)
            logging.info(
                f"Client {subscriber['id']} disconnected from stream. "
                f"Active subscribers: {subscriber_count}"
            )

    return event_generator()


if __name__ == "__main__":
    logging.info("Starting Market Data Service...")

    initial_ticks = worker.generate_market_tick()
    worker.update_snapshot(initial_ticks)

    audit_logger = AuditLogger("market-data-service")
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Market Worker started",
        entity_type="Service",
        correlation_id=None,
    )
    market_thread = threading.Thread(target=worker.market_worker)
    market_thread.daemon = True
    market_thread.start()

    audit_logger.info(
        EventType.WORKER_STARTED,
        "Metric Worker started",
        entity_type="Service",
        correlation_id=None,
    )
    metric_thread = threading.Thread(target=worker.metric_worker)
    metric_thread.daemon = True
    metric_thread.start()

    audit_logger.info(
        EventType.WORKER_STARTED,
        "Database Worker started",
        entity_type="Service",
        correlation_id=None,
    )
    db_thread = threading.Thread(target=worker.db_worker)
    db_thread.daemon = True
    db_thread.start()

    try:
        app.run(host="0.0.0.0", port=8001, server=ThreadedServer)
    finally:
        audit_logger.info(EventType.WORKER_STOPPED, "Market Worker stopped", entity_type="Service", correlation_id=None)
        audit_logger.info(EventType.WORKER_STOPPED, "Metric Worker stopped", entity_type="Service", correlation_id=None)
        audit_logger.info(EventType.WORKER_STOPPED, "Database Worker stopped", entity_type="Service", correlation_id=None)
