import json
import queue
import threading
import logging
from bottle import Bottle, HTTPResponse, response
from custom_server import ThreadedServer

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging
import worker
import calculation_service
import book_metrics_service

configure_logging("pricing-service")
app = Bottle()


@app.route("/health")
def health():
    with worker.stats_lock:
        return {
            "service": "pricing-service",
            "status": "UP",
            "market_data_connection": worker.connection_status,
            "received_events": worker.events_counter,
            "last_market_event_time": worker.last_market_event_time,
            "last_pricing_time": worker.last_pricing_time,
            "last_valuation_symbol": worker.last_valuation_symbol,
            "last_valuation_asset_class": worker.last_valuation_asset_class,
        }


@app.route("/valuations")
def valuations():
    with calculation_service.pricing_lock:
        return calculation_service.valuations_store.copy()


@app.route("/valuations/<trade_id>")
def valuations_by_trade_id(trade_id):
    with calculation_service.pricing_lock:
        valuation = calculation_service.valuations_store.get(trade_id)

    if valuation is not None:
        return {"trade_id": trade_id, **valuation}
    else:
        error_body = json.dumps({"error": "Valuation not found for trade_id: " + trade_id})
        return HTTPResponse(
            status=404, body=error_body, headers={"Content-Type": "application/json"}
        )


@app.route("/book-metrics")
def book_metrics():
    """Return the latest alpha/beta metrics for every book."""
    return {"benchmark": "MARKET_INDEX", "books": book_metrics_service.get_all_metrics()}


@app.route("/book-metrics/<book_id>")
def book_metrics_by_id(book_id):
    metrics = book_metrics_service.get_metrics_for_book(book_id)
    if metrics is not None:
        return metrics

    error_body = json.dumps({"error": "No metrics available for book_id: " + book_id})
    return HTTPResponse(
        status=404, body=error_body, headers={"Content-Type": "application/json"}
    )


@app.route("/valuation-stream")
def valuation_stream():
    logging.info("Client connected to /valuation-stream for SSE.")
    response.content_type = "text/event-stream"

    client_queue = queue.Queue()
    with calculation_service.sse_subscribers_lock:
        calculation_service.sse_subscribers.append(client_queue)

    def event_generator():
        try:
            # Send current valuations immediately so the client doesn't wait for the next tick
            with calculation_service.pricing_lock:
                snapshot = list(calculation_service.valuations_store.values())
            for valuation_data in snapshot:
                yield f"event: valuation_update\ndata: {json.dumps(valuation_data)}\n\n"

            while True:
                valuation_data = client_queue.get()
                try:
                    logging.info(
                        f"Streaming valuation update for trade {valuation_data.get('trade_id')}"
                    )
                    yield f"event: valuation_update\ndata: {json.dumps(valuation_data)}\n\n"
                except Exception as e:
                    logging.error(f"Error serializing valuation for SSE: {e}")
                finally:
                    client_queue.task_done()
        finally:
            with calculation_service.sse_subscribers_lock:
                calculation_service.sse_subscribers.remove(client_queue)
            logging.info("Client disconnected from /valuation-stream.")

    return event_generator()


if __name__ == "__main__":
    logging.info("Starting Pricing Service...")
    audit_logger = AuditLogger("pricing-service")
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Market thread started",
        entity_type="Service",
        correlation_id=None,
    )

    market_thread = threading.Thread(target=worker.pricing_worker)
    market_thread.daemon = True
    market_thread.start()

    audit_logger.info(
        EventType.WORKER_STARTED,
        "Metrics thread started",
        entity_type="Service",
        correlation_id=None,
    )

    metrics_thread = threading.Thread(target=worker.metrics_worker)
    metrics_thread.daemon = True
    metrics_thread.start()

    audit_logger.info(
        EventType.WORKER_STARTED,
        "Cache refresh thread started",
        entity_type="Service",
        correlation_id=None,
    )

    cache_thread = threading.Thread(target=worker.cache_refresh_worker)
    cache_thread.daemon = True
    cache_thread.start()

    try:
        app.run(host="0.0.0.0", port=8002, server=ThreadedServer)
    finally:
        audit_logger.info(EventType.WORKER_STOPPED, "Market thread stopped", entity_type="Service", correlation_id=None)
        audit_logger.info(EventType.WORKER_STOPPED, "Metrics thread stopped", entity_type="Service", correlation_id=None)
        audit_logger.info(EventType.WORKER_STOPPED, "Cache refresh thread stopped", entity_type="Service", correlation_id=None)
