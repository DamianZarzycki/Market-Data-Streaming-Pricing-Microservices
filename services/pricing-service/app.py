import json
import threading
import logging
from bottle import Bottle, HTTPResponse, response
from custom_server import ThreadedServer

import worker
import calculation_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
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
        error_body = json.dumps(
            {"error": "Valuation not found for trade_id: " + trade_id}
        )
        return HTTPResponse(
            status=404, body=error_body, headers={"Content-Type": "application/json"}
        )


@app.route("/valuation-stream")
def valuation_stream():
    logging.info("Client connected to /valuation-stream for SSE.")
    response.content_type = "text/event-stream"
    def event_generator():
        while True:
            valuation_data = calculation_service.sse_queue.get()
            try:
                logging.info(
                    f"Streaming valuation update for trade {valuation_data.get('trade_id')}"
                )
                yield f"event: valuation_update\ndata: {json.dumps(valuation_data)}\n\n"
            except Exception as e:
                logging.error(f"Error serializing valuation for SSE: {e}")
            finally:
                calculation_service.sse_queue.task_done()

    return event_generator()


if __name__ == "__main__":
    logging.info("Starting Pricing Service...")

    market_thread = threading.Thread(target=worker.pricing_worker)
    market_thread.daemon = True
    market_thread.start()

    metrics_thread = threading.Thread(target=worker.metrics_worker)
    metrics_thread.daemon = True
    metrics_thread.start()

    cache_thread = threading.Thread(target=worker.cache_refresh_worker)
    cache_thread.daemon = True
    cache_thread.start()

    app.run(host="0.0.0.0", port=8002, server=ThreadedServer)
