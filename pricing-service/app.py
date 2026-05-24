import json
import threading
import logging
from bottle import Bottle, HTTPResponse
from custom_server import ThreadedServer

import worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
app = Bottle()

@app.route("/health")
def health():
    with worker.state_lock:
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
    with worker.state_lock:
        return worker.valuations_store.copy()

@app.route("/valuations/<instrument_id>")
def valuations_by_instrument_id(instrument_id):
    with worker.state_lock:
        valuation = worker.valuations_store.get(instrument_id)
        
    if valuation is not None:
        return {"instrument_id": instrument_id, **valuation}
    else:
        error_body = json.dumps(
            {"error": "Valuation not found for instrument_id: " + instrument_id}
        )
        return HTTPResponse(
            status=404, body=error_body, headers={"Content-Type": "application/json"}
        )

if __name__ == "__main__":
    logging.info("Starting Pricing Service...")
    
    market_thread = threading.Thread(target=worker.pricing_worker)
    market_thread.daemon = True
    market_thread.start()
    
    app.run(host="0.0.0.0", port=8002, server=ThreadedServer)