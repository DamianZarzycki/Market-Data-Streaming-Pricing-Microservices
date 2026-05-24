import logging
import queue
import threading

from bottle import Bottle, response
from custom_server import ThreadedServer

import worker
from models import ServiceStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

app = Bottle()


@app.route("/health")
def health():
    with worker.data_lock:
        return {
            "service": "market-data-service",
            "status": ServiceStatus.UP.value,
            "generated_events": worker.stats.get("generated_events"),
            "last_event_time": worker.stats.get("last_event_time"),
        }


@app.route("/snapshot")
def snapshot():
    with worker.data_lock:
        return worker.instruments_state


@app.route("/stream")
def stream():
    response.content_type = "text/event-stream"
    client_queue = queue.Queue()
    
    with worker.data_lock:
        worker.subscribers.append(client_queue)
        logging.info(f"Client connected to stream. Active subscribers: {len(worker.subscribers)}")

    def event_generator():
        try:
            while True:
                message = client_queue.get()
                yield message
        except Exception as e:
            logging.error(f"Error while generating event: {e}")
        finally:
            with worker.data_lock:
                if client_queue in worker.subscribers:
                    worker.subscribers.remove(client_queue)
                    logging.info(f"Client disconnected from stream. Active subscribers: {len(worker.subscribers)}")

    return event_generator()


if __name__ == "__main__":
    logging.info("Starting Market Data Service...")
    
    initial_ticks = worker.generate_market_tick()
    worker.update_snapshot(initial_ticks)

    market_thread = threading.Thread(target=worker.market_worker)
    market_thread.daemon = True
    market_thread.start()
    
    app.run(host="0.0.0.0", port=8001, server=ThreadedServer)