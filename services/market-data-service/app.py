import logging
import queue
import threading

from bottle import Bottle, response
from custom_server import ThreadedServer

import worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

app = Bottle()


@app.route("/health")
def health():
    return worker.health_stats


@app.route("/snapshot")
def snapshot():
    with worker.data_lock:
        market_tick_data_state = dict(worker.market_tick_data_state)
    return market_tick_data_state


@app.route("/stream")
def stream():
    response.content_type = "text/event-stream"
    client_queue = queue.Queue()

    with worker.data_lock:
        worker.subscribers.append(client_queue)

    logging.info(
        f"Client connected to stream. Active subscribers: {len(worker.subscribers)}"
    )

    def event_generator():
        try:
            while True:
                message = client_queue.get()
                yield message
        except Exception as e:
            logging.error(f"Error while generating event: {e}")
        finally:
            if client_queue in worker.subscribers:
                with worker.data_lock:
                    worker.subscribers.remove(client_queue)
                logging.info(
                    f"Client disconnected from stream. Active subscribers: {len(worker.subscribers)}"
                )

    return event_generator()


if __name__ == "__main__":
    logging.info("Starting Market Data Service...")

    initial_ticks = worker.generate_market_tick()
    worker.update_snapshot(initial_ticks)

    market_thread = threading.Thread(target=worker.market_worker)
    market_thread.daemon = True
    market_thread.start()

    metric_thread = threading.Thread(target=worker.metric_worker)
    metric_thread.daemon = True
    metric_thread.start()

    db_thread = threading.Thread(target=worker.db_worker)
    db_thread.daemon = True
    db_thread.start()

    app.run(host="0.0.0.0", port=8001, server=ThreadedServer)
