import threading
import logging
from bottle import Bottle
from custom_server import ThreadedServer

from models import ServiceStatus
import worker 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# locks i race condition przeanalizowac
app = Bottle()

@app.route("/health")
def health():
    return {
        "service": "monitoring-service",
        "status": ServiceStatus.UP.value,
    }

@app.route("/status")
def status():
    with worker.status_lock:
        return worker.health_cache.copy()

if __name__ == "__main__":
    logging.info("Starting Monitoring service...")

    monitoring_thread = threading.Thread(target=worker.monitoring_worker)
    monitoring_thread.daemon = True
    monitoring_thread.start()
    
    app.run(host="0.0.0.0", port=8003, server=ThreadedServer)