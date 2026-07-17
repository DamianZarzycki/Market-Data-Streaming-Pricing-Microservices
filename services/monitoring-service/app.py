import threading
import logging
from bottle import Bottle
from custom_server import ThreadedServer

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType, ServiceStatus
from shared.trading_shared.logging_config import configure_logging
import worker

configure_logging("monitoring-service")

app = Bottle()


@app.route("/health")
def health():
    return {
        "service": "monitoring-service",
        "status": ServiceStatus.UP.value,
    }


@app.route("/status")
def status():
    return worker.health_cache.copy()


if __name__ == "__main__":
    logging.info("Starting Monitoring service...")
    audit_logger = AuditLogger("monitoring-service")
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Monitoring service started",
        entity_type="Service",
        correlation_id=None,
    )

    monitoring_thread = threading.Thread(target=worker.monitoring_worker)
    monitoring_thread.daemon = True
    monitoring_thread.start()

    try:
        app.run(host="0.0.0.0", port=8003, server=ThreadedServer)
    finally:
        audit_logger.info(EventType.WORKER_STOPPED, "Monitoring Worker stopped", entity_type="Service", correlation_id=None)
