import logging
import threading

from bottle import Bottle

from custom_server import ThreadedServer
from quote_service import fetch_and_store
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType, ServiceStatus
from shared.trading_shared.logging_config import configure_logging
import worker

configure_logging("market-data-service-integration")

app = Bottle()
audit_logger = AuditLogger("market-data-service-integration")

# GET /health
# GET /providers
# GET /providers/<provider>/health
# GET /market-data/snapshot
# GET /market-data/quotes?symbol=AAPL&asset_class=EQUITY
# GET /market-data/quotes/<provider>/<symbol>
# GET /market-data/stream
# GET /market-data/stream/<provider>
# GET /curves
# GET /curves/<provider>
# POST /market-data/refresh
# POST /curves/refresh


@app.route("/health")
def health() -> dict[str, str]:
    logging.info("Market Data Service Integration health check")
    return {
        "service": "market-data-service-integration",
        "status": ServiceStatus.UP.value,
    }


@app.route("/providers")
def providers():
    return {
        "providers": [
            "finnhub",
            "alpha_vantage",
            "twelve_data",
        ]
    }

if __name__ == "__main__":
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Market Data Service Integration started",
        entity_type="Service",
        correlation_id="",
    )
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Database Worker started",
        entity_type="Service",
        correlation_id="",
    )
    db_thread = threading.Thread(target=worker.db_worker)
    db_thread.daemon = True
    db_thread.start()
    try:
        app.run(host="0.0.0.0", port=8005, server=ThreadedServer)
    finally:
        audit_logger.info(
            EventType.WORKER_STOPPED,
            "Market Data Service Integration stopped",
            entity_type="Service",
            correlation_id="",
        )
        audit_logger.info(
            EventType.WORKER_STOPPED,
            "Database Worker stopped",
            entity_type="Service",
            correlation_id="",
        )
