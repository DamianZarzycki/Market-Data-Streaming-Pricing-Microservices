
import logging
import threading

import worker
import quote_service
from app import app
from custom_server import ThreadedServer
from helpers import load_providers_limits
from request_tracker import RequestTracker
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType

audit_logger = AuditLogger("market-data-service-integration")

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

    quote_service.tracker = RequestTracker.from_db(load_providers_limits())
    if not quote_service.tracker.limits:
        logging.warning(
            "No provider limits found in DB; fetches will be denied as unknown_provider"
        )
    else:
        logging.info("Loaded provider limits from DB: %s", quote_service.tracker.limits)

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
        audit_logger.info(
            EventType.WORKER_STOPPED,
            "Poll worker stopped",
            entity_type="Service",
            correlation_id="",
        )
