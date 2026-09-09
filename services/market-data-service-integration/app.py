import logging
import threading

import core.audit_worker as audit_worker
import core.worker as worker
from bottle import Bottle

from custom_server import ThreadedServer
from core.curve_service import CurveService
from core.fetch_audit import snapshot
from core.quote_service import QuoteService
from providers.limits.market_data_providers_limits import load_providers_limits
from core.request_tracker import RequestTracker
from routes import register_routes
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType

app = Bottle()
audit_logger = AuditLogger("market-data-service-integration")
_quote_service: QuoteService | None = None
_curve_service: CurveService | None = None


def quote_service() -> QuoteService:
    if _quote_service is None:
        raise RuntimeError("QuoteService not initialized")
    return _quote_service


def curve_service() -> CurveService:
    if _curve_service is None:
        raise RuntimeError("CurveService not initialized")
    return _curve_service


register_routes(app, quote_service, curve_service, snapshot)

# GET /health
# GET /providers/<provider>/health
# GET /providers
# GET /market-data/quotes?symbol=AAPL&asset_class=EQUITY
# GET /market-data/quotes/<provider>/<symbol>
# GET /market-data/stream
# GET /market-data/stream/<provider>
# GET /curves
# GET /curves/<provider>
# POST /market-data/refresh
# POST /curves/refresh

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

    audit_logger.info(
        EventType.WORKER_STARTED,
        "Fetch audit worker started",
        entity_type="Service",
        correlation_id="",
    )
    audit_thread = threading.Thread(target=audit_worker.audit_worker)
    audit_thread.daemon = True
    audit_thread.start()

    tracker = RequestTracker.from_db(load_providers_limits())
    if not tracker.limits:
        logging.warning(
            "No provider limits found in DB; fetches will be denied as unknown_provider"
        )
    else:
        logging.info("Loaded provider limits from DB: %s", tracker.limits)

    _quote_service = QuoteService.create(tracker, worker.db_queue)
    _curve_service = CurveService.create()

    try:
        app.run(host="0.0.0.0", port=8005, server=ThreadedServer)
    finally:
        audit_worker.stop()
        audit_thread.join(timeout=audit_worker.FLUSH_INTERVAL_SECONDS + 2)
        audit_logger.info(
            EventType.WORKER_STOPPED,
            "Fetch audit worker stopped",
            entity_type="Service",
            correlation_id="",
        )
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
