from providers.twelve_data_provider import TwelveDataProvider
from providers.alpha_vantage_provider import AlphaVantageProvider
from providers.finnhub import FinnhubProvider
import logging
from shared.trading_shared.enums import EventType
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import ServiceStatus

from bottle import Bottle
from custom_server import ThreadedServer

from shared.trading_shared.logging_config import configure_logging

configure_logging("market-data-service-integration")

app = Bottle()

finnhub = FinnhubProvider()
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
    return {    "service": "market-data-service-integration",
    "status": ServiceStatus.UP.value,
}

@app.route("/market-data/finn")
def finn():
    audit_logger = AuditLogger("market-data-service-integration")
    audit_logger.info(EventType.WORKER_STARTED, "Market Data Service Integration started", entity_type="Service", correlation_id="")
    return FinnhubProvider().fetch_data("AAPL")

@app.route("/market-data/alpha")
def alpha():
    audit_logger = AuditLogger("market-data-service-integration")
    audit_logger.info(EventType.WORKER_STARTED, "Market Data Service Integration started", entity_type="Service", correlation_id="")
    return AlphaVantageProvider().fetch_data("AAPL")

@app.route("/market-data/twelve")
def twelve():
    audit_logger = AuditLogger("market-data-service-integration")
    audit_logger.info(EventType.WORKER_STARTED, "Market Data Service Integration started", entity_type="Service", correlation_id="")
    return TwelveDataProvider().fetch_data("AAPL")



if __name__ == "__main__":
    audit_logger = AuditLogger("market-data-service-integration")
    audit_logger.info(EventType.WORKER_STARTED, "Market Data Service Integration started", entity_type="Service", correlation_id="")
    try:
        app.run(host="0.0.0.0", port=8005, server=ThreadedServer)
    finally:
        audit_logger.info(
            EventType.WORKER_STOPPED,
            "Market Data Service Integration stopped",
            entity_type="Service",
            correlation_id="",
        )
