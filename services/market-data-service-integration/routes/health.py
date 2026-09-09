import logging

from shared.trading_shared.enums import ServiceStatus


def register(app, snapshot):
    @app.route("/health")
    def health() -> dict:
        logging.info("Market Data Service Integration health check")
        return {
            "service": "market-data-service-integration",
            "status": ServiceStatus.UP.value,
            **snapshot(),
        }
