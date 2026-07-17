# GET /health
# GET /books/summary
# GET /trades
# GET /trades/<trade_id>
# GET /trades/<trade_id>/valuations


import logging
import threading
from bottle import Bottle, request, response
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging
import trades_service
from custom_server import ThreadedServer
import worker

configure_logging("blotter-service")

app = Bottle()


@app.route("/health")
def health():
    logging.info("Health check requested.")
    return trades_service.get_health()


@app.route("/books/summary")
def book_summary():
    try:
        return trades_service.fetch_book_summary()
    except Exception as e:
        logging.error(f"Failed to fetch book summary: {e}")
        response.status = 500
        return {"error": "Failed to fetch book summary"}


@app.route("/trades")
def trades():
    filters = {
        "book_id": request.query.get("book_id"),
        "asset_class": request.query.get("asset_class"),
        "status": request.query.get("status"),
        "symbol": request.query.get("symbol"),
        "first_only": request.query.get("first_only", False),
        "page": request.query.get("page"),
        "limit": request.query.get("limit"),
    }
    return {"trades": trades_service.fetch_trades(filters=filters)}


@app.route("/trades/<trade_id>")
def trade_by_id(trade_id):
    result = trades_service.fetch_trade_by_id(trade_id)
    if result:
        return result
    response.status = 404
    return {"error": "Trade not found"}


@app.route("/trades/<trade_id>/valuations")
def trade_valuations(trade_id):
    return {"valuations": trades_service.fetch_trade_valuations(trade_id)}


@app.route("/trades/<trade_id>/audit-logs")
def trade_audit_logs(trade_id):
    pass


if __name__ == "__main__":
    logging.info("Starting Blotter service...")
    audit_logger = AuditLogger("blotter-service")
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Valuation worker thread started",
        entity_type="Service",
        correlation_id=None,
    )

    monitoring_thread = threading.Thread(target=worker.valuation_worker)
    monitoring_thread.daemon = True
    monitoring_thread.start()

    try:
        app.run(host="0.0.0.0", port=8006, server=ThreadedServer)
    finally:
        audit_logger.info(
            EventType.WORKER_STOPPED,
            "Valuation worker thread stopped",
            entity_type="Service",
            correlation_id=None,
        )
