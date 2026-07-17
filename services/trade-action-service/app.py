import logging
import threading
from bottle import Bottle, request, response
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging
import trade_action_service
from custom_server import ThreadedServer
import worker

configure_logging("trade-action-service")
app = Bottle()


@app.route("/health")
def health():
    return trade_action_service.get_health()


@app.route("/trade-actions", method=["POST"])
def trade_actions():
    logging.info("New /trade-actions request received")
    try:
        data = request.json
    except Exception as e:
        logging.error(f"Failed to parse JSON payload: {e}")
        response.status = 400
        return {"error": "Invalid JSON format"}

    if not data:
        response.status = 400
        return {"error": "Missing payload data"}

    body, status = trade_action_service.submit_trade_action(data)
    response.status = status
    return body


@app.route("/trade-actions/batch", method=["POST"])
def trade_action_batch():
    logging.info("New /trade-actions/batch request received")
    try:
        data = request.json
    except Exception as e:
        logging.error(f"Failed to parse JSON payload: {e}")
        response.status = 400
        return {"error": "Invalid JSON format"}

    if not data or not isinstance(data, list):
        response.status = 400
        return {"error": "Payload must be a non-empty list of trade actions"}

    body, status = trade_action_service.submit_trade_action_batch(data)
    response.status = status
    return body


if __name__ == "__main__":
    logging.info("Starting Trade Action service...")
    audit_logger = AuditLogger("trade-action-service")
    audit_logger.info(
        EventType.WORKER_STARTED,
        "Trade Action service started",
        entity_type="Service",
        correlation_id=None,
    )
    trade_action_thread = threading.Thread(target=worker.trade_action_worker)
    trade_action_thread.daemon = True
    trade_action_thread.start()

    try:
        app.run(host="0.0.0.0", port=8080, server=ThreadedServer)
    finally:
        audit_logger.info(EventType.WORKER_STOPPED, "Trade Action Worker stopped", entity_type="Service", correlation_id=None)

