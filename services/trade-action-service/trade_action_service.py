import logging
import queue as queue_module

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import ActionType, EventType, ServiceStatus
import worker

VALID_ACTION_TYPES = {ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value}
audit_logger = AuditLogger("trade-action-service")


def get_health():
    return {
        "service": "trade-action-service",
        "status": ServiceStatus.UP.value,
    }


def validate_trade_action(item):
    action_type = item.get("action_type")
    if not action_type or action_type not in VALID_ACTION_TYPES:
        return f"Missing or invalid action_type ('{action_type}')"
    if not item.get("client_request_id"):
        return "Missing client_request_id"
    return None


def submit_trade_action(data):
    error = validate_trade_action(data)
    if error:
        logging.warning(f"Rejected request: {error}")
        audit_logger.warning(
            EventType.DB_REJECT,
            f"Trade action rejected: {error}",
            payload={"action_type": data.get("action_type"), "client_request_id": data.get("client_request_id")},
        )
        return {"error": error}, 400

    client_request_id = data["client_request_id"]
    try:
        worker.trade_queue.put(data, block=False)
        logging.info(f"Enqueued request {client_request_id}.")
    except queue_module.Full:
        logging.error(f"Queue full. Rejected request: {client_request_id}")
        audit_logger.error(
            EventType.DB_ERROR,
            f"Trade action queue full. Rejected: {client_request_id}",
            correlation_id=client_request_id,
        )
        return {"error": "System overloaded, please try again later"}, 503

    return {
        "message": "Trade action accepted for processing",
        "client_request_id": client_request_id,
    }, 202


def submit_trade_action_batch(items):
    accepted_count = 0
    errors = []

    for index, item in enumerate(items):
        error = validate_trade_action(item)
        if error:
            errors.append(f"Item {index}: {error}")
            continue

        try:
            worker.trade_queue.put(item, block=False)
            accepted_count += 1
        except queue_module.Full:
            logging.error("Queue full during batch processing.")
            audit_logger.error(
                EventType.DB_ERROR,
                "Trade action queue full during batch processing",
                correlation_id=item.get("client_request_id"),
            )
            return {
                "error": "System overloaded, queue is full",
                "accepted_so_far": accepted_count,
            }, 503

    logging.info(f"Batch processed. Accepted: {accepted_count}, Errors: {len(errors)}")
    return {
        "message": "Batch processed",
        "accepted_count": accepted_count,
        "errors": errors,
    }, 202
