import logging
import worker
import threading
from bottle import Bottle, request, response
from shared.trading_shared.enums import ServiceStatus, ActionType
from custom_server import ThreadedServer


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
app = Bottle()

#TODO CLEAN THIS

app.route("/health")
def health():
    return {
        "service": "trade-action-service",
        "status": ServiceStatus.UP.value,
    }


@app.route("/trade-actions", method=["POST"])
def trade_actions():
    logging.info("New /trade-actions request received")
    
    try:
        request_data = request.json
    except Exception as e:
        logging.error(f"Failed to parse JSON payload: {e}")
        response.status = 400
        return {"error": "Invalid JSON format"}

    if not request_data:
        logging.warning("Rejected request: Missing payload data.")
        response.status = 400
        return {"error": "Missing payload data"}

    logging.info(f"Successfully parsed payload: {request_data}")

    action_type = request_data.get("action_type")
    if not action_type or action_type not in [ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value]:
        logging.warning(f"Rejected request: Missing or invalid action_type ('{action_type}').")
        response.status = 400
        return {"error": "Missing or invalid action_type"}

    client_request_id = request_data.get("client_request_id")
    if not client_request_id:
        logging.warning("Rejected request: Missing client_request_id.")
        response.status = 400
        return {"error": "Missing client_request_id"}

    logging.info(f"Validation passed for request: {client_request_id} ({action_type})")

    try:
        worker.trade_queue.put(request_data, block=False)
        logging.info(f"Successfully added request {client_request_id} to the internal queue.")
    except worker.trade_queue.Full:
        logging.error(f"System overloaded! Queue is full. Rejected request: {client_request_id}")
        response.status = 503
        return {"error": "System overloaded, please try again later"}

    response.status = 202
    logging.info(f"Returning 202 Accepted for request: {client_request_id}")
    return {
        "message": "Trade action accepted for processing",
        "client_request_id": client_request_id
    }

@app.route('/trade-actions/batch', method=["POST"])
def trade_action_batch():
    logging.info("New /trade-actions/batch request received")
    
    try:
        request_data = request.json
    except Exception as e:
        logging.error(f"Failed to parse JSON payload: {e}")
        response.status = 400
        return {"error": "Invalid JSON format"}
    
    if not request_data or not isinstance(request_data, list):
        logging.warning("Rejected request: Payload must be a non-empty list.")
        response.status = 400
        return {"error": "Payload must be a non-empty list of trade actions"}

    logging.info(f"Received batch with {len(request_data)} actions.")

    accepted_count = 0
    errors = []

    for index, item in enumerate(request_data):
        action_type = item.get("action_type")
        client_request_id = item.get("client_request_id")

        if not action_type or action_type not in [ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value]:
            errors.append(f"Item {index}: Missing or invalid action_type")
            continue
            
        if not client_request_id:
            errors.append(f"Item {index}: Missing client_request_id")
            continue

        try:
            worker.trade_queue.put(item, block=False)
            accepted_count += 1
        except worker.trade_queue.Full:
            logging.error("System overloaded! Queue is full during batch processing.")
            response.status = 503
            return {
                "error": "System overloaded, queue is full", 
                "accepted_so_far": accepted_count
            }

    response.status = 202
    logging.info(f"Batch processed. Accepted: {accepted_count}, Errors: {len(errors)}")
    return {
        "message": "Batch processed",
        "accepted_count": accepted_count,
        "errors": errors
    }

if __name__ == "__main__":
    logging.info("Starting Trade Action service...")

    trade_action_thread = threading.Thread(target=worker.trade_action_worker)
    trade_action_thread.daemon = True
    trade_action_thread.start()

    app.run(host="0.0.0.0", port=8080, server=ThreadedServer)
