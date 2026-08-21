import logging

import trade_intentions_service
import worker
from bottle import Bottle, request, response
from custom_server import ThreadedServer

from shared.trading_shared.enums import ServiceStatus
from shared.trading_shared.logging_config import configure_logging

configure_logging("trade-generation-service")
app = Bottle()
logging.info("Runtime generator config: %s", trade_intentions_service.get_config())


@app.route("/health")
def health():
    return {
        "service": "trade-generation-service",
        "status": ServiceStatus.UP.value,
    }


@app.route("/generate-once", method=["GET"])
def generate_once():
    logging.info("Received request to generate a trade intention")
    if not trade_intentions_service.generator_state["is_running"]:
        logging.info("DB worker not running")

    intention = trade_intentions_service.generate_and_send_to_trade_action_service()
    return intention if intention else {"message": "No trade intention generated."}


@app.route("/generate-batch", method=["GET"])
def generate_multiple():
    logging.info("Received request to generate multiple trade intentions")
    if not trade_intentions_service.generator_state["is_running"]:
        logging.info("DB worker not running")

    intentions = trade_intentions_service.generate_and_send_to_trade_action_service_batch()
    if intentions:
        return {
            "message": f"Successfully generated {len(intentions)} intentions",
            "intentions": intentions,
        }
    else:
        return {"message": "No trade intentions generated."}


@app.route("/start", method=["POST"])
def start_generation():
    if not trade_intentions_service.start_generator(worker.trade_generation_worker):
        response.status = 400
        return {"message": "Generator is already running!"}

    return {"message": "Generator started successfully."}


@app.route("/stop", method=["POST"])
def stop_generation():
    if not trade_intentions_service.stop_generator():
        response.status = 400
        return {"message": "No data available."}

    return {"message": "Generator was stopped."}


@app.route("/config", method=["GET"])
def get_config():
    return trade_intentions_service.get_config()


@app.route("/config", method=["PUT"])
def put_config():
    payload = request.json
    try:
        config = trade_intentions_service.update_config(payload)
    except ValueError as exc:
        response.status = 400
        return {"message": str(exc)}

    return {
        "message": "Config updated successfully.",
        "config": config,
    }


@app.route("/status", method=["GET"])
def status():
    state = trade_intentions_service.generator_state
    return {
        "is_running": state["is_running"],
        "total_generated": state["total_generated"],
        "expected_rate_per_sec": trade_intentions_service.expected_rate_per_sec(),
    }


if __name__ == "__main__":
    logging.info("Starting Trade Generation service...")
    app.run(host="0.0.0.0", port=8007, server=ThreadedServer)
