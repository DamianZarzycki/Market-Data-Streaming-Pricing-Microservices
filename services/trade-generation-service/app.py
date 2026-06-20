import logging
import worker
import threading
import trade_intentions_service
from bottle import Bottle, response
from shared.trading_shared.enums import ServiceStatus
from custom_server import ThreadedServer


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
app = Bottle()


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


# TODO mozesz dodac randomizacje tego | kompresja
@app.route("/generate-batch", method=["GET"])
def generate_multiple():
    logging.info("Received request to generate multiple trade intentions")
    if not trade_intentions_service.generator_state["is_running"]:
        logging.info("DB worker not running")

    intentions = (
        trade_intentions_service.generate_and_send_to_trade_action_service_batch()
    )
    if intentions:
        return {
            "message": f"Successfully generated {len(intentions)} intentions",
            "intentions": intentions,
        }
    else:
        return {"message": "No trade intentions generated."}


@app.route("/start", method=["POST"])
def start_generation():
    if trade_intentions_service.generator_state["is_running"]:
        response.status = 400
        return {"message": "Generator is already running!"}

    trade_intentions_service.generator_state["is_running"] = True
    trade_intentions_service.generator_state["thread"] = threading.Thread(
        target=worker.trade_generation_worker
    )
    trade_intentions_service.generator_state["thread"].daemon = True
    trade_intentions_service.generator_state["thread"].start()

    return {"message": "Generator started successfully."}


@app.route("/stop", method=["POST"])
def stop_generation():
    if not trade_intentions_service.generator_state["is_running"]:
        response.status = 400
        return {"message": "No data available."}

    # TODO Czy to nam daje pewność, że wątek się zatrzyma bez wygenerowanie 'ostatniej' iteracji?
    trade_intentions_service.generator_state["is_running"] = False

    return {"message": "Generator was stopped."}


@app.route("/status", method=["GET"])
def status():
    return trade_intentions_service.generator_state


if __name__ == "__main__":
    logging.info("Starting Trade Generation service...")
    app.run(host="0.0.0.0", port=8007, server=ThreadedServer)
