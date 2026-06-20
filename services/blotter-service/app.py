# GET /health
# GET /books/summary
# GET /trades
# GET /trades/<trade_id>
# GET /trades/<trade_id>/valuations


import logging
import threading
import Bottle
import worker
from shared.trading_shared.enums import ServiceStatus

app = Bottle()


@app.route("/health")
def health():
    return {
        "service": "blotter-service",
        "status": ServiceStatus.UP.value,
    }


@app.route("/books/summary")
def book_summary():
    pass


@app.route("/trades")
def trades():
    pass


@app.route("/trades/<trade_id>")
def trade_detail():
    pass


@app.route("/trades/<trade_id>/valuations")
def trade_valuations():
    pass


@app.route("/trades/<trade_id>/audit-logs")
def trade_audit_logs():
    pass


if __name__ == "__main__":
    logging.info("Starting Blotter service...")

    monitoring_thread = threading.Thread(target=worker.valuation_worker)
    monitoring_thread.daemon = True
    monitoring_thread.start()

    app.run(host="0.0.0.0", port=8006)
