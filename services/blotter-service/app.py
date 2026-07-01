# GET /health
# GET /books/summary
# GET /trades
# GET /trades/<trade_id>
# GET /trades/<trade_id>/valuations


import json
import logging
import threading
from bottle import Bottle, request, response
import cache.valuation_cache_service as valuation_cache_service
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ServiceStatus, TradeStatus
from urllib import request as urlib_response
import trades_service
from custom_server import ThreadedServer
import worker

app = Bottle()


@app.route("/health")
def health():
    logging.info(f"Health check requested. {valuation_cache_service.valuation_cache}")
    return {
        "cache": valuation_cache_service.valuation_cache,
        "service": "blotter-service",
        "status": ServiceStatus.UP.value,
    }


@app.route("/books/summary")
def book_summary():
    with urlib_response.urlopen("http://books-service:8004/books", timeout=2) as response:
        if response.status == 200:
            data = json.loads(response.read().decode())
            return data
        else:
            logging.error(f"Failed to fetch book summary. Status code: {response.status}")
            response.status = 500
            return {"error": "Failed to fetch book summary"}

@app.route("/trades")
def trades():
    book_id = request.query.get('book_id')
    asset_class = request.query.get('asset_class')
    status = request.query.get('status')
    symbol = request.query.get('symbol')
    first_only = request.query.get('first_only', False)

    page = request.query.get('page')
    limit = request.query.get('limit')

    trades = trades_service.fetch_trades(filters={
        "book_id": book_id,
        "asset_class": asset_class,
        "status": status,
        "symbol": symbol,
        "first_only": first_only,
        "page": page,
        "limit": limit
    })

    cache = valuation_cache_service.valuation_cache
    trades = [{**trade, "valuation": cache.get(trade.get("trade_id"))} for trade in trades]

    return {"trades": trades}


@app.route("/trades/<trade_id>")
def trade_by_id(trade_id):
    # ew zobaczyc workera do walidacji lub w trade-action-service
    trade = trades_service.fetch_trade_by_id(trade_id)
    if trade:
        with DBSessionManager() as db:
            return {
                "trade": trade,
                "latest_valuation": valuation_cache_service.valuation_cache.get(trade_id),
                "valuation_history": db.valuations.get_valuations_by_trade_id(trade_id),
                "audit_logs": []  # Placeholder for future implementation FROM AUDIT LOGS TABLE
                }
    else:
        response.status = 404
        return {"error": "Trade not found"}


@app.route("/trades/<trade_id>/valuations")
def trade_valuations(trade_id):
    with DBSessionManager() as db:
        valuations = db.valuations.get_valuations_by_trade_id(trade_id)
        return {"valuations": valuations}


@app.route("/trades/<trade_id>/audit-logs")
def trade_audit_logs(trade_id):
    pass


if __name__ == "__main__":
    logging.info("Starting Blotter service...")

    monitoring_thread = threading.Thread(target=worker.valuation_worker)
    monitoring_thread.daemon = True
    monitoring_thread.start()

    app.run(host="0.0.0.0", port=8006, server=ThreadedServer)
