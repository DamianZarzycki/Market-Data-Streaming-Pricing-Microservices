import json
import logging
from urllib import request as urllib_request

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ServiceStatus
from shared.trading_shared.serialization import serialize
import cache.valuation_cache_service as valuation_cache_service


def get_health():
    return {
        "cache": valuation_cache_service.valuation_cache,
        "service": "blotter-service",
        "status": ServiceStatus.UP.value,
    }


def fetch_book_summary():
    with urllib_request.urlopen("http://books-service:8004/books", timeout=2) as resp:
        if resp.status == 200:
            return json.loads(resp.read().decode())
        raise RuntimeError(f"Failed to fetch book summary. Status code: {resp.status}")


def fetch_trades(filters=None):
    with DBSessionManager() as db:
        trades = db.trades.get_trades(
            client_request_id=filters.get("client_request_id"),
            book_id=filters.get("book_id"),
            asset_class=filters.get("asset_class"),
            status=filters.get("status"),
            symbol=filters.get("symbol"),
            first_only=filters.get("first_only"),
            page=filters.get("page"),
            limit=filters.get("limit"),
        )
        serialized = [serialize(t) for t in trades]

    cache = valuation_cache_service.valuation_cache
    return [{**trade, "valuation": cache.get(trade.get("trade_id"))} for trade in serialized]


def fetch_trade_by_id(trade_id):
    with DBSessionManager() as db:
        trade = db.trades.get_by_id(trade_id)
        if not trade:
            return None
        serialized_trade = serialize(trade)
        valuations = db.valuations.get_valuations_by_trade_id(trade_id)

    return {
        "trade": serialized_trade,
        "latest_valuation": valuation_cache_service.valuation_cache.get(trade_id),
        "valuation_history": valuations,
        "audit_logs": [],
    }


def fetch_trade_valuations(trade_id):
    with DBSessionManager() as db:
        valuations = db.valuations.get_valuations_by_trade_id(trade_id)
    return valuations
