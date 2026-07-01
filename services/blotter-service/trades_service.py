
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.serialization import serialize


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
        return [serialize(t) for t in trades]


def fetch_trade_by_id(trade_id):
    with DBSessionManager() as db:
        trade = db.trades.get_by_id(trade_id)
        return serialize(trade)
