import json
from collections import defaultdict
from urllib import request as urllib_request

from sqlalchemy import func

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ServiceStatus, TradeStatus
from shared.trading_shared.models import Trade
from shared.trading_shared.serialization import serialize
import cache.valuation_cache_service as valuation_cache_service
from cache.valuation_cache_service import ValuationCacheService

_cache = ValuationCacheService()


def get_health():
    cache = valuation_cache_service.valuation_cache
    return {
        "cache_trade_count": len(cache),
        "cache_entry_count": sum(len(v) for v in cache.values()),
        "service": "blotter-service",
        "status": ServiceStatus.UP.value,
    }


def _as_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def fetch_book_summary():
    """Return books enriched with aggregated PnL and active trade counts.

    Books metadata comes from books-service. Realized / unrealized PnL is summed
    from the latest live valuation per trade in the blotter cache. Active trade
    counts come from the trades table.
    """
    with urllib_request.urlopen("http://books-service:8004/books", timeout=2) as resp:
        if resp.status != 200:
            raise RuntimeError(
                f"Failed to fetch book summary. Status code: {resp.status}"
            )
        payload = json.loads(resp.read().decode())

    books = payload.get("books") or []
    aggregates = {
        book["book_id"]: {
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "active_trades": 0,
        }
        for book in books
        if book.get("book_id")
    }

    with DBSessionManager() as db:
        active_counts = (
            db.session.query(Trade.book_id, func.count())
            .filter(Trade.status == TradeStatus.ACTIVE.value)
            .group_by(Trade.book_id)
            .all()
        )
        for book_id, count in active_counts:
            book_id_str = str(book_id)
            if book_id_str in aggregates:
                aggregates[book_id_str]["active_trades"] = int(count)

    # Aggregate from the latest live valuation per trade (cache is the source of
    # truth for blotter UI; avoids scanning the full valuations history table).
    pnl_by_book = defaultdict(lambda: {"realized_pnl": 0.0, "unrealized_pnl": 0.0})
    for history in valuation_cache_service.valuation_cache.values():
        if not history:
            continue
        latest = history[-1]
        book_id = latest.get("book_id")
        if not book_id or book_id not in aggregates:
            continue
        pnl_by_book[book_id]["realized_pnl"] += _as_float(latest.get("realized_pnl"))
        pnl_by_book[book_id]["unrealized_pnl"] += _as_float(
            latest.get("unrealized_pnl")
        )

    for book_id, pnl in pnl_by_book.items():
        aggregates[book_id]["realized_pnl"] = pnl["realized_pnl"]
        aggregates[book_id]["unrealized_pnl"] = pnl["unrealized_pnl"]

    for book in books:
        stats = aggregates.get(
            book.get("book_id"),
            {"realized_pnl": 0.0, "unrealized_pnl": 0.0, "active_trades": 0},
        )
        book["realized_pnl"] = round(stats["realized_pnl"], 4)
        book["unrealized_pnl"] = round(stats["unrealized_pnl"], 4)
        book["active_trades"] = stats["active_trades"]

    return payload


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

    return [
        {
            **trade,
            "valuation": _cache.get_latest_valuation(trade.get("trade_id")),
        }
        for trade in serialized
    ]


def fetch_trade_by_id(trade_id):
    with DBSessionManager() as db:
        trade = db.trades.get_by_id(trade_id)
        if not trade:
            return None
        serialized_trade = serialize(trade)
        valuations = db.valuations.get_valuations_by_trade_id(trade_id)

    return {
        "trade": serialized_trade,
        "latest_valuation": _cache.get_latest_valuation(trade_id),
        "valuation_history": [serialize(v) for v in valuations],
        "audit_logs": [],
    }


def fetch_trade_valuations(trade_id):
    with DBSessionManager() as db:
        valuations = db.valuations.get_valuations_by_trade_id(trade_id)
    return [serialize(v) for v in valuations]
