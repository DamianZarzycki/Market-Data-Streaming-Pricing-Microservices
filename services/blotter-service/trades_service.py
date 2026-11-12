import asyncio
import logging

import httpx2
from sqlalchemy import func, select

from shared.trading_shared.enums import TradeStatus
from shared.trading_shared.env import env, env_float
from shared.trading_shared.models import Trade
from shared.trading_shared.repositories import TradeRepository, ValuationRepository
from shared.trading_shared.serialization import serialize
from cache.valuation_cache_service import ValuationCacheService

BOOKS_URL = f"{env('BOOKS_SERVICE_URL').rstrip('/')}/books"
BOOKS_TIMEOUT_SECONDS = env_float("BLOTTER_BOOKS_TIMEOUT_SECONDS", 2.0, minimum=0.1)
_BOOKS_CALLS = asyncio.Semaphore(10)
_http_client: httpx2.AsyncClient | None = None

_cache = ValuationCacheService()


def open_http() -> httpx2.AsyncClient:
    """One client for the process. Callers await get(); the semaphore caps how many wait at once."""
    global _http_client
    _http_client = httpx2.AsyncClient(timeout=BOOKS_TIMEOUT_SECONDS)
    return _http_client


async def close_http() -> None:
    global _http_client
    client = _http_client
    _http_client = None
    if client is not None:
        await client.aclose()


async def fetch_books_payload() -> dict:
    """Read the book catalogue. Callers open a database session only after this returns."""
    if _http_client is None:
        raise RuntimeError("books-service client is not open")
    async with _BOOKS_CALLS:
        try:
            response = await _http_client.get(BOOKS_URL)
            status_code = response.status_code
            payload = response.json() if status_code == 200 else None
        except Exception as exc:
            raise RuntimeError("books-service request failed") from exc
    if status_code != 200:
        raise RuntimeError(
            "Failed to fetch book summary. Status code: %s" % status_code
        )
    if not isinstance(payload, dict):
        raise RuntimeError("books-service payload was not an object")
    return payload


async def _active_counts(session) -> dict[str, int]:
    result = await session.execute(
        select(Trade.book_id, func.count())
        .where(Trade.status == TradeStatus.ACTIVE.value)
        .group_by(Trade.book_id)
    )
    return {str(book_id): int(count) for book_id, count in result.all()}


async def build_book_summary(payload: dict, session) -> dict:
    """Attach PnL and active-trade counts to the books-service catalogue."""
    books = payload.get("books") or []
    aggregates = {
        book["book_id"]: {
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "active_trades": 0,
        }
        for book in books
        if isinstance(book, dict) and book.get("book_id")
    }

    for book_id, count in (await _active_counts(session)).items():
        if book_id in aggregates:
            aggregates[book_id]["active_trades"] = count

    for book_id, pnl in _cache.sum_latest_pnl(set(aggregates)).items():
        aggregates[book_id]["realized_pnl"] = pnl["realized_pnl"]
        aggregates[book_id]["unrealized_pnl"] = pnl["unrealized_pnl"]

    for book in books:
        if not isinstance(book, dict):
            continue
        stats = aggregates.get(
            book.get("book_id"),
            {"realized_pnl": 0.0, "unrealized_pnl": 0.0, "active_trades": 0},
        )
        book["realized_pnl"] = round(stats["realized_pnl"], 4)
        book["unrealized_pnl"] = round(stats["unrealized_pnl"], 4)
        book["active_trades"] = stats["active_trades"]

    logging.debug("Built book summary for %s books", len(books))
    return payload


async def fetch_trades(
    session,
    *,
    book_id,
    asset_class,
    status,
    symbol,
    first_only,
    page,
    limit,
):
    rows = await TradeRepository(session).get_trades_async(
        book_id=book_id,
        asset_class=asset_class,
        status=status,
        symbol=symbol,
        first_only=first_only,
        page=page,
        limit=limit,
    )
    if first_only:
        rows = [rows] if rows is not None else []
    serialized = [serialize(row) for row in rows]
    return [
        {
            **trade,
            "valuation": _cache.get_latest_valuation(trade.get("trade_id")),
        }
        for trade in serialized
    ]


async def fetch_trade_by_id(session, trade_id: str):
    trade = await TradeRepository(session).get_by_id_async(trade_id)
    if not trade:
        return None
    serialized_trade = serialize(trade)
    valuations = await ValuationRepository(session).get_valuations_by_trade_id_async(trade_id)
    return {
        "trade": serialized_trade,
        "latest_valuation": _cache.get_latest_valuation(trade_id),
        "valuation_history": [serialize(valuation) for valuation in valuations],
        "audit_logs": [],
    }


async def fetch_trade_valuations(session, trade_id: str):
    valuations = await ValuationRepository(session).get_valuations_by_trade_id_async(trade_id)
    return [serialize(valuation) for valuation in valuations]
