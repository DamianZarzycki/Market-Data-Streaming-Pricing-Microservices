import logging
import sys
from collections.abc import AsyncIterator

from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession

from api.errors import SUMMARY_FAILED, ApiError
from shared.trading_shared.db import get_async_db
import trades_service


async def get_summary_db() -> AsyncIterator[tuple[dict, AsyncSession]]:
    """Fetch books-service before checking out a connection.

    The catalogue call has a 2 second timeout. Holding a pooled connection
    across that wait would pin a backend for a neighbour that may be down.
    The request exception is thrown into `get_async_db` so a failed request
    rolls back instead of committing.
    """
    try:
        payload = await trades_service.fetch_books_payload()
    except Exception:
        logging.exception("Failed to fetch book summary")
        raise ApiError(status.HTTP_500_INTERNAL_SERVER_ERROR, SUMMARY_FAILED) from None
    session_gen = get_async_db()
    session = await anext(session_gen)
    try:
        yield payload, session
    except Exception:
        await session_gen.athrow(sys.exception())
    else:
        try:
            await anext(session_gen)
        except StopAsyncIteration:
            pass
