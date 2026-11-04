import sys
from collections.abc import AsyncIterator
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from shared.trading_shared.db import get_async_db


async def get_book_db(book_id: UUID) -> AsyncIterator[AsyncSession]:
    """Open a session only after `book_id` has parsed as a UUID.

    FastAPI resolves `book_id` before entering this dependency, so a bad path
    does not check out a connection. The request exception is thrown into
    `get_async_db` so a failed request rolls back instead of committing.
    """
    session_gen = get_async_db()
    session = await anext(session_gen)
    try:
        yield session
    except Exception:
        await session_gen.athrow(sys.exception())
    else:
        try:
            await anext(session_gen)
        except StopAsyncIteration:
            pass
