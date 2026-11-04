import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from shared.trading_shared.http_errors import install_error_handlers
from core import runtime
from core.audit import audit_worker
from core.curve_service import CurveService
from core.polling.quote_persistence import db_queue, db_worker
from core.polling.workers import PollWorkers
from core.quotes import QuoteService, RequestTracker
from providers.limits.market_data_providers_limits import load_providers_limits
from routes import register_routes
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import dispose_async_engine
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging

configure_logging("market-data-service-integration")

audit_logger = AuditLogger("market-data-service-integration")


# Async: awaits the quote poll loop, which awaits provider fetches and asyncio.sleep.
async def quote_poll_worker() -> None:
    await runtime.get_poll_workers().quote_loop()


# Async: awaits the curve poll loop, which awaits provider fetches and asyncio.sleep.
async def curve_poll_worker() -> None:
    await runtime.get_poll_workers().curve_loop()


# Async: awaits the curve persistence queue, then AsyncSession.commit.
async def curve_db_worker() -> None:
    await runtime.get_poll_workers().persist_loop()


def _worker_table():
    return (
        ("quote-db-worker", db_worker, "Database Worker started", "Database Worker stopped"),
        ("fetch-audit-worker", audit_worker, "Fetch audit worker started", "Fetch audit worker stopped"),
        ("quota-poll-worker", quote_poll_worker, "Quota poll worker started", "Quota poll worker stopped"),
        ("curve-poll-worker", curve_poll_worker, "Curve poll worker started", "Curve poll worker stopped"),
        (
            "curve-db-worker",
            curve_db_worker,
            "Curve persistence worker started",
            "Curve persistence worker stopped",
        ),
    )


# Async: awaits provider-limit load and request-tracker hydration.
async def startup() -> None:
    runtime.open_http()
    limits = await load_providers_limits()
    if not limits:
        logging.warning(
            "No provider limits found in DB; fetches will be denied as unknown_provider"
        )
    else:
        logging.info("Loaded provider limits from DB: %s", limits)
    tracker = await RequestTracker.from_db(limits)
    runtime.quote_service = QuoteService.create(tracker, db_queue)
    runtime.curve_service = CurveService.create()
    runtime.poll_workers = PollWorkers(runtime.quote_service, runtime.curve_service)


# Async: awaits startup, audit inserts, worker cancellation, AsyncClient.aclose, and engine dispose.
@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = []
    workers = ()
    try:
        await audit_logger.ainfo(
            EventType.WORKER_STARTED,
            "Market Data Service Integration started",
            entity_type="Service",
            correlation_id="",
        )
        await startup()
        workers = _worker_table()
        for name, worker, started, _stopped in workers:
            await audit_logger.ainfo(
                EventType.WORKER_STARTED,
                started,
                entity_type="Service",
                correlation_id="",
            )
            tasks.append(asyncio.create_task(worker(), name=name))
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        for _name, _worker, _started, stopped in reversed(workers):
            await audit_logger.ainfo(
                EventType.WORKER_STOPPED,
                stopped,
                entity_type="Service",
                correlation_id="",
            )
        if workers:
            await audit_logger.ainfo(
                EventType.WORKER_STOPPED,
                "Poll workers stopped",
                entity_type="Service",
                correlation_id="",
            )
            await audit_logger.ainfo(
                EventType.WORKER_STOPPED,
                "Market Data Service Integration stopped",
                entity_type="Service",
                correlation_id="",
            )
        await runtime.close_http()
        await dispose_async_engine()


app = FastAPI(lifespan=lifespan)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Market Data Service Integration...")
    uvicorn.run("app:app", host="0.0.0.0", port=8005, workers=1)
