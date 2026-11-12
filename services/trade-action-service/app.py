import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

import metrics
import worker
from api.errors import install_error_handlers
from routes import register_routes
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import dispose_async_engine
from shared.trading_shared.enums import EventType
from shared.trading_shared.http_errors import MaxBodySizeMiddleware
from shared.trading_shared.logging_config import configure_logging

configure_logging("trade-action-service")


# Async lifespan: FastAPI drives startup and shutdown as a coroutine. The worker
# is an asyncio task on this loop, cancelled on shutdown. Start/stop audits use
# the shared async session, the same pool as the request handler.
@asynccontextmanager
async def lifespan(app: FastAPI):
    audit_logger = AuditLogger("trade-action-service")
    await audit_logger.ainfo(
        EventType.WORKER_STARTED,
        "Trade Action service started",
        entity_type="Service",
        correlation_id=None,
    )
    # Set before the task is scheduled so a health probe during startup is UP.
    metrics.set_worker_running(True)
    task = asyncio.create_task(worker.trade_action_worker(), name="trade-action-worker")
    try:
        yield
    finally:
        # task.cancel() lands on trade_queue.get() when idle, or is shielded
        # until the in-flight handler commits.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await audit_logger.ainfo(
            EventType.WORKER_STOPPED,
            "Trade Action Worker stopped",
            entity_type="Service",
            correlation_id=None,
        )
        await dispose_async_engine()


app = FastAPI(lifespan=lifespan)
app.add_middleware(MaxBodySizeMiddleware)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Trade Action service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8080, workers=1)
