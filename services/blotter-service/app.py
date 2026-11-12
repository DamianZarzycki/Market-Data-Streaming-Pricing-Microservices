from fastapi import FastAPI
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import dispose_async_engine
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging
from contextlib import asynccontextmanager
from api.errors import install_error_handlers
from routes import register_routes

import asyncio
import logging
import trades_service
import worker
import uvicorn


configure_logging("blotter-service")


# Async: awaits audit inserts, worker cancellation, AsyncClient.aclose, and engine dispose.
@asynccontextmanager
async def lifespan(app: FastAPI):
    audit_logger = AuditLogger("blotter-service")
    await audit_logger.ainfo(
        EventType.WORKER_STARTED,
        "Valuation worker started",
        entity_type="Service",
        correlation_id="",
    )
    trades_service.open_http()
    worker.open_http()
    task = asyncio.create_task(worker.valuation_worker(), name="valuation-worker")
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await worker.close_http()
        await trades_service.close_http()
        await audit_logger.ainfo(
            EventType.WORKER_STOPPED,
            "Valuation worker stopped",
            entity_type="Service",
            correlation_id="",
        )
        await dispose_async_engine()


app = FastAPI(lifespan=lifespan)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Blotter service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8006, workers=1)
