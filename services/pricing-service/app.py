import asyncio
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from shared.trading_shared.http_errors import install_error_handlers
from routes import register_routes
from shared.trading_shared.db import dispose_async_engine
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging

import worker

configure_logging("pricing-service")

_WORKER_AUDIT = (
    ("pricing-worker", "pricing_worker", "Market thread started", "Market thread stopped"),
    ("metrics-worker", "metrics_worker", "Metrics thread started", "Metrics thread stopped"),
    (
        "cache-refresh-worker",
        "cache_refresh_worker",
        "Cache refresh thread started",
        "Cache refresh thread stopped",
    ),
    (
        "valuation-worker",
        "valuation_worker",
        "Valuation thread started",
        "Valuation thread stopped",
    ),
    (
        "provider-pricing-worker",
        "provider_pricing_worker",
        "Provider pricing thread started",
        "Provider pricing thread stopped",
    ),
    (
        "curve-pricing-worker",
        "curve_pricing_worker",
        "Curve pricing thread started",
        "Curve pricing thread stopped",
    ),
)


# Async: awaits audit inserts, worker cancellation, AsyncClient.aclose, and engine dispose.
@asynccontextmanager
async def lifespan(app: FastAPI):
    worker.open_http()
    tasks = []
    for name, attr, started, _stopped in _WORKER_AUDIT:
        await worker.audit_logger.ainfo(
            EventType.WORKER_STARTED,
            started,
            entity_type="Service",
            correlation_id="",
        )
        tasks.append(asyncio.create_task(getattr(worker, attr)(), name=name))
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        for _name, _attr, _started, stopped in _WORKER_AUDIT:
            await worker.audit_logger.ainfo(
                EventType.WORKER_STOPPED,
                stopped,
                entity_type="Service",
                correlation_id="",
            )
        await worker.close_http()
        await dispose_async_engine()


app = FastAPI(lifespan=lifespan)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Pricing Service...")
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.environ.get("PRICING_PORT", "8002")),
        workers=1,
    )
