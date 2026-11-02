import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from shared.trading_shared.db import dispose_async_engine
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging

import worker
from api.errors import install_error_handlers
from routes import register_routes

configure_logging("market-data-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    initial_ticks = worker.generate_market_tick()
    worker.update_snapshot(initial_ticks)
    await worker.audit_logger.ainfo(
        EventType.WORKER_STARTED,
        "Market Worker started",
        entity_type="Service",
        correlation_id="",
    )
    market_task = asyncio.create_task(worker.market_worker(), name="market-worker")
    await worker.audit_logger.ainfo(
        EventType.WORKER_STARTED,
        "Metric Worker started",
        entity_type="Service",
        correlation_id="",
    )
    metric_task = asyncio.create_task(worker.metric_worker(), name="metric-worker")
    await worker.audit_logger.ainfo(
        EventType.WORKER_STARTED,
        "Database Worker started",
        entity_type="Service",
        correlation_id="",
    )
    db_task = asyncio.create_task(worker.db_worker(), name="db-worker")
    tasks = (market_task, metric_task, db_task)
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
        await worker.audit_logger.ainfo(
            EventType.WORKER_STOPPED,
            "Market Worker stopped",
            entity_type="Service",
            correlation_id="",
        )
        await worker.audit_logger.ainfo(
            EventType.WORKER_STOPPED,
            "Metric Worker stopped",
            entity_type="Service",
            correlation_id="",
        )
        await worker.audit_logger.ainfo(
            EventType.WORKER_STOPPED,
            "Database Worker stopped",
            entity_type="Service",
            correlation_id="",
        )
        await dispose_async_engine()


app = FastAPI(lifespan=lifespan)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Market Data Service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8001, workers=1)
