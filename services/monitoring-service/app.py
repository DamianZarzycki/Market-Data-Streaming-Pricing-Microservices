import asyncio
import logging
from contextlib import asynccontextmanager

import httpx2
import uvicorn
from fastapi import FastAPI

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging

import worker
from routes import register_routes

configure_logging("monitoring-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    audit_logger = AuditLogger("monitoring-service")
    await asyncio.to_thread(
        audit_logger.info,
        EventType.WORKER_STARTED,
        "Monitoring service started",
        entity_type="Service",
        correlation_id=None,
    )
    client = httpx2.AsyncClient(timeout=2.0)
    task = asyncio.create_task(worker.monitoring_worker(client))
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("Monitoring worker stopped with an error")
        await client.aclose()
        await asyncio.to_thread(
            audit_logger.info,
            EventType.WORKER_STOPPED,
            "Monitoring Worker stopped",
            entity_type="Service",
            correlation_id=None,
        )


app = FastAPI(lifespan=lifespan)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Monitoring service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8003, workers=1)
