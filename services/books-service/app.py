import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.audit_retention import start_retention, stop_retention
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging

from api.body_limit import MaxBodySizeMiddleware
from api.errors import install_error_handlers
from routes import register_routes

configure_logging("books-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    audit_logger = AuditLogger("books-service")
    await asyncio.to_thread(
        audit_logger.info,
        EventType.WORKER_STARTED,
        "Books service started",
        entity_type="Service",
    )
    start_retention()
    try:
        yield
    finally:
        await asyncio.to_thread(stop_retention)
        await asyncio.to_thread(
            audit_logger.info,
            EventType.WORKER_STOPPED,
            "Books service stopped",
            entity_type="Service",
        )


app = FastAPI(lifespan=lifespan)
app.add_middleware(MaxBodySizeMiddleware)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Books service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8004, workers=1)
