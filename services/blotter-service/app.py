from fastapi import FastAPI
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.logging_config import configure_logging
from contextlib import asynccontextmanager
from api.errors import install_error_handlers
from routes import register_routes

import asyncio
import logging
import threading
import worker
import uvicorn



configure_logging("blotter-service")

_JOIN_TIMEOUT_SECONDS = 5


@asynccontextmanager
async def lifespan(app: FastAPI):
    audit_logger = AuditLogger("blotter-service")
    await asyncio.to_thread(
        audit_logger.info,
        EventType.WORKER_STARTED,
        "Valuation worker thread started",
        entity_type="Service",
        correlation_id="",
    )
    worker.stop_event.clear()
    thread = threading.Thread(
        target=worker.valuation_worker,
        name="valuation-worker",
        daemon=False,
    )
    thread.start()
    try:
        yield
    finally:
        worker.stop_event.set()
        worker.close_stream()
        await asyncio.to_thread(thread.join, _JOIN_TIMEOUT_SECONDS)
        await asyncio.to_thread(
            audit_logger.info,
            EventType.WORKER_STOPPED,
            "Valuation worker thread stopped",
            entity_type="Service",
            correlation_id="",
        )


app = FastAPI(lifespan=lifespan)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Blotter service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8006, workers=1)
