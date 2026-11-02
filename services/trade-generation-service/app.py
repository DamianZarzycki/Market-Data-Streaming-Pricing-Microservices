import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from api.body_limit import MaxBodySizeMiddleware
from api.errors import install_error_handlers
from generator.config import get_config
from generator.db import dispose as dispose_engine
from generator.http import open_http, push_provider_mode_best_effort, shutdown_http
from generator.lifecycle import stop_generator
from routes import register_routes
from shared.trading_shared.logging_config import configure_logging

configure_logging("trade-generation-service")
logging.info("Runtime generator config: %s", get_config())


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One process only. Config, the worker task, and the counters are in memory.
    # The worker itself stays stopped until POST /start.
    await open_http()
    await push_provider_mode_best_effort()
    try:
        yield
    finally:
        await stop_generator()
        await shutdown_http()
        await dispose_engine()


app = FastAPI(lifespan=lifespan)
app.add_middleware(MaxBodySizeMiddleware)
install_error_handlers(app)
register_routes(app)


if __name__ == "__main__":
    logging.info("Starting Trade Generation service...")
    uvicorn.run("app:app", host="0.0.0.0", port=8007, workers=1)
