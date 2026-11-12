from fastapi import FastAPI, status

import metrics
import worker
from api.schemas import HealthResponse, StatusResponse


def register(app: FastAPI) -> None:
    # Async handler: the worker flag is an in-memory read under a short lock, so
    # this route is async and runs on the event loop.
    @app.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
    async def health() -> HealthResponse:
        return HealthResponse.model_validate(metrics.health_payload())

    # Async handler: counters and queue depth are an in-memory read under a
    # short lock, so this route is async and runs on the event loop.
    @app.get("/status", response_model=StatusResponse, status_code=status.HTTP_200_OK)
    async def read_status() -> StatusResponse:
        return StatusResponse.model_validate(metrics.build_status(worker.trade_queue))
