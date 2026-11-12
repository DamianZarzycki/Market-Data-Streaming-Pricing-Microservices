from fastapi import FastAPI, status

import worker
from api.schemas import HealthResponse


def register(app: FastAPI) -> None:
    # Sync handler: copies in-memory counters under a short lock. There is no
    # non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/health",
        response_model=HealthResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def health() -> dict:
        return worker.health_view()
