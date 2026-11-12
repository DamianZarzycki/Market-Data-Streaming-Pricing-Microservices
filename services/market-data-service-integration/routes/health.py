from fastapi import FastAPI, status

from api.schemas import HealthResponse
from core.audit import health_view


def register(app: FastAPI) -> None:
    # Sync handler: copies in-memory fetch counters under a short lock. There is
    # no non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/health",
        response_model=HealthResponse,
        status_code=status.HTTP_200_OK,
    )
    def health() -> HealthResponse:
        return HealthResponse.model_validate(health_view())
