from fastapi import FastAPI, status

from models import HealthResponse
from shared.trading_shared.enums import ServiceStatus


def register(app: FastAPI) -> None:
    @app.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
    async def health() -> HealthResponse:
        return HealthResponse(
            service="monitoring-service",
            status=ServiceStatus.UP.value,
        )
