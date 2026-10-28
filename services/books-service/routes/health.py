from fastapi import FastAPI, status

from api.schemas import HealthResponse
from shared.trading_shared.enums import ServiceStatus


def register(app: FastAPI) -> None:
    @app.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
    async def health() -> HealthResponse:
        return HealthResponse(
            service="books-service",
            status=ServiceStatus.UP.value,
        )
