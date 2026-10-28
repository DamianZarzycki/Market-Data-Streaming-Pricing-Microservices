from fastapi import FastAPI, status

from api.schemas import HealthResponse
from cache.valuation_cache_service import ValuationCacheService
from shared.trading_shared.enums import ServiceStatus

_cache = ValuationCacheService()


def register(app: FastAPI) -> None:
    @app.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
    async def health() -> HealthResponse:
        trade_count, entry_count = _cache.counts()
        return HealthResponse(
            service="blotter-service",
            status=ServiceStatus.UP.value,
            cache_trade_count=trade_count,
            cache_entry_count=entry_count,
        )
