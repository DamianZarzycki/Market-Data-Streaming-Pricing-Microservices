from fastapi import FastAPI, status

import calculation_service
from api.schemas import ValuationMapResponse, ValuationResponse


def register(app: FastAPI) -> None:
    # Sync handler: copies the in-memory valuation map under a short lock.
    # There is no non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/valuations",
        response_model=ValuationMapResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def valuations() -> ValuationMapResponse:
        return ValuationMapResponse(calculation_service.valuation_feed.valuations_view())

    # Sync handler: one dict lookup under a short lock. There is no
    # non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/valuations/{trade_id}",
        response_model=ValuationResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def valuations_by_trade_id(trade_id: str) -> ValuationResponse:
        return ValuationResponse.model_validate(
            calculation_service.valuation_feed.valuation_for(trade_id)
        )
