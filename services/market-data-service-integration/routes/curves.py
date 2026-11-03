from fastapi import FastAPI, Query, status

from api.schemas import CurveResponse, CurvesResponse, QuotesResponse
from core import state


def register(app: FastAPI) -> None:
    # Async handler: awaits the NBP curve read.
    @app.get(
        "/market-data/fx-quotes",
        response_model=QuotesResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def fx_quotes(symbol: str = Query("")) -> dict:
        return await state.get_curve_service().fx_quotes(symbol)

    # Async handler: awaits the FRED or ECB curve read.
    @app.get(
        "/market-data/curve-quotes",
        response_model=QuotesResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def curve_quotes(symbol: str = Query(""), asset_class: str = Query("")) -> dict:
        return await state.get_curve_service().curve_quotes(symbol, asset_class)

    # Async handler: awaits market_data.get_latest_curves_async.
    @app.get(
        "/curves",
        response_model=CurvesResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def curves() -> dict:
        return await state.get_curve_service().list_persisted()

    # Async handler: awaits market_data.get_latest_for_provider_async.
    @app.get(
        "/curves/{provider}",
        response_model=CurveResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def curve_by_provider(provider: str) -> dict:
        return await state.get_curve_service().curve_for_provider(provider)
