from fastapi import FastAPI, status

from api.schemas import CurveRefreshResponse, QuoteRefreshResponse
from core import state


def register(app: FastAPI) -> None:
    # Async handler: awaits the quote cycle, which awaits AsyncClient.get.
    # GET stays so the existing curl smoke tests keep working.
    @app.api_route(
        "/market-data/refresh",
        methods=["GET", "POST"],
        response_model=QuoteRefreshResponse,
        status_code=status.HTTP_200_OK,
    )
    async def refresh() -> dict:
        return await state.get_poll_workers().run_quote_cycle()

    # Async handler: awaits the curve cycle, which awaits AsyncClient.get.
    @app.api_route(
        "/curves/refresh",
        methods=["GET", "POST"],
        response_model=CurveRefreshResponse,
        status_code=status.HTTP_200_OK,
    )
    async def refresh_curves() -> dict:
        return await state.get_poll_workers().run_curve_cycle()
