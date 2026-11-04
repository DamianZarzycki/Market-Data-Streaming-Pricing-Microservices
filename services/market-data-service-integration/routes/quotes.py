from fastapi import FastAPI, Query, status

from api.schemas import QuoteResponse, QuotesResponse, SnapshotResponse, SymbolMapResponse
from core import runtime
from core.polling import symbol_catalog
from shared.trading_shared.enums import AssetClass


def register(app: FastAPI) -> None:
    # Sync handler: reads symbol lists from the environment. There is no
    # non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/market-data/symbols",
        response_model=SymbolMapResponse,
        status_code=status.HTTP_200_OK,
    )
    def market_data_symbols() -> SymbolMapResponse:
        return SymbolMapResponse.model_validate(symbol_catalog())

    # Async handler: awaits provider_quotes.get_all_latest_async.
    @app.get(
        "/market-data/snapshot",
        response_model=SnapshotResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def market_data_snapshot() -> dict:
        return await runtime.get_quote_service().get_snapshot()

    # Async handler: awaits provider_quotes.get_latest_for_symbol_async.
    @app.get(
        "/market-data/quotes",
        response_model=QuotesResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def quotes(
        symbol: str = Query(""),
        asset_class: str = Query(AssetClass.EQUITY.value),
    ) -> dict:
        return await runtime.get_quote_service().get_quotes(symbol, asset_class)

    # Async handler: awaits provider_quotes.get_latest_async.
    @app.get(
        "/market-data/quotes/{provider}/{symbol}",
        response_model=QuoteResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def quote_by_provider(provider: str, symbol: str) -> dict:
        return await runtime.get_quote_service().get_quote(provider, symbol)
