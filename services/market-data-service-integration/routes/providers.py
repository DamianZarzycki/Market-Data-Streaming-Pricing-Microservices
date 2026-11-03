from fastapi import FastAPI, status

from api.schemas import ProviderProbe, ProvidersHealthResponse, ProvidersResponse
from core import state
from core.provider_health import list_provider_names, probe_quote_providers, provider_health


def register(app: FastAPI) -> None:
    # Sync handler: returns provider names already held in memory. There is no
    # non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/providers",
        response_model=ProvidersResponse,
        status_code=status.HTTP_200_OK,
    )
    def providers() -> dict:
        return list_provider_names(state.get_quote_service(), state.get_curve_service())

    # Async handler: awaits asyncio.gather of the live quote probes.
    @app.get(
        "/providers/health",
        response_model=ProvidersHealthResponse,
        status_code=status.HTTP_200_OK,
    )
    async def providers_health() -> dict:
        return await probe_quote_providers(state.get_quote_service())

    # Async handler: awaits AsyncClient.get for that provider.
    @app.get(
        "/providers/{provider}/health",
        response_model=ProviderProbe,
        status_code=status.HTTP_200_OK,
    )
    async def one_provider_health(provider: str) -> dict:
        return await provider_health(
            state.get_quote_service(),
            state.get_curve_service(),
            provider,
        )
