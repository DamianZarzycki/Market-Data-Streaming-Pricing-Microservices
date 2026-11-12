from fastapi import FastAPI, status

from actions.provider_mode import set_use_provider_data
from api.schemas import ProviderModeResponse, ProviderModeUpdate


def register(app: FastAPI) -> None:
    # Async handler: the provider flag is an in-memory write under a short lock,
    # so this route is async and runs on the event loop.
    @app.put(
        "/provider-mode",
        response_model=ProviderModeResponse,
        status_code=status.HTTP_200_OK,
    )
    async def put_provider_mode(payload: ProviderModeUpdate) -> ProviderModeResponse:
        enabled = set_use_provider_data(payload.use_provider_data)
        return ProviderModeResponse(use_provider_data=enabled)
