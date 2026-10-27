from fastapi import FastAPI, status

from models import StatusResponse
from state import state


def register(app: FastAPI) -> None:
    @app.get(
        "/status",
        response_model=StatusResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def service_status() -> StatusResponse:
        return state.snapshot()
