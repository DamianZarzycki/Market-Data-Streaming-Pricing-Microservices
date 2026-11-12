from fastapi import FastAPI, status

import worker
from api.schemas import SnapshotResponse


def register(app: FastAPI) -> None:
    # Async handler: after the in-memory copy, awaits the audit insert on AsyncSession.
    @app.get(
        "/snapshot",
        response_model=SnapshotResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def snapshot() -> SnapshotResponse:
        return SnapshotResponse.model_validate(await worker.snapshot_view())
