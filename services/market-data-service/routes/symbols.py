from fastapi import FastAPI, status

import worker
from api.schemas import SymbolsResponse


def register(app: FastAPI) -> None:
    # Sync handler: groups the in-memory snapshot under a short lock. There is
    # no non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get("/symbols", response_model=SymbolsResponse, status_code=status.HTTP_200_OK)
    def symbols() -> SymbolsResponse:
        return SymbolsResponse(worker.symbols())
