from fastapi import FastAPI

from generator.lifecycle import snapshot_status
from shared.trading_shared.enums import ServiceStatus


def register(app: FastAPI) -> None:
    # Async: a dict copy only. Stays on the event loop when the thread pool is busy.
    @app.get("/health")
    async def health():
        return {
            "service": "trade-generation-service",
            "status": ServiceStatus.UP.value,
        }

    # Async: counters and the interval are read from memory. No I/O.
    @app.get("/status")
    async def status():
        return snapshot_status()
