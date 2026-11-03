import os

import httpx2

http_client: httpx2.AsyncClient | None = None


def provider_timeout_seconds() -> float:
    raw = os.getenv("PROVIDER_FETCH_TIMEOUT_SECONDS", "10")
    try:
        return max(float(raw), 0.1)
    except ValueError:
        return 10.0


def open_http() -> None:
    """Build the shared client. The first request is the await, not this call."""
    global http_client
    http_client = httpx2.AsyncClient(timeout=httpx2.Timeout(provider_timeout_seconds()))


def client() -> httpx2.AsyncClient:
    if http_client is None:
        raise RuntimeError("HTTP client is not open")
    return http_client


# Async: awaits AsyncClient.aclose.
async def close_http() -> None:
    global http_client
    current = http_client
    http_client = None
    if current is not None:
        await current.aclose()
