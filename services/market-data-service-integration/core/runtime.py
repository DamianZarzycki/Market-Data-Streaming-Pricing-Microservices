"""Process-wide client and the services created during lifespan."""

import httpx2

from shared.trading_shared.env import env_float

http_client: httpx2.AsyncClient | None = None

quote_service = None
curve_service = None
poll_workers = None


def provider_timeout_seconds() -> float:
    return max(env_float("PROVIDER_FETCH_TIMEOUT_SECONDS", 10.0), 0.1)


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


def get_quote_service():
    if quote_service is None:
        raise RuntimeError("QuoteService not initialized")
    return quote_service


def get_curve_service():
    if curve_service is None:
        raise RuntimeError("CurveService not initialized")
    return curve_service


def get_poll_workers():
    if poll_workers is None:
        raise RuntimeError("Poll workers not initialized")
    return poll_workers
