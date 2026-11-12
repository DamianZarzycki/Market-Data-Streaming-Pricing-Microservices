import logging

import httpx

from shared.trading_shared.http_errors import ApiError
from shared.trading_shared.env import env, env_float, env_int

from .config import get_config
from .lifecycle import pause_for_retry, record_send_failure

_CLIENT: httpx.AsyncClient | None = None

_BOOKS_URL = f"{env('BOOKS_SERVICE_URL').rstrip('/')}/books"
_SYMBOLS_URL = f"{env('MARKET_DATA_SERVICE_URL').rstrip('/')}/symbols"
_SNAPSHOT_URL = f"{env('MARKET_DATA_SERVICE_URL').rstrip('/')}/snapshot"
_INTEGRATION_SYMBOLS_URL = (
    f"{env('MARKET_DATA_INTEGRATION_URL').rstrip('/')}/market-data/symbols"
)
_INTEGRATION_SNAPSHOT_URL = (
    f"{env('MARKET_DATA_INTEGRATION_URL').rstrip('/')}/market-data/snapshot"
)
_TRADE_ACTION_URL = env("TRADE_ACTION_SERVICE_URL").rstrip("/")
_PROVIDER_MODE_URL = f"{_TRADE_ACTION_URL}/provider-mode"
_UNREACHABLE = "Failed to reach trade-action-service"


_HTTP_TIMEOUT_SECONDS = env_float("TRADE_GENERATION_HTTP_TIMEOUT_SECONDS", 5.0, minimum=0.1)
_HTTP_CONNECT_TIMEOUT_SECONDS = env_float(
    "TRADE_GENERATION_HTTP_CONNECT_TIMEOUT_SECONDS",
    2.0,
    minimum=0.1,
)
_FETCH_TIMEOUT_SECONDS = env_float("TRADE_GENERATION_FETCH_TIMEOUT_SECONDS", 2.0, minimum=0.1)
_PROVIDER_MODE_TIMEOUT_SECONDS = env_float(
    "TRADE_GENERATION_PROVIDER_MODE_TIMEOUT_SECONDS",
    5.0,
    minimum=0.1,
)
_HTTP_MAX_CONNECTIONS = env_int("TRADE_GENERATION_HTTP_MAX_CONNECTIONS", 10)


def _build_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(_HTTP_TIMEOUT_SECONDS, connect=_HTTP_CONNECT_TIMEOUT_SECONDS),
        limits=httpx.Limits(
            max_connections=_HTTP_MAX_CONNECTIONS,
            max_keepalive_connections=min(5, _HTTP_MAX_CONNECTIONS),
        ),
    )


async def open_http() -> None:
    """Open the shared client. Lifespan calls this before serving requests."""
    global _CLIENT
    if _CLIENT is None or _CLIENT.is_closed:
        _CLIENT = _build_client()


async def shutdown_http() -> None:
    """Close the shared client. Call this after the worker task has finished."""
    global _CLIENT
    if _CLIENT is not None:
        await _CLIENT.aclose()
        _CLIENT = None


def _client() -> httpx.AsyncClient:
    if _CLIENT is None or _CLIENT.is_closed:
        raise RuntimeError("HTTP client is not open")
    return _CLIENT


async def fetch_json_once(url, timeout=None):
    """One-shot HTTP JSON fetch. No retry — the generator already loops."""
    response = await _client().get(
        url,
        timeout=_FETCH_TIMEOUT_SECONDS if timeout is None else timeout,
    )
    try:
        response.raise_for_status()
        return response.json()
    finally:
        await response.aclose()


def _trade_action_url(payload):
    if isinstance(payload, list):
        return (
            f"{_TRADE_ACTION_URL}/trade-actions/batch",
            f"batch of {len(payload)} orders",
        )
    return f"{_TRADE_ACTION_URL}/trade-actions", "single order"


def _ids(payload):
    items = payload if isinstance(payload, list) else [payload]
    dicts = [item for item in items if isinstance(item, dict)]
    if not dicts:
        return None, None, None
    client_request_id = ",".join(
        str(item.get("client_request_id"))
        for item in dicts
        if item.get("client_request_id")
    )
    return (
        dicts[0].get("action_type"),
        client_request_id or None,
        dicts[0].get("symbol"),
    )


def _json_body(response):
    try:
        return response.json()
    except ValueError as exc:
        raise ApiError(502, _UNREACHABLE) from exc


def _message_from_response(response) -> str:
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    if isinstance(parsed, dict):
        message = parsed.get("error") or parsed.get("message")
        if message:
            return str(message)
    return response.text or f"trade-action-service returned HTTP {response.status_code}"


async def send_to_trade_action_service(payload, *, wake_on_stop: bool = False) -> bool:
    """POST the same body until trade-action accepts it or the failure is permanent.

    400 is not retried. 503 and connection errors retry up to three times with
    the same `client_request_id`. The worker passes `wake_on_stop=True` so
    `/stop` aborts the backoff. Route calls sleep the full delay.
    """
    url, label = _trade_action_url(payload)
    action_type, client_request_id, symbol = _ids(payload)
    delay_s = 0.5
    for attempt in range(1, 4):
        try:
            response = await _client().post(url, json=payload)
            try:
                if response.status_code == 503 and attempt < 3:
                    logging.warning(
                        "trade-action overloaded %s attempt=%s action_type=%s client_request_id=%s symbol=%s",
                        label,
                        attempt,
                        action_type,
                        client_request_id,
                        symbol,
                    )
                elif response.is_success:
                    logging.info(
                        "Sent %s client_request_id=%s",
                        label,
                        client_request_id,
                    )
                    return True
                else:
                    logging.error(
                        "trade-action rejected %s status=%s body=%s action_type=%s client_request_id=%s symbol=%s",
                        label,
                        response.status_code,
                        response.text,
                        action_type,
                        client_request_id,
                        symbol,
                    )
                    record_send_failure()
                    return False
            finally:
                await response.aclose()
        except httpx.TransportError:
            logging.exception(
                "trade-action unreachable %s attempt=%s action_type=%s client_request_id=%s symbol=%s",
                label,
                attempt,
                action_type,
                client_request_id,
                symbol,
            )
        if attempt == 3 or await pause_for_retry(delay_s, wake_on_stop=wake_on_stop):
            break
        delay_s *= 2
    record_send_failure()
    return False


async def set_trade_action_provider_mode(enabled):
    """Tell trade-action which OPEN validation to apply.

    Raises ApiError(502) when the call does not land. The caller keeps its
    own flag unchanged.
    """
    try:
        response = await _client().put(
            _PROVIDER_MODE_URL,
            json={"use_provider_data": bool(enabled)},
            timeout=_PROVIDER_MODE_TIMEOUT_SECONDS,
        )
    except httpx.TransportError as exc:
        logging.exception("Failed to set trade-action provider mode")
        raise ApiError(502, _UNREACHABLE) from exc
    try:
        if response.is_success:
            return _json_body(response)
        detail = _message_from_response(response)
        raise ApiError(502, detail or "trade-action-service rejected provider mode")
    finally:
        await response.aclose()


async def push_provider_mode_best_effort() -> None:
    """Push the env-seeded flag on startup. A down trade-action does not change it."""
    try:
        await set_trade_action_provider_mode(get_config()["use_provider_data"])
    except Exception:
        logging.exception("Failed to push provider mode on startup")


async def forward_supplied_trade_action(payload):
    """POST a caller-supplied intention once.

    4xx becomes 400 with trade-action's message. Connection failure and 503
    become 502. The body is not retried here: the caller still holds the
    original `client_request_id` and can resubmit it.
    """
    url, label = _trade_action_url(payload)
    action_type, client_request_id, symbol = _ids(payload)
    try:
        response = await _client().post(url, json=payload)
    except httpx.TransportError as exc:
        logging.exception(
            "Failed to forward %s action_type=%s client_request_id=%s symbol=%s",
            label,
            action_type,
            client_request_id,
            symbol,
        )
        record_send_failure()
        raise ApiError(502, _UNREACHABLE) from exc
    try:
        if response.is_success:
            logging.info(
                "Successfully forwarded %s client_request_id=%s",
                label,
                client_request_id,
            )
            return _json_body(response)
        detail = _message_from_response(response)
        logging.error(
            "trade-action-service rejected %s status=%s body=%s action_type=%s client_request_id=%s symbol=%s",
            label,
            response.status_code,
            response.text,
            action_type,
            client_request_id,
            symbol,
        )
        record_send_failure()
        if response.status_code == 503 or response.status_code >= 500:
            raise ApiError(502, _UNREACHABLE)
        raise ApiError(400, detail)
    finally:
        await response.aclose()
