import json
import logging
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from shared.trading_shared.decorators import retry

_HTTP_POOL = ThreadPoolExecutor(max_workers=3, thread_name_prefix="trade-gen-http")

_BOOKS_URL = "http://books-service:8004/books"
_SYMBOLS_URL = "http://market-data-service:8001/symbols"
_SNAPSHOT_URL = "http://market-data-service:8001/snapshot"
_INTEGRATION_SYMBOLS_URL = (
    "http://market-data-service-integration:8005/market-data/symbols"
)
_INTEGRATION_SNAPSHOT_URL = (
    "http://market-data-service-integration:8005/market-data/snapshot"
)
_PROVIDER_MODE_URL = "http://trade-action-service:8080/provider-mode"


def fetch_json(url, method="GET", headers=None, data=None, timeout=5):
    if headers is None:
        headers = {}

    req = urllib.request.Request(url, method=method, headers=headers, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_json_once(url, method="GET", headers=None, data=None, timeout=2):
    """One-shot HTTP JSON fetch. No retry — the generator already loops (TX-2)."""
    return fetch_json(url, method=method, headers=headers, data=data, timeout=timeout)


@retry(max_attempts=3)
def fetch_json_retry(url, method="GET", headers=None, data=None, timeout=5):
    return fetch_json(url, method=method, headers=headers, data=data, timeout=timeout)


def _trade_action_url(payload):
    if isinstance(payload, list):
        return (
            "http://trade-action-service:8080/trade-actions/batch",
            f"batch of {len(payload)} orders",
        )
    return "http://trade-action-service:8080/trade-actions", "single order"


def _trade_action_error_message(exc):
    raw = exc.read().decode("utf-8", errors="replace")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        message = parsed.get("error") or parsed.get("message")
        if message:
            return str(message)
    return raw or f"trade-action-service returned HTTP {exc.code}"


def send_to_trade_action_service(payload):
    url, message = _trade_action_url(payload)
    try:
        fetch_json_retry(
            url,
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"),
        )
        logging.info(f"Successfully sent {message} to trade-action-service.")
    except Exception:
        logging.error(f"Failed to send {message} to trade-action-service.")


def set_trade_action_provider_mode(enabled):
    """Tell trade-action-service which OPEN validation to apply.

    Raises ValueError when the call does not land, so the caller can keep
    its own flag unchanged.
    """
    try:
        return fetch_json(
            _PROVIDER_MODE_URL,
            method="PUT",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"use_provider_data": bool(enabled)}).encode("utf-8"),
            timeout=5,
        )
    except urllib.error.HTTPError as exc:
        detail = _trade_action_error_message(exc)
        raise ValueError(detail or "trade-action-service rejected provider mode") from exc
    except Exception as exc:
        logging.error("Failed to set trade-action provider mode: %s", exc)
        raise ValueError("Failed to reach trade-action-service") from exc


def forward_supplied_trade_action(payload):
    """POST a caller-supplied intention once. Surfaces trade-action errors
    instead of swallowing them, so the trading ticket can show the rejection."""
    url, message = _trade_action_url(payload)
    try:
        body = fetch_json(
            url,
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"),
        )
        logging.info(f"Successfully forwarded {message} to trade-action-service.")
        return body
    except urllib.error.HTTPError as exc:
        detail = _trade_action_error_message(exc)
        logging.error(f"trade-action-service rejected {message}: {detail}")
        raise ValueError(detail) from exc
    except Exception as exc:
        logging.error(f"Failed to forward {message} to trade-action-service.")
        raise ValueError("Failed to reach trade-action-service") from exc
