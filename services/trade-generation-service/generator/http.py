import json
import logging
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from shared.trading_shared.decorators import retry

_HTTP_POOL = ThreadPoolExecutor(max_workers=3, thread_name_prefix="trade-gen-http")

_BOOKS_URL = "http://books-service:8004/books"
_SYMBOLS_URL = "http://market-data-service:8001/symbols"
_SNAPSHOT_URL = "http://market-data-service:8001/snapshot"


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


def send_to_trade_action_service(payload):
    if isinstance(payload, list):
        url = "http://trade-action-service:8080/trade-actions/batch"
        message = f"batch of {len(payload)} orders"
    else:
        url = "http://trade-action-service:8080/trade-actions"
        message = "single order"

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
