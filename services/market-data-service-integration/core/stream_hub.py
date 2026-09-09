import json
import logging
import os
import time
from collections.abc import Callable

DEFAULT_SYMBOL = "AAPL"
MAX_SYMBOLS = 3


def parse_symbols(raw: str | None) -> list[str]:
    """Parse ?symbol=AAPL,MSFT. Empty/missing falls back to AAPL; cap the list."""
    parts = [part.strip().upper() for part in (raw or "").split(",") if part.strip()]
    return (parts or [DEFAULT_SYMBOL])[:MAX_SYMBOLS]


def poll_interval() -> float:
    try:
        return max(float(os.getenv("MARKET_DATA_POLL_INTERVAL_SECONDS", "30")), 1.0)
    except ValueError:
        return 15.0


def iter_quote_events(
    providers: list[str],
    symbols: list[str],
    fetch_all: Callable[[list[tuple[str, str]]], list[tuple[str, dict]]],
):
    yield ": connected\n\n"
    interval = poll_interval()
    logging.info(
        "Streaming quotes providers=%s symbols=%s interval=%ss",
        providers,
        symbols,
        interval,
    )
    while True:
        for symbol in symbols:
            pairs = [(provider, symbol) for provider in providers]
            for event, payload in fetch_all(pairs):
                if event == "error":
                    logging.warning(
                        "Skipping error from %s for %s: %s",
                        payload.get("provider"),
                        payload.get("symbol"),
                        payload.get("error", "")[:120],
                    )
                    continue
                yield f"event: {event}\ndata: {json.dumps(payload)}\n\n"
            time.sleep(interval)
