from datetime import datetime, timezone

DEFAULT_POLL_SYMBOL = "AAPL"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
