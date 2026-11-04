import threading

from shared.trading_shared.env import env_int

valuation_cache = {}
cache_lock = threading.Lock()

# Keep only recent live ticks per trade so SSE flood cannot grow memory unboundedly.
MAX_VALUATIONS_PER_TRADE = env_int("BLOTTER_MAX_VALUATIONS_PER_TRADE", 20)


def _as_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class ValuationCacheService:
    """Uses the module-level valuation_cache so worker writes and API reads share state."""

    def clear_cache(self):
        with cache_lock:
            valuation_cache.clear()

    def update_cache(self, trade_id, valuation_data):
        with cache_lock:
            history = valuation_cache.get(trade_id)
            if history is None:
                history = []
                valuation_cache[trade_id] = history
            history.append(valuation_data)
            if len(history) > MAX_VALUATIONS_PER_TRADE:
                del history[:-MAX_VALUATIONS_PER_TRADE]

    def get_valuation_from_cache(self, trade_id):
        with cache_lock:
            history = valuation_cache.get(trade_id)
            return list(history) if history else []

    def get_latest_valuation(self, trade_id):
        with cache_lock:
            history = valuation_cache.get(trade_id)
            if not history:
                return None
            latest = history[-1]
            return dict(latest) if isinstance(latest, dict) else latest

    def counts(self):
        with cache_lock:
            return (
                len(valuation_cache),
                sum(len(history) for history in valuation_cache.values()),
            )

    def sum_latest_pnl(self, book_ids):
        totals = {}
        with cache_lock:
            for history in valuation_cache.values():
                if not history:
                    continue
                latest = history[-1]
                if not isinstance(latest, dict):
                    continue
                book_id = latest.get("book_id")
                if book_id not in book_ids:
                    continue
                bucket = totals.get(book_id)
                if bucket is None:
                    bucket = {"realized_pnl": 0.0, "unrealized_pnl": 0.0}
                    totals[book_id] = bucket
                bucket["realized_pnl"] += _as_float(latest.get("realized_pnl"))
                bucket["unrealized_pnl"] += _as_float(latest.get("unrealized_pnl"))
        return totals
