valuation_cache = {}

# Keep only recent live ticks per trade so SSE flood cannot grow memory unboundedly.
MAX_VALUATIONS_PER_TRADE = 20


class ValuationCacheService:
    """Uses the module-level valuation_cache so worker writes and API reads share state."""

    def clear_cache(self):
        valuation_cache.clear()

    def update_cache(self, trade_id, valuation_data):
        history = valuation_cache.get(trade_id)
        if history is None:
            history = []
            valuation_cache[trade_id] = history
        history.append(valuation_data)
        if len(history) > MAX_VALUATIONS_PER_TRADE:
            del history[:-MAX_VALUATIONS_PER_TRADE]

    def get_valuation_from_cache(self, trade_id):
        return valuation_cache.get(trade_id, [])

    def get_latest_valuation(self, trade_id):
        history = valuation_cache.get(trade_id)
        return history[-1] if history else None
