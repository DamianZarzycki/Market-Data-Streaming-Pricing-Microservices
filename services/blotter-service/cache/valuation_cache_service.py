valuation_cache = {}

class ValuationCacheService:
    def __init__(self):
        self.valuation_cache = {}

    def clear_cache(self):
        self.valuation_cache.clear()

    def update_cache(self, trade_id, valuation_data):
        if trade_id not in self.valuation_cache:
            self.valuation_cache[trade_id] = []
        self.valuation_cache[trade_id].append(valuation_data)

    def get_valuation_from_cache(self, trade_id):
        return self.valuation_cache.get(trade_id, [])

    def get_latest_valuation(self, trade_id):
        history = self.valuation_cache.get(trade_id)
        return history[-1] if history else None
