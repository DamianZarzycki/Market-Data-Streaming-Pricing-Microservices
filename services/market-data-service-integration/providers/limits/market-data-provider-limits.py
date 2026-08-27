class ProvidersLimitsService:
    def __init__(self):
        self.limits = {
            "finnhub": {
                "daily": None,
                "hourly": 3600,
                "monthly": None,
            },
            "alpha_vantage": {
                "daily": 25,
                "hourly": 25,
                "monthly": 750,
            },
            "twelve_data": {
                "daily": 800,
                "hourly": 480,
                "monthly": 24000,
            },
        }