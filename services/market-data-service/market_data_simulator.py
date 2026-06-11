import random

class MarketDataSimulator:
    def __init__(self):

        self.__acme_price = 100.0
        self.__acme_volatility = 0.05

        self.__govt_yield = 0.04
        self.__yield_volatility = 0.0005

        self.__eurusd_spot = 1.0875
        self.__fx_volatility = 0.0001

        self.__usd_curve_rates = [0.041, 0.042, 0.044, 0.047]
        self.__eur_curve_rates = [0.031, 0.032, 0.033, 0.035]
        self.__curve_volatility = 0.0002

    def generate_equity_tick(self):
        price_movement = random.normalvariate(0, self.__acme_volatility)
        self.__acme_price += price_movement
        self.__acme_price = max(0.01, self.__acme_price)

        spread = random.uniform(0.02, 0.2)
        bid = self.__acme_price - (spread / 2)
        ask = self.__acme_price + (spread / 2)
        last = random.uniform(bid, ask)

        return {"bid": round(bid, 4), "ask": round(ask, 4), "last": round(last, 4)}

    def generate_bond_tick(self):
        self.__govt_yield += random.normalvariate(0, self.__yield_volatility)
        self.__govt_yield = max(0.001, self.__govt_yield)

        return round(self.__govt_yield, 4)

    def generate_fx_tick(self):
        self.__eurusd_spot += random.normalvariate(0, self.__fx_volatility)
        return round(self.__eurusd_spot, 4)

    def generate_usd_curve_tick(self):
        self.__usd_curve_rates = [max(0.001, r + random.normalvariate(0, self.__curve_volatility)) for r in self.__usd_curve_rates]
        return [round(r, 4) for r in self.__usd_curve_rates]

    def generate_eur_curve_tick(self):
        self.__eur_curve_rates = [max(0.001, r + random.normalvariate(0, self.__curve_volatility)) for r in self.__eur_curve_rates]
        return [round(r, 4) for r in self.__eur_curve_rates]