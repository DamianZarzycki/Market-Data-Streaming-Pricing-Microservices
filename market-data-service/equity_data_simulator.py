import random

class EquityDataSimulator:
    def __init__(self, initial_price=100.0, volatility=0.05):
        self.__current_price = initial_price
        self.__volatility = volatility

    def generate_tick(self):
        price_movement = random.normalvariate(0, self.__volatility)
        self.__current_price += price_movement
        spread = random.uniform(0.02, 0.2)

        bid = self.__current_price - (spread / 2)
        ask = self.__current_price + (spread / 2)
        last = random.uniform(bid, ask)

        return {
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "spread": round(spread, 4),
            "last": round(last, 4)
        }