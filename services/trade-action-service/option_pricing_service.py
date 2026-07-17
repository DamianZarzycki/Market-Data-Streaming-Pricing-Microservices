from math import erf, sqrt, log, exp

def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))

def calculate_european_call_option_price(option_details: dict) -> float:
    """Calculate the price of a European call option."""
    volatility = option_details["volatility"]
    spot = option_details["spot"]
    strike = option_details["strike"]
    maturity_years = option_details["maturity_years"]
    risk_free_rate = 0.05
    d1 = log(spot / strike) + (risk_free_rate + 0.5 * volatility ** 2) * maturity_years
    d2 = d1 - volatility * sqrt(maturity_years)

    return spot * normal_cdf(d1) - strike * exp(-risk_free_rate * maturity_years) * normal_cdf(d2)


def calculate_european_put_option_price(option_details: dict) -> float:
    volatility = option_details["volatility"]
    spot = option_details["spot"]
    strike = option_details["strike"]
    maturity_years = option_details["maturity_years"]
    risk_free_rate = 0.05
    d1 = log(spot / strike) + (risk_free_rate + 0.5 * volatility ** 2) * maturity_years
    d2 = d1 - volatility * sqrt(maturity_years)

    return strike * exp(-risk_free_rate * maturity_years) * normal_cdf(-d2) - spot * normal_cdf(-d1)
