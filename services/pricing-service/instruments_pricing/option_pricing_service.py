"""European Black-Scholes premium with zero rates and zero dividend yield.

The call site only supplies spot, strike, volatility and maturity, so r = 0
and q = 0. A non-positive maturity or volatility falls back to intrinsic value.
"""

from math import erf, log, sqrt


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _intrinsic(spot: float, strike: float, right: str) -> float:
    if right == "CALL":
        return max(spot - strike, 0.0)
    return max(strike - spot, 0.0)


def _european_price(option_details: dict, right: str) -> float:
    spot = float(option_details["spot"])
    strike = float(option_details["strike"])
    volatility = float(option_details["volatility"])
    maturity_years = float(option_details["maturity_years"])
    if spot <= 0 or strike <= 0:
        return 0.0
    if maturity_years <= 0 or volatility <= 0:
        return _intrinsic(spot, strike, right)

    sqrt_t = sqrt(maturity_years)
    d1 = (log(spot / strike) + 0.5 * volatility * volatility * maturity_years) / (
        volatility * sqrt_t
    )
    d2 = d1 - volatility * sqrt_t
    if right == "CALL":
        return spot * _norm_cdf(d1) - strike * _norm_cdf(d2)
    return strike * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def calculate_european_call_option_price(option_details: dict) -> float:
    return _european_price(option_details, "CALL")


def calculate_european_put_option_price(option_details: dict) -> float:
    return _european_price(option_details, "PUT")
