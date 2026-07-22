"""Uproszczona wycena IRS (Interest Rate Swap).

Model (zgodny z założeniami zadania):
    fixed leg PV     = suma zdyskontowanych płatności: fixed_rate * notional * accrual * DF(t_i)
    floating leg PV  = notional * (1 - DF(maturity))
    PV IRS           = receive leg PV - pay leg PV

Discount factory pochodzą z krzywej zero rates (YIELD_CURVE) zapisanej w
MarketDataCurve i streamowanej per waluta. Krzywa jest reprezentowana jako
zestaw tenorów (np. ["1M", "3M", "1Y", "5Y"]) oraz odpowiadających im stóp zero.
DF liczony jest w locie: DF(t) = 1 / (1 + z(t))**t, gdzie z(t) to zinterpolowana
liniowo stopa zero dla czasu t.
"""

_TENOR_UNIT_YEARS = {"D": 1.0 / 365.0, "W": 7.0 / 365.0, "M": 1.0 / 12.0, "Y": 1.0}


def parse_tenor_to_years(tenor: str) -> float:
    """Zamienia etykietę tenoru (np. '3M', '1Y', '10D') na ułamek roku."""
    tenor = str(tenor).strip().upper()
    unit = tenor[-1]
    if unit not in _TENOR_UNIT_YEARS:
        raise ValueError(f"Unsupported tenor unit in '{tenor}'")
    return float(tenor[:-1]) * _TENOR_UNIT_YEARS[unit]


def interpolate_zero_rate(tenor_points_years, rates, target_years: float) -> float:
    """Liniowa interpolacja stopy zero dla czasu target_years (płaska ekstrapolacja na końcach)."""
    if not tenor_points_years or len(tenor_points_years) != len(rates):
        raise ValueError("Curve tenors and rates must be non-empty and of equal length")

    curve_points = sorted(zip(tenor_points_years, rates))
    curve_times = [point[0] for point in curve_points]
    curve_rates = [point[1] for point in curve_points]

    if target_years <= curve_times[0]:
        return curve_rates[0]
    if target_years >= curve_times[-1]:
        return curve_rates[-1]

    for index in range(1, len(curve_times)):
        if target_years <= curve_times[index]:
            # searching for nearest left and right neighbor times
            lower_time, upper_time = curve_times[index - 1], curve_times[index]
            lower_rate, upper_rate = curve_rates[index - 1], curve_rates[index]
            
            interpolation_weight = (target_years - lower_time) / (upper_time - lower_time)
            return lower_rate + interpolation_weight * (upper_rate - lower_rate)
    return curve_rates[-1]


def discount_factor(zero_rate: float, t: float) -> float:
    """Discount factor przy kapitalizacji rocznej: DF(t) = 1 / (1 + z)^t."""
    return 1.0 / ((1.0 + zero_rate) ** t)


def prepare_curve(curve: dict):
    """Rozpakowuje krzywą do (tenory_w_latach, stopy_zero)."""
    rates = [float(r) for r in curve["rates"]]
    tenor_points = [parse_tenor_to_years(t) for t in curve["tenors"]]
    return tenor_points, rates


def fixed_leg_pv(
    tenor_points,
    rates,
    notional: float,
    fixed_rate: float,
    maturity_years: float,
    payments_per_year: int,
) -> float:
    """PV nogi stałej: suma zdyskontowanych płatności fixed_rate * notional * accrual."""
    payments_per_year = int(payments_per_year)
    accrual = 1.0 / payments_per_year
    number_of_payments = int(round(float(maturity_years) * payments_per_year))

    pv = 0.0
    for i in range(1, number_of_payments + 1):
        t_i = i * accrual
        z_i = interpolate_zero_rate(tenor_points, rates, t_i)
        pv += notional * fixed_rate * accrual * discount_factor(z_i, t_i)
    return pv


def floating_leg_pv(tenor_points, rates, notional: float, maturity_years: float) -> float:
    """Uproszczona PV nogi zmiennej: notional * (1 - DF(maturity))."""
    maturity_years = float(maturity_years)
    z_maturity = interpolate_zero_rate(tenor_points, rates, maturity_years)
    return notional * (1.0 - discount_factor(z_maturity, maturity_years))


def assign_legs(direction: str, fixed_pv: float, floating_pv: float):
    """Zwraca (receive_leg_pv, pay_leg_pv) zależnie od kierunku swapa."""
    if direction == "PAY_FIXED_RECEIVE_FLOAT":
        return floating_pv, fixed_pv
    if direction == "RECEIVE_FIXED_PAY_FLOAT":
        return fixed_pv, floating_pv
    raise ValueError(f"Unknown IRS direction: {direction}")


def price_irs(
    curve: dict,
    notional: float,
    fixed_rate: float,
    maturity_years: float,
    payments_per_year: int,
    direction: str,
) -> dict:
    """Zwraca PV IRS oraz PV poszczególnych nóg (PV = receive leg PV - pay leg PV).

    `curve` to słownik: {"tenors": [...], "rates": [...]} ze stopami zero.
    `direction` to jedna z wartości IRSDirection:
        PAY_FIXED_RECEIVE_FLOAT / RECEIVE_FIXED_PAY_FLOAT.
    """
    tenor_points, rates = prepare_curve(curve)

    fixed_pv = fixed_leg_pv(
        tenor_points, rates, notional, fixed_rate, maturity_years, payments_per_year
    )
    floating_pv = floating_leg_pv(tenor_points, rates, notional, maturity_years)

    receive_leg_pv, pay_leg_pv = assign_legs(direction, fixed_pv, floating_pv)

    return {
        "pv": receive_leg_pv - pay_leg_pv,
        "fixed_leg_pv": fixed_pv,
        "floating_leg_pv": floating_pv,
        "receive_leg_pv": receive_leg_pv,
        "pay_leg_pv": pay_leg_pv,
    }
