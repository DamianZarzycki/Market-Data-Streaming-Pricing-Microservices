import logging
from datetime import datetime, timezone

import httpx2
from fastapi import status

from shared.trading_shared.http_errors import ApiError
from core.audit import CURVE_SERVICE, record_error, record_query, record_success
from core.polling import read_symbol_map
from providers.curve_providers.ecb_provider import EcbProvider
from providers.curve_providers.fred_provider import FredProvider
from providers.curve_providers.nbp_provider import NbpProvider
from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.serialization import serialize

# NBP Table A quotes most currencies as PLN per 1 unit. JPY is PLN per 100 JPY.
_NBP_UNITS = {"JPY": 100}
# Yield curves we can actually read: FRED is USD Treasuries, ECB is the EUR curve.
_YIELD_PROVIDER_BY_CURRENCY = {"USD": "FRED", "EUR": "ECB"}


def _as_utc_z(value: object) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text


def _pln_per_unit(code: str, mid: float) -> float | None:
    units = _NBP_UNITS.get(code, 1)
    if units == 0:
        return None
    per_unit = mid / units
    if per_unit == 0:
        return None
    return per_unit


def nbp_cross_rate(symbol: str, tenors: list, rates: list) -> float | None:
    """Spot for a 6-letter pair from NBP Table A mids (PLN per foreign unit).

    EURUSD = EUR / USD. USDJPY scales the JPY mid by 100 because NBP quotes
    yen per 100. A missing leg returns None.
    """
    pair = symbol.strip().upper()
    if len(pair) != 6 or not pair.isalpha():
        return None
    base, quote = pair[:3], pair[3:]
    mids: dict[str, float] = {}
    for tenor, rate in zip(tenors, rates):
        if not isinstance(tenor, str) or rate is None:
            continue
        try:
            mids[tenor.strip().upper()] = float(rate)
        except (TypeError, ValueError):
            continue

    def pln(code: str) -> float | None:
        if code == "PLN":
            return 1.0
        mid = mids.get(code)
        if mid is None:
            return None
        return _pln_per_unit(code, mid)

    base_pln = pln(base)
    quote_pln = pln(quote)
    if base_pln is None or quote_pln is None:
        return None
    return round(base_pln / quote_pln, 6)


def parse_yield_symbol(symbol: str) -> tuple[str, str, str] | None:
    """USD_10Y -> (BOND, USD, 10Y). USD_10Y_IRS -> (IRS, USD, 10Y)."""
    name = symbol.strip().upper()
    asset_class = "IRS" if name.endswith("_IRS") else "BOND"
    body = name[:-4] if asset_class == "IRS" else name
    currency, separator, tenor = body.partition("_")
    if (
        not separator
        or not currency
        or not tenor
        or "_" in tenor
        or currency not in _YIELD_PROVIDER_BY_CURRENCY
    ):
        return None
    return asset_class, currency, tenor


def _rate_for_tenor(tenors: list, rates: list, tenor: str) -> float | None:
    for stored_tenor, rate in zip(tenors, rates):
        if not isinstance(stored_tenor, str) or rate is None:
            continue
        if stored_tenor.strip().upper() != tenor:
            continue
        try:
            return float(rate)
        except (TypeError, ValueError):
            return None
    return None


def quote_from_yield_curve(
    curve: dict | None,
    symbol: str,
    asset_class: str,
) -> dict | None:
    """One tenor of a FRED or ECB yield curve, shaped like a provider quote."""
    parsed = parse_yield_symbol(symbol)
    if parsed is None or curve is None:
        return None
    parsed_class, currency, tenor = parsed
    if parsed_class != asset_class.strip().upper():
        return None
    provider = _YIELD_PROVIDER_BY_CURRENCY[currency]
    if curve.get("provider") != provider or curve.get("currency") != currency:
        return None
    if curve.get("curve_type") != "YIELD_CURVE":
        return None
    last = _rate_for_tenor(curve.get("tenors") or [], curve.get("rates") or [], tenor)
    if last is None:
        return None
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "provider": provider,
        "asset_class": parsed_class,
        "symbol": symbol.strip().upper(),
        "bid": None,
        "ask": None,
        "last": last,
        "currency": currency,
        "provider_timestamp": _as_utc_z(curve.get("as_of_time")),
        "received_at": _as_utc_z(curve.get("received_at")) or _as_utc_z(curve.get("created_at")) or now,
        "raw_payload": {"curve_name": curve.get("curve_name"), "tenor": tenor},
    }


def default_curve_providers() -> dict:
    return {
        "nbp": NbpProvider(),
        "ecb": EcbProvider(),
        "fred": FredProvider(),
    }


class CurveService:
    def __init__(self, providers: dict):
        self.providers = providers

    @classmethod
    def create(cls) -> "CurveService":
        return cls(default_curve_providers())

    # Async: awaits market_data.get_latest_curves_async.
    async def list_persisted(self) -> dict:
        async with AsyncDBSessionManager() as db:
            rows = await db.market_data.get_latest_curves_async()
            return {"curves": [serialize(row) for row in rows]}

    # Async: awaits market_data.get_latest_for_provider_async.
    async def curve_for_provider(self, provider: str) -> dict:
        if provider.strip().lower() not in self.providers:
            raise ApiError(status.HTTP_404_NOT_FOUND, f"Unknown provider: {provider}")
        async with AsyncDBSessionManager() as db:
            row = await db.market_data.get_latest_for_provider_async(provider.strip().upper())
            curve = serialize(row)
        if curve is None:
            raise ApiError(status.HTTP_404_NOT_FOUND, "Curve not found")
        return curve

    # Async: awaits market_data.get_latest_for_provider_async.
    async def get_persisted(self, provider: str) -> dict | None:
        async with AsyncDBSessionManager() as db:
            return serialize(
                await db.market_data.get_latest_for_provider_async(provider.strip().upper())
            )

    # Async: awaits get_persisted, which awaits the curve read.
    async def fx_quotes(self, symbol: str) -> dict:
        """One NBP spot for an FX pair, shaped like a provider quote.

        ECB and FRED publish yield curves, not pair spots, so they are omitted.
        A missing NBP leg returns an empty list (the ticket shows MISSING).
        """
        pair = (symbol or "").strip().upper()
        if not pair:
            raise ApiError(status.HTTP_400_BAD_REQUEST, "symbol is required")
        curve = await self.get_persisted("nbp")
        if not curve or curve.get("curve_type") != "FX_SPOT":
            return {"quotes": []}
        last = nbp_cross_rate(pair, curve.get("tenors") or [], curve.get("rates") or [])
        if last is None:
            return {"quotes": []}
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            "quotes": [
                {
                    "provider": "NBP",
                    "asset_class": "FX",
                    "symbol": pair,
                    "bid": None,
                    "ask": None,
                    "last": last,
                    "currency": pair[3:],
                    "provider_timestamp": _as_utc_z(curve.get("as_of_time")),
                    "received_at": _as_utc_z(curve.get("created_at")) or now,
                }
            ]
        }

    # Async: awaits get_persisted, which awaits the curve read.
    async def curve_quotes(self, symbol: str, asset_class: str) -> dict:
        """Latest FRED (USD) or ECB (EUR) tenor for a bond or IRS symbol."""
        symbol = (symbol or "").strip().upper()
        asset_class = (asset_class or "").strip().upper()
        if not symbol:
            raise ApiError(status.HTTP_400_BAD_REQUEST, "symbol is required")
        if asset_class not in {"BOND", "IRS"}:
            raise ApiError(status.HTTP_400_BAD_REQUEST, "asset_class must be BOND or IRS")
        if symbol not in read_symbol_map().get(asset_class, []):
            raise ApiError(status.HTTP_400_BAD_REQUEST, f"Unknown {asset_class} symbol: {symbol}")
        parsed = parse_yield_symbol(symbol)
        if parsed is None or parsed[0] != asset_class:
            return {"quotes": []}
        provider = _YIELD_PROVIDER_BY_CURRENCY[parsed[1]]
        quote = quote_from_yield_curve(
            await self.get_persisted(provider),
            symbol,
            asset_class,
        )
        return {"quotes": [quote] if quote else []}

    # Async: awaits AsyncClient.get through the curve provider.
    async def get_curve(self, provider: str) -> dict:
        provider_key = provider.strip().lower()
        record_query(CURVE_SERVICE)

        def fail(payload: dict) -> dict:
            record_error(CURVE_SERVICE, payload)
            return payload

        engine = self.providers.get(provider_key)
        if engine is None:
            return fail({
                "provider": provider_key.upper(),
                "error": f"Unknown provider: {provider_key}",
                "status": "error",
            })

        try:
            raw = await engine.fetch_data()
            provider_error = engine.provider_error(raw)
            if provider_error is not None:
                return fail({
                    "provider": provider_key.upper(),
                    "error": provider_error,
                    "status": "error",
                })
            normalized = engine.normalize_data(raw)
            record_success(CURVE_SERVICE, normalized)
            return normalized
        except httpx2.TimeoutException:
            logging.error("Curve provider %s timed out", provider_key)
            return fail({
                "provider": provider_key.upper(),
                "error": "timeout",
                "status": "timeout",
            })
        except httpx2.HTTPStatusError as exc:
            code = exc.response.status_code
            logging.error("Curve provider %s HTTP %s", provider_key, code)
            return fail({
                "provider": provider_key.upper(),
                "error": str(exc),
                "status": "rate_limited" if code == 429 else "error",
            })
        except httpx2.RequestError as exc:
            logging.error("Curve provider %s network error: %s", provider_key, exc)
            return fail({
                "provider": provider_key.upper(),
                "error": str(exc),
                "status": "network_error",
            })
        except Exception as exc:
            logging.exception("Curve provider %s unexpected error", provider_key)
            return fail({
                "provider": provider_key.upper(),
                "error": str(exc),
                "status": "error",
            })
