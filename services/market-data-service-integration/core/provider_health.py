import asyncio

from fastapi import status

from shared.trading_shared.http_errors import ApiError
from core.polling import DEFAULT_POLL_SYMBOL, now_utc_iso


QUOTE_KIND = "quote"
CURVE_KIND = "curve"


def _quota_exceeded(probe_status: str | None, error: object) -> bool:
    if probe_status in ("quota_exceeded", "rate_limited"):
        return True
    return bool(error) and str(error).endswith("_limit")


def _missing_api_key(engine) -> bool:
    return not (getattr(engine, "api_key", None) or "").strip()


def list_provider_names(quote_service, curve_service) -> dict:
    """Provider keys already in memory. No I/O."""
    return {
        "providers": list(quote_service.providers.keys()),
        "curve_providers": list(curve_service.providers.keys()),
    }


# Async: awaits fetch_quote, which awaits AsyncClient.get.
async def probe_quote_provider(quote_service, provider_key: str) -> dict:
    quota = quote_service.tracker.quota_snapshot(provider_key)
    engine = quote_service.providers[provider_key]
    if _missing_api_key(engine):
        return {
            "provider": provider_key,
            "kind": QUOTE_KIND,
            "reachable": False,
            "quota_exceeded": False,
            "status": "missing_api_key",
            "error": "API key is not set",
            "quota": quota,
        }

    event_name, payload = await quote_service.fetch_quote(
        provider_key,
        DEFAULT_POLL_SYMBOL,
        persist=False,
    )
    quota = quote_service.tracker.quota_snapshot(provider_key)
    if event_name == "quote":
        return {
            "provider": provider_key,
            "kind": QUOTE_KIND,
            "reachable": True,
            "quota_exceeded": False,
            "status": "ok",
            "error": None,
            "quota": quota,
        }

    probe_status = payload.get("status") or "error"
    error = payload.get("error")
    quota_exceeded = _quota_exceeded(probe_status, error)
    if quota_exceeded and probe_status != "rate_limited":
        probe_status = "quota_exceeded"
    return {
        "provider": provider_key,
        "kind": QUOTE_KIND,
        "reachable": False,
        "quota_exceeded": quota_exceeded,
        "status": probe_status,
        "error": error,
        "quota": quota,
    }


# Async: awaits get_curve, which awaits AsyncClient.get.
async def probe_curve_provider(curve_service, provider_key: str) -> dict:
    payload = await curve_service.get_curve(provider_key)
    error = payload.get("error")
    if not error:
        return {
            "provider": provider_key,
            "kind": CURVE_KIND,
            "reachable": True,
            "quota_exceeded": False,
            "status": "ok",
            "error": None,
        }

    probe_status = payload.get("status") or "error"
    quota_exceeded = probe_status == "rate_limited"
    if "API_KEY is not set" in str(error):
        probe_status = "missing_api_key"
    return {
        "provider": provider_key,
        "kind": CURVE_KIND,
        "reachable": False,
        "quota_exceeded": quota_exceeded,
        "status": probe_status,
        "error": error,
    }


def _summarize(results: list[dict]) -> dict:
    ok_count = sum(1 for item in results if item["status"] == "ok")
    quota_exceeded_count = sum(1 for item in results if item["quota_exceeded"])
    if not results:
        probe_status = "down"
    elif ok_count == len(results):
        probe_status = "ok"
    elif ok_count == 0:
        probe_status = "down"
    else:
        probe_status = "degraded"
    return {
        "checked_at": now_utc_iso(),
        "status": probe_status,
        "ok": ok_count,
        "failed": len(results) - ok_count,
        "quota_exceeded": quota_exceeded_count,
        "providers": {item["provider"]: item for item in results},
    }


# Async: awaits the quote or curve probe, which awaits AsyncClient.get.
async def probe_provider(quote_service, curve_service, provider: str) -> dict | None:
    provider_key = provider.strip().lower()
    if provider_key in quote_service.providers:
        return await probe_quote_provider(quote_service, provider_key)
    if provider_key in curve_service.providers:
        return await probe_curve_provider(curve_service, provider_key)
    return None


# Async: awaits probe_provider. Raises when the name is not a quote or curve provider.
async def provider_health(quote_service, curve_service, provider: str) -> dict:
    result = await probe_provider(quote_service, curve_service, provider)
    if result is None:
        raise ApiError(status.HTTP_404_NOT_FOUND, f"Unknown provider: {provider}")
    return result


# Async: awaits asyncio.gather of the live quote probes.
async def probe_quote_providers(quote_service) -> dict:
    """Live-probe Finnhub, Alpha Vantage and Twelve Data together."""
    quote_keys = list(quote_service.providers)
    results = await asyncio.gather(
        *[probe_quote_provider(quote_service, key) for key in quote_keys]
    )
    return _summarize(list(results))
