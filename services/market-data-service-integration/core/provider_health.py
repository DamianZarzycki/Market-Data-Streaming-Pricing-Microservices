from concurrent.futures import ThreadPoolExecutor

from core.polling.config import DEFAULT_POLL_SYMBOL, now_utc_iso


QUOTE_KIND = "quote"
CURVE_KIND = "curve"


def _quota_exceeded(status: str | None, error: object) -> bool:
    if status in ("quota_exceeded", "rate_limited"):
        return True
    return bool(error) and str(error).endswith("_limit")


def _missing_api_key(engine) -> bool:
    return not (getattr(engine, "api_key", None) or "").strip()


def probe_quote_provider(quote_service, provider_key: str) -> dict:
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

    event_name, payload = quote_service.fetch_quote(
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

    status = payload.get("status") or "error"
    error = payload.get("error")
    quota_exceeded = _quota_exceeded(status, error)
    if quota_exceeded and status != "rate_limited":
        status = "quota_exceeded"
    return {
        "provider": provider_key,
        "kind": QUOTE_KIND,
        "reachable": False,
        "quota_exceeded": quota_exceeded,
        "status": status,
        "error": error,
        "quota": quota,
    }


def probe_curve_provider(curve_service, provider_key: str) -> dict:
    payload = curve_service.get_curve(provider_key)
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

    status = payload.get("status") or "error"
    quota_exceeded = status == "rate_limited"
    if "API_KEY is not set" in str(error):
        status = "missing_api_key"
    return {
        "provider": provider_key,
        "kind": CURVE_KIND,
        "reachable": False,
        "quota_exceeded": quota_exceeded,
        "status": status,
        "error": error,
    }


def _summarize(results: list[dict]) -> dict:
    ok_count = sum(1 for item in results if item["status"] == "ok")
    quota_exceeded_count = sum(1 for item in results if item["quota_exceeded"])
    if not results:
        status = "down"
    elif ok_count == len(results):
        status = "ok"
    elif ok_count == 0:
        status = "down"
    else:
        status = "degraded"
    return {
        "checked_at": now_utc_iso(),
        "status": status,
        "ok": ok_count,
        "failed": len(results) - ok_count,
        "quota_exceeded": quota_exceeded_count,
        "providers": {item["provider"]: item for item in results},
    }


def probe_provider(quote_service, curve_service, provider: str) -> dict | None:
    provider_key = provider.strip().lower()
    if provider_key in quote_service.providers:
        return probe_quote_provider(quote_service, provider_key)
    if provider_key in curve_service.providers:
        return probe_curve_provider(curve_service, provider_key)
    return None


def probe_quote_providers(quote_service) -> dict:
    """Live-probe Finnhub, Alpha Vantage and Twelve Data in parallel."""
    quote_keys = list(quote_service.providers)
    workers = max(len(quote_keys), 1)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(
            pool.map(
                lambda key: probe_quote_provider(quote_service, key),
                quote_keys,
            )
        )
    return _summarize(results)
