import logging
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError

from core.fetch_audit import CURVE_SERVICE, record_error, record_query, record_success
from providers.curve_providers.ecb_provider import EcbProvider
from providers.curve_providers.fred_provider import FredProvider
from providers.curve_providers.nbp_provider import NbpProvider


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

    def get_curves(self) -> dict:
        keys = list(self.providers)
        with ThreadPoolExecutor(max_workers=max(len(keys), 1)) as pool:
            return {"curves": list(pool.map(self.get_curve, keys))}

    def get_curve(self, provider: str) -> dict:
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
            raw = engine.fetch_data()
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
        except HTTPError as exc:
            logging.error("Curve provider %s HTTP %s", provider_key, exc.code)
            return fail({
                "provider": provider_key.upper(),
                "error": str(exc),
                "status": "rate_limited" if exc.code == 429 else "error",
            })
        except (URLError, TimeoutError, OSError) as exc:
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
