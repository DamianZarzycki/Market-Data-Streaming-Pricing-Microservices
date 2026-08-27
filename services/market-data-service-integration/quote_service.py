import logging

from providers.quota_providers.alpha_vantage_provider import AlphaVantageProvider
from providers.quota_providers.finnhub_provider import FinnhubProvider
from providers.quota_providers.twelve_data_provider import TwelveDataProvider
import worker

PROVIDERS = {
    "finnhub": FinnhubProvider(),
    "alpha_vantage": AlphaVantageProvider(),
    "twelve_data": TwelveDataProvider(),
}


def fetch_and_store(provider_key: str, symbol: str) -> dict:
    provider = PROVIDERS[provider_key]
    raw = provider.fetch_data(symbol)
    quote = provider.normalize_data(raw, symbol)
    persist_quote(quote, fallback_symbol=symbol)
    return quote


def persist_quote(quote: dict, fallback_symbol: str = None) -> None:
    payload = dict(quote)
    if not payload.get("symbol") and fallback_symbol:
        payload["symbol"] = fallback_symbol
    worker.db_queue.put(payload)
    logging.info(
        "Enqueued %s quote for %s last=%s",
        payload.get("provider"),
        payload.get("symbol"),
        payload.get("last"),
    )
