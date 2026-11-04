import logging
from datetime import datetime, timezone

from providers.quota_providers._base import BaseProvider, ProviderAPIError
from shared.trading_shared.env import env_str

api_key: str = env_str("ALPHAVANTAGE_API_KEY", "")


def _to_float(value):
    if value is None or value == "":
        return None
    return float(value)


class AlphaVantageProvider(BaseProvider):
    key = "alpha_vantage"
    semaphore_limit = 1

    def __init__(self):
        super().__init__(api_key)
        self.url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"

    def health(self):
        # easiest/cheapest api call to check if the provider is healthy
        pass

    def provider_error(self, data) -> ProviderAPIError | None:
        if not isinstance(data, dict):
            return ProviderAPIError("invalid Alpha Vantage response")
        if data.get("Note") or data.get("Information"):
            return ProviderAPIError(
                data.get("Note") or data.get("Information"),
                status="rate_limited",
            )
        if data.get("Error Message"):
            return ProviderAPIError(data["Error Message"])
        quote = data.get("Global Quote") or {}
        if not quote or quote.get("05. price") in (None, ""):
            return ProviderAPIError("empty Global Quote")
        return None

    def normalize_data(self, data, symbol) -> dict:
        quote = data.get("Global Quote") or {}
        trading_day = quote.get("07. latest trading day")
        provider_timestamp = None
        if trading_day:
            provider_timestamp = (
                datetime.strptime(trading_day, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ")
            )

        return {
            "provider": "ALPHA_VANTAGE",
            "asset_class": "EQUITY",
            "symbol": quote.get("01. symbol"),
            "bid": None,
            "ask": None,
            "last": _to_float(quote.get("05. price")),
            "currency": "USD",
            "provider_timestamp": provider_timestamp,
            "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "raw_payload": data,
        }

    # Async: awaits AsyncClient.get.
    async def fetch_data(self, symbol: str):
        logging.info("Fetching data from AlphaVantage for symbol: %s", symbol)
        return await self.get_json(self.url.format(symbol=symbol, api_key=self.api_key))
