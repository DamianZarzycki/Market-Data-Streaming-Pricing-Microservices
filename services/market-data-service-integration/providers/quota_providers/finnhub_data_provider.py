import logging
import os
from datetime import datetime, timezone

from providers.quota_providers._base import BaseProvider, ProviderAPIError

api_key: str = os.getenv("FINNHUB_API_KEY") or ""


class FinnhubProvider(BaseProvider):
    key = "finnhub"
    semaphore_limit = 1

    def __init__(self):
        super().__init__(api_key)
        self.url = "https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}"

    def provider_error(self, data) -> ProviderAPIError | None:
        if not isinstance(data, dict):
            return ProviderAPIError("invalid Finnhub response")
        if data.get("error"):
            return ProviderAPIError(str(data["error"]))
        return None

    def health(self):
        # easiest/cheapest api call to check if the provider is healthy
        pass

    def get_headers(self):
        return {"X-Finnhub-Token": self.api_key}

    def normalize_data(self, data, symbol) -> dict:
        timestamp = data.get("t")
        provider_timestamp = None
        if timestamp:
            provider_timestamp = datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "provider": "FINNHUB",
            "asset_class": "EQUITY",
            "symbol": symbol,
            "bid": None,
            "ask": None,
            "last": data.get("c"),
            "currency": "USD",
            "provider_timestamp": provider_timestamp,
            "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "raw_payload": data,
        }

    # Async: awaits AsyncClient.get.
    async def fetch_data(self, symbol: str):
        return await self.get_json(
            self.url.format(symbol=symbol, api_key=self.api_key),
            headers=self.get_headers(),
        )
