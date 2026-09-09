import os
import json
import urllib.request
from datetime import datetime, timezone
from providers.quota_providers._base import BaseProvider, ProviderAPIError
import logging

api_key: str = os.getenv("TWELVEDATA_API_KEY") or ""


class TwelveDataProvider(BaseProvider):
    key = "twelve_data"
    semaphore_limit = 1
    batch_is_single_request = True

    def __init__(self):
        super().__init__(api_key)
        self.url = "https://api.twelvedata.com/quote?symbol={symbol}&apikey={api_key}"

    def provider_error(self, data) -> ProviderAPIError | None:
        if not isinstance(data, dict):
            return ProviderAPIError("invalid Twelve Data response")
        code = data.get("code")
        if data.get("status") == "error" or code not in (None, 200, "200"):
            message = data.get("message") or str(code)
            status = "rate_limited" if str(code) == "429" else "error"
            return ProviderAPIError(message, status=status)
        if data.get("close") in (None, ""):
            return ProviderAPIError("missing close price")
        return None


    def health(self) -> ProviderAPIError | None:
        # easiest/cheapest api call to check if the provider is healthy
        req = urllib.request.Request(self.url.format(symbol="AAPL", api_key=self.api_key))
        with urllib.request.urlopen(req) as response:
            if response.status != 200:
                return ProviderAPIError("failed to check health")
            return None 

    def normalize_data(self, data, symbol) -> dict:
        timestamp = data.get("last_quote_at") or data.get("timestamp")
        provider_timestamp = None
        if timestamp:
            provider_timestamp = datetime.fromtimestamp(
                timestamp, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "provider": "TWELVE_DATA",
            "asset_class": "EQUITY",
            "symbol": symbol,
            "bid": None,
            "ask": None,
            "last": float(data.get("close")),
            "currency": data.get("currency"),
            "provider_timestamp": provider_timestamp,
            "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "raw_payload": data,
        }

    def fetch_data(self, symbol: str):
        logging.info("Fetching data from TwelveData for symbol: %s", symbol)
        req = urllib.request.Request(self.url.format(symbol=symbol, api_key=self.api_key))
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())

