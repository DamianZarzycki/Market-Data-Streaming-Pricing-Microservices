import os
import json
import urllib.request
from datetime import datetime, timezone
from providers._base import BaseProvider
import logging

api_key: str = os.getenv("TWELVEDATA_API_KEY") or ""


class TwelveDataProvider(BaseProvider):
    def __init__(self):
        super().__init__(api_key)
        self.url = "https://api.twelvedata.com/quote?symbol={symbol}&apikey={api_key}"


    def health(self):
        # easiest/cheapest api call to check if the provider is healthy
        pass    

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


