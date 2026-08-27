from datetime import datetime, timezone
from providers._base import BaseProvider
import urllib.request
import os
import json
import logging

api_key: str = os.getenv("ALPHAVANTAGE_API_KEY") or ""


def _to_float(value):
    if value is None or value == "":
        return None
    return float(value)


class AlphaVantageProvider(BaseProvider):
    def __init__(self):
        super().__init__(api_key)
        self.url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"

    def health(self):
        # easiest/cheapest api call to check if the provider is healthy
        pass

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

    def fetch_data(self, symbol: str):
        logging.info("Fetching data from AlphaVantage for symbol: %s", symbol)
        req = urllib.request.Request(self.url.format(symbol=symbol, api_key=self.api_key))
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())