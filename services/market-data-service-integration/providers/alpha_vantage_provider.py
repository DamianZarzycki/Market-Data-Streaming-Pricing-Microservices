from providers._base import BaseProvider
import urllib.request
import os
import json
import logging

api_key: str = os.getenv("ALPHAVANTAGE_API_KEY") or ""


class AlphaVantageProvider(BaseProvider):
    def __init__(self):
        super().__init__(api_key)
        self.url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"

    def fetch_data(self, symbol: str):
        logging.info("Fetching data from AlphaVantage for symbol: %s", symbol)
        req = urllib.request.Request(self.url.format(symbol=symbol, api_key=self.api_key))
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())