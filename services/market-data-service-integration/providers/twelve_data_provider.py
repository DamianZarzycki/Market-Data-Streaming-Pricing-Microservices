import os
import json
import urllib.request
from providers._base import BaseProvider
import logging

api_key: str = os.getenv("TWELVEDATA_API_KEY") or ""
class TwelveDataProvider(BaseProvider):
    def __init__(self):
        super().__init__(api_key)
        self.url = "https://api.twelvedata.com/quote?symbol={symbol}&apikey={api_key}"

    def fetch_data(self, symbol: str):
        logging.info("Fetching data from TwelveData for symbol: %s", symbol)
        req = urllib.request.Request(self.url.format(symbol=symbol, api_key=self.api_key))
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())

        
