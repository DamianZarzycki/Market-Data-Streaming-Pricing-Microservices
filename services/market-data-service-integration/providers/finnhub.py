import logging
from providers._base import BaseProvider
import os
from urllib import request as urllib_request
import json

api_key: str = os.getenv("FINNHUB_API_KEY") or ""

    
class FinnhubProvider(BaseProvider):
    def __init__(self):
        super().__init__(api_key)
        self.url = "https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}"

    
    def get_headers(self):
        logging.info("Getting headers for FinnhubProvider: %s", self.api_key)
        return {
            "X-Finnhub-Token": self.api_key
        }
    
    def normalize_data(self, data):
        return data


    def fetch_data(self, symbol: str):
        req = urllib_request.Request(self.url.format(symbol=symbol, api_key=self.api_key), headers=self.get_headers())

        with urllib_request.urlopen(req) as response:
            return json.loads(response.read().decode())