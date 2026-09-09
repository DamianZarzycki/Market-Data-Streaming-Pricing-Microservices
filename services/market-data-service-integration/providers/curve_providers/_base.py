import json
from datetime import datetime, timezone
from urllib import request as urllib_request


DEFAULT_HEADERS = {
    "User-Agent": "trading-infra-market-data-service-integration",
    "Accept": "application/json",
}


class CurveProviderBase:
    key: str = ""

    def __init__(self, name: str):
        self.name = name

    def fetch_data(self) -> list[dict]:
        raise NotImplementedError

    def normalize_data(self, data) -> dict:
        raise NotImplementedError

    def provider_error(self, data) -> str | None:
        return None

    def request(self, url: str, headers: dict | None = None) -> bytes:
        merged = {**DEFAULT_HEADERS, **(headers or {})}
        req = urllib_request.Request(url, headers=merged)
        with urllib_request.urlopen(req, timeout=15) as response:
            return response.read()

    def get_json(self, url: str, headers: dict | None = None):
        return json.loads(self.request(url, headers).decode())

    def get_text(self, url: str, headers: dict | None = None) -> str:
        return self.request(url, headers).decode()

    @staticmethod
    def now_utc() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def date_to_utc(value: str | None) -> str | None:
        if not value:
            return None
        if len(value) == 10:
            return f"{value}T00:00:00Z"
        return value

    @staticmethod
    def percent_to_decimal(value) -> float:
        return round(float(value) / 100.0, 6)
