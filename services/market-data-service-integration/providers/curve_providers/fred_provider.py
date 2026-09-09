import logging
import os

from providers.curve_providers._base import CurveProviderBase
from shared.trading_shared.enums import CurveType

FRED_SERIES = (
    ("DGS1MO", "1M"),
    ("DGS3MO", "3M"),
    ("DGS1", "1Y"),
    ("DGS5", "5Y"),
    ("DGS10", "10Y"),
    ("DGS30", "30Y"),
)


class FredProvider(CurveProviderBase):
    key = "fred"
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        "?series_id={series_id}&api_key={api_key}&file_type=json"
        "&sort_order=desc&limit=1"
    )

    def __init__(self):
        super().__init__(self.key)
        self.api_key = os.getenv("FRED_API_KEY") or ""

    def fetch_data(self):
        if not self.api_key:
            return {"error": "FRED_API_KEY is not set"}

        logging.info("Fetching FRED US Treasury yield curve")
        observations = {}
        for series_id, tenor in FRED_SERIES:
            payload = self.get_json(
                self.url.format(series_id=series_id, api_key=self.api_key)
            )
            if payload.get("error_code") or payload.get("error_message"):
                return payload
            rows = payload.get("observations") or []
            latest = next(
                (row for row in rows if row.get("value") not in (None, "", ".")),
                None,
            )
            if latest is not None:
                observations[tenor] = latest
        return observations

    def provider_error(self, data) -> str | None:
        if not isinstance(data, dict):
            return "invalid FRED response"
        if data.get("error"):
            return str(data["error"])
        if data.get("error_message"):
            return str(data["error_message"])
        if not data:
            return "empty FRED yield curve"
        return None

    def normalize_data(self, data, symbol: str | None = None) -> dict:
        tenors = [tenor for tenor, _ in FRED_SERIES if tenor in data]
        as_of = None
        rates = []
        for tenor in tenors:
            row = data[tenor]
            as_of = as_of or row.get("date")
            rates.append(self.percent_to_decimal(row.get("value")))

        return {
            "provider": "FRED",
            "curve_name": "USD_TREASURY_YIELD_CURVE",
            "curve_type": CurveType.YIELD_CURVE.value,
            "currency": "USD",
            "tenors": tenors,
            "rates": rates,
            "as_of_time": self.date_to_utc(as_of),
            "received_at": self.now_utc(),
            "raw_payload": {
                series_id: data[tenor]
                for series_id, tenor in FRED_SERIES
                if tenor in data
            },
        }
