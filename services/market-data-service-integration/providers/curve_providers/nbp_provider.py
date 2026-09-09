import logging

from providers.curve_providers._base import CurveProviderBase


class NbpProvider(CurveProviderBase):
    key = "nbp"
    url = "https://api.nbp.pl/api/exchangerates/tables/A?format=json"

    def __init__(self):
        super().__init__(self.key)

    def fetch_data(self):
        logging.info("Fetching NBP FX table A")
        return self.get_json(self.url)

    def provider_error(self, data) -> str | None:
        if not isinstance(data, list) or not data:
            return "invalid NBP response"
        table = data[0]
        if not isinstance(table, dict) or not table.get("rates"):
            return "empty NBP table"
        return None

    def normalize_data(self, data, symbol: str | None = None) -> dict:
        table = data[0]
        rows = sorted(table.get("rates") or [], key=lambda row: row.get("code") or "")
        return {
            "provider": "NBP",
            "curve_name": "PLN_FX_TABLE_A",
            "curve_type": "FX_SPOT",
            "currency": "PLN",
            "tenors": [row["code"] for row in rows],
            "rates": [row.get("mid") for row in rows],
            "as_of_time": self.date_to_utc(table.get("effectiveDate")),
            "received_at": self.now_utc(),
            "raw_payload": {
                "table": table.get("table"),
                "no": table.get("no"),
                "effectiveDate": table.get("effectiveDate"),
            },
        }
