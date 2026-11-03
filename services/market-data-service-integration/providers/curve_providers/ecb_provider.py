import csv
import io
import logging

from providers.curve_providers._base import CurveProviderBase
from shared.trading_shared.enums import CurveType

ECB_TENORS = ("SR_3M", "SR_1Y", "SR_2Y", "SR_5Y", "SR_10Y", "SR_30Y")


class EcbProvider(CurveProviderBase):
    key = "ecb"
    url = (
        "https://data-api.ecb.europa.eu/service/data/YC/"
        "B.U2.EUR.4F.G_N_A.SV_C_YM."
        + "+".join(ECB_TENORS)
        + "?lastNObservations=1&format=csvdata"
    )

    def __init__(self):
        super().__init__(self.key)

    # Async: awaits AsyncClient.get.
    async def fetch_data(self):
        logging.info("Fetching ECB AAA euro yield curve")
        text = await self.get_text(self.url, headers={"Accept": "text/csv"})
        return list(csv.DictReader(io.StringIO(text)))

    def provider_error(self, data) -> str | None:
        if not isinstance(data, list) or not data:
            return "empty ECB yield curve"
        return None

    def normalize_data(self, data) -> dict:
        points = []
        as_of = None
        for row in data:
            tenor_code = (row.get("DATA_TYPE_FM") or "").replace("SR_", "")
            value = row.get("OBS_VALUE")
            if not tenor_code or value in (None, ""):
                continue
            points.append((tenor_code, self.percent_to_decimal(value)))
            as_of = as_of or row.get("TIME_PERIOD")

        order = {code.replace("SR_", ""): index for index, code in enumerate(ECB_TENORS)}
        points.sort(key=lambda item: order.get(item[0], 99))

        return {
            "provider": "ECB",
            "curve_name": "EUR_AAA_YIELD_CURVE",
            "curve_type": CurveType.YIELD_CURVE.value,
            "currency": "EUR",
            "tenors": [tenor for tenor, _ in points],
            "rates": [rate for _, rate in points],
            "as_of_time": self.date_to_utc(as_of),
            "received_at": self.now_utc(),
            "raw_payload": {
                "source": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM",
                "as_of": as_of,
            },
        }
