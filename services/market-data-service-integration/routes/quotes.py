from bottle import request

from core.helpers import json_error, unknown_provider
from shared.trading_shared.enums import AssetClass


def register(app, quote_service):
    @app.route("/market-data/quotes")
    def quotes():
        symbol = (request.query.get("symbol") or "").strip().upper()
        if not symbol:
            return json_error(400, "symbol is required")

        asset_class = (
            request.query.get("asset_class") or AssetClass.EQUITY.value
        ).strip().upper()
        if asset_class not in {item.value for item in AssetClass}:
            return json_error(400, f"Invalid asset_class: {asset_class}")

        return {"quotes": quote_service().get_quotes(symbol, asset_class)}

    @app.route("/market-data/quotes/<provider>/<symbol>")
    def quote_by_provider(provider, symbol):
        if provider not in quote_service().providers:
            return unknown_provider(provider)

        quote = quote_service().get_quote(provider, symbol.strip().upper())
        if quote is None:
            return json_error(404, "Quote not found")
        return quote
