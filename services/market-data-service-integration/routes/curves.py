from bottle import request

from core.helpers import json_error, unknown_provider
from core.polling.config import read_symbol_map


def register(app, curve_service):
    @app.route("/market-data/fx-quotes")
    def fx_quotes():
        symbol = (request.query.get("symbol") or "").strip().upper()
        if not symbol:
            return json_error(400, "symbol is required")
        return {"quotes": curve_service().fx_quotes(symbol)}

    @app.route("/market-data/curve-quotes")
    def curve_quotes():
        symbol = (request.query.get("symbol") or "").strip().upper()
        asset_class = (request.query.get("asset_class") or "").strip().upper()
        if not symbol:
            return json_error(400, "symbol is required")
        if asset_class not in {"BOND", "IRS"}:
            return json_error(400, "asset_class must be BOND or IRS")
        if symbol not in read_symbol_map().get(asset_class, []):
            return json_error(400, f"Unknown {asset_class} symbol: {symbol}")
        return {"quotes": curve_service().curve_quotes(symbol, asset_class)}

    @app.route("/curves")
    def curves():
        return {"curves": curve_service().list_persisted()}

    @app.route("/curves/<provider>")
    def curve_by_provider(provider):
        if provider not in curve_service().providers:
            return unknown_provider(provider)
        curve = curve_service().get_persisted(provider)
        if curve is None:
            return json_error(404, "Curve not found")
        return curve
