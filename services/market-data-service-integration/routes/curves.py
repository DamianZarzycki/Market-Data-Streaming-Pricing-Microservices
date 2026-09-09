from core.helpers import json_error, unknown_provider


def register(app, curve_service):
    @app.route("/curves")
    def curves():
        return curve_service().get_curves()

    @app.route("/curves/<provider>")
    def curve_by_provider(provider):
        if provider not in curve_service().providers:
            return unknown_provider(provider)
        curve = curve_service().get_curve(provider)
        if curve.get("error"):
            return json_error(502, curve["error"])
        return curve
