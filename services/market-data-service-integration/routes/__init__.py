from routes.curves import register as register_curves
from routes.health import register as register_health
from routes.providers import register as register_providers
from routes.quotes import register as register_quotes
from routes.stream import register as register_stream


def register_routes(app, quote_service, curve_service, snapshot):
    register_health(app, snapshot)
    register_providers(app, quote_service)
    register_quotes(app, quote_service)
    register_stream(app, quote_service)
    register_curves(app, curve_service)
