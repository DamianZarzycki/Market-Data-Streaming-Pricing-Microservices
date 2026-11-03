from routes.curves import register as register_curves
from routes.health import register as register_health
from routes.providers import register as register_providers
from routes.quotes import register as register_quotes
from routes.refresh import register as register_refresh
from routes.stream import register as register_stream


def register_routes(app) -> None:
    register_health(app)
    register_providers(app)
    register_quotes(app)
    register_stream(app)
    # /curves/refresh must be registered before /curves/{provider}.
    register_refresh(app)
    register_curves(app)
