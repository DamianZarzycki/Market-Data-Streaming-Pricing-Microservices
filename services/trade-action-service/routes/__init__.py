from routes.health import register as register_health
from routes.provider_mode import register as register_provider_mode
from routes.trade_actions import register as register_trade_actions


def register_routes(app) -> None:
    register_health(app)
    register_provider_mode(app)
    register_trade_actions(app)
