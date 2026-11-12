from routes.health import register as register_health
from routes.summary import register as register_summary
from routes.trades import register as register_trades


def register_routes(app) -> None:
    register_health(app)
    register_summary(app)
    register_trades(app)
