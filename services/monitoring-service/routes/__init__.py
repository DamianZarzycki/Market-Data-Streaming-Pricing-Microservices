from routes.health import register as register_health
from routes.status import register as register_status


def register_routes(app) -> None:
    register_health(app)
    register_status(app)
