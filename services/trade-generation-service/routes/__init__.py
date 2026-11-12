from routes.config import register as register_config
from routes.generation import register as register_generation
from routes.health import register as register_health


def register_routes(app) -> None:
    register_health(app)
    register_config(app)
    register_generation(app)
