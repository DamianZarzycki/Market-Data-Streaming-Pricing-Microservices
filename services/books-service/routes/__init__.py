from routes.books import register as register_books
from routes.health import register as register_health


def register_routes(app) -> None:
    register_health(app)
    register_books(app)
