from routes.book_metrics import register as register_book_metrics
from routes.health import register as register_health
from routes.stream import register as register_stream
from routes.valuations import register as register_valuations


def register_routes(app) -> None:
    register_health(app)
    register_valuations(app)
    register_book_metrics(app)
    register_stream(app)
