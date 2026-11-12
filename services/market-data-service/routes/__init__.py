from routes.health import register as register_health
from routes.snapshot import register as register_snapshot
from routes.stream import register as register_stream
from routes.symbols import register as register_symbols


def register_routes(app) -> None:
    register_health(app)
    register_symbols(app)
    register_snapshot(app)
    register_stream(app)
