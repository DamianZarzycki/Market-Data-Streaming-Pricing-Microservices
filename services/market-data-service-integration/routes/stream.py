from bottle import request

from core.helpers import unknown_provider


def _wants_mock() -> bool:
    """?mock=1 (or true/yes) forces the synthetic quote generator for this
    connection, regardless of MARKET_DATA_MOCK_STREAM — handy for developing
    against the stream without real provider API keys or rate limits."""
    return (request.query.get("mock") or "").strip().lower() in ("1", "True", "yes")


def register(app, quote_service):
    @app.route("/market-data/stream")
    def stream_all():
        return quote_service().stream_quotes(None, mock=_wants_mock())

    @app.route("/market-data/stream/<provider>")
    def stream_provider(provider):
        if provider not in quote_service().providers:
            return unknown_provider(provider)
        return quote_service().stream_quotes({provider}, mock=_wants_mock())
