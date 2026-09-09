from core.helpers import unknown_provider


def register(app, quote_service):
    @app.route("/market-data/stream")
    def stream_all():
        return quote_service().stream_quotes(None)

    @app.route("/market-data/stream/<provider>")
    def stream_provider(provider):
        if provider not in quote_service().providers:
            return unknown_provider(provider)
        return quote_service().stream_quotes({provider})
