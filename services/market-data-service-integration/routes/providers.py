from core.helpers import unknown_provider


def register(app, quote_service):
    @app.route("/providers")
    def providers():
        return {
            "providers": list(quote_service().providers.keys()),
        }

    @app.route("/providers/<provider>/health")
    def provider_health(provider):
        if provider not in quote_service().providers:
            return unknown_provider(provider)
        return quote_service().providers[provider].health()
