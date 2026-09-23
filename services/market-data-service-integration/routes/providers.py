from core.helpers import unknown_provider
from core.provider_health import probe_provider, probe_quote_providers


def register(app, quote_service, curve_service):
    @app.route("/providers")
    def providers():
        return {
            "providers": list(quote_service().providers.keys()),
            "curve_providers": list(curve_service().providers.keys()),
        }

    @app.route("/providers/health")
    def providers_health():
        return probe_quote_providers(quote_service())

    @app.route("/providers/<provider>/health")
    def provider_health(provider):
        result = probe_provider(quote_service(), curve_service(), provider)
        if result is None:
            return unknown_provider(provider)
        return result
