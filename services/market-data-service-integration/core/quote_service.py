import asyncio
import logging
import os
import queue
from urllib.error import HTTPError, URLError

from bottle import response

from core.aggregator import MarketDataAggregator
from core.fetch_audit import QUOTE_SERVICE, record_error, record_query, record_success
from core.mock_quote_stream import iter_mock_quote_events
from core.polling.config import futures_quotes_for_equity, read_symbol_map
from core.request_tracker import RequestTracker
from core.stream_hub import iter_quote_events, parse_symbols
from providers.quota_providers.alpha_vantage_data_provider import AlphaVantageProvider
from providers.quota_providers.finnhub_data_provider import FinnhubProvider
from providers.quota_providers.twelve_data_provider import TwelveDataProvider
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.serialization import serialize


def default_providers() -> dict:
    return {
        "finnhub": FinnhubProvider(),
        "alpha_vantage": AlphaVantageProvider(),
        "twelve_data": TwelveDataProvider(),
    }


def _env_flag(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in ("1", "true", "yes")


class QuoteService:
    def __init__(
        self,
        providers: dict,
        tracker: RequestTracker,
        quote_queue: queue.Queue,
    ):
        self.providers = providers
        self.tracker = tracker
        self._queue = quote_queue
        self.aggregator = MarketDataAggregator(providers, self.fetch_quote)

    @classmethod
    def create(cls, tracker: RequestTracker, quote_queue: queue.Queue) -> "QuoteService":
        return cls(default_providers(), tracker, quote_queue)

    def persist_quote(self, quote: dict, fallback_symbol: str | None = None) -> None:
        payload = dict(quote)
        if not payload.get("symbol") and fallback_symbol:
            payload["symbol"] = fallback_symbol
        self._queue.put(payload)
        logging.info(
            "Enqueued %s quote for %s last=%s",
            payload.get("provider"),
            payload.get("symbol"),
            payload.get("last"),
        )
        for mirrored in futures_quotes_for_equity(payload):
            self._queue.put(mirrored)
            logging.info(
                "Enqueued %s futures quote for %s last=%s underlying=%s",
                mirrored.get("provider"),
                mirrored.get("symbol"),
                mirrored.get("last"),
                (mirrored.get("raw_payload") or {}).get("underlying_symbol"),
            )

    def get_quotes(self, symbol: str, asset_class: str):
        with DBSessionManager() as db:
            rows = db.provider_quotes.get_latest_for_symbol(symbol, asset_class)
            return [serialize(row) for row in rows]

    def get_snapshot(self):
        """Latest normalized quote per (provider, symbol) across everything on
        file — backs GET /market-data/snapshot."""
        with DBSessionManager() as db:
            rows = db.provider_quotes.get_all_latest()
            return [serialize(row) for row in rows]

    def get_quote(self, provider: str, symbol: str) -> dict | None:
        with DBSessionManager() as db:
            row = db.provider_quotes.get_latest(provider.upper(), symbol)
            return serialize(row)

    def fetch_quote(self, provider: str, symbol: str, persist: bool = True) -> tuple[str, dict]:
        provider_key = provider.strip().lower()
        provider_upper = provider_key.upper()
        symbol = symbol.strip().upper()
        record_query(QUOTE_SERVICE)

        def fail(payload: dict) -> tuple[str, dict]:
            record_error(QUOTE_SERVICE, payload)
            return "error", payload

        engine = self.providers.get(provider_key)
        if engine is None:
            return fail({
                "provider": provider_upper,
                "symbol": symbol,
                "error": f"Unknown provider: {provider_key}",
            })

        ok, reason = self.tracker.can_request(provider_key)
        if not ok:
            return fail({
                "provider": provider_upper,
                "symbol": symbol,
                "error": reason or "request_denied",
                "status": "quota_exceeded",
            })

        try:
            raw = engine.fetch_data(symbol)
            provider_error = engine.provider_error(raw)
            if provider_error is not None:
                self.tracker.record(provider_key, symbol)
                return fail({
                    "provider": provider_upper,
                    "symbol": symbol,
                    "error": str(provider_error),
                    "status": provider_error.status,
                })

            normalized = engine.normalize_data(raw, symbol)
            if persist:
                self.persist_quote(normalized, fallback_symbol=symbol)
            self.tracker.record(provider_key, symbol)
            record_success(QUOTE_SERVICE, normalized)
            return "quote", normalized
        except HTTPError as exc:
            self.tracker.record(provider_key, symbol)
            logging.error("Provider %s HTTP %s for %s", provider_key, exc.code, symbol)
            return fail({
                "provider": provider_upper,
                "symbol": symbol,
                "error": str(exc),
                "status": "rate_limited" if exc.code == 429 else "error",
            })
        except (URLError, TimeoutError, OSError) as exc:
            logging.error("Provider %s network error for %s: %s", provider_key, symbol, exc)
            return fail({
                "provider": provider_upper,
                "symbol": symbol,
                "error": str(exc),
                "status": "network_error",
            })
        except Exception as exc:
            logging.exception("Provider %s unexpected error for %s", provider_key, symbol)
            return fail({
                "provider": provider_upper,
                "symbol": symbol,
                "error": str(exc),
                "status": "error",
            })

    def stream_quotes(self, provider_keys: set[str] | None, mock: bool = False):
        response.content_type = "text/event-stream"
        response.set_header("Cache-Control", "no-cache")

        equity_symbols = read_symbol_map()["EQUITY"]
        logging.info("Market data equity symbols: %s", equity_symbols)
        symbols = parse_symbols(",".join(equity_symbols))
        providers = sorted(provider_keys) if provider_keys else list(self.providers.keys())

        use_mock = mock or _env_flag("MARKET_DATA_MOCK_STREAM")
        if use_mock:
            logging.info(
                "Client connected to MOCK market-data stream providers=%s symbols=%s",
                providers,
                symbols,
            )

            def mock_event_generator():
                try:
                    yield from iter_mock_quote_events(providers, symbols)
                except Exception as exc:
                    logging.error("Error while generating mock market-data stream: %s", exc)
                finally:
                    logging.info("Client disconnected from mock market-data stream")

            return mock_event_generator()

        logging.info(
            "Client connected to market-data stream providers=%s symbols=%s",
            providers,
            symbols,
        )

        def fetch_all(pairs: list[tuple[str, str]]) -> list[tuple[str, dict]]:
            results = asyncio.run(self.aggregator.fetch(pairs))
            expanded: list[tuple[str, dict]] = []
            for event, payload in results:
                expanded.append((event, payload))
                if event != "quote":
                    continue
                for mirrored in futures_quotes_for_equity(payload):
                    expanded.append(("quote", mirrored))
            return expanded

        def event_generator():
            try:
                yield from iter_quote_events(providers, symbols, fetch_all)
            except Exception as exc:
                logging.error("Error while generating market-data stream: %s", exc)
            finally:
                logging.info("Client disconnected from market-data stream")

        return event_generator()
