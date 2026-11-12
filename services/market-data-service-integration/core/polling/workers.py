import asyncio
import logging
import time

from core.curve_service import CurveService, quote_from_yield_curve
from core.polling.env import now_utc_iso
from shared.trading_shared.env import env_float
from core.polling.curve_persistence_worker import CurvePersistenceWorker
from core.polling.symbols import (
    futures_quotes_for_equity,
    read_poll_symbols,
    read_symbol_map,
)
from core.quotes import QuoteService
from core.stream import publish_quote


class PollWorkers:
    def __init__(
        self,
        quote_service: QuoteService,
        curve_service: CurveService,
        interval_seconds: float | None = None,
        curve_interval_seconds: float | None = None,
    ):
        self.quote_service = quote_service
        self.curve_service = curve_service
        self.interval_seconds = interval_seconds or max(
            env_float("MARKET_DATA_POLL_INTERVAL_SECONDS", 30.0),
            1.0,
        )
        self.curve_interval_seconds = (
            curve_interval_seconds
            if curve_interval_seconds is not None
            else max(env_float("CURVE_DATA_REFRESH_INTERVAL_SECONDS", 3600.0), 1.0)
        )
        self._quote_cycle_lock = asyncio.Lock()
        self._curve_cycle_lock = asyncio.Lock()
        self._curve_persistence_worker = CurvePersistenceWorker()

    # Async: awaits the quote fetches, which await AsyncClient.get, then asyncio.sleep.
    async def quote_loop(self) -> None:
        logging.info("Quota poll worker started interval=%ss", self.interval_seconds)
        try:
            while True:
                cycle_started_monotonic = time.monotonic()
                try:
                    cycle_summary = await self.run_quote_cycle()
                    logging.info("Quota poll cycle completed: %s", cycle_summary)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logging.exception("Quota poll cycle failed")
                cycle_elapsed_seconds = time.monotonic() - cycle_started_monotonic
                remaining_wait_seconds = max(
                    self.interval_seconds - cycle_elapsed_seconds,
                    0.0,
                )
                await asyncio.sleep(remaining_wait_seconds)
        except asyncio.CancelledError:
            logging.info("Quota poll worker stopped")
            raise

    # Async: awaits the curve fetches, which await AsyncClient.get, then asyncio.sleep.
    async def curve_loop(self) -> None:
        logging.info("Curve poll worker started interval=%ss", self.curve_interval_seconds)
        try:
            while True:
                cycle_started_monotonic = time.monotonic()
                try:
                    cycle_summary = await self.run_curve_cycle()
                    logging.info("Curve poll cycle completed: %s", cycle_summary)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logging.exception("Curve poll cycle failed")
                cycle_elapsed_seconds = time.monotonic() - cycle_started_monotonic
                remaining_wait_seconds = max(
                    self.curve_interval_seconds - cycle_elapsed_seconds,
                    0.0,
                )
                await asyncio.sleep(remaining_wait_seconds)
        except asyncio.CancelledError:
            logging.info("Curve poll worker stopped")
            raise

    # Async: awaits the curve persistence queue.
    async def persist_loop(self) -> None:
        await self._curve_persistence_worker.persist_loop()

    # Async: awaits aggregator.fetch, which awaits AsyncClient.get.
    # The lock is held across those awaits so a refresh and the poller share one cycle.
    async def run_quote_cycle(self) -> dict:
        async with self._quote_cycle_lock:
            poll_symbols = read_poll_symbols()
            provider_keys = list(self.quote_service.providers.keys())
            pairs = [
                (provider_key, poll_symbol)
                for poll_symbol in poll_symbols
                for provider_key in provider_keys
            ]
            results = await self.quote_service.aggregator.fetch(pairs)
            successful_requests = 0
            failed_requests = 0
            for event_name, response_payload in results:
                if event_name == "quote":
                    successful_requests += 1
                    publish_quote(response_payload)
                    for mirrored in futures_quotes_for_equity(response_payload):
                        publish_quote(mirrored)
                else:
                    failed_requests += 1
                    logging.warning(
                        "Quota poll fetch failed provider=%s symbol=%s error=%s",
                        response_payload.get("provider"),
                        response_payload.get("symbol"),
                        response_payload.get("error"),
                    )

            return {
                "time": now_utc_iso(),
                "providers": provider_keys,
                "symbols": poll_symbols,
                "requests": len(pairs),
                "success": successful_requests,
                "errors": failed_requests,
            }

    # Async: awaits get_curve, which awaits AsyncClient.get.
    async def run_curve_cycle(self) -> dict:
        async with self._curve_cycle_lock:
            provider_keys = list(self.curve_service.providers.keys())
            successful_curve_payloads: list[dict] = []
            failed_curve_fetches = 0

            for provider_key in provider_keys:
                curve_payload = await self.curve_service.get_curve(provider_key)
                if curve_payload.get("error"):
                    failed_curve_fetches += 1
                    logging.warning(
                        "Curve poll fetch failed provider=%s error=%s",
                        provider_key,
                        curve_payload.get("error"),
                    )
                    continue
                successful_curve_payloads.append(curve_payload)

            enqueued_count, dropped_count = self._curve_persistence_worker.enqueue_many(
                successful_curve_payloads
            )
            self._persist_yield_quotes(successful_curve_payloads)
            persisted_total, persist_errors_total = self._curve_persistence_worker.totals()
            return {
                "time": now_utc_iso(),
                "providers": provider_keys,
                "fetched": len(successful_curve_payloads),
                "enqueued": enqueued_count,
                "dropped": dropped_count,
                "queue_size": self._curve_persistence_worker.queue_size(),
                "persisted_total": persisted_total,
                "persist_errors_total": persist_errors_total,
                "errors": failed_curve_fetches,
            }

    def _persist_yield_quotes(self, curve_payloads: list[dict]) -> None:
        """Write bond and IRS tenors onto the quote queue so a ticket can book them."""
        symbol_map = read_symbol_map()
        for curve_payload in curve_payloads:
            for asset_class in ("BOND", "IRS"):
                for symbol in symbol_map.get(asset_class, []):
                    quote = quote_from_yield_curve(curve_payload, symbol, asset_class)
                    if quote is not None:
                        self.quote_service.persist_quote(quote)
