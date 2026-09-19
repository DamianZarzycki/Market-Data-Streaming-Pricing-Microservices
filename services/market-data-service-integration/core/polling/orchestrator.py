import logging
import threading
import time

from core.curve_service import CurveService
from core.polling.config import now_utc_iso, read_poll_interval_seconds, read_poll_symbols
from core.polling.curve_persistence_worker import CurvePersistenceWorker
from core.quote_service import QuoteService


class PollWorkers:
    def __init__(
        self,
        quote_service: QuoteService,
        curve_service: CurveService,
        interval_seconds: float | None = None,
    ):
        self.quote_service = quote_service
        self.curve_service = curve_service
        self.interval_seconds = interval_seconds or read_poll_interval_seconds()
        self._stop_event = threading.Event()
        self._quote_cycle_lock = threading.Lock()
        self._curve_cycle_lock = threading.Lock()
        self._quote_thread: threading.Thread | None = None
        self._curve_thread: threading.Thread | None = None
        self._curve_persistence_worker = CurvePersistenceWorker()

    def start(self) -> None:
        self._stop_event.clear()
        if self._quote_thread is None or not self._quote_thread.is_alive():
            self._quote_thread = threading.Thread(
                target=self._quote_loop,
                name="market-data-quota-poll-worker",
                daemon=True,
            )
            self._quote_thread.start()
        if self._curve_thread is None or not self._curve_thread.is_alive():
            self._curve_thread = threading.Thread(
                target=self._curve_loop,
                name="market-data-curve-poll-worker",
                daemon=True,
            )
            self._curve_thread.start()
        self._curve_persistence_worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._curve_persistence_worker.stop()

    def join(self, timeout: float | None = None) -> None:
        if self._quote_thread is not None:
            self._quote_thread.join(timeout=timeout)
        if self._curve_thread is not None:
            self._curve_thread.join(timeout=timeout)
        self._curve_persistence_worker.join(timeout=timeout)

    def trigger_now(self) -> dict:
        quote_summary = self.run_quote_cycle()
        curve_summary = self.run_curve_cycle()
        return {
            "triggered_at": now_utc_iso(),
            "interval_seconds": self.interval_seconds,
            "quote_worker": quote_summary,
            "curve_worker": curve_summary,
        }

    def run_quote_cycle(self) -> dict:
        with self._quote_cycle_lock:
            poll_symbols = read_poll_symbols()
            provider_keys = list(self.quote_service.providers.keys())
            successful_requests = 0
            failed_requests = 0

            for poll_symbol in poll_symbols:
                for provider_key in provider_keys:
                    event_name, response_payload = self.quote_service.fetch_quote(
                        provider_key,
                        poll_symbol,
                    )
                    if event_name == "quote":
                        successful_requests += 1
                    else:
                        failed_requests += 1
                        logging.warning(
                            "Quota poll fetch failed provider=%s symbol=%s error=%s",
                            provider_key,
                            poll_symbol,
                            response_payload.get("error"),
                        )

            return {
                "time": now_utc_iso(),
                "providers": provider_keys,
                "symbols": poll_symbols,
                "requests": len(provider_keys) * len(poll_symbols),
                "success": successful_requests,
                "errors": failed_requests,
            }

    def run_curve_cycle(self) -> dict:
        with self._curve_cycle_lock:
            provider_keys = list(self.curve_service.providers.keys())
            successful_curve_payloads: list[dict] = []
            failed_curve_fetches = 0

            for provider_key in provider_keys:
                curve_payload = self.curve_service.get_curve(provider_key)
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

    def _quote_loop(self) -> None:
        logging.info("Quota poll worker started interval=%ss", self.interval_seconds)
        while not self._stop_event.is_set():
            cycle_started_monotonic = time.monotonic()
            try:
                cycle_summary = self.run_quote_cycle()
                logging.info("Quota poll cycle completed: %s", cycle_summary)
            except Exception:
                logging.exception("Quota poll cycle failed")
            cycle_elapsed_seconds = time.monotonic() - cycle_started_monotonic
            remaining_wait_seconds = max(
                self.interval_seconds - cycle_elapsed_seconds,
                0.0,
            )
            if self._stop_event.wait(remaining_wait_seconds):
                break
        logging.info("Quota poll worker stopped")

    def _curve_loop(self) -> None:
        logging.info("Curve poll worker started interval=%ss", self.interval_seconds)
        while not self._stop_event.is_set():
            cycle_started_monotonic = time.monotonic()
            try:
                cycle_summary = self.run_curve_cycle()
                logging.info("Curve poll cycle completed: %s", cycle_summary)
            except Exception:
                logging.exception("Curve poll cycle failed")
            cycle_elapsed_seconds = time.monotonic() - cycle_started_monotonic
            remaining_wait_seconds = max(
                self.interval_seconds - cycle_elapsed_seconds,
                0.0,
            )
            if self._stop_event.wait(remaining_wait_seconds):
                break
        logging.info("Curve poll worker stopped")
