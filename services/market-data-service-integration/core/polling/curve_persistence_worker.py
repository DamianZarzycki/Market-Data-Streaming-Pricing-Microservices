import logging
import queue
import threading
from datetime import datetime, timezone

from core.polling.config import (
    DEFAULT_CURVE_PERSIST_BATCH_SIZE,
    DEFAULT_CURVE_PERSIST_FLUSH_SECONDS,
    DEFAULT_CURVE_QUEUE_MAXSIZE,
    parse_curve_as_of_time,
    read_positive_float_env,
    read_positive_int_env,
)
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.models import MarketDataCurve


class CurvePersistenceWorker:
    def __init__(
        self,
        queue_max_size: int | None = None,
        persist_batch_size: int | None = None,
        flush_interval_seconds: float | None = None,
    ):
        self._queue_max_size = (
            queue_max_size
            if queue_max_size is not None
            else read_positive_int_env(
                "MARKET_DATA_CURVE_QUEUE_MAXSIZE",
                DEFAULT_CURVE_QUEUE_MAXSIZE,
            )
        )
        self._persist_batch_size = (
            persist_batch_size
            if persist_batch_size is not None
            else read_positive_int_env(
                "MARKET_DATA_CURVE_PERSIST_BATCH_SIZE",
                DEFAULT_CURVE_PERSIST_BATCH_SIZE,
            )
        )
        self._flush_interval_seconds = (
            flush_interval_seconds
            if flush_interval_seconds is not None
            else read_positive_float_env(
                "MARKET_DATA_CURVE_PERSIST_FLUSH_SECONDS",
                DEFAULT_CURVE_PERSIST_FLUSH_SECONDS,
            )
        )
        self._persist_queue: queue.Queue = queue.Queue(maxsize=self._queue_max_size)
        self._stop_requested = threading.Event()
        self._metrics_lock = threading.Lock()
        self._persisted_total = 0
        self._persist_error_total = 0
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop_requested.clear()
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._persist_loop,
                name="market-data-curve-db-worker",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def enqueue_many(self, curve_payloads: list[dict]) -> tuple[int, int]:
        enqueued_count = 0
        dropped_count = 0
        for curve_payload in curve_payloads:
            try:
                self._persist_queue.put_nowait(curve_payload)
                enqueued_count += 1
            except queue.Full:
                dropped_count += 1
                logging.error(
                    "Curve queue full (size=%s max=%s): dropping provider=%s curve=%s",
                    self._persist_queue.qsize(),
                    self._persist_queue.maxsize,
                    curve_payload.get("provider"),
                    curve_payload.get("curve_name"),
                )
        return enqueued_count, dropped_count

    def queue_size(self) -> int:
        return self._persist_queue.qsize()

    def totals(self) -> tuple[int, int]:
        with self._metrics_lock:
            return self._persisted_total, self._persist_error_total

    def _persist_loop(self) -> None:
        logging.info(
            "Curve DB worker started batch_size=%s flush=%ss queue_max=%s",
            self._persist_batch_size,
            self._flush_interval_seconds,
            self._persist_queue.maxsize,
        )
        curve_payload_batch: list[dict] = []
        while True:
            try:
                queued_payload = self._persist_queue.get(
                    timeout=self._flush_interval_seconds
                )
            except queue.Empty:
                self._flush_curve_payload_batch(curve_payload_batch)
                if self._stop_requested.is_set() and self._persist_queue.empty():
                    break
                continue

            curve_payload_batch.append(queued_payload)
            self._persist_queue.task_done()

            if len(curve_payload_batch) >= self._persist_batch_size:
                self._flush_curve_payload_batch(curve_payload_batch)
            if self._stop_requested.is_set() and self._persist_queue.empty():
                self._flush_curve_payload_batch(curve_payload_batch)
                break

        logging.info("Curve DB worker stopped")

    def _flush_curve_payload_batch(self, curve_payload_batch: list[dict]) -> None:
        if not curve_payload_batch:
            return
        curve_records = [
            curve_record
            for curve_record in (
                self._to_curve_record(curve_payload)
                for curve_payload in curve_payload_batch
            )
            if curve_record is not None
        ]
        dropped_payload_count = len(curve_payload_batch) - len(curve_records)
        curve_payload_batch.clear()

        if dropped_payload_count > 0:
            logging.error(
                "Dropped %s invalid curve payload(s) before DB persist",
                dropped_payload_count,
            )
        if not curve_records:
            return

        try:
            with DBSessionManager() as db:
                db.market_data.add_all(curve_records)
                db.commit()
            with self._metrics_lock:
                self._persisted_total += len(curve_records)
            logging.info("Persisted %s curve record(s) to DB", len(curve_records))
        except Exception:
            with self._metrics_lock:
                self._persist_error_total += len(curve_records)
            logging.exception(
                "Failed to persist curve batch of %s record(s); dropping batch",
                len(curve_records),
            )

    def _to_curve_record(self, curve_payload: dict) -> MarketDataCurve | None:
        curve_name = curve_payload.get("curve_name")
        curve_type = curve_payload.get("curve_type")
        provider = curve_payload.get("provider")
        tenors = curve_payload.get("tenors")
        rates = curve_payload.get("rates")
        if (
            not curve_name
            or not curve_type
            or not provider
            or not isinstance(tenors, list)
            or not isinstance(rates, list)
        ):
            logging.error("Skipping invalid curve payload: %s", curve_payload)
            return None
        if len(tenors) != len(rates):
            logging.error(
                "Skipping invalid curve payload (tenor/rate mismatch): %s",
                curve_payload,
            )
            return None

        as_of_time = parse_curve_as_of_time(curve_payload.get("as_of_time")) or datetime.now(
            timezone.utc
        )
        return MarketDataCurve(
            event_id=curve_payload.get("event_id"),
            curve_name=curve_name,
            curve_type=curve_type,
            currency=curve_payload.get("currency"),
            tenors=tenors,
            rates=rates,
            as_of_time=as_of_time,
            provider=provider,
            raw_payload=curve_payload.get("raw_payload") or curve_payload,
        )
