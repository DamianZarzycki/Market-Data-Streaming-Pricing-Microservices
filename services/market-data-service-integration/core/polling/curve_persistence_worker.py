import asyncio
import logging
import threading
from datetime import datetime, timezone

from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.env import env_float, env_int
from shared.trading_shared.models import MarketDataCurve, MarketDataSnapshot


def parse_curve_as_of_time(as_of_time_value: object) -> datetime | None:
    if as_of_time_value is None:
        return None
    if isinstance(as_of_time_value, datetime):
        if as_of_time_value.tzinfo is None:
            return as_of_time_value.replace(tzinfo=timezone.utc)
        return as_of_time_value
    if not isinstance(as_of_time_value, str) or not as_of_time_value.strip():
        return None

    timestamp_text = as_of_time_value.strip()
    try:
        if timestamp_text.endswith("Z"):
            return datetime.strptime(timestamp_text, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        parsed_datetime = datetime.fromisoformat(timestamp_text)
        if parsed_datetime.tzinfo is None:
            return parsed_datetime.replace(tzinfo=timezone.utc)
        return parsed_datetime
    except ValueError:
        logging.warning(
            "Invalid curve as_of_time=%r; using current UTC time",
            as_of_time_value,
        )
        return None


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
            else env_int("MARKET_DATA_CURVE_QUEUE_MAXSIZE", 1000)
        )
        self._persist_batch_size = (
            persist_batch_size
            if persist_batch_size is not None
            else env_int("MARKET_DATA_CURVE_PERSIST_BATCH_SIZE", 20)
        )
        self._flush_interval_seconds = (
            flush_interval_seconds
            if flush_interval_seconds is not None
            else env_float("MARKET_DATA_CURVE_PERSIST_FLUSH_SECONDS", 2.0, minimum=0.1)
        )
        self._persist_queue: asyncio.Queue = asyncio.Queue(maxsize=self._queue_max_size)
        self._metrics_lock = threading.Lock()
        self._persisted_total = 0
        self._persist_error_total = 0

    def enqueue_many(self, curve_payloads: list[dict]) -> tuple[int, int]:
        """put_nowait does not wait, so a full queue drops the curve instead of blocking."""
        enqueued_count = 0
        dropped_count = 0
        for curve_payload in curve_payloads:
            try:
                self._persist_queue.put_nowait(curve_payload)
                enqueued_count += 1
            except asyncio.QueueFull:
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

    # Async task: awaits the curve queue, then AsyncSession.commit for each batch.
    async def persist_loop(self) -> None:
        logging.info(
            "Curve DB worker started batch_size=%s flush=%ss queue_max=%s",
            self._persist_batch_size,
            self._flush_interval_seconds,
            self._persist_queue.maxsize,
        )
        curve_payload_batch: list[dict] = []
        try:
            while True:
                try:
                    queued_payload = await asyncio.wait_for(
                        self._persist_queue.get(),
                        timeout=self._flush_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    await self._flush_curve_payload_batch(curve_payload_batch)
                    continue

                curve_payload_batch.append(queued_payload)
                self._persist_queue.task_done()
                if len(curve_payload_batch) >= self._persist_batch_size:
                    await self._flush_curve_payload_batch(curve_payload_batch)
        except asyncio.CancelledError:
            await self._flush_curve_payload_batch(curve_payload_batch)
            logging.info("Curve DB worker stopped")
            raise

    # Async: awaits AsyncSession.commit.
    async def _flush_curve_payload_batch(self, curve_payload_batch: list[dict]) -> None:
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

        snapshot_records = [
            MarketDataSnapshot(
                snapshot_type="CURVE",
                snapshot_time=curve_record.as_of_time,
                provider=curve_record.provider,
                symbol_or_curve_name=curve_record.curve_name,
                normalized_payload={
                    "curve_type": curve_record.curve_type,
                    "currency": curve_record.currency,
                    "tenors": curve_record.tenors,
                    "rates": curve_record.rates,
                },
                payload=curve_record.raw_payload or {},
            )
            for curve_record in curve_records
        ]

        try:
            async with AsyncDBSessionManager() as db:
                db.market_data.add_all(curve_records)
                for snapshot_record in snapshot_records:
                    db.market_data_snapshots.add(snapshot_record)
                await db.commit()
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
        if len(tenors) == 0 or len(rates) == 0 or len(tenors) != len(rates):
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
