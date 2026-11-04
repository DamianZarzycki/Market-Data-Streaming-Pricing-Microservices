import asyncio
import logging

from shared.trading_shared.http_errors import ApiError
from generator.config import get_config
from shared.trading_shared.db import AsyncDBSessionManager
from generator.intentions import generate_and_send_to_trade_action_service
from generator.lifecycle import (
    generation_lock,
    generator_should_stop,
    mark_generator_stopped,
    record_send_failure,
    stop_event,
)
from shared.trading_shared.env import env_int
from shared.trading_shared.audit import append_audit
from shared.trading_shared.enums import EventType, Severity

# One OPEN or CLOSE at a time, slow enough to watch against live provider quotes.
PROVIDER_LOOP_INTERVAL_MS = env_int("TRADE_GENERATION_PROVIDER_LOOP_INTERVAL_MS", 15_000)
_SERVICE_NAME = "trade-generation-service"

async def _audit(event_type, message) -> None:
    """Write one worker lifecycle row on the async session.

    `append_audit` only stages the row. The commit is awaited on the shared
    async session.
    """
    try:
        async with AsyncDBSessionManager() as db:
            append_audit(
                db.session,
                event_type,
                message,
                service_name=_SERVICE_NAME,
                severity=Severity.INFO,
            )
            await db.commit()
    except Exception:
        logging.exception("Failed to persist audit log")


# Awaits one generate/send under the generation lock and handles its errors,
# so a cancelled worker's `await current` re-raises the cancel, not a send error.
async def _one_iteration() -> None:
    async with generation_lock():
        if generator_should_stop():
            return
        try:
            await generate_and_send_to_trade_action_service(wake_on_stop=True)
        except ApiError as exc:
            # The send path already counted this failure and logged the ids.
            logging.error("trade generation send failed: %s", exc.message)
        except Exception:
            logging.exception("trade generation iteration failed")
            record_send_failure()

# Each turn awaits one shielded generate/send, then the interval.
# POST /stop sets stop_event, which ends that wait.
async def trade_generation_worker():
    logging.info("Trade generation worker started.")
    await _audit(EventType.WORKER_STARTED, "Trade generation worker started")
    try:
        while not generator_should_stop():
            current = asyncio.create_task(_one_iteration())
            try:
                await asyncio.shield(current)
            except asyncio.CancelledError:
                await current
                raise
            if generator_should_stop():
                break
            config = get_config()
            interval_ms = config["interval_ms"]
            if config.get("use_provider_data"):
                interval_ms = max(interval_ms, PROVIDER_LOOP_INTERVAL_MS)
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    max(interval_ms, 50) / 1000.0,
                )
            except asyncio.TimeoutError:
                pass
    finally:
        mark_generator_stopped()
        logging.info("Trade generation worker stopped.")
        await _audit(EventType.WORKER_STOPPED, "Trade generation worker stopped")
