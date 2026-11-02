import asyncio
import logging
from typing import TypedDict

from .config import generator_config

_JOIN_TIMEOUT_S = 25


class GeneratorState(TypedDict):
    is_running: bool
    total_generated: int
    send_failures: int
    task: asyncio.Task | None


# One event loop (`uvicorn --workers 1`). Dict updates do not await, so they
# do not need a thread lock. `stop_event` is set while the worker is stopped
# and stays clear for the whole run, so `Event.wait()` blocks. `/stop` sets it
# and the interval wait returns.
stop_event = asyncio.Event()
stop_event.set()

# Held for one generate/send, and for a config publish that includes the
# provider-mode PUT. The worker releases it before waiting out the interval.
_generation_lock = asyncio.Lock()

generator_state: GeneratorState = {
    "is_running": False,
    "total_generated": 0,
    "send_failures": 0,
    "task": None,
}


def generation_lock() -> asyncio.Lock:
    return _generation_lock


def generator_is_running() -> bool:
    
    return generator_state["is_running"]


def snapshot_status() -> dict:
    running = generator_state["is_running"]
    interval_ms = generator_config["interval_ms"]
    if not running or interval_ms <= 0:
        rate = 0.0
    else:
        rate = round(1000.0 / interval_ms, 4)
    return {
        "is_running": running,
        "total_generated": generator_state["total_generated"],
        "send_failures": generator_state["send_failures"],
        "expected_rate_per_sec": rate,
    }


def expected_rate_per_sec():
    return snapshot_status()["expected_rate_per_sec"]


def record_generated(count: int = 1) -> None:
    if count <= 0:
        return
    generator_state["total_generated"] += count


def record_send_failure() -> None:
    """Count a failed send in memory. Does not open a database session."""
    generator_state["send_failures"] += 1


def generator_should_stop() -> bool:
    return stop_event.is_set()


async def pause_for_retry(delay_s: float, *, wake_on_stop: bool) -> bool:
    """Wait up to `delay_s`. Return True when the worker should stop retrying.

    Route calls pass `wake_on_stop=False` and sleep the full delay. The worker
    passes True. `/stop` sets `stop_event`, and that ends the wait early.
    """
    if not wake_on_stop:
        await asyncio.sleep(delay_s)
        return False
    try:
        await asyncio.wait_for(stop_event.wait(), delay_s)
    except asyncio.TimeoutError:
        return False
    return True


async def start_generator(target) -> bool:
    """Schedule the generation task. Returns False if one is already running.

    There is no `await` between the check and `create_task`, so a second
    `POST /start` cannot pass the guard.
    """
    task = generator_state["task"]
    if task is not None and not task.done():
        return False
    stop_event.clear()
    generator_state["is_running"] = True
    try:
        generator_state["task"] = asyncio.create_task(
            target(),
            name="trade-generation-worker",
        )
    except Exception:
        generator_state["task"] = None
        generator_state["is_running"] = False
        stop_event.set()
        raise
    return True


async def stop_generator() -> bool:
    """Stop the worker and wait for the in-flight send. False if it was not running.

    After `_JOIN_TIMEOUT_S`, `wait_for` cancels the worker and still waits
    while it finishes the shielded send. The task reference is cleared only
    after that same task has finished, so a stop cannot drop a newer task.
    """
    task = generator_state["task"]
    alive = task is not None and not task.done()
    if not generator_state["is_running"] and not alive:
        return False
    generator_state["is_running"] = False
    stop_event.set()
    if task is not None and not task.done():
        try:
            await asyncio.wait_for(task, _JOIN_TIMEOUT_S)
        except asyncio.TimeoutError:
            logging.error(
                "trade generation worker did not stop within %ss and was cancelled",
                _JOIN_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            # Swallow the worker's own cancellation; re-raise one aimed at this call.
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
    if generator_state["task"] is task and (task is None or task.done()):
        generator_state["task"] = None
    return True


def mark_generator_stopped() -> None:
    """Worker exit hook. Does not clear the task reference.

    `stop_generator` drops the reference only after `task.done()`.
    """
    generator_state["is_running"] = False
    stop_event.set()
