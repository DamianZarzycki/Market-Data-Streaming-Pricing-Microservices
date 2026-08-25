import threading
from typing import TypedDict

from .config import generator_config


class GeneratorState(TypedDict):
    is_running: bool
    total_generated: int
    thread: threading.Thread | None


# Check-and-set of is_running is not atomic on ThreadedServer; two POST /start
# calls can both pass the guard (RACE-3). All start/stop transitions take this
# lock. _stop_event lets the worker wake immediately on /stop instead of
# sleeping out the remaining interval.
_state_lock = threading.Lock()
_stop_event = threading.Event()
_stop_event.set()

generator_state: GeneratorState = {
    "is_running": False,
    "total_generated": 0,
    "thread": None,
}


def expected_rate_per_sec():
    if not generator_state["is_running"]:
        return 0.0
    interval_ms = generator_config["interval_ms"]
    if interval_ms <= 0:
        return 0.0
    return round(1000.0 / interval_ms, 4)


def start_generator(target):
    """Start the generation worker. Returns False if one is already running."""
    with _state_lock:
        thread = generator_state["thread"]
        if generator_state["is_running"] or (thread is not None and thread.is_alive()):
            return False
        _stop_event.clear()
        generator_state["is_running"] = True
        worker_thread = threading.Thread(target=target, daemon=True)
        generator_state["thread"] = worker_thread
        worker_thread.start()
        return True


def stop_generator():
    """Request the generation worker to stop. Returns False if it was not running."""
    with _state_lock:
        thread = generator_state["thread"]
        if not generator_state["is_running"] and not (
            thread is not None and thread.is_alive()
        ):
            return False
        generator_state["is_running"] = False
        _stop_event.set()
        return True


def generator_should_stop():
    return _stop_event.is_set()


def wait_for_stop(timeout_s):
    """Block until /stop or timeout. Returns True if stop was requested."""
    return _stop_event.wait(timeout_s)


def mark_generator_stopped():
    with _state_lock:
        if generator_state["thread"] is not threading.current_thread():
            return
        generator_state["is_running"] = False
        generator_state["thread"] = None
        _stop_event.set()
