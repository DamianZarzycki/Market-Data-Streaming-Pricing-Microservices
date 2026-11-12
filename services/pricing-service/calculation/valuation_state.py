import asyncio
import threading


class ValuationState:
    """In-memory marks for one pricing process.

    Sync GET handlers run on the threadpool and the workers run on the event
    loop, so the mark map and the subscriber list stay behind locks. The queues
    are public because the workers await them directly.
    """

    def __init__(self) -> None:
        self._marks_lock = threading.Lock()
        self.valuations_store: dict[str, dict] = {}
        self.metrics_queue: asyncio.Queue = asyncio.Queue()
        self.trade_valuation_queue: asyncio.Queue = asyncio.Queue()
        self.sse_subscribers: list[asyncio.Queue] = []
        self._subscribers_lock = threading.Lock()
        self._external_yield_currencies: set[str] = set()
        self._curve_lock = threading.Lock()

    def store_mark(self, trade_id: str, valuation: dict, unchanged) -> bool:
        """Keep the mark when it changed. `unchanged` compares the previous payload."""
        with self._marks_lock:
            previous = self.valuations_store.get(trade_id)
            if unchanged(previous, valuation):
                return False
            self.valuations_store[trade_id] = valuation
            return True

    def mark_snapshot(self) -> list[dict]:
        with self._marks_lock:
            return list(self.valuations_store.values())

    def marks_copy(self) -> dict[str, dict]:
        with self._marks_lock:
            return self.valuations_store.copy()

    def mark_for(self, trade_id: str) -> dict | None:
        with self._marks_lock:
            return self.valuations_store.get(trade_id)

    def drop_inactive(self, active_ids) -> set[str]:
        """Remove marks whose trade left ACTIVE. Returns book ids that still have a mark."""
        with self._marks_lock:
            stale_ids = [
                trade_id for trade_id in list(self.valuations_store) if trade_id not in active_ids
            ]
            for trade_id in stale_ids:
                del self.valuations_store[trade_id]
            return {
                valuation.get("book_id")
                for valuation in self.valuations_store.values()
                if valuation.get("book_id")
            }

    def add_subscriber(self, client_queue: asyncio.Queue) -> None:
        with self._subscribers_lock:
            self.sse_subscribers.append(client_queue)

    def remove_subscriber(self, client_queue: asyncio.Queue) -> None:
        with self._subscribers_lock:
            if client_queue in self.sse_subscribers:
                self.sse_subscribers.remove(client_queue)

    def subscriber_queues(self) -> list[asyncio.Queue]:
        with self._subscribers_lock:
            return list(self.sse_subscribers)

    def set_external_yield_currencies(self, currencies) -> None:
        with self._curve_lock:
            self._external_yield_currencies = set(currencies)

    def has_external_yield_curve(self, currency: str) -> bool:
        with self._curve_lock:
            return currency in self._external_yield_currencies


valuation_state = ValuationState()
