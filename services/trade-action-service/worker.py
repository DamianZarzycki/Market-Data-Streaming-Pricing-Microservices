import asyncio
import logging

import metrics
from processing.handler import trade_action_handler

# One asyncio.Queue, one consumer task. Producer is the request coroutine via
# put_nowait; both run on the same event loop (workers=1).
trade_queue: asyncio.Queue = asyncio.Queue(maxsize=metrics.QUEUE_MAXSIZE)


async def trade_action_worker() -> None:
    """Drain `trade_queue` on the event loop. Cancelled on shutdown.

    The in-flight handler is shielded so cancellation cannot tear a commit in
    half: the current item finishes, then the loop stops and the rest of the
    queue is dropped.
    """
    metrics.set_worker_running(True)
    try:
        while True:
            payload = await trade_queue.get()
            action_type = payload.get("action_type") if isinstance(payload, dict) else None
            client_request_id = (
                payload.get("client_request_id") if isinstance(payload, dict) else None
            )
            logging.info("Processing %s %s", action_type, client_request_id)
            current = asyncio.ensure_future(trade_action_handler(payload))
            try:
                await asyncio.shield(current)
            except asyncio.CancelledError:
                await current
                raise
            except Exception:
                logging.exception("Error processing %s %s", action_type, client_request_id)
            finally:
                trade_queue.task_done()
    finally:
        metrics.set_worker_running(False)
