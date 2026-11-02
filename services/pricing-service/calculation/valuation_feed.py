import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from fastapi import status

from api.errors import ApiError
from calculation.valuation_state import valuation_state

SSE_QUEUE_MAX = 256
_UNCHANGED_FIELDS = ("fair_value", "current_price", "unrealized_pnl", "is_stale")


def _json_safe(value):
    """Copy a valuation payload into types json.dumps and JSONB accept.

    Curve rows carry as_of_time as a datetime. Leaving it in the payload
    makes /valuation-stream and the valuations insert return 500.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _same_mark(previous, valuation_data):
    if previous is None:
        return False
    return all(previous.get(field) == valuation_data.get(field) for field in _UNCHANGED_FIELDS)


class ValuationFeed:
    """Publishes a changed mark to the store, the DB queue, metrics, and SSE."""

    def offer_sse(self, subscriber_queue, valuation_data):
        """Keep the newest frame. A slow client loses the oldest one, not the live mark."""
        try:
            subscriber_queue.put_nowait(valuation_data)
            return
        except asyncio.QueueFull:
            pass
        try:
            subscriber_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            subscriber_queue.put_nowait(valuation_data)
        except asyncio.QueueFull:
            pass

    def store_and_publish(self, trade, valuation_data):
        valuation_data = _json_safe(valuation_data)
        trade_id = valuation_data["trade_id"]
        if not valuation_state.store_mark(trade_id, valuation_data, _same_mark):
            return

        logging.debug(
            f"Priced trade {trade_id} ({valuation_data.get('symbol')}, {valuation_data.get('asset_class')}): "
            f"fair_value={valuation_data.get('fair_value')}, unrealized_pnl={valuation_data.get('unrealized_pnl')}"
        )

        valuation_state.trade_valuation_queue.put_nowait(valuation_data)
        valuation_state.metrics_queue.put_nowait(
            {
                "type": "PRICING_DONE",
                "timestamp": valuation_data["valuation_time"],
                "trade_id": trade_id,
                "instrument": trade_id,
                "symbol": valuation_data.get("symbol"),
                "asset_class": valuation_data.get("asset_class"),
                "value": valuation_data.get("fair_value"),
            }
        )
        for subscriber_queue in valuation_state.subscriber_queues():
            self.offer_sse(subscriber_queue, valuation_data)

    def subscribe(self):
        client_queue = asyncio.Queue(maxsize=SSE_QUEUE_MAX)
        valuation_state.add_subscriber(client_queue)
        logging.info("Client connected to /valuation-stream for SSE.")
        return client_queue

    def unsubscribe(self, client_queue):
        valuation_state.remove_subscriber(client_queue)
        logging.info("Client disconnected from /valuation-stream.")

    def _sse_frame(self, valuation_data):
        try:
            payload = json.dumps(valuation_data)
        except Exception as exc:
            logging.error(f"Error serializing valuation for SSE: {exc}")
            return None
        return f"event: valuation_update\ndata: {payload}\n\n"

    # Async generator: awaits asyncio.Queue.get, which suspends this task and holds no thread.
    async def iter_events(self, client_queue):
        try:
            for valuation_data in valuation_state.mark_snapshot():
                frame = self._sse_frame(valuation_data)
                if frame is None:
                    continue
                yield frame
            while True:
                valuation_data = await client_queue.get()
                if valuation_data is None:
                    break
                logging.debug(
                    f"Streaming valuation update for trade {valuation_data.get('trade_id')}"
                )
                frame = self._sse_frame(valuation_data)
                if frame is None:
                    continue
                yield frame
        finally:
            self.unsubscribe(client_queue)

    def valuations_view(self):
        return valuation_state.marks_copy()

    def valuation_for(self, trade_id):
        valuation = valuation_state.mark_for(trade_id)
        if valuation is None:
            raise ApiError(
                status.HTTP_404_NOT_FOUND,
                "Valuation not found for trade_id: " + trade_id,
            )
        return {"trade_id": trade_id, **valuation}

    def evict_inactive(self, active_ids):
        """Drop marks and book metrics for trades that left the ACTIVE set."""
        from books import book_metrics_service

        remaining_books = valuation_state.drop_inactive(active_ids)
        book_metrics_service.prune_books(remaining_books)


valuation_feed = ValuationFeed()
