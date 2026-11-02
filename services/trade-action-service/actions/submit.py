import asyncio
import logging
import time
from typing import NoReturn

from fastapi import status
from pydantic import ValidationError

import metrics
import worker
from actions.provider_mode import use_provider_data
from actions.staleness import check_provider_data_staleness
from api.errors import ApiError, QueueFull
from api.schemas import (
    AcceptedResponse,
    ActionEnvelope,
    BatchAcceptedResponse,
    OpenTradeAction,
    parse_action,
)
from db.async_session import AsyncDBSessionManager
from shared.trading_shared.audit import append_audit
from shared.trading_shared.enums import ActionType, EventType, Severity

MAX_BATCH_SIZE = 100
_SERVICE_NAME = "trade-action-service"
_NOT_AN_OBJECT = "payload must be an object"
_OVERLOADED = "System overloaded, please try again later"
_BATCH_OVERLOADED = "System overloaded, queue is full"


def batch_too_long() -> str:
    return f"Batch exceeds the maximum of {MAX_BATCH_SIZE} trade actions"


async def _audit(event_type, severity, message, *, correlation_id="", payload=None) -> None:
    """Stage one audit row on its own async session and commit it.

    The 400/503 rows have no request transaction to join, so they open and
    commit their own session, the same way `AuditLogger.write(db=None)` did.
    """
    try:
        async with AsyncDBSessionManager() as db:
            append_audit(
                db.session,
                event_type,
                message,
                service_name=_SERVICE_NAME,
                severity=severity,
                correlation_id=correlation_id,
                payload=payload,
            )
            await db.commit()
    except Exception:
        logging.exception("Failed to persist audit log")


async def _rejection_reason(action: ActionEnvelope) -> str | None:
    error = action.identity_error()
    if error:
        return error
    if isinstance(action, OpenTradeAction):
        missing = action.missing_provider_field(use_provider_data())
        if missing:
            return missing
    provider = action.market_data_provider
    require_provider = use_provider_data()
    if provider or (
        require_provider and action.action_type == ActionType.OPEN_TRADE.value
    ):
        result = await check_provider_data_staleness(provider, action.symbol)
        if result.stale:
            return result.detail
    return None


async def _validate_item(item: dict) -> tuple[ActionEnvelope | None, str | None]:
    try:
        action = parse_action(item)
    except ValidationError:
        return None, "Invalid trade action"
    error = await _rejection_reason(action)
    if error:
        return None, error
    return action, None


async def _reject_single(item: dict, error: str) -> None:
    logging.warning("Rejected request: %s", error)
    await _audit(
        EventType.DB_REJECT,
        Severity.WARNING,
        f"Trade action rejected: {error}",
        payload={
            "action_type": item.get("action_type"),
            "client_request_id": item.get("client_request_id"),
        },
    )
    metrics.record_rejected(
        item.get("action_type"),
        item.get("client_request_id"),
        item.get("symbol"),
        error,
    )


def _enqueue(action: ActionEnvelope, item: dict) -> None:
    payload = action.model_dump()
    payload["_enqueued_at"] = time.time()
    client_request_id = item.get("client_request_id")
    worker.trade_queue.put_nowait(payload)
    logging.info("Enqueued request %s.", client_request_id)
    metrics.record_accepted(
        item.get("action_type"),
        client_request_id,
        item.get("symbol"),
    )


async def _overload(item: dict, *, batch: bool, accepted_so_far: int | None) -> NoReturn:
    client_request_id = item.get("client_request_id")
    if batch:
        logging.error("Queue full during batch processing.")
        await _audit(
            EventType.DB_ERROR,
            Severity.ERROR,
            "Trade action queue full during batch processing",
            correlation_id=client_request_id,
        )
        message = _BATCH_OVERLOADED
    else:
        logging.error("Queue full. Rejected request: %s", client_request_id)
        await _audit(
            EventType.DB_ERROR,
            Severity.ERROR,
            f"Trade action queue full. Rejected: {client_request_id}",
            correlation_id=client_request_id,
        )
        message = _OVERLOADED
    metrics.record_overload(
        item.get("action_type"),
        client_request_id,
        item.get("symbol"),
    )
    raise QueueFull(message, accepted_so_far=accepted_so_far)


async def submit_trade_action(item: dict) -> AcceptedResponse:
    logging.info("New /trade-actions request received")
    action, error = await _validate_item(item)
    if error or action is None:
        message = error or "Invalid trade action"
        await _reject_single(item, message)
        raise ApiError(status.HTTP_400_BAD_REQUEST, message)
    try:
        _enqueue(action, item)
    except asyncio.QueueFull:
        await _overload(item, batch=False, accepted_so_far=None)
    return AcceptedResponse(
        message="Trade action accepted for processing",
        client_request_id=item.get("client_request_id"),
    )


async def submit_trade_action_batch(items: list) -> BatchAcceptedResponse:
    logging.info("New /trade-actions/batch request received")
    accepted_count = 0
    errors = []

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            errors.append(f"Item {index}: {_NOT_AN_OBJECT}")
            metrics.record_rejected(None, None, None, _NOT_AN_OBJECT)
            continue

        action, error = await _validate_item(item)
        if error or action is None:
            errors.append(f"Item {index}: {error}")
            metrics.record_rejected(
                item.get("action_type"),
                item.get("client_request_id"),
                item.get("symbol"),
                error,
            )
            continue

        try:
            _enqueue(action, item)
        except asyncio.QueueFull:
            await _overload(item, batch=True, accepted_so_far=accepted_count)
        accepted_count += 1

    logging.info("Batch processed. Accepted: %s, Errors: %s", accepted_count, len(errors))
    return BatchAcceptedResponse(
        message="Batch processed",
        accepted_count=accepted_count,
        errors=errors,
    )
