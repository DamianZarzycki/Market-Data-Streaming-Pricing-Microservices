import logging
from typing import Any

from fastapi import Body, FastAPI

from api.errors import ApiError
from generator.config import get_config, publish_config, validated_config
from generator.http import set_trade_action_provider_mode
from generator.lifecycle import generation_lock


async def apply_config(payload) -> dict:
    """Validate, push provider mode, then publish. The local dict changes last.

    The generation lock waits until the in-flight intention has finished, so
    the worker cannot emit under a flag trade-action has not accepted.
    """
    async with generation_lock():
        nxt = validated_config(payload)
        previous = get_config()
        if nxt["use_provider_data"] != previous["use_provider_data"]:
            await set_trade_action_provider_mode(nxt["use_provider_data"])
        publish_config(nxt)
        logging.info("Config updated: %s", nxt)
        return get_config()


def register(app: FastAPI) -> None:
    # Async: returns a copy of the in-memory config. No I/O.
    @app.get("/config")
    async def get_config_route():
        return get_config()

    # Async: the provider-mode PUT is awaited while the generation lock is held.
    @app.put("/config")
    async def put_config(payload: Any = Body(default=None)):
        try:
            config = await apply_config(payload)
        except ValueError as exc:
            raise ApiError(400, str(exc)) from exc
        return {
            "message": "Config updated successfully.",
            "config": config,
        }
