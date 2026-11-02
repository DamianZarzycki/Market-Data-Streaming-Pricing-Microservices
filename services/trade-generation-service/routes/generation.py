import logging
from typing import Any

from fastapi import Body, FastAPI

import worker
from api.errors import ApiError
from generator.intentions import (
    generate_and_send_to_trade_action_service,
    generate_and_send_to_trade_action_service_batch,
)
from generator.lifecycle import (
    generation_lock,
    generator_is_running,
    start_generator,
    stop_generator,
)


def register(app: FastAPI) -> None:
    # Async: may read Postgres and post to trade-action. The lock is shared
    # with the worker task on this loop.
    @app.get("/generate-once")
    async def generate_once():
        logging.info("Received request to generate a trade intention")
        async with generation_lock():
            if not generator_is_running():
                logging.info("DB worker not running")
            intention = await generate_and_send_to_trade_action_service()
        return intention if intention else {"message": "No trade intention generated."}

    # Async: forwards the caller's body. 400 is a bad payload or a trade-action
    # rejection. 502 means the POST did not land; resubmit the same body.
    @app.post("/generate-trade")
    async def generate_trade(payload: Any = Body(default=None)):
        logging.info("Received request to generate a trade intention with payload")
        if not payload:
            raise ApiError(400, "Missing payload data")
        try:
            async with generation_lock():
                intention = await generate_and_send_to_trade_action_service(payload)
        except ValueError as exc:
            raise ApiError(400, str(exc)) from exc
        return intention if intention else {"message": "No trade intention generated."}

    # Async: one close query, then one batch POST.
    @app.get("/generate-batch")
    async def generate_multiple():
        logging.info("Received request to generate multiple trade intentions")
        async with generation_lock():
            if not generator_is_running():
                logging.info("DB worker not running")
            intentions = await generate_and_send_to_trade_action_service_batch()
        if intentions:
            return {
                "message": f"Successfully generated {len(intentions)} intentions",
                "intentions": intentions,
            }
        return {"message": "No trade intentions generated."}

    # Async: returns after the task is scheduled. No await before create_task.
    @app.post("/start")
    async def start_generation():
        if not await start_generator(worker.trade_generation_worker):
            raise ApiError(400, "Generator is already running!")
        return {"message": "Generator started successfully."}

    # Async: awaits the in-flight iteration, up to the stop timeout.
    @app.post("/stop")
    async def stop_generation():
        if not await stop_generator():
            raise ApiError(400, "No data available.")
        return {"message": "Generator was stopped."}
