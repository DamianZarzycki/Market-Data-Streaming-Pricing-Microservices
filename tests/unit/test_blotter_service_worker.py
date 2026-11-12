import asyncio
import sys

import pytest

from service_loader import StopWorker, load_service, silence_audit


@pytest.fixture(scope="module")
def blotter():
    load_service("blotter-service")
    return {"worker": sys.modules["worker"]}


@pytest.fixture(autouse=True)
def _reset(blotter):
    silence_audit(blotter["worker"].audit_logger)
    blotter["worker"].http_client = None
    yield
    blotter["worker"].http_client = None


def test_iter_valuations_yields_a_mark_and_skips_bad_frames(blotter):
    worker = blotter["worker"]

    class Response:
        def raise_for_status(self):
            return None

        async def aiter_lines(self):
            yield 'data: {"trade_id": "t1", "fair_value": 1}'
            yield 'data: {"fair_value": 2}'
            yield "data: not-json"
            yield "event: ping"
            yield "data: [1, 2]"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Client:
        def stream(self, method, url):
            return Response()

    worker.http_client = Client()

    async def read():
        return [item async for item in worker.iter_valuations()]

    assert asyncio.run(read()) == [{"trade_id": "t1", "fair_value": 1}]


def test_iter_valuations_propagates_a_connection_failure(blotter):
    worker = blotter["worker"]

    class Down:
        def stream(self, method, url):
            raise OSError("down")

    worker.http_client = Down()

    async def fail():
        async for _item in worker.iter_valuations():
            pass

    with pytest.raises(OSError):
        asyncio.run(fail())


def test_iter_valuations_requires_an_open_client(blotter):
    worker = blotter["worker"]

    async def fail():
        async for _item in worker.iter_valuations():
            pass

    with pytest.raises(RuntimeError):
        asyncio.run(fail())


def test_valuation_worker_updates_cache_and_backs_off(blotter, monkeypatch):
    worker = blotter["worker"]
    updated = []
    monkeypatch.setattr(
        worker.cache_service,
        "update_cache",
        lambda trade_id, data: updated.append((trade_id, data)),
    )
    connects = {"n": 0}

    async def iter_valuations():
        connects["n"] += 1
        if connects["n"] == 1:
            yield {"trade_id": "t1", "fair_value": 1}
            return
        raise ConnectionError("lost")

    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 2:
            raise StopWorker()

    monkeypatch.setattr(worker, "iter_valuations", iter_valuations)
    monkeypatch.setattr(worker.asyncio, "sleep", sleep)

    with pytest.raises(StopWorker):
        asyncio.run(worker.valuation_worker())

    assert updated == [("t1", {"trade_id": "t1", "fair_value": 1})]
    assert sleeps == [1, 2]


def test_valuation_worker_shutdown_does_not_audit_disconnect(blotter, monkeypatch):
    worker = blotter["worker"]
    calls = []

    async def iter_valuations():
        raise asyncio.CancelledError()
        yield None

    async def aerror(*args, **kwargs):
        calls.append(args)

    monkeypatch.setattr(worker, "iter_valuations", iter_valuations)
    monkeypatch.setattr(worker.audit_logger, "aerror", aerror)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker.valuation_worker())

    assert calls == []
