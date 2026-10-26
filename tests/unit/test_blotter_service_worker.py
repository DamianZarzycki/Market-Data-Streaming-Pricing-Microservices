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
    yield


def test_connect_to_valuation_stream_success_and_failure(blotter, monkeypatch):
    worker = blotter["worker"]
    response = object()
    monkeypatch.setattr(worker.request, "urlopen", lambda url, timeout=30: response)

    assert worker.connect_to_valuation_stream() is response

    def explode(url, timeout=30):
        raise OSError("down")

    monkeypatch.setattr(worker.request, "urlopen", explode)
    with pytest.raises(OSError):
        worker.connect_to_valuation_stream()


def test_valuation_worker_updates_cache_and_backs_off(blotter, monkeypatch):
    worker = blotter["worker"]
    updated = []
    monkeypatch.setattr(
        worker.cache_service,
        "update_cache",
        lambda trade_id, data: updated.append((trade_id, data)),
    )
    lines = [
        b'data: {"trade_id": "t1", "fair_value": 1}',
        b'data: {"fair_value": 2}',
        b"data: not-json",
        b"event: ping",
    ]
    connects = {"n": 0}

    def connect():
        connects["n"] += 1
        if connects["n"] == 1:
            return iter(lines)
        raise ConnectionError("lost")

    sleeps = []

    def sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 2:
            raise StopWorker()

    monkeypatch.setattr(worker, "connect_to_valuation_stream", connect)
    monkeypatch.setattr(worker, "sleep", sleep)

    with pytest.raises(StopWorker):
        worker.valuation_worker()

    assert updated == [("t1", {"trade_id": "t1", "fair_value": 1})]
    assert sleeps == [1, 2]
