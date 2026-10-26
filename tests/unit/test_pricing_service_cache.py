import sys

import pytest

from service_loader import load_service


class Trade:
    def __init__(self, trade_id):
        self.trade_id = trade_id


@pytest.fixture(scope="module")
def pricing():
    load_service("pricing-service")
    return {"cache": sys.modules["cache_service"]}


@pytest.fixture(autouse=True)
def _reset(pricing):
    pricing["cache"].active_trades_cache.clear()
    yield


def test_reload_replaces_the_active_trade_map(pricing, monkeypatch):
    cache = pricing["cache"]
    cache.active_trades_cache["old"] = Trade("old")

    class Trades:
        def get_trades(self, **kwargs):
            assert kwargs["status"] == "ACTIVE"
            assert kwargs["first_only"] is False
            return [Trade("new")]

    class DB:
        def __enter__(self):
            self.trades = Trades()
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "DBSessionManager", DB)
    cache.reload_active_trades_cache()

    assert list(cache.active_trades_cache) == ["new"]
    assert cache.active_trades_cache["new"].trade_id == "new"


def test_reload_keeps_the_previous_cache_when_the_database_fails(pricing, monkeypatch):
    cache = pricing["cache"]
    cache.active_trades_cache["kept"] = Trade("kept")

    class DB:
        def __enter__(self):
            raise RuntimeError("db down")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "DBSessionManager", DB)
    cache.reload_active_trades_cache()

    assert list(cache.active_trades_cache) == ["kept"]
