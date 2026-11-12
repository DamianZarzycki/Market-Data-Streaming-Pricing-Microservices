import sys

import pytest

from service_loader import load_service


class Trade:
    def __init__(self, trade_id, **kwargs):
        self.trade_id = trade_id
        self.book_id = kwargs.get("book_id", "b")
        self.symbol = kwargs.get("symbol", "ACME")
        self.asset_class = kwargs.get("asset_class", "EQUITY")
        self.side = kwargs.get("side", "BUY")
        self.quantity = kwargs.get("quantity", 1)
        self.trade_price = kwargs.get("trade_price", 1)
        self.trade_currency = kwargs.get("trade_currency", "USD")
        self.metadata_payload = kwargs.get("metadata_payload")
        self.market_data_provider = kwargs.get("market_data_provider")


@pytest.fixture(scope="module")
def pricing():
    load_service("pricing-service")
    return {"cache": sys.modules["cache_service"]}


@pytest.fixture(autouse=True)
def _reset(pricing):
    cache = pricing["cache"]
    cache.active_trades_cache.clear()
    cache.symbol_index.clear()
    yield


def _row(trade_id, **kwargs):
    trade = Trade(trade_id, **kwargs)
    return trade


def test_reload_keeps_every_active_trade(pricing, monkeypatch):
    cache = pricing["cache"]
    cache.active_trades_cache["old"] = Trade("old")
    rows = [_row(str(index)) for index in range(51)]

    class Trades:
        async def list_active_for_pricing(self):
            return rows

    class DB:
        async def __aenter__(self):
            self.trades = Trades()
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "AsyncDBSessionManager", lambda: DB())

    import asyncio

    active_ids = asyncio.run(cache.reload_active_trades_cache())

    assert active_ids == {str(index) for index in range(51)}
    assert "old" not in cache.active_trades_cache
    assert len(cache.active_trades_cache) == 51
    assert len(cache.simulator_trades("EQUITY", "ACME")) == 51
    assert cache.active_trades_cache["0"].trade_id == "0"


def test_reload_keeps_the_previous_cache_when_the_database_fails(pricing, monkeypatch):
    cache = pricing["cache"]
    cache.active_trades_cache["kept"] = Trade("kept")

    class DB:
        async def __aenter__(self):
            raise RuntimeError("db down")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "AsyncDBSessionManager", lambda: DB())

    import asyncio

    assert asyncio.run(cache.reload_active_trades_cache()) is None
    assert list(cache.active_trades_cache) == ["kept"]


def test_symbol_index_skips_provider_trades(pricing):
    cache = pricing["cache"]
    cache.replace_cache(
        {
            "sim": Trade("sim", symbol="ACME", asset_class="EQUITY"),
            "other": Trade("other", symbol="MSFT", asset_class="EQUITY"),
            "prov": Trade(
                "prov",
                symbol="ACME",
                asset_class="EQUITY",
                market_data_provider="FINNHUB",
            ),
        }
    )

    matched = cache.simulator_trades("EQUITY", "ACME")
    assert [trade.trade_id for trade in matched] == ["sim"]
