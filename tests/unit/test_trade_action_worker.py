import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy import create_engine, event
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateIndex

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from service_loader import load_service
from shared.trading_shared.models import MarketDataProviderQuote
from shared.trading_shared.repositories import MarketDataProviderQuoteRepository


def _loaded():
    load_service("trade-action-service")
    return SimpleNamespace(
        open_trade=sys.modules["processing.open_trade"],
        close_trade=sys.modules["processing.close_trade"],
        pnl=sys.modules["processing.pnl"],
        metrics=sys.modules["metrics"],
        worker=sys.modules["worker"],
    )


def _instrument(**overrides):
    fields = {
        "instrument_id": uuid4(),
        "asset_class": "EQUITY",
        "multiplier": 1,
        "symbol": "AAPL",
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _open_db(instrument, inserted):
    db = MagicMock()
    db.instruments.get_by_symbol = AsyncMock(return_value=instrument)
    db.instruments.add = MagicMock()
    result = MagicMock()
    result.first.return_value = inserted
    db.execute = AsyncMock(return_value=result)
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.session = MagicMock()
    return db


def _trade(**overrides):
    fields = {
        "trade_id": uuid4(),
        "book_id": uuid4(),
        "asset_class": "EQUITY",
        "symbol": "AAPL",
        "side": "BUY",
        "trade_price": 10,
        "quantity": 2,
        "trade_currency": "USD",
        "market_data_provider": None,
        "metadata_payload": {},
        "status": "ACTIVE",
        "source": "GENERATED",
        "reference_price": None,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _close_db(trade):
    db = MagicMock()
    db.lock_trade = AsyncMock(return_value=trade)
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.provider_quotes.get_latest_close_price = AsyncMock(return_value=None)
    db.valuations.add = MagicMock()
    db.session = MagicMock()
    return db


def test_quote_index_orders_received_at_descending():
    index = next(
        item
        for item in MarketDataProviderQuote.__table__.indexes
        if item.name == "ix_provider_quotes_provider_symbol_received_at"
    )
    sql = str(CreateIndex(index).compile(dialect=postgresql.dialect()))
    assert "received_at DESC" in sql
    assert "(provider, symbol, received_at DESC)" in sql


def _capture_sql(method_name):
    engine = create_engine("sqlite://")
    session = sessionmaker(bind=engine)()
    captured = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture(conn, cursor, statement, parameters, context, executemany):
        captured.append(statement)
        return statement, parameters

    try:
        getattr(MarketDataProviderQuoteRepository(session), method_name)("ALPHA", "AAPL")
    except OperationalError:
        pass
    assert captured, method_name
    return captured[0].lower()


def test_freshness_and_close_reads_skip_the_raw_payload():
    freshness = _capture_sql("get_latest_freshness")
    close_price = _capture_sql("get_latest_close_price")
    full = _capture_sql("get_latest")
    assert "raw_payload" not in freshness
    assert "asset_class" in freshness
    assert "received_at" in freshness
    assert "raw_payload" not in close_price
    assert "bid" in close_price
    assert "ask" in close_price
    assert "raw_payload" in full


def test_zero_pnl_stays_zero_and_futures_use_the_contract_multiplier():
    pnl = _loaded().pnl
    assert pnl.realized_pnl(True, 10, 10, 5, 1) == 0.0
    assert pnl.realized_pnl(True, 10, 12, 2, 50) == 200.0
    assert pnl.realized_pnl(False, 12, 10, 2, 50) == 200.0
    assert pnl.futures_multiplier("EQUITY", {"contract_multiplier": 50}) == 1.0
    assert pnl.closing_market_value(12, 2, 50) == 1200.0


def test_reference_timestamp_accepts_iso_8601_with_z():
    parse = _loaded().open_trade.parse_reference_timestamp
    assert parse("2024-05-06T07:08:09Z") == datetime(2024, 5, 6, 7, 8, 9, tzinfo=timezone.utc)
    assert parse("2024-05-06T07:08:09+02:00") == datetime(2024, 5, 6, 5, 8, 9, tzinfo=timezone.utc)
    assert parse("2024-05-06T07:08:09") == datetime(2024, 5, 6, 7, 8, 9, tzinfo=timezone.utc)
    assert parse("06/05/2024") is None
    assert parse("") is None


def test_open_duplicate_uses_the_conflict_clause_not_a_select():
    mods = _loaded()
    db = _open_db(_instrument(), None)
    data = {
        "action_type": "OPEN_TRADE",
        "client_request_id": "c1",
        "symbol": "AAPL",
        "asset_class": "EQUITY",
        "trade_price": 10,
        "source": "trading_ticket",
        "reference_price_timestamp": "2024-05-06T07:08:09Z",
    }
    with patch.object(mods.metrics, "record_duplicate") as duplicate:
        opened = asyncio.run(
            mods.open_trade.open_trade(
                db,
                data,
                action_type="OPEN_TRADE",
                client_request_id="c1",
                symbol="AAPL",
                enqueued_at=1.0,
            )
        )
    assert opened is None
    duplicate.assert_called_once()
    db.trades.get_trades.assert_not_called()
    db.commit.assert_awaited_once()


def test_open_keeps_ticket_source_and_parsed_timestamp_until_commit():
    mods = _loaded()
    db = _open_db(_instrument(), (uuid4(),))
    data = {
        "action_type": "OPEN_TRADE",
        "client_request_id": "c1",
        "symbol": "AAPL",
        "asset_class": "EQUITY",
        "trade_price": 10,
        "source": "trading_ticket",
        "reference_price_timestamp": "not-a-date",
    }
    opened = asyncio.run(
        mods.open_trade.open_trade(
            db,
            data,
            action_type="OPEN_TRADE",
            client_request_id="c1",
            symbol="AAPL",
            enqueued_at=1.0,
        )
    )
    assert opened.trade_values["source"] == "TRADING_TICKET"
    assert opened.trade_values["reference_price_timestamp"] is None
    assert opened.process_note == "trade created"
    db.commit.assert_not_awaited()


def test_futures_open_stores_the_multiplier_on_the_instrument():
    mods = _loaded()
    instrument = _instrument(asset_class="FUTURES")
    db = _open_db(instrument, (uuid4(),))
    data = {
        "action_type": "OPEN_TRADE",
        "client_request_id": "c1",
        "symbol": "ES",
        "asset_class": "FUTURES",
        "trade_price": "10",
        "contract_multiplier": "50",
        "underlying_symbol": "SPY",
    }
    opened = asyncio.run(
        mods.open_trade.open_trade(
            db,
            data,
            action_type="OPEN_TRADE",
            client_request_id="c1",
            symbol="ES",
            enqueued_at=1.0,
        )
    )
    assert instrument.multiplier == 50.0
    assert opened.trade_values["metadata"]["contract_multiplier"] == 50.0
    assert opened.trade_values["trade_price"] == 10.0


def test_futures_open_without_a_multiplier_is_counted_as_an_error():
    mods = _loaded()
    db = _open_db(_instrument(asset_class="FUTURES"), (uuid4(),))
    data = {
        "action_type": "OPEN_TRADE",
        "client_request_id": "c1",
        "symbol": "ES",
        "asset_class": "FUTURES",
        "trade_price": 10,
    }
    with patch.object(mods.metrics, "record_error") as record_error:
        opened = asyncio.run(
            mods.open_trade.open_trade(
                db,
                data,
                action_type="OPEN_TRADE",
                client_request_id="c1",
                symbol="ES",
                enqueued_at=1.0,
            )
        )
    assert opened is None
    assert "FUTURES" in record_error.call_args[0][3]
    db.commit.assert_awaited_once()


def test_flat_close_writes_zero_pnl_and_does_not_commit_yet():
    mods = _loaded()
    trade = _trade()
    db = _close_db(trade)
    closed = asyncio.run(
        mods.close_trade.close_trade(
            db,
            {
                "action_type": "CLOSE_TRADE",
                "client_request_id": "c",
                "trade_id": str(trade.trade_id),
                "close_price": 10,
            },
            action_type="CLOSE_TRADE",
            client_request_id="c",
            symbol="AAPL",
            enqueued_at=1.0,
        )
    )
    valuation = db.valuations.add.call_args[0][0]
    assert closed.realized_pnl == 0.0
    assert valuation.realized_pnl == 0.0
    assert valuation.total_pnl == 0.0
    assert valuation.fair_value == 20.0
    assert trade.status == "CLOSED"
    db.commit.assert_not_awaited()


def test_futures_close_applies_the_contract_multiplier():
    mods = _loaded()
    trade = _trade(
        asset_class="FUTURES",
        symbol="ES",
        metadata_payload={"contract_multiplier": 50},
        trade_price=10,
        quantity=2,
    )
    db = _close_db(trade)
    closed = asyncio.run(
        mods.close_trade.close_trade(
            db,
            {
                "action_type": "CLOSE_TRADE",
                "client_request_id": "c",
                "trade_id": str(trade.trade_id),
                "close_price": 12,
            },
            action_type="CLOSE_TRADE",
            client_request_id="c",
            symbol="ES",
            enqueued_at=1.0,
        )
    )
    valuation = db.valuations.add.call_args[0][0]
    assert closed.realized_pnl == 200.0
    assert valuation.fair_value == 1200.0
    assert valuation.market_value == 1200.0


def test_provider_close_uses_the_book_side_and_not_the_full_quote_row():
    mods = _loaded()
    now = datetime.now(timezone.utc)
    quote = SimpleNamespace(bid=11, ask=13, last=9, provider_timestamp=None, received_at=now)

    long_trade = _trade(market_data_provider="ALPHA", trade_price=10, quantity=1)
    long_db = _close_db(long_trade)
    long_db.provider_quotes.get_latest_close_price = AsyncMock(return_value=quote)
    asyncio.run(
        mods.close_trade.close_trade(
            long_db,
            {"action_type": "CLOSE_TRADE", "client_request_id": "c", "trade_id": "t", "close_price": 99},
            action_type="CLOSE_TRADE",
            client_request_id="c",
            symbol="AAPL",
            enqueued_at=1.0,
        )
    )
    assert long_trade.close_price == 11
    long_db.provider_quotes.get_latest.assert_not_called()

    short_trade = _trade(side="SELL", market_data_provider="ALPHA", trade_price=10, quantity=1)
    short_db = _close_db(short_trade)
    short_db.provider_quotes.get_latest_close_price = AsyncMock(return_value=quote)
    asyncio.run(
        mods.close_trade.close_trade(
            short_db,
            {"action_type": "CLOSE_TRADE", "client_request_id": "c2", "trade_id": "t2"},
            action_type="CLOSE_TRADE",
            client_request_id="c2",
            symbol="AAPL",
            enqueued_at=1.0,
        )
    )
    assert short_trade.close_price == 13


def test_invalid_close_price_leaves_the_trade_active():
    mods = _loaded()
    trade = _trade()
    db = _close_db(trade)
    closed = asyncio.run(
        mods.close_trade.close_trade(
            db,
            {
                "action_type": "CLOSE_TRADE",
                "client_request_id": "c",
                "trade_id": str(trade.trade_id),
                "close_price": "nope",
            },
            action_type="CLOSE_TRADE",
            client_request_id="c",
            symbol="AAPL",
            enqueued_at=1.0,
        )
    )
    assert closed is None
    assert trade.status == "ACTIVE"
    db.rollback.assert_awaited_once()
    db.valuations.add.assert_not_called()


def test_shutdown_finishes_the_current_item_and_leaves_the_rest():
    mods = _loaded()
    worker = mods.worker

    async def scenario():
        worker.trade_queue = asyncio.Queue()
        seen = []
        started = asyncio.Event()

        async def handle(payload):
            seen.append(payload["client_request_id"])
            assert mods.metrics.is_worker_running()
            started.set()
            await asyncio.sleep(0.05)

        with patch.object(worker, "trade_action_handler", handle):
            worker.trade_queue.put_nowait({"action_type": "OPEN_TRADE", "client_request_id": "first"})
            worker.trade_queue.put_nowait({"action_type": "CLOSE_TRADE", "client_request_id": "second"})
            task = asyncio.create_task(worker.trade_action_worker())
            await started.wait()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert seen == ["first"]
        assert worker.trade_queue.get_nowait()["client_request_id"] == "second"
        assert mods.metrics.is_worker_running() is False

    asyncio.run(scenario())


def test_one_bad_payload_does_not_stop_the_consumer():
    mods = _loaded()
    worker = mods.worker

    async def scenario():
        worker.trade_queue = asyncio.Queue()
        seen = []
        done = asyncio.Event()

        async def handle(payload):
            if payload["client_request_id"] == "bad":
                raise RuntimeError("boom")
            seen.append(payload["client_request_id"])
            done.set()

        with patch.object(worker, "trade_action_handler", handle):
            worker.trade_queue.put_nowait({"action_type": "OPEN_TRADE", "client_request_id": "bad"})
            worker.trade_queue.put_nowait({"action_type": "OPEN_TRADE", "client_request_id": "good"})
            task = asyncio.create_task(worker.trade_action_worker())
            await asyncio.wait_for(done.wait(), timeout=2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert seen == ["good"]

    asyncio.run(scenario())
