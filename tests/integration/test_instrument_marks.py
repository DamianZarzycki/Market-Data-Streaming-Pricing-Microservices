"""Launch pricing-service and read the valuations it commits.

`python app.py` is whatever framework is in the tree. The test seeds the same
trades, serves the same tick and quote, then checks the rows in `valuations`.
Run it on a Bottle checkout and on a FastAPI checkout: both have to write these
numbers. Postgres is the compose database on 127.0.0.1:5433, in a database
created for this run.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SERVICE_DIR = BACKEND_ROOT / "services" / "pricing-service"
_OPENED = datetime(2026, 11, 5, tzinfo=timezone.utc)
_DB_NAME = "pricing_parity"

_BOOK_ID = uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:book")
_EQUITY_ID = uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:equity")
_QUOTE_ID = uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:quote")
_IRS_ID = uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:irs")

# bid 110 / ask 120 → mid 115; qty 10; entry 100.
_EQUITY = {"fair_value": 1150, "unrealized_pnl": 150, "current_price": 115, "provider": None}
# Long exits at the bid, not the mid. qty 4, entry 9, bid 10.
_QUOTE = {"fair_value": 40, "unrealized_pnl": 4, "current_price": 10, "provider": "TEST"}
# Flat 1Y zero curve: fixed leg 20_000, floating leg 0, pay fixed.
_IRS = {"fair_value": -20000, "unrealized_pnl": -20000, "provider": "FRED"}


def _dotenv_database_url():
    for line in (BACKEND_ROOT / ".env").read_text().splitlines():
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("DATABASE_URL is missing from backend/.env")


def _database_urls():
    """Admin URL and the empty database this run owns.

    Compose publishes Postgres on 127.0.0.1:5433. The URL inside .env uses the
    Docker hostname `postgres`, which a process on the host cannot resolve.
    """
    raw = os.environ.get("DATABASE_URL", "")
    if not raw or raw.startswith("sqlite"):
        raw = _dotenv_database_url()
    parsed = urlparse(raw)
    hostname = parsed.hostname or "127.0.0.1"
    port = parsed.port or 5432
    if hostname == "postgres":
        hostname = "127.0.0.1"
        port = 5433
    auth = parsed.username or ""
    if parsed.password:
        auth = f"{auth}:{parsed.password}"
    netloc = f"{auth}@{hostname}:{port}" if auth else f"{hostname}:{port}"
    admin = urlunparse(parsed._replace(netloc=netloc, path="/postgres"))
    owned = urlunparse(parsed._replace(netloc=netloc, path=f"/{_DB_NAME}"))
    return admin, owned


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _MarketData(BaseHTTPRequestHandler):
    """One equity tick on the stream, one quote for the provider trade."""

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/stream":
            self._stream()
            return
        if path == "/market-data/quotes/test/PARITY_Q":
            self._quote()
            return
        self.send_error(404)

    def _stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        tick = json.dumps(
            {"asset_type": "EQUITY", "symbol": "PARITY_EQ", "bid": 110, "ask": 120}
        )
        while True:
            try:
                self.wfile.write(f"data: {tick}\n\n".encode())
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                return
            time.sleep(0.25)

    def _quote(self):
        body = json.dumps(
            {
                "bid": 10,
                "ask": 12,
                "last": 11,
                "provider": "TEST",
                "provider_timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve(port):
    server = ThreadingHTTPServer(("127.0.0.1", port), _MarketData)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _engines():
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    return create_engine, text, sessionmaker


def _models():
    from shared.trading_shared.models import (
        Base,
        Book,
        Instrument,
        MarketDataCurve,
        Trade,
        Valuation,
    )

    return Base, Book, Instrument, MarketDataCurve, Trade, Valuation


def _create_database(admin_url):
    create_engine, text, _sessionmaker = _engines()
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": _DB_NAME},
        )
        conn.execute(text(f'DROP DATABASE IF EXISTS "{_DB_NAME}"'))
        conn.execute(text(f'CREATE DATABASE "{_DB_NAME}"'))
    engine.dispose()


def _drop_database(admin_url):
    create_engine, text, _sessionmaker = _engines()
    engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :name AND pid <> pg_backend_pid()"
                ),
                {"name": _DB_NAME},
            )
            conn.execute(text(f'DROP DATABASE IF EXISTS "{_DB_NAME}"'))
    finally:
        engine.dispose()


def _seed(url):
    create_engine, _text, sessionmaker = _engines()
    Base, Book, Instrument, MarketDataCurve, Trade, _Valuation = _models()
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(
        Book(
            book_id=_BOOK_ID,
            name="parity-book",
            expected_asset_class="MIXED",
            is_active=True,
        )
    )
    instruments = {
        ("EQUITY", "PARITY_EQ"): uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:inst:eq"),
        ("EQUITY", "PARITY_Q"): uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:inst:q"),
        ("IRS", "PARITY_IRS"): uuid.uuid5(uuid.NAMESPACE_URL, "pricing-parity:inst:irs"),
    }
    for (asset_class, symbol), instrument_id in instruments.items():
        session.add(
            Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                asset_class=asset_class,
                multiplier=1,
            )
        )

    def trade(trade_id, asset_class, symbol, quantity, price, provider, metadata):
        session.add(
            Trade(
                trade_id=trade_id,
                book_id=_BOOK_ID,
                asset_class=asset_class,
                instrument_id=instruments[(asset_class, symbol)],
                symbol=symbol,
                side="BUY",
                quantity=quantity,
                trade_price=price,
                trade_currency="USD",
                trade_date=_OPENED,
                status="ACTIVE",
                opened_at=_OPENED,
                source="GENERATED",
                market_data_provider=provider,
                metadata_payload=metadata,
            )
        )

    trade(_EQUITY_ID, "EQUITY", "PARITY_EQ", 10, 100, None, None)
    trade(_QUOTE_ID, "EQUITY", "PARITY_Q", 4, 9, "TEST", None)
    trade(
        _IRS_ID,
        "IRS",
        "PARITY_IRS",
        1,
        0,
        None,
        {
            "notional": 1_000_000,
            "fixed_rate": 0.02,
            "maturity_years": 1,
            "payments_per_year": 1,
            "direction": "PAY_FIXED_RECEIVE_FLOAT",
        },
    )
    session.add(
        MarketDataCurve(
            curve_name="USD_ZERO",
            curve_type="YIELD_CURVE",
            currency="USD",
            tenors=["1Y"],
            rates=[0.0],
            as_of_time=_OPENED,
            provider="FRED",
            raw_payload={},
        )
    )
    session.commit()
    session.close()
    engine.dispose()


def _start_service(database_url, market_port, pricing_port, log_path):
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_URL": database_url,
            "MARKET_DATA_SERVICE_URL": f"http://127.0.0.1:{market_port}",
            "MARKET_DATA_INTEGRATION_URL": f"http://127.0.0.1:{market_port}",
            "PRICING_SERVICE_URL": f"http://127.0.0.1:{pricing_port}",
            "PRICING_PORT": str(pricing_port),
            "PRICING_POLL_INTERVAL_SECONDS": "0.2",
            "PRICING_BASE_RECONNECT_DELAY_SECONDS": "0.2",
            "PYTHONPATH": os.pathsep.join((str(SERVICE_DIR), str(BACKEND_ROOT))),
            "PYTHONUNBUFFERED": "1",
        }
    )
    log = open(log_path, "w")
    process = subprocess.Popen(
        [sys.executable, "-u", "app.py"],
        cwd=SERVICE_DIR,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return process, log


def _stop_service(process, log):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
    log.close()


def _wait_until_up(port, log_path, process):
    deadline = time.time() + 120
    url = f"http://127.0.0.1:{port}/health"
    while time.time() < deadline:
        if process.poll() is not None:
            pytest.fail(_failure("pricing-service exited during startup", log_path))
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except URLError:
            time.sleep(0.2)
    pytest.fail(_failure("pricing-service did not open /health", log_path))


def _failure(message, log_path):
    log = Path(log_path).read_text(errors="replace")[-4000:]
    return f"{message}\n--- pricing-service log ---\n{log}"


def _latest_by_trade(session):
    _Base, _Book, _Instrument, _Curve, _Trade, Valuation = _models()
    rows = session.query(Valuation).order_by(Valuation.valuation_time.asc()).all()
    latest = {}
    for row in rows:
        latest[row.trade_id] = row
    return latest


def _close(actual, expected):
    assert float(actual) == pytest.approx(float(expected), abs=1e-4)


def test_running_service_writes_the_same_valuations(tmp_path):
    create_engine, text, sessionmaker = _engines()
    try:
        admin_url, database_url = _database_urls()
        probe = create_engine(admin_url, connect_args={"connect_timeout": 3})
        with probe.connect() as conn:
            conn.execute(text("SELECT 1"))
        probe.dispose()
    except Exception as exc:
        pytest.skip(f"Postgres on 127.0.0.1:5433 is not reachable ({exc})")

    market = None
    process = None
    log = None
    log_path = tmp_path / "pricing-service.log"
    _create_database(admin_url)
    try:
        _seed(database_url)
        market_port = _free_port()
        pricing_port = _free_port()
        market = _serve(market_port)
        process, log = _start_service(database_url, market_port, pricing_port, log_path)
        _wait_until_up(pricing_port, log_path, process)

        engine = create_engine(database_url)
        session = sessionmaker(bind=engine)()
        deadline = time.time() + 20
        latest = {}
        while time.time() < deadline:
            session.rollback()
            latest = _latest_by_trade(session)
            if {_EQUITY_ID, _QUOTE_ID, _IRS_ID} <= set(latest):
                break
            time.sleep(0.3)
        else:
            seen = sorted(str(trade_id) for trade_id in latest)
            pytest.fail(
                _failure(
                    f"valuations missing, rows so far: {seen or 'none'}",
                    log_path,
                )
            )

        equity = latest[_EQUITY_ID]
        _close(equity.fair_value, _EQUITY["fair_value"])
        _close(equity.market_value, _EQUITY["fair_value"])
        _close(equity.unrealized_pnl, _EQUITY["unrealized_pnl"])
        _close(equity.realized_pnl, 0)
        _close(equity.total_pnl, _EQUITY["unrealized_pnl"])
        _close(equity.valuation_payload["current_price"], _EQUITY["current_price"])
        assert equity.market_data_provider is None

        quote = latest[_QUOTE_ID]
        _close(quote.fair_value, _QUOTE["fair_value"])
        _close(quote.unrealized_pnl, _QUOTE["unrealized_pnl"])
        _close(quote.valuation_payload["current_price"], _QUOTE["current_price"])
        assert quote.market_data_provider == _QUOTE["provider"]
        assert quote.valuation_payload["is_stale"] is False

        irs = latest[_IRS_ID]
        _close(irs.fair_value, _IRS["fair_value"])
        _close(irs.unrealized_pnl, _IRS["unrealized_pnl"])
        _close(irs.total_pnl, _IRS["unrealized_pnl"])
        assert irs.market_data_provider == _IRS["provider"]
        assert irs.valuation_payload["pricing_details"]["direction"] == "PAY_FIXED_RECEIVE_FLOAT"

        session.close()
        engine.dispose()
    finally:
        if process is not None and log is not None:
            _stop_service(process, log)
        if market is not None:
            market.shutdown()
            market.server_close()
        _drop_database(admin_url)
