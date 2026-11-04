"""Load one flat Bottle service at a time and call it over WSGI."""

import io
import json
import os
import sys
from pathlib import Path

if not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SERVICES_ROOT = BACKEND_ROOT / "services"

_COLLIDING = (
    "app",
    "worker",
    "calculation",
    "calculation_service",
    "trades_service",
    "books",
    "cache",
    "cache_service",
    "valuation_service",
    "custom_server",
    "market_data_simulator",
    "instruments_pricing",
    "models",
    "state",
    "routes",
    "api",
    "core",
    "providers",
    "metrics",
    "actions",
    "processing",
)


class StopWorker(BaseException):
    """Ends a `while True` worker. `except Exception` does not swallow it."""


def _colliding(name):
    return name in _COLLIDING or any(name.startswith(prefix + ".") for prefix in _COLLIDING)


def purge_service_modules():
    for name in list(sys.modules):
        if _colliding(name):
            del sys.modules[name]


def _prepare_path(service_dir):
    service_dirs = {str(path) for path in SERVICES_ROOT.iterdir() if path.is_dir()}
    sys.path[:] = [entry for entry in sys.path if entry not in service_dirs]
    for entry in (str(BACKEND_ROOT), str(service_dir)):
        if entry in sys.path:
            sys.path.remove(entry)
    sys.path.insert(0, str(BACKEND_ROOT))
    sys.path.insert(0, str(service_dir))


def load_service(service_name):
    """Import `app` (and the modules it pulls in) from one service directory."""
    service_dir = SERVICES_ROOT / service_name
    purge_service_modules()
    _prepare_path(service_dir)
    __import__("app")
    return sys.modules["app"]


def invoke(app, path, query_string=""):
    """Run one GET. Returns (status_code, headers, body_iterator)."""
    captured = {}

    def start_response(status, headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = headers

    environ = {
        "REQUEST_METHOD": "GET",
        "SCRIPT_NAME": "",
        "PATH_INFO": path,
        "QUERY_STRING": query_string,
        "SERVER_NAME": "localhost",
        "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(b""),
        "wsgi.errors": io.StringIO(),
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "CONTENT_TYPE": "",
        "CONTENT_LENGTH": "0",
        "HTTP_HOST": "localhost",
    }
    iterator = app.wsgi(environ, start_response)
    status_code = int(captured["status"].split()[0])
    headers = {key.lower(): value for key, value in captured["headers"]}
    return status_code, headers, iterator


def collect(iterator):
    """Drain a WSGI body. A RuntimeError from an SSE generator is returned, not raised."""
    chunks = []
    error = None
    try:
        for chunk in iterator:
            if isinstance(chunk, str):
                chunk = chunk.encode()
            if chunk:
                chunks.append(chunk)
    except RuntimeError as exc:
        error = exc
    finally:
        close = getattr(iterator, "close", None)
        if close:
            close()
    return b"".join(chunks), error


def read_json(app, path, query_string=""):
    status, headers, iterator = invoke(app, path, query_string)
    body, error = collect(iterator)
    assert error is None
    payload = json.loads(body.decode() or "null")
    return status, headers, payload


class RecordingDB:
    """Stand-in for DBSessionManager. Records writes and can fail on enter or commit."""

    def __init__(self, error=None, commit_error=None):
        self.error = error
        self.commit_error = commit_error
        self.records = []
        self.commits = 0
        self.market_data = self
        self.valuations = self
        self.trades = self
        self.session = self

    def __enter__(self):
        if self.error:
            raise self.error
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def add_all(self, rows):
        self.records.extend(list(rows))

    def add(self, row):
        self.records.append(row)

    def commit(self):
        if self.commit_error:
            raise self.commit_error
        self.commits += 1
        return _Immediate()

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb):
        return self.__exit__(exc_type, exc, tb)


class _Immediate:
    """Await finishes immediately. The commit side effect already ran."""

    def __await__(self):
        return iter(())


def silence_audit(audit_logger):
    audit_logger.info = lambda *args, **kwargs: None
    audit_logger.error = lambda *args, **kwargs: None
    audit_logger.warning = lambda *args, **kwargs: None

    async def _quiet(*args, **kwargs):
        return None

    audit_logger.ainfo = _quiet
    audit_logger.aerror = _quiet
    audit_logger.awarning = _quiet
