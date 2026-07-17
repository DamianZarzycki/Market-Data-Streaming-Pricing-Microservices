import logging
import os
import sys

DEFAULT_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(service)s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _resolve_level(level=None):
    """Resolve the effective log level from an explicit value or the LOG_LEVEL env var."""
    if level is not None:
        return level
    env_level = os.getenv("LOG_LEVEL", DEFAULT_LEVEL).upper()
    return getattr(logging, env_level, logging.INFO)


class _ServiceFilter(logging.Filter):
    """Injects the service name onto every record so it can be used in the format string."""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "service"):
            record.service = self.service_name
        return True


def configure_logging(service_name: str = "app", level=None) -> logging.Logger:
    """Configure the root logger once per process with a consistent, service-tagged format.

    Call this exactly once at each service entrypoint. All module-level ``logging.*``
    calls and ``get_logger(__name__)`` loggers then inherit this configuration.
    """
    resolved_level = _resolve_level(level)
    root = logging.getLogger()
    root.setLevel(resolved_level)

    # Drop any handlers from a previous call (or a stray basicConfig) to avoid duplicate lines.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    handler.addFilter(_ServiceFilter(service_name))
    root.addHandler(handler)

    return root


def get_logger(name: str = None) -> logging.Logger:
    """Return a named logger that inherits the centrally configured handlers/format."""
    return logging.getLogger(name)
