import logging
import queue
from datetime import datetime, timezone
from decimal import Decimal

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import EntityType, EventType
from shared.trading_shared.models import MarketDataProviderQuote

db_queue = queue.Queue()
audit_logger = AuditLogger("market-data-service-integration")


def _parse_timestamp(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def db_worker():
    logging.info("Provider quote DB worker started")
    while True:
        quote = db_queue.get()
        provider_name = quote.get("provider")
        symbol = quote.get("symbol")
        try:
            record = MarketDataProviderQuote(
                provider=provider_name,
                asset_class=quote["asset_class"],
                symbol=symbol,
                bid=None if quote.get("bid") is None else Decimal(str(quote["bid"])),
                ask=None if quote.get("ask") is None else Decimal(str(quote["ask"])),
                last=None if quote.get("last") is None else Decimal(str(quote["last"])),
                currency=quote.get("currency"),
                provider_timestamp=_parse_timestamp(quote.get("provider_timestamp")),
                received_at=_parse_timestamp(quote.get("received_at")) or datetime.now(timezone.utc),
                raw_payload=quote.get("raw_payload") or {},
            )
            with DBSessionManager() as db:
                db.provider_quotes.add(record)
                db.commit()
            logging.info("Stored %s quote for %s last=%s", provider_name, symbol, quote.get("last"))
            audit_logger.info(
                EventType.DB_CREATE,
                f"Stored {provider_name} quote for {symbol}",
                entity_type=EntityType.MARKET_DATA.value,
                payload={"provider": provider_name, "symbol": symbol, "last": quote.get("last")},
            )
        except Exception as e:
            logging.error("Failed to persist provider quote for %s %s: %s", provider_name, symbol, e)
            audit_logger.error(
                EventType.DB_ERROR,
                f"Failed to persist {provider_name} quote for {symbol}: {e}",
                entity_type=EntityType.MARKET_DATA.value,
                payload={"provider": provider_name, "symbol": symbol, "error": str(e), "quote": quote},
            )
        finally:
            db_queue.task_done()
