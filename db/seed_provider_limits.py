"""Insert default market-data provider limits if they are missing.

Usage (from the db-seed container): python seed_provider_limits.py
"""
import logging
import os
import sys
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from shared.trading_shared.models import MarketDataProviderLimits

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# --- Free-tier limits ---
# Finnhub:       60 req/min (rolling). No hard daily cap.
# Alpha Vantage: 25 req/day (resets at midnight US Eastern ≈ 05:00 UTC).
#                No official per-minute cap, but bursts >1 req/s get throttled.
# Twelve Data:   800 credits/day (resets at midnight UTC).
#                No per-minute cap.
DEFAULT_LIMITS = (
    {
        "provider": "finnhub",
        "minute_limit": 60,
        "hourly_limit": None,
        "daily_limit": None,
        "monthly_limit": None,
    },
    {
        "provider": "alpha_vantage",
        "minute_limit": None,
        "hourly_limit": None,
        "daily_limit": 25,
        "monthly_limit": None,
    },
    {
        "provider": "twelve_data",
        "minute_limit": None,
        "hourly_limit": None,
        "daily_limit": 800,
        "monthly_limit": None,
    },
)


def seed() -> None:
    if not os.getenv("DATABASE_URL"):
        logging.error("DATABASE_URL is not set")
        sys.exit(1)

    from shared.trading_shared.db import DBSessionManager

    now = datetime.now(timezone.utc)
    inserted = []

    with DBSessionManager() as db:
        for row in DEFAULT_LIMITS:
            stmt = (
                insert(MarketDataProviderLimits)
                .values(**row, created_at=now, updated_at=now)
                .on_conflict_do_update(
                    index_elements=["provider"],
                    set_={
                        "minute_limit": row["minute_limit"],
                        "hourly_limit": row["hourly_limit"],
                        "daily_limit": row["daily_limit"],
                        "monthly_limit": row["monthly_limit"],
                        "updated_at": now,
                    },
                )
            )
            db.session.execute(stmt)
            inserted.append(row["provider"])
        db.commit()

    logging.info("Provider limits seed complete (upserted): %s", inserted)


if __name__ == "__main__":
    seed()
