from shared.trading_shared.db import DBSessionManager


def load_providers_limits():
    with DBSessionManager() as db:
        return {
            row.provider: {
                "minute": row.minute_limit,
                "hourly": row.hourly_limit,
                "daily": row.daily_limit,
                "monthly": row.monthly_limit,
            }
            for row in db.provider_limits.list_all()
        }
