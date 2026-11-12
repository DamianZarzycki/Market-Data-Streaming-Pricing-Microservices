from shared.trading_shared.db import AsyncDBSessionManager


# Async: awaits provider_limits.list_all_async.
async def load_providers_limits():
    async with AsyncDBSessionManager() as db:
        rows = await db.provider_limits.list_all_async()
    return {
        row.provider: {
            "minute": row.minute_limit,
            "hourly": row.hourly_limit,
            "daily": row.daily_limit,
            "monthly": row.monthly_limit,
        }
        for row in rows
    }
