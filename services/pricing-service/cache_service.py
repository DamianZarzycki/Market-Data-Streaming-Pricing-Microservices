import logging
import threading
from dataclasses import dataclass

from shared.trading_shared.db import AsyncDBSessionManager

active_trades_cache = {}
symbol_index = {}
cache_lock = threading.Lock()


@dataclass(frozen=True)
class CachedTrade:
    """Plain copy of an ACTIVE trade. The DB session is already closed."""

    trade_id: str
    book_id: str
    symbol: str
    asset_class: str
    side: str
    quantity: float
    trade_price: float
    trade_currency: str
    metadata_payload: dict | None
    market_data_provider: str | None


def _cached_from_row(row) -> CachedTrade:
    return CachedTrade(
        trade_id=str(row.trade_id),
        book_id=str(row.book_id),
        symbol=row.symbol,
        asset_class=row.asset_class,
        side=row.side,
        quantity=float(row.quantity),
        trade_price=float(row.trade_price),
        trade_currency=row.trade_currency,
        metadata_payload=row.metadata_payload,
        market_data_provider=row.market_data_provider,
    )


def replace_cache(trades: dict[str, CachedTrade]) -> None:
    """Swap the trade map and the symbol index in one assignment each."""
    global active_trades_cache, symbol_index
    index: dict[tuple[str, str], list[CachedTrade]] = {}
    for trade in trades.values():
        if trade.market_data_provider:
            continue
        index.setdefault((trade.asset_class, trade.symbol), []).append(trade)
    frozen = {key: tuple(bucket) for key, bucket in index.items()}
    with cache_lock:
        active_trades_cache = trades
        symbol_index = frozen


def all_trades() -> tuple:
    with cache_lock:
        return tuple(active_trades_cache.values())


def simulator_trades(asset_class: str, symbol: str) -> tuple:
    """Trades a simulator tick may price. Provider trades stay on the quote worker."""
    with cache_lock:
        if symbol_index:
            return symbol_index.get((asset_class, symbol), ())
        return tuple(
            trade
            for trade in active_trades_cache.values()
            if trade.symbol == symbol
            and trade.asset_class == asset_class
            and not trade.market_data_provider
        )


# Async: awaits TradeRepository.list_active_for_pricing (AsyncSession.execute).
async def reload_active_trades_cache():
    """Replace the cache with every ACTIVE trade. On failure the previous cache stays."""
    try:
        logging.info("Reloading active trades cache...")
        async with AsyncDBSessionManager() as db:
            rows = await db.trades.list_active_for_pricing()
            trades = {str(row.trade_id): _cached_from_row(row) for row in rows}
        replace_cache(trades)
        return set(trades)
    except Exception as exc:
        logging.error(f"Error reloading active trades cache: {exc}")
        return None
