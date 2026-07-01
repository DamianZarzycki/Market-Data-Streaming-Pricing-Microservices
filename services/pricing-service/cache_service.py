import logging
import threading

from shared.trading_shared.enums import TradeStatus
from shared.trading_shared.db import DBSessionManager

active_trades_cache = {}
cache_lock = threading.Lock()

# Utrzymanie cache - sprawdz potencjane rozwiazanie (kiedy czyscic, validowac, jak czesto odswiezac)
def reload_active_trades_cache():
    try:
        logging.info("Reloading active trades cache...")
        with DBSessionManager() as db:
            trades = db.trades.get_trades(client_request_id=None, status=TradeStatus.ACTIVE.value, symbol=None, first_only=False)

        with cache_lock:
            new_cache = {trade.trade_id: trade for trade in trades}
            active_trades_cache.clear()
            active_trades_cache.update(new_cache)
    except Exception as e:
        logging.error(f"Error reloading active trades cache: {e}")
