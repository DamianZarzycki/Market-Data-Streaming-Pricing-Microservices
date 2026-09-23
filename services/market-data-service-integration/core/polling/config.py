import logging
import os
from datetime import datetime, timezone

DEFAULT_POLL_INTERVAL_SECONDS = 30.0
DEFAULT_CURVE_POLL_INTERVAL_SECONDS = 3600.0
DEFAULT_POLL_SYMBOL = "AAPL"
DEFAULT_FX_SYMBOLS = "EURUSD,GBPUSD,USDJPY"
DEFAULT_BOND_SYMBOLS = "USD_5Y,USD_10Y,EUR_10Y"
DEFAULT_IRS_SYMBOLS = "USD_5Y_IRS,USD_10Y_IRS,EUR_10Y_IRS"
DEFAULT_FUTURES_CONTRACTS = "ES:SPY:50,MES:SPY:5,NQ:QQQ:20"
DEFAULT_CURVE_QUEUE_MAXSIZE = 1000
DEFAULT_CURVE_PERSIST_BATCH_SIZE = 20
DEFAULT_CURVE_PERSIST_FLUSH_SECONDS = 2.0


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_poll_interval_seconds() -> float:
    interval_seconds_raw = os.getenv(
        "MARKET_DATA_POLL_INTERVAL_SECONDS",
        str(DEFAULT_POLL_INTERVAL_SECONDS),
    )
    try:
        return max(float(interval_seconds_raw), 1.0)
    except ValueError:
        logging.warning(
            "Invalid MARKET_DATA_POLL_INTERVAL_SECONDS=%r, falling back to %s",
            interval_seconds_raw,
            DEFAULT_POLL_INTERVAL_SECONDS,
        )
        return DEFAULT_POLL_INTERVAL_SECONDS


def read_curve_poll_interval_seconds() -> float:
    interval_seconds_raw = os.getenv(
        "CURVE_DATA_REFRESH_INTERVAL_SECONDS",
        str(DEFAULT_CURVE_POLL_INTERVAL_SECONDS),
    )
    try:
        return max(float(interval_seconds_raw), 1.0)
    except ValueError:
        logging.warning(
            "Invalid CURVE_DATA_REFRESH_INTERVAL_SECONDS=%r, falling back to %s",
            interval_seconds_raw,
            DEFAULT_CURVE_POLL_INTERVAL_SECONDS,
        )
        return DEFAULT_CURVE_POLL_INTERVAL_SECONDS


def read_positive_int_env(variable_name: str, default_value: int) -> int:
    env_value_raw = os.getenv(variable_name, str(default_value))
    try:
        parsed_value = int(env_value_raw)
        if parsed_value <= 0:
            raise ValueError
        return parsed_value
    except ValueError:
        logging.warning(
            "Invalid %s=%r, falling back to %s",
            variable_name,
            env_value_raw,
            default_value,
        )
        return default_value


def read_positive_float_env(variable_name: str, default_value: float) -> float:
    env_value_raw = os.getenv(variable_name, str(default_value))
    try:
        parsed_value = float(env_value_raw)
        if parsed_value <= 0:
            raise ValueError
        return parsed_value
    except ValueError:
        logging.warning(
            "Invalid %s=%r, falling back to %s",
            variable_name,
            env_value_raw,
            default_value,
        )
        return default_value


def parse_symbol_list(symbols_raw: str) -> list[str]:
    seen_symbols: set[str] = set()
    symbols: list[str] = []
    for symbol_token in symbols_raw.split(","):
        normalized_symbol = symbol_token.strip().upper()
        if not normalized_symbol or normalized_symbol in seen_symbols:
            continue
        seen_symbols.add(normalized_symbol)
        symbols.append(normalized_symbol)
    return symbols


def read_symbol_map() -> dict[str, list[str]]:
    """Symbols configured per asset class.

    FX pairs come from MARKET_DATA_FX_SYMBOLS. Bond and IRS names come from
    MARKET_DATA_BOND_SYMBOLS and MARKET_DATA_IRS_SYMBOLS and are priced off
    persisted FRED/ECB curves. Everything else in MARKET_DATA_SYMBOLS is
    equity. Quote polling and the SSE stream use only the equity list.
    """
    fx_symbols = parse_symbol_list(
        os.getenv("MARKET_DATA_FX_SYMBOLS", DEFAULT_FX_SYMBOLS)
    )
    bond_symbols = parse_symbol_list(
        os.getenv("MARKET_DATA_BOND_SYMBOLS", DEFAULT_BOND_SYMBOLS)
    )
    irs_symbols = parse_symbol_list(
        os.getenv("MARKET_DATA_IRS_SYMBOLS", DEFAULT_IRS_SYMBOLS)
    )
    excluded = set(fx_symbols) | set(bond_symbols) | set(irs_symbols)
    equity_symbols = [
        symbol
        for symbol in parse_symbol_list(os.getenv("MARKET_DATA_SYMBOLS", ""))
        if symbol not in excluded
    ]
    return {
        "EQUITY": equity_symbols,
        "FX": fx_symbols,
        "BOND": bond_symbols,
        "IRS": irs_symbols,
        "FUTURES": [contract.symbol for contract in read_futures_contracts()],
    }


class FuturesContract:
    def __init__(self, symbol: str, underlying: str, multiplier: float):
        self.symbol = symbol
        self.underlying = underlying
        self.multiplier = multiplier


def read_futures_contracts() -> list[FuturesContract]:
    """SYMBOL:UNDERLYING:MULTIPLIER list. Price is the underlying equity quote."""
    raw = os.getenv("MARKET_DATA_FUTURES_CONTRACTS", DEFAULT_FUTURES_CONTRACTS)
    contracts: list[FuturesContract] = []
    seen: set[str] = set()
    for token in raw.split(","):
        parts = [part.strip().upper() for part in token.split(":")]
        if len(parts) != 3:
            if token.strip():
                logging.warning("Skipping futures contract %r", token.strip())
            continue
        symbol, underlying, multiplier_raw = parts
        if not symbol or not underlying or symbol in seen:
            continue
        try:
            multiplier = float(multiplier_raw)
        except ValueError:
            logging.warning("Skipping futures contract %r", token.strip())
            continue
        if multiplier <= 0:
            logging.warning("Skipping futures contract %r", token.strip())
            continue
        seen.add(symbol)
        contracts.append(FuturesContract(symbol, underlying, multiplier))
    return contracts


def futures_quotes_for_equity(quote: dict) -> list[dict]:
    """Clone an equity quote onto each futures contract that tracks that symbol.

    The clone is a new dict. Callers persist or stream it; this does not
    recurse, because the clone's asset class is FUTURES.
    """
    if str(quote.get("asset_class") or "").upper() != "EQUITY":
        return []
    underlying = str(quote.get("symbol") or "").upper()
    if not underlying:
        return []
    mirrored: list[dict] = []
    for contract in read_futures_contracts():
        if contract.underlying != underlying:
            continue
        cloned = dict(quote)
        cloned["asset_class"] = "FUTURES"
        cloned["symbol"] = contract.symbol
        raw = cloned.get("raw_payload")
        raw_payload = dict(raw) if isinstance(raw, dict) else {}
        raw_payload["underlying_symbol"] = contract.underlying
        raw_payload["contract_multiplier"] = contract.multiplier
        cloned["raw_payload"] = raw_payload
        mirrored.append(cloned)
    return mirrored


def read_poll_symbols() -> list[str]:
    equity_symbols = read_symbol_map()["EQUITY"]
    return equity_symbols or [DEFAULT_POLL_SYMBOL]


def parse_curve_as_of_time(as_of_time_value: object) -> datetime | None:
    if as_of_time_value is None:
        return None
    if isinstance(as_of_time_value, datetime):
        if as_of_time_value.tzinfo is None:
            return as_of_time_value.replace(tzinfo=timezone.utc)
        return as_of_time_value
    if not isinstance(as_of_time_value, str) or not as_of_time_value.strip():
        return None

    timestamp_text = as_of_time_value.strip()
    try:
        if timestamp_text.endswith("Z"):
            return datetime.strptime(timestamp_text, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        parsed_datetime = datetime.fromisoformat(timestamp_text)
        if parsed_datetime.tzinfo is None:
            return parsed_datetime.replace(tzinfo=timezone.utc)
        return parsed_datetime
    except ValueError:
        logging.warning(
            "Invalid curve as_of_time=%r; using current UTC time",
            as_of_time_value,
        )
        return None
