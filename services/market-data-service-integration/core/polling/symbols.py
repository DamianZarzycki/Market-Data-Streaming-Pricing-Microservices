import logging

from core.polling.env import DEFAULT_POLL_SYMBOL
from shared.trading_shared.env import env_str

DEFAULT_FX_SYMBOLS = "EURUSD,GBPUSD,USDJPY"
DEFAULT_BOND_SYMBOLS = "USD_5Y,USD_10Y,EUR_10Y"
DEFAULT_IRS_SYMBOLS = "USD_5Y_IRS,USD_10Y_IRS,EUR_10Y_IRS"
DEFAULT_FUTURES_CONTRACTS = "ES:SPY:50,MES:SPY:5,NQ:QQQ:20"


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


class FuturesContract:
    def __init__(self, symbol: str, underlying: str, multiplier: float):
        self.symbol = symbol
        self.underlying = underlying
        self.multiplier = multiplier


def read_futures_contracts() -> list[FuturesContract]:
    """SYMBOL:UNDERLYING:MULTIPLIER list. Price is the underlying equity quote."""
    raw = env_str("MARKET_DATA_FUTURES_CONTRACTS", DEFAULT_FUTURES_CONTRACTS)
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


def _symbol_map(contracts: list[FuturesContract]) -> dict[str, list[str]]:
    """Symbols configured per asset class.

    FX pairs come from MARKET_DATA_FX_SYMBOLS. Bond and IRS names come from
    MARKET_DATA_BOND_SYMBOLS and MARKET_DATA_IRS_SYMBOLS and are priced off
    persisted FRED/ECB curves. Everything else in MARKET_DATA_SYMBOLS is
    equity. Quote polling and the SSE stream use only the equity list.
    """
    fx_symbols = parse_symbol_list(env_str("MARKET_DATA_FX_SYMBOLS", DEFAULT_FX_SYMBOLS))
    bond_symbols = parse_symbol_list(env_str("MARKET_DATA_BOND_SYMBOLS", DEFAULT_BOND_SYMBOLS))
    irs_symbols = parse_symbol_list(env_str("MARKET_DATA_IRS_SYMBOLS", DEFAULT_IRS_SYMBOLS))
    excluded = set(fx_symbols) | set(bond_symbols) | set(irs_symbols)
    equity_symbols = [
        symbol
        for symbol in parse_symbol_list(env_str("MARKET_DATA_SYMBOLS", ""))
        if symbol not in excluded
    ]
    return {
        "EQUITY": equity_symbols,
        "FX": fx_symbols,
        "BOND": bond_symbols,
        "IRS": irs_symbols,
        "FUTURES": [contract.symbol for contract in contracts],
    }


def read_symbol_map() -> dict[str, list[str]]:
    return _symbol_map(read_futures_contracts())


def read_poll_symbols() -> list[str]:
    equity_symbols = read_symbol_map()["EQUITY"]
    return equity_symbols or [DEFAULT_POLL_SYMBOL]


def symbol_catalog() -> dict:
    """Symbol lists for GET /market-data/symbols. Reads environment variables only."""
    contracts = read_futures_contracts()
    return {
        **_symbol_map(contracts),
        "FUTURES_CONTRACTS": {
            contract.symbol: {
                "underlying": contract.underlying,
                "multiplier": contract.multiplier,
            }
            for contract in contracts
        },
    }


def futures_quotes_for_equity(quote: dict) -> list[dict]:
    """Clone an equity quote onto each futures contract that tracks that symbol.

    The clone is a new dict. Callers persist or stream it; this does not
    recurse, because the clone's asset class is FUTURES.
    """
    asset_class = quote.get("asset_class")
    symbol = quote.get("symbol")
    if not isinstance(asset_class, str) or asset_class.upper() != "EQUITY":
        return []
    if not isinstance(symbol, str):
        return []
    underlying = symbol.strip().upper()
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
