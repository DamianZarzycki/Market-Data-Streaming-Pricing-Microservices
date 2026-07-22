from enum import Enum

class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class TradeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"

class ActionType(str, Enum):
    OPEN_TRADE = "OPEN_TRADE"
    CLOSE_TRADE = "CLOSE_TRADE"

class TradeSource(str, Enum):
    GENERATED = "GENERATED"
    MANUAL = "MANUAL"
    SYSTEM = "SYSTEM"

class CurveType(str, Enum):
    YIELD_CURVE = "YIELD_CURVE"
    FX_FORWARD_CURVE = "FX_FORWARD_CURVE"
    DISCOUNT_CURVE = "DISCOUNT_CURVE"

class SnapshotType(str, Enum):
    FULL = "FULL"
    SPOT = "SPOT"
    CURVE = "CURVE"

class EventType(str, Enum):
    DB_CREATE = "DB_CREATE"
    DB_UPDATE = "DB_UPDATE"
    DB_DELETE = "DB_DELETE"
    DB_CLOSE = "DB_CLOSE"
    DB_REJECT = "DB_REJECT"
    DB_ERROR = "DB_ERROR"
    STREAM_CONNECTED = "STREAM_CONNECTED"
    STREAM_DISCONNECTED = "STREAM_DISCONNECTED"
    STREAM_RECONNECTED = "STREAM_RECONNECTED"
    SNAPSHOT_GENERATED = "SNAPSHOT_GENERATED"
    WORKER_STARTED = "WORKER_STARTED"
    WORKER_STOPPED = "WORKER_STOPPED"

class EntityType(str, Enum):
    TRADE = "TRADE"
    BOOK = "BOOK"
    VALUATION = "VALUATION"
    MARKET_DATA = "MARKET_DATA"

class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    BOND = "BOND"
    FX = "FX"
    OPTION = "OPTION"
    IRS = "IRS"
    FUTURES = "FUTURES"
    COMMODITY = "COMMODITY"

class IRSDirection(str, Enum):
    PAY_FIXED_RECEIVE_FLOAT = "PAY_FIXED_RECEIVE_FLOAT"
    RECEIVE_FIXED_PAY_FLOAT = "RECEIVE_FIXED_PAY_FLOAT"

class OptionType(str, Enum):
    EUROPEAN = "EUROPEAN"

class OptionRightType(str, Enum):
    CALL = "CALL"
    PUT = "PUT"

class ServiceStatus(str, Enum):
    UP = "UP"
    DOWN = "DOWN"

class ConnectionStatus(str, Enum):
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"
