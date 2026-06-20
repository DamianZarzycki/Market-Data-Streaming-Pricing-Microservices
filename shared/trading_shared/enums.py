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
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"
    ERROR = "ERROR"
    STREAM_CONNECTED = "STREAM_CONNECTED"

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

class ServiceStatus(str, Enum):
    UP = "UP"
    DOWN = "DOWN"