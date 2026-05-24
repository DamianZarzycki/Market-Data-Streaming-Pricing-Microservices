from enum import Enum

class ServiceStatus(str, Enum):
    UP = "UP"
    DOWN = "DOWN"

class AssetType(str, Enum):
    EQUITY = "EQUITY"
    BOND = "BOND"
    FX = "FX"