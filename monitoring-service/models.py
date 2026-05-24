from enum import Enum

class ServiceStatus(str, Enum):
    UP = "UP"
    DOWN = "DOWN"