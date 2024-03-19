from enum import Enum

class ExecType(Enum):
    NEW = "New"
    CANCELLED = "Cancelled"
    REPLACED = "Replaced"
    TRADE = "Trade"
    TRIGGERED = "Triggered"
    REJECTED = "Rejected"
    RESTATED = "Restated"