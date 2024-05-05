from enum import Enum


class ExecType(Enum):
    # TODO
    #   Slava Ermakov: Don't sure that we need it here.
    NEW = "New"
    CANCELLED = "Cancelled"
    REPLACED = "Replaced"
    TRADE = "Trade"
    TRIGGERED = "Triggered"
    REJECTED = "Rejected"
    RESTATED = "Restated"
