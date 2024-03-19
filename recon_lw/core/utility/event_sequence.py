from dataclasses import dataclass

@dataclass
class EventSequence:
    name: str
    timestamp: str
    n: int

    def to_dict(self):
        return {"name": self.name, "stamp": self.timestamp, "n": self.n}