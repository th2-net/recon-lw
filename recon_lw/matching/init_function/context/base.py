from abc import ABC, abstractmethod

class AbstractMatcherContext(ABC):
    @abstractmethod
    def to_dict(self) -> dict:
        pass