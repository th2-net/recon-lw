from abc import ABC, abstractmethod
from typing import Protocol

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message


class Filter(ABC):
    def __call__(self, message: Message, adapter: Adapter) -> bool:
        return self.filter(message, adapter)

    @abstractmethod
    def filter(self, message: Message, adapter: Adapter) -> bool:
        pass

class FilterProtocol(Protocol):
    def __call__(self, message: Message, adapter: Adapter) -> bool:
        pass
