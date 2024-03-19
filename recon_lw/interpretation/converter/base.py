from abc import ABC, abstractmethod
from typing import Any, Protocol
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class Converter(ABC):

    def __call__(self, message: Message, field: str, val: Any, adapter: Adapter) -> Any:
        return self.convert(message, field, val, adapter)

    @abstractmethod
    def convert(self, message: Message, field: str, val: Any, adapter: Adapter):
        pass

class ConverterProtocol(Protocol):
    def __call__(self, message: Message, field: str, val: Any, adapter: Adapter):
        pass