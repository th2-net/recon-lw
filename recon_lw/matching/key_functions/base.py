from abc import ABC, abstractmethod
from typing import Protocol, Any

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import KeyFunctionType

class KeyFunctionProvider(ABC):
    @abstractmethod
    def provide(self, adapter: Adapter) -> KeyFunctionType:
        pass

class KeyFunction(Protocol):
    def __call__(self, msg) -> Any:
        pass
