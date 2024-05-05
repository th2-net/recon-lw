from abc import ABC, abstractmethod
from typing import Protocol, Any

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import KeyFunctionType


class KeyFunctionProvider(ABC):
    """
    The interface for builder class that provides (provide method) the function.
    The provided function is a handler to define `Matching key`.
    """
    @abstractmethod
    def provide(self, adapter: Adapter) -> KeyFunctionType:
        pass


class KeyFunction(Protocol):
    def __call__(self, msg) -> Any:
        pass
