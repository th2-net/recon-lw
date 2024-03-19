from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message
from typing import Optional, Any, Protocol, runtime_checkable
from abc import ABC, abstractmethod

class Extractor(ABC):
    """
    An abstract base class for all extractors.
    """
    NOT_EXTRACTED = "_NE_"

    def __init__(self, field_name: str):
        self.field_name = field_name

    def __call__(self, message: Message, adapter: Adapter) -> Optional[Any]:
        return self.extract(message, adapter)

    def extract(self, message: Message, adapter: Adapter) -> Optional[Any]:
        pass

class ExtractorProtocol(Protocol):
    def __call__(self, message: Message, adapter: Adapter):
        pass