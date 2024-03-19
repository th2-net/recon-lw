from abc import ABC, abstractmethod
from typing import Optional, Protocol
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message


class ReconEventEnhancement(ABC):

    def __call__(self, event, msg: Optional[Message], adapter: Adapter):
        return self.enhance_event(event, msg, adapter)

    @abstractmethod
    def enhance_event(self, event, msg: Optional[Message], adapter: Adapter):
        pass

class ReconEventEnhancementProtocol(Protocol):
    def __call__(self, event, msg: Optional[Message], adapter: Adapter):
        pass