from recon_lw.interpretation.interpretation_functions.event_enhancement.base import ReconEventEnhancement, \
    ReconEventEnhancementProtocol
from typing import List, Optional
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message


class ReconEventChainEnhancement:
    def __init__(self, enhancements: List[ReconEventEnhancementProtocol] = []):
        self.enhancements = enhancements

    def add_enhancement(self, enhancement: ReconEventEnhancementProtocol):
        self.enhancements.append(enhancement)
        return self

    def apply(self, event, msg: Optional[Message], adapter: Adapter):
        for enhancement in self.enhancements:
            enhancement(event, msg, adapter)
