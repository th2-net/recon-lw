from recon_lw.interpretation.filter.base import Filter
from typing import Set, Dict, Any
from recon_lw.interpretation.adapter.base import Adapter


class MessageTypeFilter(Filter):
    def __init__(self, message_types: Set[str]):
        self.message_types = message_types

    def filter(self, message: Dict[str, Any], adapter: Adapter) -> bool:
        message_type = adapter.get_metadata(message)['messageType']
        return message_type not in self.message_types
