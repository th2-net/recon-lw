from recon_lw.interpretation.filter.base import Filter, FilterProtocol
from typing import List, Union
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class FilterChain:
    def __init__(self):
        self.filters: List[Union[Filter, FilterProtocol]] = []

    def filter(self, message: Message, adapter: Adapter) -> bool:
        if len(self.filters) == 0:
            return False

        for filter in self.filters:
            result = filter(message, adapter)
            if result:
                return True

        return False

    def add_filter(self, filter: Union[Filter, FilterProtocol]):
        self.filters.append(filter)
        return self
