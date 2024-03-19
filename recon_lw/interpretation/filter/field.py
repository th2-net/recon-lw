from recon_lw.interpretation.field_extractor import Extractor
from recon_lw.interpretation.filter.base import Filter
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message
from typing import List, Any


class FieldFilter(Filter):
    def __init__(self, field_name: str, field_values: List[Any], whitelist: bool = True):
        super().__init__()
        self.field_name = field_name
        self.field_values = field_values
        self.whitelist = whitelist

    def filter(self, message: Message, adapter: Adapter) -> bool:
        val = adapter.get(message, self.field_name)
        if val == Extractor.NOT_EXTRACTED:
            val = adapter.get_body(message).get(self.field_name, Extractor.NOT_EXTRACTED)
        if self.whitelist:
            return val not in self.field_values
        else:
            return val in self.field_values
