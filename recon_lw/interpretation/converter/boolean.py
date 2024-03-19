from base import Converter
from typing import Dict

from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class BooleanConverter(Converter):
    def __init__(self, mapping: Dict[str, str], default_val: str=''):
        self.mapping = mapping
        self.default_val = default_val

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        if field in self.mapping:
            return self.mapping[field]
        return self.default_val