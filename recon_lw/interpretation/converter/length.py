from typing import Any

from base import Converter
from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class LengthConverter(Converter):
    def convert(self, message: Message, field: str, val: Any, adapter: Adapter):
        return len(val)
