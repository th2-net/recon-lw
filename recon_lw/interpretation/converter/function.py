from recon_lw.interpretation.converter.base import Converter
from typing import Callable, Any
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class FunctionConverter(Converter):
    def __init__(self, function: Callable[[str, Adapter], Any]):
        self.function = function

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        return self.function(val, adapter)
