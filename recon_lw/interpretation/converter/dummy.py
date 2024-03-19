from base import Converter
from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class DummyConverter(Converter):
    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        return val