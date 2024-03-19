from recon_lw.interpretation.converter.base import Converter
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class ConstantConverter(Converter):

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        return val
