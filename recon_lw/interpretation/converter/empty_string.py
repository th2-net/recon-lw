from recon_lw.interpretation.converter.base import Converter
from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class EmptyStringConverter(Converter):

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        if val is None or val == '':
            return Extractor.NOT_EXTRACTED

        return val
