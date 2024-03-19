from recon_lw.interpretation.converter.base import Converter
from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class MappingConverter(Converter):
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        if field in self.mapping:
            return self.mapping[field]
        return Extractor.NOT_EXTRACTED
