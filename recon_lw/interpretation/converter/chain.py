from typing import List, Any

from recon_lw.interpretation.converter.base import Converter, ConverterProtocol
from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message
from recon_lw.interpretation.field_extractor import Extractor


class ChainConverter(Converter):
    def __init__(self, converters: List[ConverterProtocol]):
        self._converters = converters

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        for converter in self._converters:
            val = converter(message, field, val, adapter)
            if val is None or val == Extractor.NOT_EXTRACTED:
                return val
        return val


class FirstNonNullChainConverter(Converter):
    def __init__(self, converters: List[ConverterProtocol]):
        self._converters = converters

    def convert(self, message: Message, field: str, val: Any, adapter: Adapter):
        for converter in self._converters:
            converted = converter(message, field, val, adapter)
            if converted != Extractor.NOT_EXTRACTED:
                return converted
        return Extractor.NOT_EXTRACTED
