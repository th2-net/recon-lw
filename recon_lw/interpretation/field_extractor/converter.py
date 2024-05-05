from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.interpretation.adapter.base import Adapter
from typing import Optional, List
from recon_lw.core.type.types import Message
from recon_lw.interpretation.converter.base import Converter, ConverterProtocol


class BasicConverterExtractor(Extractor):
    def __init__(self, field_name: str, converter: ConverterProtocol,
                 base_extractor: Extractor):
        super().__init__(field_name)
        self.converter = converter
        self.base_extractor = base_extractor

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        val = self.base_extractor(message, adapter)

        return self.converter(message, self.field_name, val, adapter)


class ChainConverterExtractor(Extractor):
    def __init__(self, field_name: str, base_extractor: Extractor,
                 converter_chain: List[ConverterProtocol]):
        super().__init__(field_name)
        self.base_extractor = base_extractor
        self.converter_chain = converter_chain

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        val = self.base_extractor(message, adapter)
        if val != Extractor.NOT_EXTRACTED:
            val = str(val)

        for converter in self.converter_chain:
            val = converter(message, self.field_name, val, adapter)
        return val
