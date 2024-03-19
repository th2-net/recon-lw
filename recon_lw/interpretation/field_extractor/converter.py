from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.interpretation.adapter.base import Adapter
from typing import Optional, List
from recon_lw.core.type.types import Message
from recon_lw.interpretation.converter.base import Converter, ConverterProtocol


class BasicConverterExtractor(Extractor):
    def __init__(self, field_name: str, converter: ConverterProtocol, base_extractor: Extractor):
        super().__init__(field_name)
        self.converter = converter
        self.base_extractor = base_extractor

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        val = self.base_extractor(message, adapter)

        return self.converter(message, self.field_name, val, adapter)


class BasicConverterExtractorBuilder:
    def __init__(self):
        self.field_name = ""
        self.converter = None
        self.base_extractor = None

    def set_field_name(self, field_name: str) -> 'BasicConverterExtractorBuilder':
        self.field_name = field_name
        return self

    def set_converter(self, converter: Converter) -> 'BasicConverterExtractorBuilder':
        self.converter = converter
        return self

    def set_base_extractor(self, base_extractor: Extractor):
        self.base_extractor = base_extractor
        return self

    def build(self) -> BasicConverterExtractor:
        if not self.field_name or not self.converter:
            raise ValueError("Field name and converter must be set.")
        return BasicConverterExtractor(self.field_name, self.converter, self.base_extractor)


class ChainConverterExtractor(Extractor):
    def __init__(self, field_name: str, base_extractor: Extractor, converter_chain: List[ConverterProtocol]):
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


class ChainConverterExtractorBuilder(ChainConverterExtractor):
    def __init__(self, field_name: str, base_extractor: Extractor, converter_chain: List[Converter]):
        super().__init__(field_name, base_extractor, converter_chain)
        self.field_name = ""
        self.converter_chain: List[Converter] = []
        self.base_extractor = None

    def set_field_name(self, field_name: str) -> 'ChainConverterExtractorBuilder':
        self.field_name = field_name
        return self

    def add_converter(self, converter: Converter) -> 'ChainConverterExtractorBuilder':
        self.converter_chain.append(converter)
        return self

    def set_base_extractor(self, base_extractor: Extractor) -> 'ChainConverterExtractorBuilder':
        self.base_extractor = base_extractor
        return self

    def build(self) -> ChainConverterExtractor:
        if not self.field_name or len(self.converter_chain) == 0:
            raise ValueError("Field name and converter must be set.")
        return ChainConverterExtractor(self.field_name, self.base_extractor, self.converter_chain)
