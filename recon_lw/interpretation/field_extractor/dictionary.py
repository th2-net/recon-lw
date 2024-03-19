from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.interpretation.adapter.base import Adapter
from typing import Optional
from recon_lw.core.type.types import Message


class BasicDictExtractor(Extractor):
    def __init__(self, field_name: str, default_value: str = None, strip: bool = False, cast_to_str: bool = True):
        super().__init__(field_name)
        self.strip = strip
        self.default_value = default_value
        self.cast_to_str = cast_to_str

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        val = message.get(self.field_name, self.default_value if self.default_value else Extractor.NOT_EXTRACTED)
        if self.cast_to_str:
            val = str(val)
            if self.strip:
                val = val.strip()

        return val


class BasicDictExtractorBuilder:
    def __init__(self):
        self.field_name = ""

    def set_field_name(self, field_name: str) -> 'BasicDictExtractorBuilder':
        self.field_name = field_name
        return self

    def set_strip(self, strip: bool) -> 'BasicDictExtractorBuilder':
        self.strip = strip
        return self

    def build(self) -> BasicDictExtractor:
        if not self.field_name:
            raise ValueError("Field name must be set.")
        return BasicDictExtractor(self.field_name)
