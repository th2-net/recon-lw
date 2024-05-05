from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.interpretation.adapter.base import Adapter
from typing import Optional
from recon_lw.core.type.types import Message


class BasicDictExtractor(Extractor):
    def __init__(self, field_name: str, default_value: str = None,
                 strip: bool = False, cast_to_str: bool = True):
        super().__init__(field_name)
        self.strip = strip
        self.default_value = default_value
        self.cast_to_str = cast_to_str

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        val = message.get(self.field_name,
                          self.default_value if self.default_value else Extractor.NOT_EXTRACTED)
        if self.cast_to_str:
            val = str(val)
            if self.strip:
                val = val.strip()

        return val
