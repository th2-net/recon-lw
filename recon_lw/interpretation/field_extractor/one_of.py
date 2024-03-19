from typing import Optional, Dict

from base import Extractor
from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class OneOfExtractor(Extractor):
    def __init__(self, extractors: Dict[str, Extractor]):
        super().__init__('any')
        self.extractors = extractors

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        body = adapter.get_body(message)
        for field_name, extractor in self.extractors.items():
            if field_name in body:
                return extractor(message, adapter)
        return Extractor.NOT_EXTRACTED
