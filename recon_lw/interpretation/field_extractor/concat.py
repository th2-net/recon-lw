from typing import List, Optional, Any

from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter import Adapter

from recon_lw.interpretation.field_extractor.base import Extractor

class ConcatExtractor(Extractor):
    def __init__(self, extractors: List[Extractor], separator: str=''):
        super().__init__('any')
        self.extractors = extractors
        self.separator = separator

    def extract(self, message: Message, adapter: Adapter) -> Optional[Any]:
        vals = []
        for extractor in self.extractors:
            vals.append(extractor(message, adapter))
        return self.separator.join(vals)