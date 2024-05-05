from recon_lw.interpretation.field_extractor.base import Extractor
from typing import Optional, Any

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message


class ConstantExtractor(Extractor):
    """
    Always returns a constant value.
    """

    def __init__(self, return_value: Any):
        super().__init__('any')
        self.return_value = return_value

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        return self.return_value


class NEConstantExtractor(ConstantExtractor):
    def __init__(self):
        super().__init__(Extractor.NOT_EXTRACTED)

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        return super().extract(message, adapter)
