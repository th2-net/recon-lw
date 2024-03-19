from typing import Any

from recon_lw.interpretation.adapter import Adapter
from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.core.type.types import Message


class AnyVal:

    def __eq__(self, other):
        return Extractor.NOT_EXTRACTED != other

    def __ne__(self, other):
        return False

    def __str__(self):
        return "*"


class AnyValExtractor(Extractor):
    def __init__(self):
        super().__init__('AnyVal')

    def extract(self, message: Message, adapter: Adapter) -> Any:
        return AnyVal()
