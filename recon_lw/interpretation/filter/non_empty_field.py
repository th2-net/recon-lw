from typing import List

from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter import Adapter
from recon_lw.interpretation.filter.base import Filter

class NonEmptyFilter(Filter):
    def __init__(self, field_path: List[str]):
        self.field_path = field_path

    def filter(self, message: Message, adapter: Adapter) -> bool:
        body = adapter.get_body(message)
        val = None

        for field in self.field_path:
            val = body.get(field)
            if val is None:
                return True

        return val is None