from recon_lw.interpretation.filter.base import Filter
from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class DummyFilter(Filter):
    def filter(self, message: Message, adapter: Adapter) -> bool:
        return False