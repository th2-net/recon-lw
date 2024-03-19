from recon_lw.interpretation.adapter import Adapter
from recon_lw.interpretation.filter.base import Filter
from recon_lw.core.type.types import Message


class FunctionFilter(Filter):
    def __init__(self, filter_function):
        self.filter_function = filter_function

    def filter(self, message: Message, adapter: Adapter) -> bool:
        return self.filter_function(message, adapter)
