from recon_lw.interpretation.condition.base import Condition
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message
from typing import Callable


class FunctionCondition(Condition):

    def __init__(self, function: Callable[[Message, Adapter], bool]):
        self.function = function
        self.cache = None

    def __call__(self, message: Message, adapter: Adapter) -> bool:
        if self.cache:
            return self.cache
        result = self.function(message, adapter)
        self.cache = result
        return result
