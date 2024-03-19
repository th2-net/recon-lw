from typing import Callable
from typing import Optional

from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.interpretation.condition import Condition
from recon_lw.core.type.types import Message


class MaskValueProvider:
    def __init__(self, mask_value_function: Callable[[Message, Adapter], str]) -> None:
        self.mask_value_function = mask_value_function

    def get_mask_value(self, message: Message, adapter: Adapter):
        return self.mask_value_function(message, adapter)


class ConditionMaskExtractor(Extractor):
    def __init__(self,
                 base_extractor: Extractor,
                 condition: Condition,
                 mask_value_provider: MaskValueProvider
                 ):
        super().__init__('any')
        self.condition = condition
        self.mask_value_provider = mask_value_provider
        self.base_extractor = base_extractor

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        if self.condition(message, adapter):
            return self.mask_value_provider.get_mask_value(message, adapter)

        return self.base_extractor(message, adapter)


class ConditionExtractor(Extractor):
    def __init__(self,
                 true_extractor: Extractor,
                 false_extractor: Extractor,
                 condition: Condition
                 ):
        super().__init__('any')
        self.true_extractor = true_extractor
        self.false_extractor = false_extractor
        self.condition = condition

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        if self.condition(message, adapter):
            return self.true_extractor(message, adapter)
        else:
            return self.false_extractor(message, adapter)
