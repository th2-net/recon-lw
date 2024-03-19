from recon_lw.interpretation.check_rule.base import IFieldCheckRule
from abc import ABC
from recon_lw.interpretation.adapter.base import Adapter

class IAdapterFieldCheckRule(IFieldCheckRule, ABC):
    def __init__(self, stream1_adapter: Adapter,
                 stream2_adapter: Adapter):
        self.stream1_adapter = stream1_adapter
        self.stream2_adapter = stream2_adapter

    def get_field_values(self, field, msg1, msg2):
        v1 = self.stream1_adapter.get(msg1, field)
        v2 = self.stream2_adapter.get(msg2, field)
        return v1, v2