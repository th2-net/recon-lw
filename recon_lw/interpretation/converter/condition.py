from base import Converter, ConverterProtocol
from recon_lw.interpretation.adapter import Adapter
from recon_lw.interpretation.condition import Condition
from dummy import DummyConverter
from recon_lw.core.type.types import Message


class ConditionConverter(Converter):
    def __init__(self, condition: Condition, true_converter: ConverterProtocol=None, false_converter: ConverterProtocol = None):
        self.condition = condition
        if true_converter is None:
            true_converter = DummyConverter()
        if false_converter is None:
            false_converter = DummyConverter()

        self.true_converter = true_converter
        self.false_converter = false_converter

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        if self.condition(message, adapter):
            self.true_converter(message, field, val, adapter)
        else:
            self.false_converter(message, field, val, adapter)