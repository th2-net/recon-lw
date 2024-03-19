from recon_lw.interpretation.converter.base import Converter
from enum import Enum
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter

class TypeAlias(Enum):
    Int = 'int'
    Float = 'float'
    string = 'str'
    list = 'list'
    dict = 'dict'

class TypeConverter(Converter):
    def __init__(self, type_alias: TypeAlias):
        self.type_alias = type_alias
        self.converter_func = TypeConverter.converter_func(type_alias)

    @staticmethod
    def converter_func(type_alias: TypeAlias):
        type_converters = {
            'int': int,
            'float': float,
            'str': str,
            'list': list,
            'dict': dict
        }

        return type_converters[type_alias]

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        self.converter_func(val)
