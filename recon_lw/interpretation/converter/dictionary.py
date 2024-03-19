from abc import ABC

from recon_lw.interpretation.converter.base import Converter
from typing import Set, Any

from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class DictPathConverter(Converter):
    def __init__(self,
                 path: Set[str] = None,
                 ):
        self.path = path

    def convert(self, message: Message, field: str, val: Any, adapter: Adapter):
        if not isinstance(val, dict):
            raise ValueError('DictPathConverter expects a dictionary value passed from the converter that is before '
                             'in the chain.')
        for key in self.path:
            val = val[key]
            if not isinstance(val, dict):
                raise ValueError('DictPathConverter expects a dictionary value passed from the converter that is '
                                 'before in the chain.')

        return val


class DictKeysConverter(Converter, ABC):
    def __init__(self,
                 keys: Set[str] = None,
                 separator: str = "/"):
        self.key = separator.join(keys)

    def extract(self, message: Message, field: str, val: Any, adapter: Adapter):
        assert isinstance(val, dict)
        return val[self.key]
