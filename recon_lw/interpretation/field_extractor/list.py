from collections import defaultdict
from typing import Optional, List, Dict

from base import Extractor
from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class ListAggregationExtractor(Extractor):
    def __init__(self,
                 field_name: str,
                 keys: Optional[List[str]] = None,
                 values: Optional[List[str]] = None,
                 separator: str = "/",
                 result_type=dict,
                 keys_remap: Optional[Dict[str, str]] = None
                 ):
        super().__init__('any')
        self.keys = keys
        self.values = values
        self.separator = separator
        self.result_type = result_type
        self.keys_remap = keys_remap
        self.field_name = field_name
        self.cache = None

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        if self.cache is not None:
            return self.cache

        body = adapter.get_body(message)

        buffer = defaultdict(dict)
        field = self.field_name
        field += '.'
        for k, v in body.items():
            if k.startswith(field):
                _, n, sub_field = k.split('.', maxsplit=2)
                if self.keys_remap:
                    sub_field = self.keys_remap.get(sub_field, sub_field)
                    if not sub_field:
                        continue
                buffer[n][sub_field] = v

        result = None
        if self.result_type == dict:
            result = {}

            for _, v in buffer.items():
                key = self.separator.join([str(v[k]) for k in self.keys])
                value = self.separator.join([str(v[k]) for k in self.values])

                if key in result:
                    raise ValueError(f"Duplicate key = {key}, message - {message}")

                result[key] = value
        elif self.result_type == list:
            result = list({k: str(v) for k, v in val.items()} for val in buffer.values())

        self.cache = result

        return result
