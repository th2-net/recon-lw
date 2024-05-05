from __future__ import annotations
from typing import List, Optional, Dict, Union

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.interpretation.field_extractor.base import Extractor, ExtractorProtocol


class SimpleAdapter(Adapter):
    def __init__(self,
                 body_path: Optional[List[str]] = None,
                 metadata_path: Optional[List[str]] = None,
                 mapping: Optional[Dict[str, Union[Extractor, ExtractorProtocol, str]]] = None
                 ):
        super().__init__(body_path, metadata_path, mapping)

    def get(self, message, field, strict=False):
        extractor = self.mapping[field]
        if isinstance(extractor, Extractor):
            val = extractor(self.get_body(message), self)
        elif isinstance(extractor, str):
            val = self.get_body(message).get(extractor, Extractor.NOT_EXTRACTED)
        else:
            val = extractor(message)
        if strict and val == Extractor.NOT_EXTRACTED:
            raise KeyError(field)

        if val != Extractor.NOT_EXTRACTED:
            val = str(val)

        return val

    def get_root_message_field(self, message, parameter_name, strict=False):
        extractor = self.mapping[parameter_name]
        val = extractor(message, self)
        if strict and val == Extractor.NOT_EXTRACTED:
            raise KeyError(parameter_name)

        if val != Extractor.NOT_EXTRACTED:
            val = str(val)

        return val

    def get_metadata_field(self, message, field_name, strict=False):
        extractor = self.mapping[field_name]
        val = extractor(message['metadata'], self)
        if strict and val == Extractor.NOT_EXTRACTED:
            raise KeyError(field_name)

        if val != Extractor.NOT_EXTRACTED:
            val = str(val)

        return val

    def on_message(self, m):
        pass

    def on_message_exit(self, m):
        pass

    def get_fields_group(self, m, group_name):
        pass


class SimpleAdapterBuilder:
    def __init__(self):
        super().__init__()
        self.metadata_path: Optional[List[str]] = None
        self.body_path: Optional[List[str]] = None
        self.mapping: Optional[Dict[str, Extractor]] = None

    def with_body_path(self, body_path: List[str]) -> 'SimpleAdapterBuilder':
        self.body_path = body_path
        return self

    def with_metadata_path(self, metadata_path: List[str]) -> 'SimpleAdapterBuilder':
        self.metadata_path = metadata_path
        return self

    def with_mapping(self, mapping: Dict[str, Extractor]) -> 'SimpleAdapterBuilder':
        self.mapping = mapping
        return self

    def build(self) -> SimpleAdapter:
        return SimpleAdapter(
            self.body_path,
            self.metadata_path,
            self.mapping
        )
