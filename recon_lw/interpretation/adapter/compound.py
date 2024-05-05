from __future__ import annotations
from recon_lw.interpretation.condition.base import Condition
from recon_lw.interpretation.adapter.base import Adapter
from typing import List, Optional, Tuple, Dict, Any

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from recon_lw.interpretation.field_extractor import Extractor


class CompoundAdapter(Adapter):
    """
        A compound adapter composed of multiple adapters with associated conditions.

        This adapter selects the appropriate adapter based on conditions and delegates
        message handling to the selected adapter.

        Attributes:
            adapters (List[Tuple[Condition, Adapter]]): A list of tuples containing
                conditions and associated adapters.
            body_path (Optional[List[str]]): The path to the body field in the message.
            mapping_path (Optional[List[str]]): The path to the mapping field in the message.
            mapping (Optional[Dict[str, Extractor]]): A mapping of field names to extractors.
        """

    def __init__(self,
                 adapters: List[Tuple[Condition, Adapter]],
                 body_path: Optional[List[str]] = None,
                 mapping_path: Optional[List[str]] = None,
                 mapping: Optional[Dict[str, Extractor]]=None
                 ):
        super().__init__(body_path, mapping_path, mapping)
        self.adapters = adapters

    def get_adapter(self, message):
        for condition, adapter in self.adapters:
            if condition(message[self.body_field], adapter):
                return adapter

        raise SystemError(f"No adapter for {message[self.body_field]}")

    def get(self, message, field, strict=False):
        handler = self.get_adapter(message)

        return handler.get(message, field, strict)

    def on_message(self, m):
        handler = self.get_adapter(m)
        return handler.on_message(m)

    def on_message_exit(self, m):
        handler = self.get_adapter(m)
        return handler.on_message_exit(m)

    def get_fields_group(self, m, group_name):
        handler = self.get_adapter(m)
        return handler.get_fields_group(m, group_name)

    def get_root_message_field(self, message, parameter_name,
                               strict=False) -> Any:
        # FIXME: Not implemented
        pass

    def get_metadata_field(self, message, field_name, strict=False) -> Any:
        # FIXME: Not implemented
        pass


class CompoundAdapterBuilder:
    """
    A builder class for constructing instances of the CompoundAdapter class.

    Attributes:
        _conditions_and_adapters (List[Tuple[Condition, Adapter]]): A list of tuples
            containing conditions and associated adapters.
        _mapping (Dict[str, Extractor]): A mapping of field names to extractors.
        _body_path (Optional[List[str]]): The path to the body field in the message.
        _metadata_path (Optional[List[str]]): The path to the metadata field in the message.

    Methods:
        __init__: Constructor method for the CompoundAdapterBuilder class.
        with_mapping: Method to set the mapping for the CompoundAdapter.
        with_body_path: Method to set the body path for the CompoundAdapter.
        with_metadata_path: Method to set the metadata path for the CompoundAdapter.
        add_adapter: Method to add an adapter with its associated condition.
        build: Method to build and return the constructed CompoundAdapter instance.
    """

    def __init__(self):
        super().__init__()
        self._conditions_and_adapters: List[Tuple[Condition, Adapter]] = []
        self._mapping: Dict[str, Extractor] = {}
        self._body_path = None
        self._metadata_path = None

    def with_mapping(self, mapping: Dict[str, Extractor]) -> 'CompoundAdapterBuilder':
        self._mapping = mapping
        return self

    def with_body_path(self, body_path: List[str]) -> 'CompoundAdapterBuilder':
        self._body_path = body_path
        return self

    def with_metadata_path(self, metadata_path: List[str]) -> 'CompoundAdapterBuilder':
        self._metadata_path = metadata_path
        return self

    def add_adapter(self, condition: Condition, adapter: Adapter) -> 'CompoundAdapterBuilder':
        self._conditions_and_adapters.append((condition, adapter))
        return self

    def build(self) -> CompoundAdapter:
        return CompoundAdapter(
            self._conditions_and_adapters,
            self._body_path,
            self._metadata_path,
            self._mapping
        )

