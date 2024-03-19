from abc import ABC, abstractmethod
from typing import Dict
from recon_lw.interpretation.adapter.adapter_context import AdapterContext
from typing import List, Any, Optional, Set


class Adapter(ABC):
    """
        Abstract base class representing an adapter.

        This class defines the interface for adapter implementations. Adapters are
        used to transform messages from one format to another.
        It is required to make it possible to compare two streams in different formats.

        Attributes:
            covered_fields (set): A set containing the fields covered by the adapter.
            body_field (list of str): The path to the body field in the message.
            metadata_path (list of str): The path to the metadata field in the message.

        Methods:
            __init__: Constructor method for the Adapter class.
            get_fields_group: Abstract method to get fields group from a message.
            on_message: Abstract method to handle incoming messages.
            on_message_exit: Abstract method to handle exiting messages.
            get: Abstract method to get a field from a message.
            get_root_message_field: Abstract method to get the root message field.
            get_metadata_field: Abstract method to get a metadata field from a message.
            get_fields_coverage: Method to get the coverage of fields.
            get_body: Method to get the body of a message.
            get_metadata: Method to get the metadata of a message.
            get_direct_body: Method to get a direct body field from a message.
            get_message_type: Method to get the message type from metadata.
            set: Method to set a field in a message.
        """

    covered_fields = set()
    body_field = ["body", "fields"]
    metadata_path = ["body", "metadata"]

    def __init__(
            self,
            body_path: Optional[List[str]] = None,
            metadata_path: Optional[List['str']] = None,
            extractors_mapping: Optional[Dict[str, Any]]=None
    ):
        self.adapter_context = AdapterContext()
        self.mapping = extractors_mapping if extractors_mapping else {}
        if body_path is not None:
            self.body_field = body_path

        if metadata_path is not None:
            self.metadata_path = metadata_path

    @abstractmethod
    def get_fields_group(self, message, group_name) -> Dict[str, Any]:
        pass

    @abstractmethod
    def on_message(self, m):
        pass

    @abstractmethod
    def on_message_exit(self, m):
        pass

    @abstractmethod
    def get(self, message, field, strict=False) -> Any:
        pass

    @abstractmethod
    def get_root_message_field(self, message, parameter_name, strict=False) -> Any:
        pass

    @abstractmethod
    def get_metadata_field(self, message, field_name, strict=False) -> Any:
        pass

    def get_fields_coverage(self) -> Set[str]:
        pass

    def get_body(self, m) -> Dict[str, Any]:
        for key in self.body_field:
            m = m[key]
            if isinstance(m, List):
                m = m[0]
        return m

    def get_metadata(self, m) -> Dict[str, Any]:
        for key in self.metadata_path:
            m = m[key]
            if isinstance(m, List):
                m = m[0]
        return m

    def get_direct_body(self, message, field) -> Dict[str, Any]:
        return self.get_body(message).get(field)

    def get_message_type(self, message) -> str:
        return self.get_metadata(message)['messageType']

    def set(self, message, field, val):
        message[field] = val
