from recon_lw.interpretation.field_extractor.base import Extractor
from recon_lw.interpretation.adapter.base import Adapter
from typing import Optional, Any, Dict
from recon_lw.core.type.types import Message


class SimpleRefDataFieldExtractor(Extractor):
    """
    Compares extracted data against a reference dataset, highlighting discrepancies.
    """

    def __init__(self, field_name: str, base_extractor: Extractor, ref_data_key_extractor: Extractor,
                 ref_data_dict: Dict[str, Any], not_found_field_prefix="NOT_FOUND"):
        super().__init__(field_name)
        self.base_extractor = base_extractor
        self.ref_data_key_extractor = ref_data_key_extractor
        self.ref_data_dict = ref_data_dict
        self.not_found_field_prefix = not_found_field_prefix

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        message_value = self.base_extractor(message, adapter)
        refdata_key = self.ref_data_key_extractor(message, adapter)
        refdata_value = self.ref_data_dict.get(refdata_key)

        if message_value != Extractor.NOT_EXTRACTED and refdata_value != message_value:
            discrepancy = f"[{refdata_value} != {message_value}]" if refdata_value \
                else f"{self.not_found_field_prefix} != {message_value}"
            return discrepancy
        return message_value


class SimpleRefDataFieldExtractorBuilder:
    def __init__(self):
        self.field_name = ""
        self.base_extractor = None
        self.ref_data_key_extractor = None
        self.ref_data_dict = {}
        self.not_found_field_prefix = "NOT_FOUND"

    def set_field_name(self, field_name: str) -> 'SimpleRefDataFieldExtractorBuilder':
        self.field_name = field_name
        return self

    def set_base_extractor(self, extractor: Extractor) -> 'SimpleRefDataFieldExtractorBuilder':
        self.base_extractor = extractor
        return self

    def set_ref_data_key_extractor(self, extractor: Extractor) -> 'SimpleRefDataFieldExtractorBuilder':
        self.ref_data_key_extractor = extractor
        return self

    def set_ref_data_dict(self, ref_data_dict: Dict[str, Any]) -> 'SimpleRefDataFieldExtractorBuilder':
        self.ref_data_dict = ref_data_dict
        return self

    def set_not_found_field_prefix(self, prefix: str) -> 'SimpleRefDataFieldExtractorBuilder':
        self.not_found_field_prefix = prefix
        return self

    def build(self) -> SimpleRefDataFieldExtractor:
        if not self.field_name or not self.base_extractor or not self.ref_data_key_extractor:
            raise ValueError("Field name, base extractor, and ref data key extractor must be set.")
        return SimpleRefDataFieldExtractor(
            self.field_name,
            self.base_extractor,
            self.ref_data_key_extractor,
            self.ref_data_dict,
            self.not_found_field_prefix
        )
