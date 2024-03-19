from recon_lw.interpretation.field_extractor.base import Extractor
from typing import Optional

from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import Message
from recon_lw.interpretation.condition import Condition


class SimpleCacheExtractor(Extractor):
    """
    Caches and extracts field values, using an entity ID to differentiate cache entries.
    """

    def __init__(self, field_name: str, entity_id_extractor: Extractor, field_extractor: Extractor, empty_values=None):
        super().__init__(field_name)
        self.empty_values = empty_values if empty_values is not None else {Extractor.NOT_EXTRACTED, ""}
        self.field_name = field_name
        self.entity_id_extractor = entity_id_extractor
        self.field_extractor = field_extractor

    def extract(self, message: Message, adapter) -> Optional[str]:
        entity_id = self.entity_id_extractor(message, adapter)
        val = self.field_extractor(message, adapter)
        if val not in self.empty_values:
            adapter.adapter_context.cache.setdefault(entity_id, {})[self.field_name] = val
            return val
        return adapter.adapter_context.cache.get(entity_id, {}).get(self.field_name, Extractor.NOT_EXTRACTED)


class SimpleCacheExtractorBuilder:
    def __init__(self):
        self.field_name = ""
        self.entity_id_extractor = None
        self.field_extractor = None
        self.empty_values = None

    def set_field_name(self, field_name: str) -> 'SimpleCacheExtractorBuilder':
        self.field_name = field_name
        return self

    def set_entity_id_extractor(self, extractor: Extractor) -> 'SimpleCacheExtractorBuilder':
        self.entity_id_extractor = extractor
        return self

    def set_field_extractor(self, extractor: Extractor) -> 'SimpleCacheExtractorBuilder':
        self.field_extractor = extractor
        return self

    def set_empty_values(self, empty_values: set) -> 'SimpleCacheExtractorBuilder':
        self.empty_values = empty_values
        return self

    def build(self) -> SimpleCacheExtractor:
        if not self.field_name or not self.entity_id_extractor or not self.field_extractor:
            raise ValueError("Field name, entity ID extractor, and field extractor must be set.")
        return SimpleCacheExtractor(self.field_name, self.entity_id_extractor, self.field_extractor, self.empty_values)


class CacheFillWithConditionExtractor(Extractor):
    """
    Extracts and caches field values based on a condition.
    """

    def __init__(self,
                 field_name: str,
                 entity_id_extractor: Extractor,
                 field_extractor: Extractor,
                 no_val_in_cache=b"404",
                 condition=None):
        super().__init__(field_name)
        self.field_name = field_name
        self.entity_id_extractor = entity_id_extractor
        self.field_extractor = field_extractor
        self.no_val_in_cache = no_val_in_cache
        self.condition = condition if condition is not None else lambda msg: False

    def extract(self, message: Message, adapter: Adapter) -> Optional[str]:
        entity_id = self.entity_id_extractor(message, adapter)
        val = self.field_extractor(message, adapter)
        if val in [self.no_val_in_cache, Extractor.NOT_EXTRACTED] or self.condition(message):
            adapter.adapter_context.cache.setdefault(entity_id, {})[self.field_name] = val
        else:
            val = adapter.adapter_context.cache.get(entity_id, {}).get(self.field_name, self.no_val_in_cache)
        return val


class CacheFillWithConditionExtractorBuilder:
    def __init__(self):
        self.field_name = ""
        self.entity_id_extractor = None
        self.field_extractor = None
        self.no_val_in_cache = b"404"
        self.condition = lambda msg: False

    def set_field_name(self, field_name: str) -> 'CacheFillWithConditionExtractorBuilder':
        self.field_name = field_name
        return self

    def set_entity_id_extractor(self, extractor: Extractor) -> 'CacheFillWithConditionExtractorBuilder':
        self.entity_id_extractor = extractor
        return self

    def set_field_extractor(self, extractor: Extractor) -> 'CacheFillWithConditionExtractorBuilder':
        self.field_extractor = extractor
        return self

    def set_no_val_in_cache(self, no_val: bytes) -> 'CacheFillWithConditionExtractorBuilder':
        self.no_val_in_cache = no_val
        return self

    def set_condition(self, condition: Condition) -> 'CacheFillWithConditionExtractorBuilder':
        self.condition = condition
        return self

    def build(self) -> CacheFillWithConditionExtractor:
        if not self.field_name or not self.entity_id_extractor or not self.field_extractor:
            raise ValueError("Field name, entity ID extractor, and field extractor must be set.")
        return CacheFillWithConditionExtractor(
            self.field_name,
            self.entity_id_extractor,
            self.field_extractor,
            self.no_val_in_cache,
            self.condition
        )
