from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Protocol

from recon_lw.core.type.types import Message


@dataclass
class CacheStore:
    cache: dict

class ICacheProcessor(Protocol):
    @abstractmethod
    def __call__(self, msg: Message, cache: CacheStore):
        pass

class CacheManager:
    def __init__(self,
                 unfiltered_message_process: Optional[ICacheProcessor]=None,
                 filtered_message_processor: Optional[ICacheProcessor]=None,
                 cache_store: Optional[CacheStore] = CacheStore({})
    ):
        self.cache = cache_store
        self.unfiltered_message_process= unfiltered_message_process
        self.filtered_message_processor = filtered_message_processor

    def process_unfiltered_message(self, msg: Message):
        if self.unfiltered_message_process:
            self.unfiltered_message_process(msg, self.cache)

    def process_filtered_message(self, msg: Message):
        if self.filtered_message_processor:
            self.filtered_message_processor(msg, self.cache)


