from typing import List

from recon_lw.core.cache.processor.base import ICacheProcessor, CacheStore
from recon_lw.core.type.types import Message


class ChainCacheProcessor(ICacheProcessor):
    def __init__(self, processors: List[ICacheProcessor]):
        self.processors = processors
    def __call__(self, msg: Message, cache: CacheStore):
        for processor in self.processors:
            processor(msg, cache)