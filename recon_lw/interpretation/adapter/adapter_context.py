from typing import Any, Dict, Optional

from recon_lw.core.cache.processor.base import CacheStore

class AdapterContext:

    """
    A class representing an adapter context. This can be used to store something to cache / access something from cache

    Attributes:
        _cache (dict): A dictionary to store cache data.
    """

    def __init__(self, cache_store: Optional[CacheStore]=None):
        if cache_store is None:
            self._cache_store = CacheStore({})
        else:
            self._cache_store = cache_store

    def get_cache(self) -> Dict[str, Any]:
        return self._cache_store.cache
