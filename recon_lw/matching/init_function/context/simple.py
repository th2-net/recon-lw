from recon_lw.matching.init_function.context.base import AbstractMatcherContext
from sortedcontainers import SortedKeyList

class SimpleMatcherContext(AbstractMatcherContext):

    def __init__(self, match_index: dict, time_index: dict, message_cache: dict):
        self.match_index = match_index
        self.time_index = time_index
        self.message_cache = message_cache

    def to_dict(self) -> dict:
        return {
            'match_index': self.match_index,
            'time_index': self.time_index,
            'message_cache': self.message_cache
        }

