from sortedcontainers import SortedKeyList

from recon_lw.core.stream import Streams
from recon_lw.matching.init_function.base import MatcherContextProvider

from recon_lw.core.ts_converters import time_stamp_key
from recon_lw.matching.init_function.context.simple import SimpleMatcherContext


class DefaultMatcherContextProvider(MatcherContextProvider):

    def __init__(self):
        super().__init__()
        self.context = SimpleMatcherContext(
            match_index={},
            time_index=Streams(),
            message_cache={}
        )

    def get_context(self) -> SimpleMatcherContext:
        return self.context
