from recon_lw.matching.LastStateMatcher import LastStateMatcher
from typing import List, Optional, Dict, Union
from recon_lw.core.rule.base import AbstractRule
from typing import Callable

from recon_lw.matching.stream_matcher import ReconMatcher
from recon_lw.matching.collect_matcher.base import CollectMatcher


class DefaultCollectMatcher(CollectMatcher):

    def __init__(self,
                 match_function: Union[ReconMatcher, Callable],
                 last_state_matcher: Optional[LastStateMatcher]=None,
                 ):
        self.last_state_matcher = last_state_matcher
        self.match_function = match_function

    def collect_matches(self, batch: List[Optional[Dict]], rule: AbstractRule):
        match_func = self.match_function

        if isinstance(match_func, Callable):
            return match_func(batch, rule.to_dict())
        elif isinstance(match_func, ReconMatcher):
            return match_func.match(batch, rule)

        if self.last_state_matcher:
            return self.last_state_matcher.process_objects_batch(batch)
