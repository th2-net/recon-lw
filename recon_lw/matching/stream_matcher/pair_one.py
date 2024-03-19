from recon_lw.matching.stream_matcher.base import ReconMatcher
from recon_lw.core.rule.pair_one import PairOneRule
from typing import Optional, List, Dict
from recon_lw.core.utility.recon_utils import time_index_add, message_cache_add
from th2_data_services.config import options

class PairOneMatcher(ReconMatcher):

    def __init__(self, rule: PairOneRule):
        super().__init__()
        self.rule = rule

    def match(self, next_batch: List[Optional[Dict]], rule: PairOneRule):
        match_index = self.rule.context.match_index
        time_index = self.rule.context.time_index
        message_cache = self.rule.context.message_cache

        pair_key_func = self.rule.pair_key_func
        one_key_func = self.rule.one_key_func

        for m in next_batch:
            pair_key = pair_key_func(m)
            message_id = options.mfr.get_id(m)
            if pair_key is not None:
                if pair_key not in match_index:
                    match_index[pair_key] = [message_id, None, None]
                    time_index_add(pair_key, m, time_index)
                    message_cache_add(m, message_cache)
                else:
                    element = match_index[pair_key]
                    if element[0] is None:
                        element[0] = message_id
                        message_cache_add(m, message_cache)
                    elif element[1] is None:
                        element[1] = message_id
                        message_cache_add(m, message_cache)
            one_key = one_key_func(m)
            if one_key is not None:
                if one_key not in match_index:
                    match_index[one_key] = [None, None, message_id]
                    time_index_add(one_key, m, time_index)
                    message_cache_add(m, message_cache)
                else:
                    element = match_index[one_key]
                    if element[2] is None:
                        element[2] = message_id
                        message_cache_add(m, message_cache)