from recon_lw.matching.init_function import SimpleMatcherContext
from recon_lw.matching.stream_matcher.base import ReconMatcher
from recon_lw.core.rule.one_many import OneManyRuleConfig
from typing import List, Optional, Dict
from th2_data_services.config import options
from recon_lw.core.utility.recon_utils import time_index_add, message_cache_add


class OneManyMatcher(ReconMatcher):

    def match(self, next_batch: List[Optional[Dict]], rule: OneManyRuleConfig):
        context = rule.matcher_context
        if not isinstance(context, SimpleMatcherContext):
            raise ValueError(f'Expected matcher_context type is SimpleMatcherContext or its extension.\
             Actual type is {type(rule.matcher_context)}')
        match_index = context.match_index
        time_index = context.time_index
        message_cache = context.message_cache
        first_key_func = rule.first_key_func
        second_key_func = rule.second_key_func

        n_duplicates = 0
        for m in next_batch:

            if rule.cache_manager:
                rule.cache_manager.process_unfiltered_message(m)

            first_keys = first_key_func(m)
            message_id = options.mfr.get_id(m)
            if first_keys is not None:
                match_index_element = [message_id, None]
                for first_key in first_keys:
                    if first_key not in match_index:
                        match_index[first_key] = match_index_element
                        time_index_add(first_key, m, time_index)
                        message_cache_add(m, message_cache)
                        continue
                    else:
                        existing = match_index[first_key]
                        if existing[0] is not None:
                            n_duplicates += 1
                        else:
                            existing[0] = message_id
                            message_cache_add(m, message_cache)
                            continue

            second_key = second_key_func(m)
            if second_key is not None:
                if second_key not in match_index:
                    match_index[second_key] = [None, message_id]
                    time_index_add(second_key, m, time_index)
                    message_cache_add(m, message_cache)
                else:
                    existing = match_index[second_key]
                    if existing[1] is None:
                        existing[1] = message_id
                        message_cache_add(m, message_cache)
                    else:
                        existing.append(message_id)
                        message_cache_add(m, message_cache)
            if second_key is not None or first_keys is not None:
                if rule.cache_manager:
                    rule.cache_manager.process_filtered_message(m)
        if n_duplicates > 0:
            pass
