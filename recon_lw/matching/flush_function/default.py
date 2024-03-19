from recon_lw.core.type.types import InterpretationFunctionType
from recon_lw.core.utility.recon_utils import *
from recon_lw.matching.LastStateMatcher import LastStateMatcher
from recon_lw.matching.flush_function.base import FlushFunction
from recon_lw.core.rule.base import AbstractRule
from typing import Callable

from recon_lw.matching.init_function import SimpleMatcherContext


class DefaultFlushFunction(FlushFunction):
    def __init__(self,
                 interpretation_function: InterpretationFunctionType,
                 last_state_matcher: LastStateMatcher = None
                 ):
        self.interpretation_function = interpretation_function
        self.last_state_matcher = last_state_matcher

    def flush(self,
              timestamp: Optional[float],
              rule: AbstractRule,
              save_events_func: Callable[[dict], None]
              ):
        if not isinstance(rule.matcher_context, SimpleMatcherContext):
            raise ValueError('Matcher context must be SimpleMatcherContext or its extension.')
        DefaultFlushFunction.rule_flush(
            timestamp,
            rule.horizon_delay,
            rule.matcher_context.match_index,
            rule.matcher_context.time_index,
            rule.matcher_context.message_cache,
            self.interpretation_function,
            rule.get_event_sequence(),
            save_events_func,
            rule.get_root_event(),
            self.last_state_matcher
        )

    @staticmethod
    def rule_flush(current_ts, horizon_delay, match_index: dict, time_index, message_cache,
                   interpret_func, event_sequence: dict, send_events_func,
                   parent_event, live_orders_cache):

        def flush_old(current_ts, horizon_delay, time_index):
            result = []
            horizon_edge = len(time_index)
            if current_ts is not None:
                edge_timestamp = {"epochSecond": current_ts["epochSecond"] - horizon_delay,
                                  "nano": 0}
                horizon_edge = time_index.bisect_key_left(
                    time_stamp_key(edge_timestamp))

            if horizon_edge > 0:
                n = 0
                while n < horizon_edge:
                    nxt = time_index.pop(0)
                    result.append(nxt[1])
                    n += 1
            return result

        old_keys = flush_old(current_ts, horizon_delay, time_index)
        events = []
        for match_key in old_keys:
            elem = match_index.pop(match_key)
            if elem[0] is not None and elem[0] not in message_cache:
                continue
            if isinstance(interpret_func, Callable):
                results = interpret_func(
                    [message_cache_pop(item, message_cache) for item in elem],
                    live_orders_cache,
                    event_sequence
                )

            if results is not None:
                for r in results:
                    r["parentEventId"] = parent_event["eventId"]
                    events.append(r)

        send_events_func(events)