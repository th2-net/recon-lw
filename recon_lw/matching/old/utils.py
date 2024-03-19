from recon_lw.core.ts_converters import time_stamp_key
from recon_lw.core.utility import message_cache_pop


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

def rule_flush(current_ts, horizon_delay, match_index: dict, time_index, message_cache,
               interpret_func, event_sequence: dict, send_events_func,
               parent_event, live_orders_cache):
    old_keys = flush_old(current_ts, horizon_delay, time_index)
    events = []
    for match_key in old_keys:
        elem = match_index.pop(match_key)  # elem -- can have 2 or 3 elements inside
        if elem[0] is not None and elem[0] not in message_cache:
            # request already processed through different key
            continue

        # interpret_func function has exact format
        #   arg0 - list of matched messages
        #   arg1 - ??
        #   arg2 - EventSequence
        results = interpret_func(
            [message_cache_pop(item, message_cache) for item in elem],
            live_orders_cache,
            event_sequence
        )
        #       result = interpret_func(message_cache_pop(elem[0], message_cache),
        #                                message_cache_pop(elem[1], message_cache), event_sequence)
        if results is not None:
            for r in results:
                r["parentEventId"] = parent_event["eventId"]
                events.append(r)

    send_events_func(events)