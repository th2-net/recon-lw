from typing import Any

from sortedcontainers import SortedKeyList
from th2_data_services.config import options

from recon_lw.core.ts_converters import time_stamp_key
from recon_lw.core.utility import time_index_add, message_cache_add
from recon_lw.matching.old.utils import rule_flush


def init_matcher(rule_settings):
    rule_settings["match_index"] = {}
    rule_settings["time_index"] = SortedKeyList(key=lambda t: time_stamp_key(t[0]))
    rule_settings["message_cache"] = {}

def collect_matcher(batch, rule_settings):
    rule_match_func = rule_settings["rule_match_func"]
    rule_match_func(batch, rule_settings)
    if "live_orders_cache" in rule_settings:
        rule_settings["live_orders_cache"].process_objects_batch(batch)

def flush_matcher(ts, rule_settings, event_sequence: dict, save_events_func):
    rule_flush(ts,
               rule_settings["horizon_delay"],
               rule_settings["match_index"],
               rule_settings["time_index"],
               rule_settings["message_cache"],
               rule_settings["interpret_func"],
               event_sequence,
               save_events_func,
               rule_settings["rule_root_event"],
               rule_settings["live_orders_cache"] if "live_orders_cache" in rule_settings else None)

def one_many_match(next_batch, rule_dict):
    """
    One to Many matching algorithm.

    It's expected that `first_key_func` will return [ke1, key2, ...] for
    this type of matching

    If first_key_func will return the same value for keys -- they will be
    removed as duplicates.
    Second key func -- messages with the same key will be added to result and
    provided to interpr func as [_, 2nd_key_match1,  2nd_key_match2, ...]

    Args:
        next_batch:
        rule_dict:

    Returns:

    """
    # match_index: dict[Any, MatchIndexElement] = rule_dict["match_index"]
    match_index: dict[Any, list] = rule_dict["match_index"]
    time_index = rule_dict["time_index"]
    message_cache = rule_dict["message_cache"]
    first_key_func = rule_dict["first_key_func"]
    second_key_func = rule_dict["second_key_func"]

    n_duplicates = 0
    for m in next_batch:
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
                if existing[1] is None:  # existing[1] - stream 2 message ID
                    existing[1] = message_id
                    # match_index[second_key] = [existing[0], message_id]
                    message_cache_add(m, message_cache)
                else:
                    existing.append(message_id)
                    message_cache_add(m, message_cache)

    if n_duplicates > 0:
        print(n_duplicates, " duplicates detected")

def pair_one_match(next_batch, rule_dict):
    # first_key_func takes m  returns string(key)
    match_index = rule_dict["match_index"]
    time_index = rule_dict["time_index"]
    message_cache = rule_dict["message_cache"]
    pair_key_func = rule_dict["pair_key_func"]
    one_key_func = rule_dict["one_key_func"]

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