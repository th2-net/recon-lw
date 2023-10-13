import pathlib
from datetime import datetime
from typing import List

from recon_lw import recon_lw
from recon_lw.EventsSaver import EventsSaver
from recon_lw.LastStateMatcher import LastStateMatcher
from recon_lw.message_utils import message_to_dict
import copy
from th2_data_services.config import options


def ob_compare_stats_get_state_ts_key_order(o, settings):
    if "eventId" not in o:
        return None, None, None

    if o["body"]["sessionId"] != settings["top_session"]:
        return None, None, None

    return recon_lw.epoch_nano_str_to_ts(o["body"]["time_of_event"]), o["body"]["book_id"], \
           o["body"]["v"]


def ob_compare_stats_interpret(match: List, custom_settings, create_event, save_events):
    if match[1] is None:
        error_event = create_event("StatsNotFound" + options.smfr.get_type(options.mfr.get_body(match[0])),
                                   "StatsNotFound",
                                   False,
                                   {"stats_message": match[0],
                                    "book_id": match[2]['key1'],
                                    "tech": copy.deepcopy(match[2])})
        error_event["attachedMessageIds"] = [match[0]["messageId"]]
        save_events([error_event])
        return

    stats = custom_settings["get_expected_stats_func"](match[0])
    fails = {}
    for k, v in stats.items():
        if v is None:
            if k in match[1]["body"] and match[1]["body"][k] is not None:
                fails[k] = [v, match[1]["body"][k]]
            continue
        if k not in match[1]["body"]:
            fails[k] = [v, "not initialized"]
            continue

        if str(v) != str(match[1]["body"][k]):
            fails[k] = [v, str(match[1]["body"][k])]

    result_event = create_event("StatsCheck" + options.smfr.get_type(options.mfr.get_body(match[0])),
                                "StatsCheck",
                                len(fails) == 0,
                                {"stats_message": match[0],
                                 "book_id": match[2]['key1'],
                                 "order_book": match[1]["body"],
                                 "fails": fails})
    result_event["attachedMessageIds"] = [match[0]["messageId"]]
    save_events([result_event])


def ob_compare_stats(source_stat_messages_path: pathlib.PosixPath,
                     source_ob_events_path: pathlib.PosixPath,
                     results_path: pathlib.PosixPath,
                     rules_dict: dict,
                     data_objects: list = None) -> None:
    events_saver = EventsSaver(results_path)
    processors = []
    root_event = events_saver.create_event("recon_lw_ob_streams " + datetime.now().isoformat(),
                                           "Microservice")
    events_saver.save_events([root_event])
    all_stat_sessions = set()
    for rule_name, rule_params in rules_dict.items():
        rule_root_event = events_saver.create_event(rule_name, "OBStatCompareRule",
                                                    parentId=root_event["eventId"])
        events_saver.save_events([rule_root_event])
        top_session = rule_params["top_session"]
        stat_sessions = rule_params["stat_sessions"]
        all_stat_sessions.update(stat_sessions)
        get_expected_stats_func = rule_params["get_expected_stats_func"]
        processor = LastStateMatcher(
            rule_params["horizon_delay"],
            rule_params["get_search_ts_key"],  # search_ts_key
            ob_compare_stats_get_state_ts_key_order,  # state_ts_key_order
            ob_compare_stats_interpret,  # interpret
            {"top_session": top_session, "stat_sessions": stat_sessions,
             "get_expected_stats_func": get_expected_stats_func},
            lambda name, ev_type, ok, body: events_saver.create_event(
                name, ev_type, ok, body, parentId=rule_root_event["eventId"]),
            lambda ev_batch: events_saver.save_events(ev_batch)
        )
        processors.append(processor)

    streams = recon_lw.open_scoped_events_streams(source_ob_events_path,
                                                  lambda n: "default_" not in n)
    streams2 = recon_lw.open_streams(source_stat_messages_path,
                                     lambda n: any(s in n for s in all_stat_sessions),
                                     expanded_messages=True, data_objects=data_objects)
    for elem in streams2:
        streams.add(elem)

    message_buffer = [None] * 100
    buffer_len = 100
    while len(streams) > 0:
        next_batch_len = recon_lw.get_next_batch(streams, message_buffer, buffer_len, get_timestamp)
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]
        for p in processors:
            p.process_objects_batch(buffer_to_process)

    for p in processors:
        p.flush_all()

    events_saver.flush()


def get_timestamp(o):
    # TODO - how it works?? Why do we expect timestamp in body ? -- o["body"]["timestamp"] ?
    if "messageId" in o:
        return o["timestamp"]
    else:
        return o["body"]["timestamp"]


# Example of usage Not the real code
##############################################
def get_search_stats_ts_key(m, settings):
    if m["sessionId"] not in settings["stat_sessions"]:
        return None, None

    if m["sessionType"] not in ["TradeStatisticsIntraday", "TradeStatisticsEOD"]:
        return None, None

    mm = message_to_dict(m)
    return recon_lw.epoch_nano_str_to_ts(mm["TimeOfEvent"]), mm["TradableInstrumentID"]
    # epoch_nano_str_to_ts is in recon_ob_stats module


def get_stats_example(m):
    mm = message_to_dict(m)
    stats = {
        "open_price": mm["OpenPrice"],
        "max_price": mm["TradeHigh"],
        "min_price": mm["TradeLow"],
        "last_price": mm["ClosingPrice"] if "ClosingPrice" in mm else None
    }
    return stats


def usage_example():
    splited_messages_files_path = "p1"  # splited mesages folder
    ob_events_files_path = "p2"  # orderbook events
    results_path = "p2"
    rules_dict = {
        "rule 1": {
            "horizon_delay": 180,
            "top_session": "md_session_01",
            "stat_sessions": ["md_session_04", "md_session_05"],
            "get_search_ts_key": get_search_stats_ts_key,
            "get_expected_stats_func": get_stats_example
        },
        "rule 2": {
            "horizon_delay": 180,
            "top_session": "md_session_06",
            "stat_sessions": ["md_session_09", "md_session_10"],
            "get_search_ts_key": get_search_stats_ts_key,
            "get_expected_stats_func": get_stats_example
        }
    }

    return
