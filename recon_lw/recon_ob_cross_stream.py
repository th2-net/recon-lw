import pathlib
from datetime import datetime
from itertools import islice

from recon_lw import recon_lw
from recon_lw.EventsSaver import EventsSaver
from recon_lw.TimeCacheMatcher import TimeCacheMatcher


def synopsys(price_condition: bool, num_orders_condition: bool, size_condition: bool) -> str:
    syn = ""
    if not price_condition:
        syn += "p"
    if not num_orders_condition:
        syn += "n"
    if not size_condition:
        syn += "s"
    return syn

def compare_keys(keys_collection, book1, book2):
    problems = []
    for k in keys_collection:
        if k not in book1 or book1[k] is None:
            if k in book2 and book2[k] is not None:
                problems.append({"mismatch_key": k, "1": None, "2": book2[k]})
                continue
            else:
                continue
        if k not in book2 or book2[k] is None:
            problems.append({"mismatch_key": k, "1": book1[k], "2": None})
            continue
        if book1[k] != book2[k]:
            problems.append({"mismatch_key": k, "1": book1[k], "2": book2[k]})
    return problems


def compare_full_vs_aggr(full_book: dict, aggr_book: dict) -> list:
    problems = []
    for side in ["ask", "bid"]:
        full_levels = list(full_book[side].keys())
        if side == "ask":
            full_levels.sort()
        else:
            full_levels.sort(reverse=True)
        aggr_levels = [level for level in aggr_book[side + "_aggr"] if level["real_num_orders"] != 0]
        for i in range(aggr_book["aggr_max_levels"]):
            if i < len(full_levels) and i < len(aggr_levels):
                price_condition = full_levels[i] == aggr_levels[i]["price"]
                num_orders_condition = len(full_book[side][full_levels[i]]) == \
                                       (aggr_levels[i]["real_num_orders"])
                size_condition = sum(full_book[side][full_levels[i]].values()) == \
                                 (aggr_levels[i]["real_qty"])
                if not (price_condition and num_orders_condition and size_condition):
                    problems.append({"synopsys": synopsys(price_condition, num_orders_condition, size_condition),
                                     "side": side,
                                     "level": i + 1})
            elif i >= len(full_levels) and i >= len(aggr_levels):
                break
            elif i >= len(full_levels):
                problems.append({"synopsys": " full_miss_level", "side": side, "level": i + 1})
            elif i >= len(aggr_levels):
                problems.append({"synopsys": " aggr_miss_level", "side": side, "level": i + 1})
    problems.extend(compare_keys(["open_price",
                                  "last_price",
                                  "max_price",
                                  "min_price",
                                  "ind_open_price",
                                  "ind_open_size",
                                  "ind_open_mid_price"], full_book, aggr_book))
    
    return problems


def compare_aggr_vs_top(aggr_book: dict, top_book: dict):
    problems = []
    if len(aggr_book["ask_aggr"]) > 0:
        if top_book["ask_real_qty"] == 0 and top_book["ask_impl_qty"] == 0:
            problems.append({"synopsys": "top_miss_level", "side": "ask"})
        else:
            price_condition = aggr_book["ask_aggr"][0]["price"] == top_book["ask_price"]
            num_orders_condition1 = (aggr_book["ask_aggr"][0]["real_num_orders"]) == \
                                    (top_book["ask_real_n_orders"])
            num_orders_condition2 = (aggr_book["ask_aggr"][0]["impl_num_orders"]) == \
                                    (top_book["ask_impl_n_orders"])
            size_condition1 = aggr_book["ask_aggr"][0]["real_qty"] == (top_book["ask_real_qty"])
            size_condition2 = aggr_book["ask_aggr"][0]["impl_qty"] == (top_book["ask_impl_qty"])
            if not (price_condition and num_orders_condition1
                    and num_orders_condition2 and size_condition1 and size_condition2):
                problems.append({"synopsys": synopsys(price_condition,
                                                      num_orders_condition1 and num_orders_condition2,
                                                      size_condition1 and size_condition2),
                                 "side": "ask"})
    else:
        if top_book["ask_real_qty"] != 0 or top_book["ask_impl_qty"] != 0:
            problems.append({"synopsys": "aggr_miss_level", "side": "ask"})

    if len(aggr_book["bid_aggr"]) > 0:
        if top_book["bid_real_qty"] == 0 and top_book["bid_impl_qty"] == 0:
            problems.append({"synopsys": "top_miss_level", "side": "bid"})
        else:
            price_condition = aggr_book["bid_aggr"][0]["price"] == top_book["bid_price"]
            num_orders_condition1 = (aggr_book["bid_aggr"][0]["real_num_orders"]) == \
                                    (top_book["bid_real_n_orders"])
            num_orders_condition2 = (aggr_book["bid_aggr"][0]["impl_num_orders"]) == \
                                    (top_book["bid_impl_n_orders"])
            size_condition1 = aggr_book["bid_aggr"][0]["real_qty"] == (top_book["bid_real_qty"])
            size_condition2 = aggr_book["bid_aggr"][0]["impl_qty"] == (top_book["bid_impl_qty"])
            if not (price_condition and num_orders_condition1
                    and num_orders_condition2 and size_condition1 and size_condition2):
                problems.append({"synopsys": synopsys(price_condition,
                                                      num_orders_condition1 and num_orders_condition2,
                                                      size_condition1 and size_condition2),
                                 "side": "bid"})
    else:
        if top_book["bid_real_qty"] != 0 or top_book["bid_impl_qty"] != 0:
            problems.append({"synopsys": "full_miss_level", "side": "bid"})
    problems.extend(compare_keys(["open_price",
                                  "last_price",
                                  "max_price",
                                  "min_price",
                                  "ind_open_price",
                                  "ind_open_size",
                                  "ind_open_mid_price"], aggr_book, top_book))
    return problems


def compare_full_vs_top(full_book: dict, top_book: dict):
    problems = []
    if top_book["ask_real_n_orders"] == 0 and top_book["ask_impl_n_orders"] != 0:
        problems = []
    elif len(full_book["ask"]) > 0:
        if top_book["ask_real_qty"] == 0:
            problems.append({"synopsys": "top_miss_level", "side": "ask"})
        else:
            top_p = min(full_book["ask"].keys())
            price_condition = top_p == top_book["ask_price"]
            num_orders_condition = len(full_book["ask"][top_p]) == \
                                   (top_book["ask_real_n_orders"])
            size_condition = sum(full_book["ask"][top_p].values()) == (top_book["ask_real_qty"])
            if not (price_condition and num_orders_condition and size_condition):
                problems.append({"synopsys": synopsys(price_condition, num_orders_condition, size_condition),
                                 "side": "ask"})
    else:
        if top_book["ask_real_qty"] != 0:
            problems.append({"synopsys": "full_miss_level", "side": "ask"})

    if top_book["bid_real_n_orders"] == 0 and top_book["bid_impl_n_orders"] != 0:
        problems = problems
    elif len(full_book["bid"]) > 0:
        if top_book["bid_real_qty"] == 0:
            problems.append({"synopsys": "top_miss_level", "side": "bid"})
        else:
            top_p = max(full_book["bid"].keys())
            price_condition = top_p == top_book["bid_price"]
            num_orders_condition = len(full_book["bid"][top_p]) == \
                                   (top_book["bid_real_n_orders"])
            size_condition = sum(full_book["bid"][top_p].values()) == (top_book["bid_real_qty"])
            if not (price_condition and num_orders_condition and size_condition):
                problems.append({"synopsys": synopsys(price_condition, num_orders_condition, size_condition),
                                 "side": "bid"})
    else:
        if top_book["bid_real_qty"] != 0:
            problems.append({"synopsys": "full_miss_level", "side": "bid"})
    problems.extend(compare_keys(["open_price",
                                  "last_price",
                                  "max_price",
                                  "min_price",
                                  "ind_open_price",
                                  "ind_open_size",
                                  "ind_open_mid_price"], full_book, top_book))
    return problems


def ob_compare_get_timestamp_key1_key2_aggr(o, custom_settings):
    if o["body"]["aggr_seq"]["limit_delta"] not in [1, 2]:
        return None, None, None
    if o["body"]["sessionId"] == custom_settings["full_session"]:
        return o["body"]["timestamp"], "{0}_{1}_{2}".format(o["body"]["book_id"],
                                                            o["body"]["time_of_event"],
                                                            o["body"]["aggr_seq"]["limit_v2"]), None

    if o["body"]["sessionId"] == custom_settings["comp_session"]:
        return o["body"]["timestamp"], None, "{0}_{1}_{2}".format(o["body"]["book_id"],
                                                                  o["body"]["time_of_event"],
                                                                  o["body"]["aggr_seq"]["limit_v2"])

    return None, None, None


def ob_compare_get_timestamp_key1_key2_top(o, custom_settings):
    if o["body"]["aggr_seq"]["top_delta"] not in [1, 2]:
        return None, None, None

    if o["body"]["sessionId"] == custom_settings["full_session"]:
        return o["body"]["timestamp"], "{0}_{1}_{2}".format(o["body"]["book_id"],
                                                            o["body"]["time_of_event"],
                                                            o["body"]["aggr_seq"]["top_v2"]), None

    if o["body"]["sessionId"] == custom_settings["comp_session"]:
        return o["body"]["timestamp"], None, "{0}_{1}_{2}".format(o["body"]["book_id"],
                                                                  o["body"]["time_of_event"],
                                                                  o["body"]["aggr_seq"]["top_v2"])

    return None, None, None


def ob_compare_get_timestamp_key1_key2_top_aggr(o, custom_settings):
    if o["body"]["aggr_seq"]["top_delta"] not in [1, 2]:
        return None, None, None

    if o["body"]["sessionId"] == custom_settings["top_session"]:
        return o["body"]["timestamp"], "{0}_{1}_{2}".format(o["body"]["book_id"],
                                                            o["body"]["time_of_event"],
                                                            o["body"]["aggr_seq"]["top_v2"]), None

    if o["body"]["sessionId"] == custom_settings["aggr_session"]:
        return o["body"]["timestamp"], None, "{0}_{1}_{2}".format(o["body"]["book_id"],
                                                                  o["body"]["time_of_event"],
                                                                  o["body"]["aggr_seq"]["top_v2"])

    return None, None, None


def ob_compare_interpret_match_aggr(match, custom_settings, create_event, save_events):
    if match[0] is not None and match[1] is not None:
        comp_res = compare_full_vs_aggr(match[0]["body"], match[1]["body"])
        if len(comp_res) > 0:
            error_event = create_event("23:mismatch",
                                       "23:mismatch",
                                       False,
                                       {"full_book_event": match[0]["eventId"],
                                        "aggr_book_event": match[1]["eventId"],
                                        "book_id": match[0]["body"]["book_id"],
                                        "time_of_event": match[0]["body"]["time_of_event"],
                                        "limit_v2": match[0]["body"]["aggr_seq"]["limit_v2"],
                                        "errors": comp_res})
            save_events([error_event])
    elif match[0] is not None:
        tech_info = ob_compare_get_timestamp_key1_key2_aggr(match[0], custom_settings)
        error_event = create_event("23:missing2",
                                   "23:missing2",
                                   False,
                                   {"full_book_event": match[0]["eventId"],
                                    "book_id": match[0]["body"]["book_id"],
                                    "time_of_event": match[0]["body"]["time_of_event"],
                                    "limit_v2": match[0]["body"]["aggr_seq"]["limit_v2"],
                                    "sessionId": match[0]["body"]["sessionId"],
                                    "tech_info": tech_info})
        save_events([error_event])
    elif match[1] is not None:
        e_type = "23:missing3 impl" if match[1]["body"]["implied_only"] else "23:missing3"
        error_event = create_event(e_type,
                                   e_type,
                                   False,
                                   {"aggr_book_event": match[1]["eventId"],
                                    "book_id": match[1]["body"]["book_id"],
                                    "time_of_event": match[1]["body"]["time_of_event"],
                                   "limit_v2": match[1]["body"]["aggr_seq"]["limit_v2"],
                                    "sessionId": match[1]["body"]["sessionId"]})
        save_events([error_event])


def ob_compare_interpret_match_top(match, custom_settings, create_event, save_events):
    if match[0] is not None and match[1] is not None:
        comp_res = compare_full_vs_top(match[0]["body"], match[1]["body"])
        if len(comp_res) > 0:
            error_event = create_event("13:mismatch",
                                       "13:mismatch",
                                       False,
                                       {"full_book_event": match[0]["eventId"],
                                        "top_book_event": match[1]["eventId"],
                                        "book_id": match[0]["body"]["book_id"],
                                        "time_of_event": match[0]["body"]["time_of_event"],
                                        "top_v2": match[0]["body"]["aggr_seq"]["top_v2"],
                                        "errors": comp_res})
            save_events([error_event])
    elif match[0] is not None:
        error_event = create_event("13:missing1",
                                   "13:missing1",
                                   False,
                                   {"full_book_event": match[0]["eventId"],
                                    "book_id": match[0]["body"]["book_id"],
                                    "time_of_event": match[0]["body"]["time_of_event"],
                                    "top_v2": match[0]["body"]["aggr_seq"]["top_v2"],
                                    "sessionId": match[0]["body"]["sessionId"]})
        save_events([error_event])
    elif match[1] is not None:
        e_type = "13:missing3 impl" if match[1]["body"]["implied_only"] else "13:missing3"
        error_event = create_event(e_type,
                                   e_type,
                                   False,
                                   {"top_book_event": match[1]["eventId"],
                                    "book_id": match[1]["body"]["book_id"],
                                    "time_of_event": match[1]["body"]["time_of_event"],
                                    "top_v2": match[1]["body"]["aggr_seq"]["top_v2"],
                                    "sessionId": match[1]["body"]["sessionId"]})
        save_events([error_event])


def ob_compare_interpret_match_top_aggr(match, custom_settings, create_event, save_events):
    if match[0] is not None and match[1] is not None:
        comp_res = compare_aggr_vs_top(match[1]["body"], match[0]["body"])
        if len(comp_res) > 0:
            error_event = create_event("12:mismatch",
                                       "12:mismatch",
                                       False,
                                       {"top_book_event": match[0]["eventId"],
                                        "aggr_book_event": match[1]["eventId"],
                                        "book_id": match[0]["body"]["book_id"],
                                        "time_of_event": match[0]["body"]["time_of_event"],
                                        "top_v2": match[0]["body"]["aggr_seq"]["top_v2"],
                                        "errors": comp_res})
            save_events([error_event])
    elif match[0] is not None:
        error_event = create_event("12:missing2",
                                   "12:missing2",
                                   False,
                                   {"top_book_event": match[0]["eventId"],
                                    "book_id": match[0]["body"]["book_id"],
                                    "time_of_event": match[0]["body"]["time_of_event"],
                                    "top_v2": match[0]["body"]["aggr_seq"]["top_v2"],
                                    "sessionId": match[0]["body"]["sessionId"]})
        save_events([error_event])
    elif match[1] is not None:
        error_event = create_event("12:missing1",
                                   "12:missing1",
                                   False,
                                   {"aggr_book_event": match[1]["eventId"],
                                    "book_id": match[1]["body"]["book_id"],
                                    "time_of_event": match[1]["body"]["time_of_event"],
                                    "limit_v2": match[1]["body"]["aggr_seq"]["limit_v2"],
                                    "sessionId": match[1]["body"]["sessionId"]})
        save_events([error_event])


def split_every(n, data):
    iterable = iter(data)
    while 1:
        piece = list(islice(iterable, n))
        if not piece:
            break
        yield piece


# {"horizon_dely": 180, full_session: "aaa", aggr_session: "bbb", top_session: "ccc"}
def ob_compare_streams(source_events_path: pathlib.PosixPath, results_path: pathlib.PosixPath,
                       rules_dict: dict) -> None:
    """The entrypoint function for comparing order-books.

    Generates pickle files as result.

    Args:
        source_events_path: The path to pickle files that were generated as a result of recon_ob.py.
        results_path: The path where this function will put results in the pickle format.
        rules_dict:

    Returns:
        None
    """
    events_saver = EventsSaver(results_path)
    processors = []
    root_event = events_saver.create_event("recon_lw_ob_streams " + datetime.now().isoformat(), "Microservice")
    events_saver.save_events([root_event])
    for rule_name, rule_params in rules_dict.items():
        rule_root_event = events_saver.create_event(rule_name, "OBStreamsCompareRule", parentId=root_event["eventId"])
        events_saver.save_events([rule_root_event])
        full_session = rule_params["full_session"]
        if "aggr_session" in rule_params:
            aggr_session = rule_params["aggr_session"]
            processor_aggr = TimeCacheMatcher(
                rule_params["horizon_delay"],
                ob_compare_get_timestamp_key1_key2_aggr,
                ob_compare_interpret_match_aggr,
                {"full_session": full_session, "comp_session": aggr_session},
                lambda name, ev_type, ok, body: events_saver.create_event(
                    name, ev_type, ok, body, parentId=rule_root_event["eventId"]),
                lambda ev_batch: events_saver.save_events(ev_batch)
            )
            processors.append(processor_aggr)
        if "top_session" in rule_params:
            top_session = rule_params["top_session"]
            processor_top = TimeCacheMatcher(
                rule_params["horizon_delay"],
                ob_compare_get_timestamp_key1_key2_top,
                ob_compare_interpret_match_top,
                {"full_session": full_session, "comp_session": top_session},
                lambda name, ev_type, ok, body: events_saver.create_event(
                    name, ev_type, ok, body, parentId=rule_root_event["eventId"]),
                lambda ev_batch: events_saver.save_events(ev_batch)
            )
            processors.append(processor_top)
        if "top_session" in rule_params and "aggr_session" in rule_params:
            aggr_session = rule_params["aggr_session"]
            top_session = rule_params["top_session"]
            processor_top_aggr = TimeCacheMatcher(
                rule_params["horizon_delay"],
                ob_compare_get_timestamp_key1_key2_top_aggr,
                ob_compare_interpret_match_top_aggr,
                {"top_session": top_session, "aggr_session": aggr_session},
                lambda name, ev_type, ok, body: events_saver.create_event(
                    name, ev_type, ok, body, parentId=rule_root_event["eventId"]),
                lambda ev_batch: events_saver.save_events(ev_batch)
            )
            processors.append(processor_top_aggr)


    # order_books_events = source_events.filter(lambda e: e["eventType"] == "OrderBook")

    # buffers = split_every(100, order_books_events)
    streams = recon_lw.open_scoped_events_streams(source_events_path, lambda n: "default_" not in n)
    message_buffer = [None] * 100
    buffer_len = 100
    while len(streams) > 0:
        next_batch_len = recon_lw.get_next_batch(streams, message_buffer, buffer_len, lambda e: e["body"]["timestamp"])
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]  # List[dict]
        for p in processors:
            p.process_objects_batch(buffer_to_process)

    for p in processors:
        p.flush_all()

    events_saver.flush()


def _example_of_usage_run_recon():
    events = None  # This are the events generated by previous round lw_recon
    path = None  # This should be empty folder for new events generated by this new round of recon

    rules_dict = {
        "rule 1": {
            "horizon_delay": 180,
            "full_session": "sess1_1",
            "aggr_session": "sess1_2",
            "top_session": "sess1_3",
        },
        "rule 2": {
            "horizon_delay": 180,
            "full_session": "sess2_1",
            "aggr_session": "sess2_2",
            "top_session": "sess2_3",
        }
    }
    ob_compare_streams(events, path, rules_dict)


def _example_of_usage_see_results():
    previous_events = None  # previous events initially generated by lw_recon
    new_events = None  # get_events_from_dir("dir for second run")

    # Use same method to categorize that fter first round of lw_recon

    # additional analysis
    # in the ErrorEvent you can get book_id - instrument and version - number of update of the specific book
    # To investigate we can get list of books for one session and for other and compare
    ver = None  # see it in the error event
    book_id = None  # see it in the error event

    books_session1 = previous_events.filter(lambda e: e["sessionId"] == "session1" and
                                                      e["body"]["book_id"] == book_id and
                                                      ver - 3 <= e["body"]["aggr_seq"]["limit_v"] <= ver + 3)
    books_session2 = previous_events.filter(lambda e: e["sessionId"] == "session2" and
                                                      e["body"]["book_id"] == book_id and
                                                      ver - 3 <= e["body"]["aggr_seq"]["limit_v"] <= ver + 3)

    for e1, e2 in zip(books_session1, books_session2):
        print(e1)
        print(e2)
        print(" ============= ")
        # We can use pandas to show it better
