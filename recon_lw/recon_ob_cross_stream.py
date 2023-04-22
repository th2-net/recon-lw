from recon_lw import recon_lw
from recon_lw.EventsSaver import EventsSaver
from recon_lw.TimeCacheMatcher import TimeCacheMatcher
from datetime import datetime


def synopsys(price_condition, num_orders_condition, size_condition):
    syn = ""
    if not price_condition:
        syn += "p"
    if not num_orders_condition:
        syn += "n"
    if not size_condition:
        syn += "s"
    return syn


def compare_full_vs_aggr(full_book, aggr_book):
    problems = []
    for side in ["ask", "bid"]:
        full_levels = list(full_book[side].keys())
        if side == "ask":
            full_levels.sort()
        else:
            full_levels.sort(reverse=True)
        aggr_levels = aggr_book[side + "_aggr"]
        for i in range(aggr_book["aggr_max_levels"]):
            if i < len(full_levels) and i < len(aggr_levels):
                price_condition = full_levels[i] == aggr_levels[i]["price"]
                num_orders_condition = len(full_book[side][full_levels[i]]) == \
                                       (aggr_levels[i]["real_num_orders"] + aggr_levels[i]["impl_num_orders"])
                size_condition = sum(full_book[side][full_levels[i]].vales()) == \
                                 (aggr_levels[i]["real_qty"] + aggr_levels[i]["impl_qty"])
                if not (price_condition and num_orders_condition and size_condition):
                    problems.append({"synopsys": synopsys(price_condition, num_orders_condition, size_condition),
                                     "side": side,
                                     "level": i+1})
            elif i >= len(full_levels) and i >= len(aggr_levels):
                break
            elif i >= len(full_levels):
                problems.append({"synopsys": " full_miss_level", "side": side, "level": i + 1})
            elif i >= len(aggr_levels):
                problems.append({"synopsys": " aggr_miss_level", "side": side, "level": i + 1})

    return problems


def compare_full_vs_top(full_book, top_book):
    problems = []
    if len(full_book["ask"]) > 0:
        if top_book["ask_price"] is None:
            problems.append({"synopsys": "top_miss_level", "side": "ask"})
        else:
            top_p = min(full_book["ask"].keys())
            price_condition = top_p == top_book["ask_price"]
            num_orders_condition = len(full_book["ask"][top_p]) ==\
                                   (top_book["ask_impl_n_orders"]+top_book["ask_real_n_orders"])
            size_condition = sum(full_book["ask"][top_p].values()) == (top_book["ask_real_qty"]+top_book["ask_impl_qty"])
            if not (price_condition and num_orders_condition and size_condition):
                problems.append({"synopsys": synopsys(price_condition, num_orders_condition, size_condition),
                                 "side": "ask"})
    else:
        if top_book["ask_price"] is not None:
            problems.append({"synopsys": "full_miss_level", "side": "ask"})

    if len(full_book["bid"]) > 0:
        if top_book["bid_price"] is None:
            problems.append({"synopsys": "top_miss_level", "side": "bid"})
        else:
            top_p = max(full_book["bid"].keys())
            price_condition = top_p == top_book["bid_price"]
            num_orders_condition = len(full_book["bid"][top_p]) ==\
                                   (top_book["bid_impl_n_orders"]+top_book["bid_real_n_orders"])
            size_condition = sum(full_book["bid"][top_p].values()) == (top_book["bid_real_qty"]+top_book["bid_impl_qty"])
            if not (price_condition and num_orders_condition and size_condition):
                problems.append({"synopsys": synopsys(price_condition, num_orders_condition, size_condition),
                                 "side": "bid"})
    else:
        if top_book["bid_price"] is not None:
            problems.append({"synopsys": "full_miss_level", "side": "bid"})
    return problems


def ob_compare_get_timestamp_key1_key2_aggr(full_session, comp_session, o):
    if o["sessionId"] == full_session:
        return o["timestamp"], "{0}_{1}".format(o["body"]["book_id"], o["body"]["aggr_seq"]["limit_v"]), None

    if o["sessionId"] == comp_session:
        return o["timestamp"], None ,"{0}_{1}".format(o["body"]["book_id"], o["body"]["aggr_seq"]["limit_v"])

    return None, None, None


def ob_compare_get_timestamp_key1_key2_top(full_session, comp_session, o):
    if o["sessionId"] == full_session:
        return o["body"]["timestamp"], "{0}_{1}".format(o["body"]["book_id"], o["body"]["aggr_seq"]["top_v"]), None

    if o["sessionId"] == comp_session:
        return o["body"]["timestamp"], None, "{0}_{1}".format(o["body"]["book_id"], o["body"]["aggr_seq"]["top_v"])

    return None, None, None


def ob_compare_interpret_match_aggr(match, create_event, save_events):
    if match[0]["body"] is not None and match[1]["body"] is not None:
        comp_res = compare_full_vs_aggr(match[0]["body"],match[1]["body"])
        if len(comp_res) > 0:
            error_event = create_event("StreamMismatch",
                                       "StreamMismatch",
                                       False,
                                       {"full_book_event": match[0]["eventId"],
                                        "aggr_book_event": match[1]["eventId"],
                                        "book_id": match[0]["body"]["book_id"],
                                        "version": match[0]["body"]["aggr_seq"]["limit_v"],
                                        "errors": comp_res})
            save_events([error_event])
    elif match[0]["body"] is not None:
        error_event = create_event("StreamMismatchNoAggr",
                                   "StreamMismatchNoAggr",
                                   False,
                                   {"full_book_event": match[0]["eventId"],
                                    "book_id": match[0]["body"]["book_id"],
                                    "version": match[0]["body"]["aggr_seq"]["limit_v"]})
        save_events([error_event])
    elif match[1]["body"] is not None:
        error_event = create_event("StreamMismatchNoFull",
                                   "StreamMismatchNoFull",
                                   False,
                                   {"aggr_book_event": match[1]["eventId"],
                                    "book_id": match[1]["body"]["book_id"],
                                    "version": match[1]["body"]["aggr_seq"]["limit_v"]})
        save_events([error_event])


def ob_compare_interpret_match_top(match, create_event, save_events):
    if match[0]["body"] is not None and match[1]["body"] is not None:
        comp_res = compare_full_vs_top(match[0]["body"],match[1]["body"])
        if len(comp_res) > 0:
            error_event = create_event("StreamMismatch",
                                       "StreamMismatch",
                                       False,
                                       {"full_book_event": match[0]["eventId"],
                                        "top_book_event": match[1]["eventId"],
                                        "book_id": match[0]["body"]["book_id"],
                                        "version": match[0]["body"]["aggr_seq"]["limit_v"],
                                        "errors": comp_res})
            save_events([error_event])
    elif match[0]["body"] is not None:
        error_event = create_event("StreamMismatchNoTop",
                                   "StreamMismatchNoTop",
                                   False,
                                   {"full_book_event": match[0]["eventId"],
                                    "book_id": match[0]["body"]["book_id"],
                                    "version": match[0]["body"]["aggr_seq"]["limit_v"]})
        save_events([error_event])
    elif match[1]["body"] is not None:
        error_event = create_event("StreamMismatchNoFull",
                                   "StreamMismatchNoFull",
                                   False,
                                   {"top_book_event": match[1]["eventId"],
                                    "book_id": match[1]["body"]["book_id"],
                                    "version": match[1]["body"]["aggr_seq"]["limit_v"]})
        save_events([error_event])


#{"horizon_dely": 180, full_session: "aaa", aggr_session: "bbb", top_session: "ccc"}
def ob_compare_streams(source_events, results_path, rules_dict):
    events_saver = EventsSaver(results_path)
    processors = []
    root_event = events_saver.create_event("recon_lw_ob_streams " + datetime.now().isoformat(), "Microservice")
    events_saver.save_events([root_event])
    for rule_name, rule_params in rules_dict.items:
        rule_root_event = events_saver.create_event(rule_name,"OBStreamsCompareRule", parentId=root_event["parentId"])
        events_saver.save_events([rule_root_event])
        full_session = rule_params["full_session"]
        if "aggr_session" in rule_params:
            aggr_session = rule_params["aggr_session"]
            processor_aggr = TimeCacheMatcher(rule_params["horizon_delay"],
                                              lambda o: ob_compare_get_timestamp_key1_key2_aggr(full_session,aggr_session,o),
                                              ob_compare_interpret_match_aggr,
                                              lambda name, ev_type, ok, body: events_saver.create_event(name,
                                                                                                        ev_type,
                                                                                                        ok,
                                                                                                        body,
                                                                                                        parentId=rule_root_event["parentId"]),
                                              lambda ev_batch: events_saver.save_events(ev_batch)
                                              )
            processors.append(processor_aggr)
        if "top_session" in rule_params:
            top_session = rule_params["top_session"]
            processor_top = TimeCacheMatcher(rule_params["horizon_delay"],
                                             lambda o: ob_compare_get_timestamp_key1_key2_top(full_session,top_session,o),
                                             ob_compare_interpret_match_aggr,
                                             lambda name, ev_type, ok, body: events_saver.create_event(name,
                                                                                                       ev_type,
                                                                                                       ok,
                                                                                                       body,
                                                                                                       parentId=rule_root_event["parentId"]),
                                             lambda ev_batch: events_saver.save_events(ev_batch)
                                             )
            processors.append(processor_top)

    order_books_events = source_events.filter(lambda e: e["eventType"] == "OrderBook")
    events_buffer = [None] * 100
    buffer_len = 100
    buffer_index = 0
    for e in order_books_events:
        events_buffer[buffer_index] = e
        buffer_index += 1
        if buffer_index == buffer_len:
            for p in processors:
                p.process_objects_batch(events_buffer)
    for p in processors:
        p.process_objects_batch(events_buffer[:buffer_index])
        p.flush_all()


def _example_of_usage_run_recon():
    events = None # This are the events generated by previous round lw_recon
    path = None # This should be empty folder for new events generated by this new round of recon

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
    previous_events = None # previous events initially generated by lw_recon
    new_events = None # get_events_from_dir("dir for second run")

    # Use same method to categorize that fter first round of lw_recon

    # additional analysis
    # in the ErrorEvent you can get book_id - instrument and version - number of update of the specific book
    # To investigate we can get list of books for one session and for other and compare
    ver = None # see it in the error event
    book_id = None #see it in the error event

    books_session1 = previous_events.filter(lambda e: e["sessionId"] == "session1" and
                                                      e["body"]["book_id"] == book_id and
                                                      ver - 3 <= e["body"]["aggr_seq"]["limit_v"] <= ver + 3)
    books_session2 = previous_events.filter(lambda e: e["sessionId"] == "session2" and
                                                      e["body"]["book_id"] == book_id and
                                                      ver - 3 <= e["body"]["aggr_seq"]["limit_v"] <= ver + 3)

    for e1,e2 in zip(books_session1, books_session2):
        print(e1)
        print(e2)
        print(" ============= ")
        # We can use pandas to show it better


