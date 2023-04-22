from datetime import datetime, timedelta
from sortedcontainers import SortedKeyList
from th2_data_services.utils.message_utils import message_utils
from recon_lw import recon_lw
import copy


def sequence_cache_add(seq_num, ts, m, sequence_cache):
    seq_element = (seq_num, m)
    sequence = sequence_cache["sequence"]
    #gaps = sequence_cache["gaps"]
    gap = {"gap":True}
    duplicates = sequence_cache["duplicates"]
    times = sequence_cache["times"]
    if len(sequence) > 0:
        last_elem = sequence[-1]
        first_elem = sequence[0]
        if seq_num > last_elem[0]:
            sequence.add(seq_element)
            sequence.update([(i,gap) for i in range(last_elem[0]+1,seq_num)])
            times.add((ts, seq_num))
        elif seq_num < first_elem[0]:
            sequence.update([(i,gap) for i in range(seq_num+1,first_elem[0])])
            sequence.add(seq_element)
            times.add((ts, seq_num))
        else:
            if "gap" in sequence[seq_num-first_elem[0]][1]:
                del sequence[seq_num - first_elem[0]]
                sequence.add(seq_element)
                times.add((ts, seq_num))
            else:
                duplicates.add((seq_num,m["messageId"], sequence[seq_num-first_elem[0]][1]["messageId"]))
    else:
        sequence.add(seq_element)
        times.add((ts, seq_num))


def flush_sequence_get_collection(current_ts, horizon_delay, sequence_cache):
    times = sequence_cache["times"]
    sequence = sequence_cache["sequence"]
    sub_seq = None
    if current_ts is not None:
        edge_timestamp = {"epochSecond": current_ts["epochSecond"] - horizon_delay,
                          "nano": 0}
        horizon_edge = times.bisect_key_left(recon_lw.time_stamp_key(edge_timestamp))
        if horizon_edge < len(times):
            seq_index = times[horizon_edge][1]
            sub_seq = sequence.irange(None, (seq_index,None))
            for i in range(0,horizon_edge):
                times.pop(0)
        else:
            sub_seq = sequence
            times.clear()

        return sub_seq
    else:
        times.clear()
        return sequence


def flush_sequence_clear_cache(processed_len, sequence_cache):
    sequence = sequence_cache["sequence"]
    for i in range(0, processed_len):
        sequence.pop(0)


def process_market_data_update(mess, events,  books_cache, get_book_id_func ,update_book_rule,
                               check_book_rule, event_sequence, parent_event, initial_book_params, log_books_filter):
    book_id, result = get_book_id_func(mess)
    if result is not None:
        book_id_event = recon_lw.create_event("GetBookEroor:" + parent_event["eventName"], "GetBookEroor", event_sequence,
                                              ok=False,
                                              body=result,
                                              parentId=parent_event["eventId"])
        book_id_event["attachedMessageIds"] = [mess["messageId"]]
        events.append(book_id_event)

    if book_id is not None:
        if book_id not in books_cache:
            books_cache[book_id] = copy.deepcopy(initial_book_params)            
            #books_cache[book_id] = {"ask": {}, "bid": {}, "status": "?"}
        book = books_cache[book_id]
        operations = update_book_rule(book, mess)
        if operations is None:
            return
        
        for operation, parameters in operations:
            initial_book = copy.deepcopy(book)
            initial_parameters = copy.copy(parameters)
            parameters["order_book"] = book
            result, log_entries = operation(**parameters)
            if len(result) > 0:
                result["operation"] = operation.__name__
                result["operation_params"] = initial_parameters
                result["initial_book"] = initial_book
                result["book_id"] = book_id
                update_event = recon_lw.create_event("UpdateBookError:" + parent_event["eventName"], "UpdateBookError",
                                                     event_sequence,
                                                     ok=False,
                                                     body=result,
                                                     parentId=parent_event["eventId"])
                update_event["attachedMessageIds"] = [mess["messageId"]]
                events.append(update_event)
            if log_entries is not None:
                for log_book in log_entries:
                    log_book["timestamp"] = mess["timestamp"]
                    log_book["sessionId"] = mess["sessionId"]
                    log_book["book_id"] = book_id
                    log_book["operation"] = operation.__name__
                    log_book["operation_params"] = initial_parameters
                    if log_books_filter is None or log_books_filter(log_book):
                        log_event = recon_lw.create_event("OrderBook:" + mess["sessionId"],
                                                          "OrderBook",
                                                          event_sequence,
                                                          ok=True,
                                                          body=log_book,
                                                          parentId=parent_event["eventId"])
                        log_event["attachedMessageIds"] = [mess["messageId"]]
                        events.append(log_event)

            results = check_book_rule(book, event_sequence)
            if results is not None:
                for r in results:
                    if not r["successful"]:
                        r["body"]["operation_params"] = initial_parameters
                        r["body"]["initial_book"] = initial_book
                    r["body"]["operation"] = operation.__name__
                    r["body"]["book_id"] = book_id
                    r["parentEventId"] = parent_event["eventId"]
                    r["attachedMessageIds"] = [mess["messageId"]]
                    events.append(r)


def process_ob_rules(sequenced_batch, books_cache, get_book_id_func ,update_book_rule,
                     check_book_rule, event_sequence, send_events_func, parent_event, initial_book_params,
                     log_books_filter):
    events = []
    n_processed = 0
    for m in sequenced_batch:
        seq = m[0]
        mess = m[1]
        #process gaps
        if "gap" in mess:
            gap_event = recon_lw.create_event("SeqGap:" + parent_event["eventName"],"SeqGap",event_sequence,ok=False,
                                              body={"seq_num": seq} ,parentId=parent_event["eventId"])
            events.append(gap_event)
            continue
        chunk = message_utils.expand_message(mess)
        for m_upd in chunk:
            process_market_data_update(m_upd, events, books_cache, get_book_id_func, update_book_rule,
                                       check_book_rule, event_sequence, parent_event, initial_book_params,
                                       log_books_filter)
        n_processed += 1

    send_events_func(events)
    return n_processed


def init_ob_stream(rule_settings):
    rule_settings["sequence_cache"] = {"sequence": SortedKeyList(key=lambda item: item[0]),
                                       "times": SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0])),
                                       "duplicates": SortedKeyList(key=lambda item: item[0])}
    rule_settings["books_cache"] = {}


def collect_ob_stream(next_batch, rule_dict):
    sequence_cache = rule_dict["sequence_cache"]
    sequence_timestamp_extract = rule_dict["sequence_timestamp_extract"]
    for m in next_batch:
        seq_list = sequence_timestamp_extract(m)

        #seq, ts = sequence_timestamp_extract(m)
        if seq_list is not None:
            for mess, seq, ts in seq_list:
                sequence_cache_add(seq, ts, mess, sequence_cache)


def flush_ob_stream(ts,rule_settings,event_sequence, save_events_func):
    seq_batch = flush_sequence_get_collection(ts, rule_settings["horizon_delay"], rule_settings["sequence_cache"])
    n_processed = process_ob_rules(seq_batch,
                                   rule_settings["books_cache"],
                                   rule_settings["get_book_id"],
                                   rule_settings["update_book_rule"],
                                   rule_settings["check_book_rule"],
                                   event_sequence,
                                   save_events_func,
                                   rule_settings["rule_root_event"],
                                   rule_settings["initial_book_params"],
                                   rule_settings["log_books_filter"] if "log_books_filter" in rule_settings else None)
    ## Process duplicated
    duplicates = rule_settings["sequence_cache"]["duplicates"]
    n_dupl = len(duplicates)
    dupl_events = []
    for i in range(0,n_dupl):
        item = duplicates.pop(0)
        d_ev = recon_lw.create_event("Duplicate:" + rule_settings["rule_root_event"]["eventName"],"Duplicate",
                                     event_sequence,
                                     ok=False,
                                     body={"seq_num": item[0]},
                                     parentId=rule_settings["rule_root_event"]["eventId"])
        d_ev["attachedMessageIds"] = [item[1], item[2]]
        dupl_events.append(d_ev)
    save_events_func(dupl_events)
    duplicates.clear()
    flush_sequence_clear_cache(n_processed,rule_settings["sequence_cache"])


def init_aggr_seq(order_book):
    order_book["aggr_seq"] = {"top_v": 0, "top_delta": 0, "limit_v": 0, "limit_delta": 0}


def reflect_price_update_in_version(side, price, order_book):
    level = get_price_level(side, price, order_book)
    max_levels = order_book["aggr_max_levels"]
    if level <= max_levels:
        order_book["aggr_seq"]["limit_v"] += 1
        order_book["aggr_seq"]["limit_delta"] = 1
    if level == 1:
        order_book["aggr_seq"]["top_v"] += 1
        order_book["aggr_seq"]["top_delta"] = 1


def ob_add_order(order_id, price, size, side, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})

    if find_order_position(order_id, order_book)[0] is not None:
        return {"error": order_id + " already exists"}, []
    if price not in order_book[side]:
        order_book[side][price] = {order_id: size}
    else:
        order_book[side][price][order_id] = size

    reflect_price_update_in_version(side, price, order_book)
    return {}, [copy.deepcopy(order_book)]


def ob_update_order(order_id, price, size, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})

    old_side, old_price, old_size = find_order_position(order_id, order_book)
    if old_side is None:
        return {"error": order_id + " not found"}, []

    log = []
    if price == old_price:
        order_book[old_side][old_price][order_id] = size
        reflect_price_update_in_version(old_side, old_price, order_book)
        log.append(copy.deepcopy(order_book))
    else:
        # should no get here but will monitor
        reflect_price_update_in_version(old_side, old_price, order_book)
        order_book[old_side][old_price].pop(order_id)
        if len(order_book[old_side][old_price]) == 0:
            order_book[old_side].pop(old_price)
        log.append(copy.deepcopy(order_book))
        if price not in order_book[old_side]:
            order_book[old_side][price] = {}
        order_book[old_side][price][order_id] = size
        reflect_price_update_in_version(old_side, price, order_book)
        log.append(copy.deepcopy(order_book))

    return {}, log


def ob_delete_order(order_id, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})

    old_side, old_price, old_size = find_order_position(order_id, order_book)
    if old_side is None:
        return {"error": order_id + " not found"}, []

    log = []
    reflect_price_update_in_version(old_side, old_price, order_book)
    order_book[old_side][old_price].pop(order_id)
    if len(order_book[old_side][old_price]) == 0:
        order_book[old_side].pop(old_price)
        log.append(copy.deepcopy(order_book))
        max_levels = order_book["aggr_max_levels"]
        if len(order_book[old_side]) >= max_levels and order_book["aggr_seq"]["limit_delta"] == 1:
            #back from horizon update
            order_book["aggr_seq"]["limit_delta"] = 2
            order_book["aggr_seq"]["limit_v"] += 1
            log.append(copy.deepcopy(order_book))
    else:
        log.append(copy.deepcopy(order_book))

    return {}, log


def ob_trade_order(order_id, traded_size ,order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})
    old_side, old_price, old_size = find_order_position(order_id, order_book)
    log = []
    if old_side is None:
        return {"error": order_id + " not found"}, []
    if traded_size > old_size:
        return {"error": "traded size > resting size"}, []
    elif traded_size == old_size:
        reflect_price_update_in_version(old_side, old_price, order_book)
        order_book[old_side][old_price].pop(order_id)
        if len(order_book[old_side][old_price]) == 0:
            order_book[old_side].pop(old_price)
            log.append(copy.deepcopy(order_book))
            max_levels = order_book["aggr_max_levels"]
            if len(order_book[old_side]) >= max_levels and order_book["aggr_seq"]["limit_delta"] == 1:
                # back from horizon update
                order_book["aggr_seq"]["limit_delta"] = 2
                order_book["aggr_seq"]["limit_v"] += 1
                log.append(copy.deepcopy(order_book))
        else:
            log.append(copy.deepcopy(order_book))
    else:
        reflect_price_update_in_version(old_side, old_price, order_book)
        order_book[old_side][old_price][order_id] -= traded_size
        log.append(copy.deepcopy(order_book))
    return {}, log


def ob_clean_book(order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})
    for side_key in ["ask", "bid"]:
        if side_key in order_book:
            order_book[side_key].clear()
    order_book["aggr_seq"]["limit_delta"] = 1
    order_book["aggr_seq"]["limit_v"] += 1
    order_book["aggr_seq"]["top_delta"] = 1
    order_book["aggr_seq"]["top_v"] += 1
    return {}, [copy.deepcopy(order_book)]


def ob_change_status(new_status, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["status"] = new_status
    order_book["aggr_seq"]["limit_delta"] = 1
    order_book["aggr_seq"]["limit_v"] += 1
    order_book["aggr_seq"]["top_delta"] = 1
    order_book["aggr_seq"]["top_v"] += 1
    return {}, [copy.deepcopy(order_book)]


def find_order_position(order_id, order_book):
    for side in ["ask", "bid"]:
        for pr,orders in order_book[side].items():
            if order_id in orders:
                return side, pr, orders[order_id]
    return None, None, None


# levels start with 1 (1,2,......
def get_price_level(side, p, order_book):
    if p not in order_book[side]:
        return -1
    levels = list(order_book[side].keys())
    levels.sort()
    return levels.index(p)+1 if side == "ask" else len(levels)-levels.index(p)


def ob_aggr_add_level(side, level, price, real_qty, real_num_orders, impl_qty, impl_num_orders, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})
    result_body = {}
    max_levels = order_book["aggr_max_levels"]
    side_key = side+"_aggr"
    new_index = level - 1
    if not 0 <= new_index <= len(order_book[side_key]):
        result_body["error"] = "Unexpected level {0}, against existing {1}".format(level, len(order_book[side_key]))
        return result_body,[]

    new_level = {"price": price, "real_qty": real_qty, "real_num_orders": real_num_orders, "impl_qty": impl_qty,
                 "impl_num_orders": impl_num_orders}
    order_book[side_key].insert(new_index, new_level)
    if len(order_book[side_key]) == max_levels+1:
        order_book[side_key].pop()
    order_book["aggr_seq"]["limit_delta"] = 1
    order_book["aggr_seq"]["limit_v"] += 1

    return {}, [copy.deepcopy(order_book)]


def ob_aggr_delete_level(side, level, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})
    result_body = {}
    side_key = side+"_aggr"
    del_index = level - 1
    if not 0 <= del_index < len(order_book[side_key]):
        result_body["error"] = "Unexpected level {0}, against existing {1}".format(level, len(order_book[side_key]))
        return result_body, []

    order_book[side_key].pop(del_index)

    order_book["aggr_seq"]["limit_delta"] = 1
    order_book["aggr_seq"]["limit_v"] += 1
    return {}, [copy.deepcopy(order_book)]


def ob_aggr_update_level(side, level, price, real_qty, real_num_orders, impl_qty, impl_num_orders, order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})
    result_body = {}
    max_levels = order_book["aggr_max_levels"]
    side_key = side+"_aggr"
    update_index = level - 1
    if not 0 <= update_index < len(order_book[side_key]):
        result_body["error"] = "Unexpected level {0}, against existing {1}".format(level, len(order_book[side_key]))
        return result_body, []

    upd_level = {"price": price, "real_qty": real_qty, "real_num_orders": real_num_orders, 
                 "impl_qty" : impl_qty, "impl_num_orders": impl_num_orders}
    order_book[side_key][update_index].update(upd_level)
    order_book["aggr_seq"]["limit_delta"] = 1
    order_book["aggr_seq"]["limit_v"] += 1

    return {}, [copy.deepcopy(order_book)]


def ob_aggr_clean_book(order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["aggr_seq"].update({"top_delta": 0, "limit_delta": 0})
    for side_key in ["ask_aggr", "bid_aggr"]:
        if side_key in order_book:
            order_book[side_key].clear()
    order_book["aggr_seq"]["limit_delta"] = 1
    order_book["aggr_seq"]["limit_v"] += 1
    return {}, [copy.deepcopy(order_book)]


def ob_top_update(ask_price, ask_real_qty, ask_impl_qty, ask_real_n_orders, ask_impl_n_orders,
                  bid_price, bid_real_qty, bid_impl_qty, bid_real_n_orders, bid_impl_n_orders,
                  order_book):
    if "aggr_seq" not in order_book:
        init_aggr_seq(order_book)
    order_book["ask_price"] = ask_price
    order_book["ask_real_qty"] = ask_real_qty
    order_book["ask_impl_qty"] = ask_impl_qty
    order_book["ask_real_n_orders"] = ask_real_n_orders
    order_book["ask_impl_n_orders"] = ask_impl_n_orders
    order_book["bid_price"] = bid_price
    order_book["bid_real_qty"] = bid_real_qty
    order_book["bid_impl_qty"] = bid_impl_qty
    order_book["bid_real_n_orders"] = bid_real_n_orders
    order_book["bid_impl_n_orders"] = bid_impl_n_orders

    order_book["aggr_seq"]["top_delta"] = 1
    order_book["aggr_seq"]["top_v"] += 1

    return {}, [copy.deepcopy(order_book)]

