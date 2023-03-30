from datetime import datetime, timedelta
from sortedcontainers import SortedKeyList
from th2_data_services.utils.message_utils import message_utils
from recon_lw import recon_lw

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
            if "gap" in sequence[seq_num-first_elem][1]:
                sequence[seq_num - first_elem] = seq_element
                times.add((ts, seq_num))
            else:
                duplicates.add((seq_num,m["messageId"], sequence[seq_num-first_elem][1]["messageId"]))


def flush_sequence_get_collection(current_ts, horizon_delay, sequence_cache):
    times = sequence_cache["times"]
    sequence = sequence_cache["sequence"]
    if current_ts is not None:
        edge_timestamp = {"epochSecond": current_ts["epochSecond"] - horizon_delay,
                          "nano": 0}
        horizon_edge = times.bisect_key_left(recon_lw.time_stamp_key(edge_timestamp))
        seq_index = times[horizon_edge][1]
        for i in range(0,horizon_edge):
            times.pop(0)
        return sequence.irange(None, seq_index)
    else:
        times.clear()
        return sequence


def flush_sequence_clear_cache(processed_len, sequence_cache):
    sequence = sequence_cache["sequence"]
    for i in range(0, processed_len):
        sequence.pop(0)


def process_ob_rules(sequenced_batch, books_cache, get_book_id_func ,update_book_rule,
                     check_book_rule, event_sequence, send_events_func, parent_event):
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

        book_id = get_book_id_func(mess)
        if book_id is not None:
            if book_id not in books_cache:
                books_cache[book_id] = {"ask": {}, "bid": {}}
            book = books_cache[book_id]
            results = update_book_rule(book, mess, event_sequence)
            if results is not None:
                for r in results:
                    r["parentEventId"] = parent_event["eventId"]
                    events.append(r)
            results = check_book_rule(book, event_sequence)
            if results is not None:
                for r in results:
                    r["parentEventId"] = parent_event["eventId"]
                    events.append(r)
        n_processed += 1

    send_events_func(events)
    return n_processed


def init_ob_stream(rule_settings):
    rule_settings["sequence_cache"] = {"sequence": SortedKeyList(key=lambda item: item[0]),
                                       "times": SortedKeyList(key=lambda item: item[0]),
                                       "duplicates": SortedKeyList(key=lambda item: item[0])}
    rule_settings["books_cache"] = {}


def collect_ob_stream(next_batch, rule_dict):
    sequence_cache = rule_dict["sequence_cache"]
    sequence_timestamp_extract = rule_dict["sequence_timestamp_extract"]
    for m in next_batch:
        seq, ts = sequence_timestamp_extract(m)
        sequence_cache_add(seq, ts, m, sequence_cache)


def flush_ob_stream(ts,rule_settings,event_sequence, save_events_func):
    seq_batch = flush_sequence_get_collection(ts, rule_settings["horizon_delay"], rule_settings["sequence_cache"])
    n_processed = process_ob_rules(seq_batch,
                                   rule_settings["books_cache"],
                                   rule_settings["get_book_id"],
                                   rule_settings["update_book_rule"],
                                   rule_settings["check_book_rule"],
                                   event_sequence,
                                   save_events_func,
                                   rule_settings["rule_root_event"])
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
