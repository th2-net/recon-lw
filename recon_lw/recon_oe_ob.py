from datetime import datetime
from recon_lw import recon_lw
from recon_lw.EventsSaver import EventsSaver
from recon_lw import recon_ob_cross_stream
from recon_lw.TimeCacheMatcher import TimeCacheMatcher
from recon_lw.message_utils import message_to_dict
from recon_lw.StateSequenceGenerator import StateSequenceGenerator


def process_order_states(message_pickle_path, sessions_list, result_events_path, settings,
                         data_objects=None):
    events_saver = EventsSaver(result_events_path)
    root_event = events_saver.create_event(
        "recon_lw_oe_ob_order_states_images " + datetime.now().isoformat(), "Microservice")
    events_saver.save_events([root_event])

    if data_objects:
        streams = recon_lw.open_streams(None, data_objects=data_objects)
    else:
        if sessions_list is not None and len(sessions_list):
            sessions_set = set(sessions_list)
            streams = recon_lw.open_streams(message_pickle_path,
                                            lambda n: n[:n.rfind('_')] in sessions_set)
        else:
            streams = recon_lw.open_streams(message_pickle_path)

    create_event = lambda n, t, ok, b: events_saver.create_event(n, t, ok, b,
                                                                 parentId=root_event["eventId"])
    save_events = lambda ev_batch: events_saver.save_events(ev_batch)
    seq_gen = StateSequenceGenerator(settings["horizon_delay_seconds"],
                                     settings["stream_sequence_timestamp_extract"],
                                     settings["key_ts_new_key_extract"],
                                     oe_er_state_update,
                                     settings["report_images"],
                                     {},
                                     create_event,
                                     save_events)
    message_buffer = [None] * 100
    buffer_len = 100
    while len(streams) > 0:
        next_batch_len = recon_lw.get_next_batch(streams, message_buffer, buffer_len,
                                                 lambda m: m["timestamp"])
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]
        seq_gen.process_objects_batch(buffer_to_process)

    # final flush
    seq_gen.flush_all()
    events_saver.flush()


def oe_er_state_update(er, current_state, create_event, send_events):
    current_state["no_state"] = False
    current_state["last_er"] = er


def process_oe_md_comparison(ob_events_path, oe_images_events_path, md_sessions_list,
                             result_events_path,
                             horizon_delay_seconds, is_book_open, keeper):
    events_saver = EventsSaver(result_events_path)
    root_event = events_saver.create_event("recon_lw_oe_ob_compare" + datetime.now().isoformat(),
                                           "Microservice")
    events_saver.save_events([root_event])

    def create_event(n, t, ok, b, am=None):
        return events_saver.create_event(n, t, ok, b, parentId=root_event["eventId"],
                                         attached_messages=am)

    # create_event = lambda n, t, ok, b: events_saver.create_event(n, t, ok, b, parentId=root_event["eventId"])
    save_events = lambda ev_batch: events_saver.save_events(ev_batch)
    processor = TimeCacheMatcher(horizon_delay_seconds,
                                 oe_ob_get_timestamp_key1_key2,
                                 oe_ob_interpret_func,
                                 {"is_book_open": is_book_open, "orders_keeper": keeper},
                                 create_event,
                                 save_events)

    data_filter = lambda e: e['body'] and e['body'].get("timestamp", -1) != -1
    streams = recon_lw.open_scoped_events_streams(ob_events_path,
                                                  lambda n: any(s in n for s in md_sessions_list))
    streams2 = recon_lw.open_scoped_events_streams(oe_images_events_path, data_filter=data_filter)
    for elem in streams2:
        streams.add(elem)

    message_buffer = [None] * 100
    buffer_len = 100
    while len(streams) > 0:
        next_batch_len = recon_lw.get_next_batch(streams, message_buffer, buffer_len,
                                                 oe_ob_get_timestamp)
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]
        processor.process_objects_batch(buffer_to_process)

    processor.flush_all()
    events_saver.flush()


def oe_ob_get_timestamp_key1_key2(o, custom_settings):
    keeper = custom_settings['orders_keeper']
    if o["eventType"] == "OrderBook":
        if "order_id" in o["body"]["operation_params"]:
            order_id = o["body"]["operation_params"]["order_id"]
            if order_id and int(order_id) not in keeper:
                # Exclude orders from unknown users
                return None, None, None
            str_toe = o["body"]["operation_params"]["str_time_of_event"]
            ts = recon_lw.epoch_nano_str_to_ts(str_toe)
            return ts, f'{str_toe}.{order_id}.{o["body"]["otv"]}', None
        else:
            return None, None, None
    elif o["eventType"] == "OEMDImage":
        is_book_open = custom_settings["is_book_open"]
        str_toe = o["body"]["operation_params"]["str_time_of_event"]
        ts = recon_lw.epoch_nano_str_to_ts(str_toe)
        if not is_book_open(o["body"]["operation_params"]["instr"], ts):
            return None, None, None
        return ts, None, f'{str_toe}.{o["body"]["operation_params"]["order_id"]}.{o["body"]["otv"]}'
    else:
        return None, None, None


def clear_bookid(book_id):
    book_id = book_id.split("(", maxsplit=1)[0]
    return book_id


def oe_ob_interpret_func(match, custom_settings, create_event, send_events):
    attached_messages = []
    if match[0] is not None and match[1] is not None:
        operation_problem = None
        book_id = clear_bookid(match[0]['body']['book_id'])
        match[0]['body']['book_id'] = book_id
        match[0]['body']['operation_params']['instr'] = book_id
        if match[0]["body"]["operation"] != match[1]["body"]["operation"]:
            operation_problem = f'{match[0]["body"]["operation"]} != {match[1]["body"]["operation"]}'
        comparison = recon_ob_cross_stream.compare_keys(match[1]["body"]["operation_params"].keys(),
                                                        match[0]["body"]["operation_params"],
                                                        match[1]["body"]["operation_params"])
        instr_problem = None
        if match[0]["body"]["book_id"] != match[1]["body"]["operation_params"]["instr"]:
            instr_problem = f'{match[0]["body"]["book_id"]} != {match[1]["body"]["operation_params"]["instr"]}'
        problems = {}
        if operation_problem is not None:
            problems["operation_problem"] = operation_problem
        if instr_problem is not None:
            problems["instr_problem"] = instr_problem
        if len(comparison) > 0:
            problems["comparison"] = comparison

        ok = len(problems) == 0
        body = {"md": match[0]["body"], "oe": match[1]["body"]}
        if not ok:
            body["problems"] = problems
        attached_messages.extend(match[0]['attachedMessageIds'])
        attached_messages.extend(match[1]['attachedMessageIds'])
        ev = create_event("OEMDMatch", "OEMDMatch", ok, body, attached_messages)
        send_events([ev])
    elif match[0] is None:
        body = {"oe": match[1]["body"]}
        attached_messages.extend(match[1]['attachedMessageIds'])
        ev = create_event("OEMDMissingMD", "OEMDMissingMD", False, body, attached_messages)
        send_events([ev])
    else:
        body = {"md": match[0]["body"]}
        attached_messages.extend(match[0]['attachedMessageIds'])
        ev = create_event("OEMDMissingOE", "OEMDMissingOE", False, body, attached_messages)
        send_events([ev])


def oe_ob_get_timestamp(o):
    return o["body"]["timestamp"]


############################# example functions - must be changed according business rules
def oe_m_stream_sequence_timestamp(m):
    if m["direction"] != "IN":
        return None, None, None
    stream = m["sessionId"]
    mm = message_to_dict(m)
    if "header.MsgSeqNum" in mm:
        return stream, mm["header.MsgSeqNum"], m["timestamp"]
    else:
        return stream, mm["header.SeqNum"], m["timestamp"]


def ts_from_tag_val(tag_val):
    sec_part_str = tag_val[:tag_val.index(".")]
    # "20230519-13:04:27"
    dt = datetime.strptime(tag_val, '%Y%m%d-%H:%M:%S')
    idt = int(datetime.timestamp(dt))
    nanosec_part_str = tag_val[tag_val.index(".") + 1:]
    return {"epochSecond": idt, "nano": int(nanosec_part_str)}


def oe_er_key_ts_new_key_extract(er):
    if er["messageType"] != "ExecutionReport":
        return None, None, None
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        if mm["ExecType"] in ["6", "8", "E", "H"]:
            return None, None, None
        ts = ts_from_tag_val(mm["TransactTime"])
        if mm["ExecType"] in ["4", "5"]:
            key = mm["OrigClOrdID"]
            new_key = mm["ClOrdID"]
        else:
            key = mm["ClOrdID"]
            new_key = mm["ClOrdID"]
        return key, ts, new_key
    else:  # need to update tags
        if mm["ExecType"] in ["8"]:
            return None, None, None
        ts = recon_lw.epoch_nano_str_to_ts(mm["TransactTime"])
        if mm["ExecType"] in ["4", "5"]:
            key = mm["OriginalClientOrderID"]
            new_key = mm["ClientOrderID"]
        else:
            key = mm["ClientOrderID"]
            new_key = mm["ClientOrderID"]
        return key, ts, new_key


def get_resting_price(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return float(mm["Price"])
    else:
        return float(mm["OrderPrice"])


def get_resting_qty(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return int(mm["LeavesQty"])
    else:
        return int(mm["LeavesQuantity"])


def get_trade_price(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return float(mm["LastPx"])
    else:
        return float(mm["LastPrice"])


def get_trade_qty(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return int(mm["LastQty"])
    else:
        return int(mm["LastQuantity"])


def get_transact_time(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return ts_from_tag_val(mm["TransactTime"])
    else:
        return recon_lw.epoch_nano_str_to_ts(mm["TransactTime"])


def get_order_type(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return int(mm["OrdType"])
    else:
        return int(mm["OrderType"])


def get_order_status(er):
    mm = message_to_dict(er)
    if recon_lw.protocol(er) == "FIX":
        return int(mm["OrdStatus"])
    else:
        return int(mm["OrderStatus"])


def get_instrument_id(er):
    mm = message_to_dict(er)
    return mm["SecurityID"]


def get_order_id(er):
    mm = message_to_dict(er)
    return mm["OrderID"]


def get_side(er):
    mm = message_to_dict(er)
    return "bid" if mm["Side"] == 1 else "ask"


def report_add_order(er, v: int, create_event):
    body = {"operation": "ob_add_order",
            "operation_params": {"instr": get_instrument_id(er),
                                 "order_id": get_order_id(er),
                                 "price": get_resting_price(er),
                                 "size": get_resting_qty(er),
                                 "side": get_side(er),
                                 "str_time_of_event": recon_lw.ts_to_epoch_nano_str(
                                     get_transact_time(er))},
            "otv": v}
    return create_event("OEMDImage", "OEMDImage", True, body)


def report_delete_order(er, v: int, create_event, str_time_of_event=None):
    tov = recon_lw.ts_to_epoch_nano_str(
        get_transact_time(er)) if str_time_of_event is None else str_time_of_event
    body = {"operation": "ob_delete_order",
            "operation_params": {"instr": get_instrument_id(er),
                                 "order_id": get_order_id(er),
                                 "str_time_of_event": tov},
            "otv": v}
    return create_event("OEMDImage", "OEMDImage", True, body)


def report_update_order(er, v: int, create_event):
    body = {"operation": "ob_update_order",
            "operation_params": {"instr": get_instrument_id(er),
                                 "order_id": get_order_id(er),
                                 "price": get_resting_price(er),
                                 "size": get_resting_qty(er),
                                 "str_time_of_event": recon_lw.ts_to_epoch_nano_str(
                                     get_transact_time(er))},
            "otv": v}
    return create_event("OEMDImage", "OEMDImage", True, body)


def report_trade_order(er, v: int, create_event):
    body = {"operation": "ob_trade_order",
            "operation_params": {"instr": get_instrument_id(er),
                                 "order_id": get_order_id(er),
                                 "traded_price": get_trade_price(er),
                                 "traded_size": get_trade_qty(er),
                                 "str_time_of_event": recon_lw.ts_to_epoch_nano_str(
                                     get_transact_time(er))},
            "otv": v}
    return create_event("OEMDImage", "OEMDImage", True, body)


def oe_report_md_images(update_states, create_event, send_events):
    # simplest events
    if len(update_states) == 1:
        er = update_states[0][0]
        prev_state = update_states[0][1]
        mm_er = message_to_dict(er)
        if mm_er["ExecType"] == "0" and get_order_type(er) not in ["3", "4"]:
            # new not stop
            ev = report_add_order(er, 0, create_event)
            ev["attachedMessageIds"] = [er["messageId"]]
            ev["scope"] = er["sessionId"]
            ev["body"]["timestamp"] = er["timestamp"]
            send_events([ev])
        elif mm_er["ExecType"] in ["4", "C"] and get_order_type(er) not in ["3", "4"]:
            # cancel, expired not stop
            ev = report_delete_order(er, 0, create_event)
            ev["attachedMessageIds"] = [er["messageId"]]
            ev["scope"] = er["sessionId"]
            ev["body"]["timestamp"] = er["timestamp"]
            send_events([ev])
        elif mm_er["ExecType"] == "5" and get_order_type(er) not in ["3", "4"]:
            # amend not stop
            prev_er = prev_state["last_er"]
            if get_resting_price(prev_er) == get_resting_price(er) and get_resting_qty(
                    prev_er) > get_resting_qty(er):
                ev = report_update_order(er, 0, create_event)
                ev["attachedMessageIds"] = [er["messageId"]]
                ev["scope"] = er["sessionId"]
                ev["body"]["timestamp"] = er["timestamp"]
                send_events([ev])
            elif get_resting_price(prev_er) != get_resting_price(er) or get_resting_qty(
                    prev_er) < get_resting_qty(er):
                ev1 = report_delete_order(prev_er, 0, create_event,
                                          str_time_of_event=recon_lw.ts_to_epoch_nano_str(
                                              get_transact_time(er)))
                ev1["attachedMessageIds"] = [er["messageId"]]
                ev1["scope"] = er["sessionId"]
                ev1["body"]["timestamp"] = er["timestamp"]
                ev2 = report_add_order(er, 1, create_event)
                ev2["attachedMessageIds"] = [er["messageId"]]
                ev2["scope"] = er["sessionId"]
                ev2["body"]["timestamp"] = er["timestamp"]
                send_events([ev1, ev2])
        elif mm_er["ExecType"] == "F":
            ev = report_trade_order(er, 0, create_event)
            ev["attachedMessageIds"] = [er["messageId"]]
            ev["scope"] = er["sessionId"]
            ev["body"]["timestamp"] = er["timestamp"]
            send_events([ev])
    else:
        first_er = update_states[0][0]
        prev_state = update_states[0][1]
        mm_er = message_to_dict(first_er)
        if mm_er["ExecType"] == "0":
            # aggressive new
            # skipping trades/ adding only if order is still alive
            last_er = update_states[-1][0]
            if get_order_status(last_er) in ["0", "1"]:
                ev = report_add_order(last_er, 0, create_event)
                ev["attachedMessageIds"] = [upd[0]["messageId"] for upd in update_states]
                ev["scope"] = last_er["sessionId"]
                ev["body"]["timestamp"] = last_er["timestamp"]
                send_events([ev])
        elif mm_er["ExecType"] == "5":
            # aggressive amend
            # skipping trades/ adding only if order is still alive
            prev_er = prev_state["last_er"]
            last_er = update_states[-1][0]
            ev1 = report_delete_order(prev_er, 0, create_event,
                                      str_time_of_event=recon_lw.ts_to_epoch_nano_str(
                                          get_transact_time(last_er)))
            ev1["attachedMessageIds"] = [upd[0]["messageId"] for upd in update_states]
            ev1["scope"] = prev_er["sessionId"]
            ev1["body"]["timestamp"] = prev_er["timestamp"]
            send_events([ev1])
            if get_order_status(last_er) in ["0", "1"]:
                ev = report_add_order(last_er, 1, create_event)
                ev["attachedMessageIds"] = [upd[0]["messageId"] for upd in update_states]
                ev["scope"] = last_er["sessionId"]
                ev1["body"]["timestamp"] = last_er["timestamp"]
                send_events([ev])
        else:
            # unexpected event
            ev = create_event("OEMD_Unexpected", "OEMD_Unexpected", True,
                              update_states_synopsis(update_states))
            ev["attachedMessageIds"] = [upd[0]["messageId"] for upd in update_states]
            send_events([ev])


def update_states_synopsis(update_states):
    prev_state = update_states[0][1]
    prev_state_syn = "no_state" if prev_state["no_state"] else get_er_synopsis(
        prev_state["last_er"])
    result = {"prev_state": prev_state_syn}
    result["updates"] = [get_er_synopsis(elem[0]) for elem in update_states]
    return result


def get_er_synopsis(er):
    mm_er = message_to_dict(er)
    return f'exectype={mm_er["ExecType"]}, orderstatus={get_order_status(er)}'
