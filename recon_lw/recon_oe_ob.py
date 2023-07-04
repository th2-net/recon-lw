import pathlib
from datetime import datetime
from itertools import islice

from recon_lw import recon_lw
from recon_lw.EventsSaver import EventsSaver
from recon_lw.LastStateMatcher import LastStateMatcher
from th2_data_services.utils.message_utils import message_utils
from recon_lw.StateSequenceGenerator import StateSequenceGenerator


def process_order_states(message_pickle_path, sessions_list, result_events_path, settings):
    events_saver = EventsSaver(result_events_path)
    root_event = events_saver.create_event("recon_lw_oe_ob_order_states_images " + datetime.now().isoformat(), "Microservice")
    events_saver.save_events([root_event])

    if sessions_list is not None and len(sessions_list):
        sessions_set = set(sessions_list)
        streams = recon_lw.open_streams(message_pickle_path,
                               lambda n: n[:n.rfind('_')] in sessions_set)
    else:
        streams = recon_lw.open_streams(message_pickle_path)

    create_event = lambda n, t, ok, b: events_saver.create_event(n, t, ok, b, parentId=root_event["eventId"])
    save_events = lambda ev_batch: events_saver.save_events(ev_batch)
    seq_gen = StateSequenceGenerator(settings["horizon_delay_seconds"],
                                     settings["stream_sequence_timestamp_extract"],
                                     settings["key_ts_new_key_extract"],
                                     oe_er_state_update,
                                     settings["report_images"],
                                     {},
                                     create_event,
                                     save_events)
    message_buffer = [None]*100
    buffer_len = 100
    while len(streams)>0:
        next_batch_len = recon_lw.get_next_batch(streams, message_buffer, buffer_len, lambda m: m["timestamp"])
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]
        seq_gen.process_objects_batch(buffer_to_process)
    
    #final flush
    seq_gen.flush_all()
    events_saver.flush()


def oe_er_state_update(er, current_state, create_event, send_events):
    current_state["no_state"] = False
    current_state["last_er"] = er


############################# example functions - must be changed according business rules
def oe_m_stream_sequence_timestamp(m):
    if m["direction"] != "IN":
        return None, None, None
    stream = m["sessionId"]
    mm = message_utils.message_to_dict(m)
    if "header.MsgSeqNum" in mm:
        return stream, mm["header.MsgSeqNum"], m["timestamp"]
    else:
        return stream, mm["header.SeqNum"], m["timestamp"]

def ts_from_tag_val(tag_val):
    sec_part_str = tag_val[:tag_val.index(".")]
    #"20230519-13:04:27"
    dt = datetime.strptime(tag_val,'%Y%m%d-%H:%M:%S')
    idt = int(datetime.timestamp(dt))
    nanosec_part_str = tag_val[tag_val.index(".")+1:]
    return {"epochSecond": idt, "nano": int(nanosec_part_str)}


def oe_er_key_ts_new_key_extract(er):
    if er["messageType"] != "ExecutionReport":
        return None, None, None
    mm = message_utils.message_to_dict(m)
    if recon_lw.protocol(er) == "FIX":
        if mm["ExecType"] in ["8"]:
            return None, None, None
        ts = ts_from_tag_val(mm["TransactTime"])
        if mm["ExecType"] in ["4", "5"]:
            key = mm["OrigClOrdID"]
            new_key = mm["ClOrdID"]
        else:
            key = mm["ClOrdID"]
            new_key = mm["ClOrdID"]
        return key, ts, new_key
    else: #need to update tags
        if mm["ExecType"] in ["8"]:
            return None, None, None
        ts = ts_from_tag_val(mm["TransactTime"])
        if mm["ExecType"] in ["4", "5"]:
            key = mm["OrigClOrdID"]
            new_key = mm["ClOrdID"]
        else:
            key = mm["ClOrdID"]
            new_key = mm["ClOrdID"]
        return key, ts, new_key

    def oe_report_md_images(update_states, create_event, send_events):
        return

