from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils
from recon_lw.SequenceCache import SequenceCache
from recon_lw.EventsSaver import EventsSaver
from datetime import datetime
from recon_lw.LastStateMatcher import LastStateMatcher


class StateStream:
    def __init__(self, get_next_update_func, get_snapshot_id_func, state_transition_func,
                 events_saver, combine_instantenious_snapshots=True) -> None:
        self._get_next_update_func = get_next_update_func
        self._get_snapshot_id_func = get_snapshot_id_func
        self._state_transition_func = state_transition_func
        self._combine_instantenious_snapshots = combine_instantenious_snapshots
        self._events_saver: EventsSaver = events_saver

    def state_updates(self, stream):
        for o in stream:
            key, ts, action, state = self._get_next_update_func(o)
            if key is not None:
                yield (key, ts, action, state)

    def snapshots(self, stream):
        snapshots_collection = {}
        updated_snapshots = set()
        last_ts = None
        for tpl in self.state_updates(stream):
            key = tpl[0]
            ts = tpl[1]
            action = tpl[2]
            state = tpl[3]
            snap_id = self._get_snapshot_id_func(state)
            if last_ts is not None:
                if not self._combine_instantenious_snapshots:
                    for k in updated_snapshots:
                        yield self.create_snapshot_event(last_ts, snapshots_collection[k],
                                                         updated_snapshots)  # (last_ts, snapshots_collection[k])
                    updated_snapshots.clear()
                elif recon_lw.time_stamp_key(ts) != recon_lw.time_stamp_key(last_ts):
                    for k in updated_snapshots:
                        yield self.create_snapshot_event(last_ts, snapshots_collection[k],
                                                         updated_snapshots)  # (last_ts, snapshots_collection[k])
                    updated_snapshots.clear()

            if snap_id is None:
                continue
            if snap_id not in snapshots_collection:
                snapshots_collection[snap_id] = {}
            snapshot = snapshots_collection[snap_id]
            if action == 'c':
                if key in snapshot:  # TODO - perhaps there is should be key NOT IN snapshot ?
                    # Consistency error
                    err = self._events_saver.create_event("ObjectAlreadyPresent",
                                                          "StateStreamError", False,
                                                          {'update': tpl})
                    self._events_saver.save_events([err])
                else:
                    snapshot[key] = state
                    updated_snapshots.add(snap_id)
                    last_ts = ts
            elif action == 'u':
                if key not in snapshot:
                    # Consistency error
                    err = self._events_saver.create_event("ObjectDontExist",
                                                          "StateStreamError", False,
                                                          {'update': tpl})
                    self._events_saver.save_events([err])
                else:
                    state = self._state_transition_func(state, snapshot[key])
                    snapshot[key] = state
                    updated_snapshots.add(snap_id)
                    last_ts = ts
            elif action == 'd':
                if key not in snapshot:
                    # Consistency error
                    err = self._events_saver.create_event("ObjectDontExist",
                                                          "StateStreamError", False,
                                                          {'update': tpl})
                    self._events_saver.save_events([err])
                else:
                    snapshot.pop(key)
                    updated_snapshots.add(snap_id)
                    last_ts = ts
        for k in updated_snapshots:
            yield self.create_snapshot_event(last_ts, snapshots_collection[k],
                                             updated_snapshots)  # (last_ts, snapshots_collection[k])
        updated_snapshots.clear()

    def create_snapshot_event(self, ts, snap_id, snapshot_source):
        return self._events_saver.create_event("Snapshot",
                                               "Snapshot", True,
                                               {'snap_id': snap_id,
                                                'ts': ts,
                                                'sn': snapshot_source.copy()})


#### example
## Filter for orders state updates

def ts_from_fix_transacttime(tag_val):
    sec_part_str = tag_val[:tag_val.index(".")]
    # "20230519-13:04:27"
    dt = datetime.strptime(tag_val, '%Y%m%d-%H:%M:%S')
    idt = int(datetime.timestamp(dt))
    nanosec_part_str = tag_val[tag_val.index(".") + 1:]
    return {"epochSecond": idt, "nano": int(nanosec_part_str)}


def is_it_fix(m):
    return 'OrdStatus' in m['body']['fields']


def order_updates_filter(m):
    return m['messageType'] == 'ExecutionReport' and 'TransactTime' in m['body']['fields']


def order_updates_ts(m):
    if is_it_fix(m):
        return ts_from_fix_transacttime(m['body']['fields']['TransactTime'])
    else:
        return recon_lw.epoch_nano_str_to_ts(m['body']['fields']['TransactTime'])


def state_transition_oe(new_s, old_s):
    if old_s['u_s'] and new_s['u_t'] == 'L':
        new_s['u_t'] = False
    else:
        new_s['u_t'] = old_s['u_t']
    return new_s


def get_snapshot_id_oe(state):
    return state['sec']


def get_next_update_oe(m):
    # key, ts, action, state
    is_fix = is_it_fix(m)
    key = str(m['body']['fields']["OrderID"])
    if is_fix:
        ts = ts_from_fix_transacttime(m['body']['fields']['TransactTime'])
    else:
        ts = recon_lw.epoch_nano_str_to_ts(m['body']['fields']['TransactTime'])
    exec_type = str(m['body']['fields']["ExecType"])
    ord_status = m['body']['fields']["OrdStatus"] if is_fix else str(
        m['body']['fields']["OrderStatus"])
    ord_type = m['body']['fields']["OrdType"] if is_fix else str(m['body']['fields']["OrderType"])
    target_comp_id = m['body']['fields']["header"]["TargetCompID"] if is_fix else str(
        m['body']['fields']["CompID"])
    sec_id = int(m['body']['fields']["SecurityID"])
    unelected_stop = False
    if exec_type == '0':
        op = 'c'  # TODO - why it was changed from 'a' ?
        if ord_type == '4':
            unelected_stop = True
        # if m['body']['fields']["OrdStatus"]
    elif ord_status in ['2', '3', '4', 'C']:
        op = 'd'
    else:
        op = 'u'
    status = {'s': str(m['body']['fields']["Side"]),
              'p': float(m['body']['fields']["Price"]),
              'q': int(m['body']['fields']["LastQty"]) if is_fix else m['body']['fields'][
                  "LastQuantity"],
              'u_s': unelected_stop,
              'tr': target_comp_id,
              'sec': sec_id,
              'u_t': exec_type,
              'm': m}

    return key, ts, op, status


def get_ts_security_mnc_stream(o):
    ts = None
    seq_id = None
    # extract timestamp (ds format) from mnc event
    # extract seq_id from mnc event
    return ts, seq_id


def get_ts_security_oe_state_stream(o):
    ts = o["body"]["ts"]
    seq_id = o["body"]["snap_id"]
    # extract timestamp (ds format) from mnc event
    # extract seq_id from mnc event
    return ts, seq_id, 0


def mnc_oe_state_stream_compare(match, custom_settings, create_event, save_events):
    if match[1] is None:
        error_event = create_event("OEStateNotFound", "OEStateNotFound", False,
                                   {"mnc_event": match[0]})
        save_events([error_event])
        return
    # 
    # perform comparison
    #
    #

    result_event = create_event("MNC_L3_vs_OE_Check",
                                "MNC_L3_vs_OE_Check",
                                False,  # result of comparison
                                {"mnc_event": match[0],
                                 "diff": {}})  # comparison result
    # result_event["attachedMessageIds"] = [match[0]["messageId"]]
    save_events([result_event])


def get_mnc_oe_state_ts(o):
    if o["eventType"] == "Snapshot":
        return o["body"]["ts"]

    # return mnc object ts
    return None


def create_oe_snapshots_streams(oe_streams, result_events_path):
    events_saver = EventsSaver(result_events_path)
    filtered_streams = [stream.filter(order_updates_filter) for stream in oe_streams]
    strm_list = recon_lw.open_streams(None, None, False, filtered_streams)
    m_stream = recon_lw.sync_stream(strm_list, order_updates_ts)
    state_stream = StateStream(get_next_update_oe,
                               get_snapshot_id_oe,
                               state_transition_oe,
                               events_saver)

    processor = LastStateMatcher(
        180,
        get_ts_security_mnc_stream,  # search_ts_key
        get_ts_security_oe_state_stream,  # state_ts_key_order
        mnc_oe_state_stream_compare,  # interpret
        {},
        lambda name, ev_type, ok, body: events_saver.create_event(name, ev_type, ok, body),
        lambda ev_batch: events_saver.save_events(ev_batch))

    stream1 = None  # Please create your MNC stream
    stream2 = state_stream.snapshots(m_stream)
    streams = recon_lw.open_streams(None, data_objects=[stream1, stream2])

    message_buffer = [None] * 100
    buffer_len = 100
    while len(streams) > 0:
        next_batch_len = recon_lw.get_next_batch(streams, message_buffer, buffer_len,
                                                 get_mnc_oe_state_ts)
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]
        processor.process_objects_batch(buffer_to_process)

    processor.flush_all()
    events_saver.flush()
# .....
