import abc
from datetime import datetime, timedelta
from typing import Iterable, List, Tuple, Iterator, Optional, Dict, Any

from sortedcontainers import SortedKeyList
from th2_data_services.data import Data
from recon_lw.message_utils import message_to_dict
from os import listdir
from os import path
from recon_lw.EventsSaver import EventsSaver
from th2_data_services.config import options


def epoch_nano_str_to_ts(s_nanos: str) -> Dict[str, int]:
    nanos = int(s_nanos)
    return {"epochSecond": nanos // 1_000_000_000, "nano": nanos % 1_000_000_000}


def ts_to_epoch_nano_str(ts):
    return f'{ts["epochSecond"]}{str(ts["nano"]).zfill(9)}'


def time_stamp_key(ts):
    return 1_000_000_000 * ts["epochSecond"] + ts["nano"]
    # nanos_str = str(ts["nano"]).zfill(9)
    # return str(ts["epochSecond"]) + "." + nanos_str


def time_index_add(key, m, time_index):
    time_index.add((options.mfr.get_timestamp(m), key))


def message_cache_add(m, message_cache):
    message_cache[options.mfr.get_id(m)] = m


def message_cache_pop(m_id, message_cache):
    if m_id is None:
        return None
    return message_cache.pop(m_id)


def pair_one_match(next_batch, rule_dict):
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


# first_key_func takes m  returns string(key)
def one_many_match(next_batch, rule_dict):
    match_index = rule_dict["match_index"]
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
                if existing[1] is None:
                    existing[1] = message_id
                    # match_index[second_key] = [existing[0], message_id]
                    message_cache_add(m, message_cache)
                else:
                    existing.append(message_id)
                    message_cache_add(m, message_cache)

    if n_duplicates > 0:
        print(n_duplicates, " duplicates detected")


def flush_old(current_ts, horizon_delay, time_index):
    result = []
    horizon_edge = len(time_index)
    if current_ts is not None:
        edge_timestamp = {"epochSecond": current_ts["epochSecond"] - horizon_delay,
                          "nano": 0}
        horizon_edge = time_index.bisect_key_left(time_stamp_key(edge_timestamp))

    if horizon_edge > 0:
        n = 0
        while n < horizon_edge:
            nxt = time_index.pop(0)
            result.append(nxt[1])
            n += 1
    return result


# match_compare_func takes m1, m2  returns e
# end_events_func tekes iterable of events
def rule_flush(current_ts, horizon_delay, match_index: dict, time_index, message_cache,
               interpret_func, event_sequence, send_events_func, parent_event, live_orders_cache):
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


def create_event_id(event_sequence):
    event_sequence["n"] += 1
    return event_sequence["name"] + "_" + event_sequence["stamp"] + "-" + str(event_sequence["n"])


def create_event(name, type, event_sequence, ok=True, body=None, parentId=None):
    # TODO - description is required.
    ts = datetime.now()
    e = {"eventId": create_event_id(event_sequence),
         "successful": ok,
         "eventName": name,
         "eventType": type,
         "body": body,
         "parentEventId": parentId,
         "startTimestamp": {"epochSecond": int(ts.timestamp()), "nano": ts.microsecond * 1000},
         "attachedMessageIds": []}
    return e


class RuleSettings:
    """Prototype of the RuleSettings class.

    This class will be used instead of current Dict settings config.
    This class should be backward-compatible with current Dict settings config solution.


    It describes
        - configuration function interfaces.
        - ...
    """

    def __init__(self):
        """
        TODO - every parameter should be described in detail.

        """
        self.horizon_delay = None
        self.match_index = None
        self.time_index = None
        self.message_cache = None
        self.rule_root_event = None
        self.live_orders_cache = None

    @abc.abstractmethod
    def interpret_func(self, match, _, event_sequence):
        pass

    @abc.abstractmethod
    def rule_match_func(self, next_batch, rule_dict):
        pass

    @abc.abstractmethod
    def first_key_func(self, message):
        pass

    @abc.abstractmethod
    def second_key_func(self, message):
        pass

    def flush_func(self, ts, event_sequence, save_events_func):
        """
        TODO - description is required.

        Args:
            ts:  ??
            event_sequence:  ??
            save_events_func: a function that will store result events to (file/DB/..).

        Returns:

        """
        rule_flush(current_ts=ts,
                   horizon_delay=self.horizon_delay,
                   match_index=self.match_index,
                   time_index=self.time_index,
                   message_cache=self.message_cache,
                   interpret_func=self.interpret_func,
                   event_sequence=event_sequence,
                   send_events_func=save_events_func,
                   parent_event=self.rule_root_event,
                   live_orders_cache=self.live_orders_cache)


# {"first_key_func":..., "second_key_func",... "interpret_func"}
def execute_standalone(message_pickle_path, sessions_list, result_events_path,
                       rules_settings_dict: Dict[str, Dict[str, Any]],
                       data_objects=None):
    """Entrypoint for recon-lw.

    If you provide data_objects, message_pickle_path -- will be ignored.


    Args:
        message_pickle_path:
        sessions_list:
        result_events_path:
        rules_settings_dict: { ReconRuleName: {}, ... }
        data_objects:

    Returns:

    """
    box_ts = datetime.now()
    events_saver = EventsSaver(result_events_path)
    event_sequence = {"name": "recon_lw", "stamp": str(box_ts.timestamp()), "n": 0}
    root_event = create_event("recon_lw " + box_ts.isoformat(), "Microservice", event_sequence)

    events_saver.save_events([root_event])
    # TODO -- rule_settings -- will be changed to Class.
    #   We should leave backward compatibility with current DICT solution.
    for rule_key, rule_settings in rules_settings_dict.items():
        rule_settings["rule_root_event"] = create_event(rule_key, "LwReconRule",
                                                        event_sequence,
                                                        parentId=root_event["eventId"])
        if "init_func" not in rule_settings:
            rule_settings["init_func"] = init_matcher
        if "collect_func" not in rule_settings:
            rule_settings["collect_func"] = collect_matcher
        if "flush_func" not in rule_settings:
            rule_settings["flush_func"] = flush_matcher
        rule_settings["init_func"](rule_settings)

    events_saver.save_events([r["rule_root_event"] for r in rules_settings_dict.values()])
    if data_objects:
        streams = open_streams(message_pickle_path, data_objects=data_objects)
    else:
        if sessions_list is not None and len(sessions_list):
            sessions_set = set(sessions_list)
            streams = open_streams(message_pickle_path,
                                   lambda n: n[:n.rfind('_')] in sessions_set)
        else:
            streams = open_streams(message_pickle_path)

    buffer_len = 100
    message_buffer = [None] * buffer_len

    while len(streams) > 0:
        next_batch_len = get_next_batch(streams, message_buffer, buffer_len,
                                        lambda m: m["timestamp"])
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]
        for rule_settings in rules_settings_dict.values():
            rule_settings["collect_func"](buffer_to_process, rule_settings)
            ts = buffer_to_process[len(buffer_to_process) - 1]["timestamp"]
            rule_settings["flush_func"](ts, rule_settings, event_sequence,
                                        lambda ev_batch: events_saver.save_events(ev_batch))
    # final flush
    for rule_settings in rules_settings_dict.values():
        rule_settings["flush_func"](None, rule_settings, event_sequence,
                                    lambda ev_batch: events_saver.save_events(ev_batch))
    # one final flush
    events_saver.flush()


def init_matcher(rule_settings):
    rule_settings["match_index"] = {}
    rule_settings["time_index"] = SortedKeyList(key=lambda t: time_stamp_key(t[0]))
    rule_settings["message_cache"] = {}


def collect_matcher(batch, rule_settings):
    rule_match_func = rule_settings["rule_match_func"]
    rule_match_func(batch, rule_settings)
    if "live_orders_cache" in rule_settings:
        rule_settings["live_orders_cache"].process_objects_batch(batch)


def flush_matcher(ts, rule_settings, event_sequence, save_events_func):
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


def simplify_message(m):
    """Returns a copy of m with changed fields:

    Added:
        - simpleBody
        - protocol

    Removed
        - body
        - bodyBase64

    :param m:
    :return:
    """
    mm = m.copy()
    if len(m["body"]) > 0:
        mm["simpleBody"] = message_to_dict(m)
        mm["protocol"] = protocol(m)
    else:
        mm["simpleBody"] = {}

    # TODO
    #  - it's better to get these names from DataSource message description.
    #  - it's possible that sometime the path of the body will be changed.
    mm.pop("body")
    mm.pop("bodyBase64")
    return mm


def load_to_list(messages: Iterable[dict], simplify: bool) -> List[dict]:
    if simplify:
        return list(map(simplify_message, messages))
    else:
        return list(messages)


def split_messages_pickle_for_recons(message_pickle_path, output_path, sessions_list,
                                     simplify=True):
    """DEPRECATED FUNCTIONS SINCE WE HAVE DownloadCommand in LwDP data source.

    :param message_pickle_path:
    :param output_path:
    :param sessions_list:
    :param simplify:
    :return:
    """
    messages = Data.from_cache_file(message_pickle_path)
    for s in sessions_list:
        messages_session_in = messages.filter(
            lambda m: options.mfr.get_session_id(m) == s and options.mfr.get_direction(m) == "IN")
        print("Sorting ", s, " IN ", datetime.now())
        arr = load_to_list(messages_session_in, simplify)
        arr.sort(key=lambda m: time_stamp_key(m["timestamp"]))
        messages_session_in_to_save = Data(arr)
        file_name = output_path + "/" + s + "_IN.pickle"
        print("Saving ", file_name, " ", datetime.now())
        messages_session_in_to_save.build_cache(file_name)

        messages_session_out = messages.filter(
            lambda m: options.mfr.get_session_id(m) == s and options.mfr.get_direction(m) == "OUT")
        print("Sorting ", s, " OUT ", datetime.now())
        arr = load_to_list(messages_session_out, simplify)
        arr.sort(key=lambda m: time_stamp_key(m["timestamp"]))
        messages_session_out_to_save = Data(arr)

        file_name = output_path + "/" + s + "_OUT.pickle"
        print("Saving ", file_name, " ", datetime.now())
        messages_session_out_to_save.build_cache(file_name)


def protocol(m):
    """

    Expects the message after expand_message function.

    :param m:
    :return:
    """
    # Simplified message
    if "body" not in m:
        return m["protocol"]

    if len(m["body"]) == 0:
        return "error"

    pr = options.smsr.get_protocol(options.mfr.get_body())
    return "not_defined" if pr is None else pr


def open_scoped_events_streams(
        streams_path,
        name_filter=None,
        data_filter=None
) -> SortedKeyList[Tuple[dict, Iterator, Optional[dict]]]:
    streams = SortedKeyList(key=lambda t: time_stamp_key(t[0]))
    files = listdir(streams_path)
    files.sort()
    # This part to replace Data+Data to Data([Data,Data])
    scopes_streams_temp: Dict[str, list] = {}
    for f in files:
        if ".pickle" not in f:
            continue
        if name_filter is not None and not name_filter(f):
            continue
        scope = f[:f.index("_scope_")]
        if scope not in scopes_streams_temp:
            scopes_streams_temp[scope] = [Data.from_cache_file(path.join(streams_path, f))]
        else:
            scopes_streams_temp[scope].append(Data.from_cache_file(path.join(streams_path, f)))

    scopes_streams: Dict[str, Data] = {scope: Data(scopes_streams_temp[scope])
                                       for scope in scopes_streams_temp}
    for strm in scopes_streams.values():
        if data_filter:
            strm = strm.filter(data_filter)
        ts0 = {"epochSecond": 0, "nano": 0}
        streams.add((ts0, iter(strm), None))
    return streams


def open_streams(
        streams_path: Optional[str],
        name_filter=None,
        expanded_messages: bool = False,
        data_objects: List[Data] = None
) -> SortedKeyList[Tuple[dict, Iterator, Optional[dict]]]:
    streams = SortedKeyList(key=lambda t: time_stamp_key(t[0]))

    if data_objects:
        for do in data_objects:
            ts0 = {"epochSecond": 0, "nano": 0}
            if expanded_messages:
                stream = (mm for m in do for mm in options.mfr.expand_message(m))
            else:
                stream = do
            streams.add((ts0, iter(stream), None))
    else:
        files = listdir(streams_path)
        for f in files:
            if ".pickle" not in f:
                continue
            if name_filter is not None and not name_filter(f):
                continue
            data_object = Data.from_cache_file(path.join(streams_path, f))
            if expanded_messages:
                stream = (mm for m in data_object for mm in
                          options.MESSAGE_FIELDS_RESOLVER.expand_message(m))
            else:
                stream = Data.from_cache_file(path.join(streams_path, f))
            ts0 = {"epochSecond": 0, "nano": 0}
            streams.add((ts0, iter(stream), None))

    return streams


def get_next_batch(streams: SortedKeyList[Tuple[dict, Iterator, Optional[dict]]],
                   batch: List[Optional[dict]], b_len, get_timestamp_func) -> int:
    """

    Args:
        streams: [(Th2ProtobufTimestamp,
                    iterator for Data object,
                    First object from Data object or None), ...]
        batch:
        b_len:
        get_timestamp_func:

    Returns:

    """
    batch_pos = 0
    batch_len = b_len  # len(batch)
    while batch_pos < batch_len and len(streams) > 0:
        next_stream = streams.pop(0)
        try:
            if next_stream[2] is not None:
                batch[batch_pos] = next_stream[2]
                batch_pos += 1
            o = next(next_stream[1])
            streams.add((get_timestamp_func(o), next_stream[1], o))
        except StopIteration as e:
            continue
    return batch_pos


def sync_stream(streams: SortedKeyList[Tuple[dict, Iterator, Optional[dict]]],
                get_timestamp_func):
    while len(streams) > 0:
        next_stream = streams.pop(0)
        try:
            if next_stream[2] is not None:
                yield next_stream[2]
            o = next(next_stream[1])
            streams.add((get_timestamp_func(o), next_stream[1], o))
        except StopIteration as e:
            continue
