from th2_data_services.config import options
from datetime import datetime
from recon_lw.core.message_utils import message_to_dict
from recon_lw.core.stream import Streams
from typing import Iterable, List, Optional, Dict
from recon_lw.core.ts_converters import time_stamp_key
from th2_data_services.data import Data
from os import listdir
from os import path

def time_index_add(key, m, time_index):
    time_index.add((options.mfr.get_timestamp(m), key))

def message_cache_add(m, message_cache):
    message_cache[options.mfr.get_id(m)] = m

def message_cache_pop(m_id, message_cache):
    if m_id is None:
        return None
    return message_cache.pop(m_id)

def create_event_id(event_sequence: dict):
    event_sequence["n"] += 1
    return event_sequence["name"] + "_" + event_sequence["stamp"] + "-" + str(event_sequence["n"])


def create_event(
        name,
        type,
        event_sequence: dict,
        ok=True,
        body=None,
        parentId=None,
        recon_name='',
):
    # TODO - description is required.
    ts = datetime.now()
    e = {"eventId": create_event_id(event_sequence),
         "successful": ok,
         "eventName": name,
         "eventType": type,
         "recon_name": recon_name,
         "body": body,
         "parentEventId": parentId,
         "startTimestamp": {"epochSecond": int(ts.timestamp()), "nano": ts.microsecond * 1000},
         "attachedMessageIds": []}
    return e

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
) -> Streams:
    """
    Get Streams object for Th2 events.

    Args:
        streams_path:
        name_filter:
        data_filter:

    Returns:
        Streams: [(Th2ProtobufTimestamp,
                  iterator for Data object,
                  First object from Data object or None), ...]
    """
    streams = Streams()
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
        streams.add_stream(strm)
    return streams


def open_streams(
        streams_path: Optional[str],
        name_filter=None,
        expanded_messages: bool = False,
        data_objects: List[Data] = None
) -> Streams:
    """
    Get Streams object for Th2 messages.

    Args:
        streams_path:
        name_filter:
        expanded_messages:
        data_objects:

    Returns:
        Streams: [(Th2ProtobufTimestamp,
                  iterator for Data object,
                  First object from Data object or None), ...]
    """
    streams = Streams()

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
            streams.add_stream(stream)

    return streams


def get_next_batch(streams: Streams,
                   batch: List[Optional[dict]],
                   batch_len,
                   get_timestamp_func) -> int:
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
    # DEPRECATED.
    return streams.get_next_batch(
        batch=batch,
        batch_len=batch_len,
        get_timestamp_func=get_timestamp_func
    )


def sync_stream(streams: Streams,
                get_timestamp_func):
    # DEPRECATED.
    #   Use streams.sync_streams instead.
    yield from streams.sync_streams(get_timestamp_func)

