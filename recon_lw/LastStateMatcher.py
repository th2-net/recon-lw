from sortedcontainers import SortedKeyList
from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils


class LastStateMatcher:
    def __init__(self, horizon_delay_seconds, get_search_ts_key,
                 get_state_ts_key_order, interpret_func, custom_settings, create_event,
                 send_events):
        self._search_time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))

        # {key2 : { "prior_ts" : ts, "prior_obj" ; obj, SortedKeyList(timestamps)}  }
        self._state_cache = {}

        # sorted list ts, key2, order, object
        self._state_time_index = SortedKeyList(key=lambda t: f"{recon_lw.time_stamp_key(t[0])}_{t[1]}")

        self._get_search_ts_key = get_search_ts_key
        self._get_state_ts_key_order = get_state_ts_key_order
        self._interpret_func = interpret_func
        self._create_event = create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = False

    def _state_cache_add_item(self, ts, key2):
        if key2 not in self._state_cache:
            self._state_cache[key2] = {"prior_ts": None, "prior_o": None,
                                       "records_times": SortedKeyList(key=recon_lw.time_stamp_key)}
        self._state_cache[key2]["records_times"].add(ts)

    def _state_cache_delete_item(self, ts, key2, o):
        p_ts = self._state_cache[key2]["prior_ts"]
        if p_ts is None or time_utils.timestamp_delta_us(p_ts, ts) > 0:
            self._state_cache[key2]["prior_ts"] = ts
            self._state_cache[key2]["prior_o"] = o

        self._state_cache[key2]["records_times"].remove(ts)

    def process_objects_batch(self, batch: list) -> None:
        stream_time = None
        for o in batch:
            ts1, key1 = self._get_search_ts_key(o, self._custom_settings)
            if key1 is not None:
                stream_time = ts1
                self._search_time_index.add((ts1, key1, o))
                continue
            ts2, key2, order = self._get_state_ts_key_order(o, self._custom_settings)
            if key2 is not None:
                stream_time = ts2
                index_key = f"{recon_lw.time_stamp_key(ts2)}_{key2}"
                i = self._state_time_index.bisect_key_left(index_key)
                current_len = len(self._state_time_index)
                next_key = None
                if i < current_len:
                    next_key = f"{recon_lw.time_stamp_key(self._state_time_index[i][0])}_{self._state_time_index[i][1]}"
                if i == current_len or index_key != next_key:
                    self._state_time_index.add([ts2, key2, order, o])
                    self._state_cache_add_item(ts2, key2)
                else:
                    rec = self._state_time_index[i]
                    if order >= rec[2]:
                        rec[2] = order
                        rec[3] = o

        #flush
        if stream_time is not None:
            self._flush(stream_time)

    def _flush(self, horizon_time):
        if horizon_time is not None:
            edge_timestamp = {"epochSecond": horizon_time["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            search_edge = self._search_time_index.bisect_key_left(recon_lw.time_stamp_key(edge_timestamp))
        else:
            search_edge = len(self._search_time_index)

        for n in range(search_edge):
            nxt = self._search_time_index.pop(0)
            ts1 = nxt[0]
            key1 = nxt[1]
            o1 = nxt[2]

            if key1 not in self._state_cache:
                o2 = None
            else:
                records_times = self._state_cache[key1]["records_times"]
                i = records_times.bisect_key_right(recon_lw.time_stamp_key(ts1))
                if i == 0:
                    o2 = self._state_cache[key1]["prior_o"]
                else:
                    ts2 = records_times[i-1]
                    sti = self._state_time_index.bisect_key_left(f"{recon_lw.time_stamp_key(ts2)}_{key1}")
                    o2 = self._state_time_index[sti][3]
            tech = {"key1": key1, "ts1": ts1, "state_cache": self._state_cache.get(key1)}
            self._interpret_func([o1, o2, tech], self._custom_settings, self._create_event, self._send_events)

        if horizon_time is not None:
            edge_timestamp = {"epochSecond": horizon_time["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            state_edge = self._state_time_index.bisect_key_left(f"{recon_lw.time_stamp_key(edge_timestamp)}_")
        else:
            state_edge = len(self._state_time_index)

        for n in range(state_edge):
            nxt_state = self._state_time_index.pop(0)
            self._state_cache_delete_item(nxt_state[0], nxt_state[1], nxt_state[3])

    def flush_all(self) -> None:
        self._flush(None)
