from sortedcontainers import SortedKeyList
from recon_lw import recon_lw


class TimeCacheMatcher:
    def __init__(self, horizon_delay_seconds, get_timestamp_key1_key2, interpret_func, custom_settings, create_event,
                 send_events):
        self._match_index = {}
        self._time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))
        self._get_timestamp_key1_key2 = get_timestamp_key1_key2
        self._interpret_func = interpret_func
        self._create_event = create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = True

    def debug(self, body):
        ev = self._create_event("TCMDebug", "TCMDebug", True, message_dict)
        self._send_events([ev])

    def process_objects_batch(self, batch: list) -> None:
        stream_time = None
        for o in batch:
            ts, key1, key2 = self._get_timestamp_key1_key2(o, self._custom_settings)
            if ts is None:
                continue
            stream_time = ts
            if key1 is not None:
                if key1 not in self._match_index:
                    self._match_index[key1] = [o, None]
                    self._time_index.add([ts, key1])
                    if self._debug:
                        self.debug({"event": "key1 added", "key": key1, "ts": ts})
                else:
                    self._match_index[key1][0] = o
                    if self._debug:
                        self.debug({"event": "key1 updated", "key": key1, "ts": ts})
            elif key2 is not None:
                if key2 not in self._match_index:
                    self._match_index[key2] = [None, o]
                    self._time_index.add([ts, key2])
                    if self._debug:
                        self.debug({"event": "key2 added", "key": key2, "ts": ts})
                else:
                    self._match_index[key2][1] = o
                    if self._debug:
                        self.debug({"event": "key2 updated", "key": key2, "ts": ts})

        if stream_time is not None:
            edge_timestamp = {"epochSecond": stream_time["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            horizon_edge = self._time_index.bisect_key_left(recon_lw.time_stamp_key(edge_timestamp))
            if horizon_edge > 0:
                for n in range(horizon_edge):
                    nxt = self._time_index.pop(0)
                    match = self._match_index.pop(nxt[1])
                    self._interpret_func(match, self._custom_settings, self._create_event, self._send_events)
                    if self._debug:
                        self.debug({"event": "key processed", "key": nxt, "ts": stream_time})

    def flush_all(self) -> None:
        self._time_index.clear()
        for key, match in self._match_index.items():
            self._interpret_func(match, self._custom_settings, self._create_event, self._send_events)
            if self._debug:
                self.debug({"event": "key processed at the end", "key": key, "ts": None})
        self._match_index.clear()
