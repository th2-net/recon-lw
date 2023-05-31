from sortedcontainers import SortedKeyList
from recon_lw import recon_lw


class LastStateMatcher:
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
