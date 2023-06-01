from sortedcontainers import SortedKeyList
from recon_lw import recon_lw


class LastStateMatcher:
    def __init__(self, horizon_delay_seconds, get_search_ts_key,
                 get_state_ts_key_order, interpret_func, custom_settings, create_event,
                 send_events):
        self._search_index = {}
        self._state_cache = {}
        self._search_time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))
        self._state_sequence = {}
        self._state_time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))
        self._state_cache = {}
        self._get_search_ts_key = get_search_ts_key
        self.get_state_ts_key_order = get_state_ts_key_order
        self._interpret_func = interpret_func
        self._create_event = create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = False

    def process_objects_batch(self, batch: list) -> None:
        return

    def flush_all(self) -> None:
        return
