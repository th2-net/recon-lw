from sortedcontainers import SortedKeyList
from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils


class StateSequenceGenerator:
    def __init__(self, horizon_delay_seconds,sequence_timestamp_extract,custom_settings,create_event,
                 send_events):
        self._sequence_timestamp_extract = sequence_timestamp_extract
        self._search_time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))

        # {key2 : { "prior_ts" : ts, "prior_obj" ; obj, SortedKeyList(timestamps)}  }
        self._state_cache = {}

        # sorted list ts, key2, order, object
        self._state_time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]) + "_" + t[1])

        #self._get_search_ts_key = get_search_ts_key
        #self._get_state_ts_key_order = get_state_ts_key_order
        #self._interpret_func = interpret_func
        self._create_event = create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = False
