from sortedcontainers import SortedKeyList
from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils


class StateSequenceGenerator:
    def __init__(self, horizon_delay_seconds,
                 stream_sequence_timestamp_extract,
                 object_key_extract,
                 state_update,
                 report_images,
                 custom_settings,create_event,
                 send_events):
        self._sequence_timestamp_extract = stream_sequence_timestamp_extract
        self._state_cache = {}
        

        self._create_event = create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = False
