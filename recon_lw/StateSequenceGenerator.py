from sortedcontainers import SortedKeyList
from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils
from recon_lw.SequenceCache import SequenceCache
import copy

class StateSequenceGenerator:
    def __init__(self, horizon_delay_seconds,
                 stream_sequence_timestamp_extract,
                 object_key_ts_extract,
                 state_update,
                 report_images,
                 custom_settings,create_event,
                 send_events):
        self._stream_sequence_timestamp_extract = stream_sequence_timestamp_extract
        self._object_key_ts_extract = object_key_ts_extract
        self._state_update = state_update
        self._report_images = report_images

        self._state_cache = {}
        self._streams = {}

        self._create_event = create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = True

    def process_objects_batch(self, batch: list) -> None:
        current_ts = None
        #add to sequence streams
        for o in batch:
            stream, seq, ts = self._stream_sequence_timestamp_extract(o)
            if stream is not None:
                if stream not in self._streams:
                    self._streams[stream] = SequenceCache(self._horizon_delay_seconds)
                current_ts = ts
                self._streams[stream].add_object(seq, ts, o)
                
        #flush old records
        self.process_streams(current_ts)
    
    def process_streams(self, current_ts):
        for stream, stream_cache in self._streams.items():
            next_chunk = stream_cache.get_next_chunk(current_ts)
            processed = 0
            arranged = {}
            chains = {}
            for o in next_chunk:
                key , ts, new_key = self._object_key_ts_extract(o)
                if key is not None:
                    ts_key = recon_lw.time_stamp_key(ts)
                    if ts_key not in arranged:
                        arranged[ts_key] = {}
                    chained_key = key
                    if key in chains:
                        chained_key = chains[key]                        
                    if new_key is not None and new_key != key:
                        chains[new_key] = chained_key
                    if chained_key not in arranged[ts_key]:
                        arranged[ts_key][chained_key] = [(o, key, new_key)]
                    else:
                        arranged[ts_key][chained_key].append((o, key, new_key))
                processed += 1

            tss = list(arranged.keys())
            tss.sort()
            for ts_key in tss:
                for chain_key, o_lst in arranged[ts_key].items():
                    if len(o_lst) == 0:
                        continue
                    first_key = o_lst[0][1]
                    if first_key not in self._state_cache:
                        self._state_cache[first_key] = {"no_state": True}
                    current_state = self._state_cache[first_key]
                    updates_states = []
                    for o, key, new_key in o_lst:
                        self._state_update(o, current_state, self._create_event, self._send_events)
                        updates_states.append((o,copy.deepcopy(current_state)))
                    last_new_key = o_lst[-1][2]
                    if last_new_key is not None and last_new_key != chain_key:
                        self._state_cache[last_new_key] = self._state_cache.pop(first_key)
                    if self._debug:
                        body = {"ts_key" : ts_key, "chain_key": chain_key, "last_new_key": last_new_key, "stream": stream}
                        body["all_keys"] = {item[1] for item in o_lst}
                        body["all_new_keys"] = {item[2] for item in o_lst}
                        body["o_list"] = o_lst
                        ev = self._create_event("SSGDebug", "SSGDebug", True, body)
                        self._send_events([ev])

                    self._report_images(updates_states, self._create_event, self._send_events)
        
            stream_cache.clear_processed_chunk(processed)


    def flush_all(self) -> None:
        self.process_streams(None)