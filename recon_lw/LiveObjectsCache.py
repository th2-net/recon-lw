from sortedcontainers import SortedKeyList
from recon_lw import recon_lw
from datetime import datetime


class LiveObjectsCache:
    def __init__(self,horizon_delay_seconds, key_timestamp, update_object, new_object, is_alive):
        self._horizon_delay_seconds = horizon_delay_seconds
        self._key_timestamp = key_timestamp
        self._update_object = update_object
        self._new_object = new_object
        self._is_alive = is_alive
        self._time_index = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))
        self._objects_index = {}

    def process_objects_batch(self, batch):
        last_ts = None
        for o in batch:
            old_key, ts = self._key_timestamp(o)
            last_ts = ts

            if old_key is not None and old_key in self._objects_index:
                ts_entry, status, history = self._objects_index[old_key]
                new_status = self._update_object(o)
                if new_status is not None:
                    history.append(o)
                    new_ts_entry = [ts, old_key]
                    ts_entry[1] = None
                    self._objects_index[old_key] = (new_ts_entry, new_status, history)
                    self._time_index.add(new_ts_entry)

            new_key, new_status = self._new_object(o)
            if new_key is not None:
                new_ts_entry = [ts, new_key]
                self._objects_index[new_key] = (new_ts_entry, new_status, [o])

        if last_ts is not None:
            edge_timestamp = {"epochSecond": last_ts["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            horizon_edge = self._time_index.bisect_key_left(recon_lw.time_stamp_key(edge_timestamp))
            if horizon_edge > 0:
                for n in range(horizon_edge):
                    nxt = self._time_index.pop(0)
                    if nxt[1] is not None:
                        ts_entry, status, history = self._objects_index[nxt[1]]
                        if not self._is_alive(status):
                            self._objects_index.pop(nxt[1])

    def get_object_status_history(self, obj_id):
        if obj_id not in self._objects_index:
            return None
        ts_item, status, history = self._objects_index[obj_id]
        return status, history

    def flush_all(self):
        self._time_index.clear()
        for k in [key for key, val in self._objects_index.items() if not self._is_alive(val[1])]:
            self._objects_index.pop(k)

