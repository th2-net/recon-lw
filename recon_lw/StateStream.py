from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils
from recon_lw.SequenceCache import SequenceCache
from recon_lw.EventsSaver import EventsSaver

class StateStream:
    def __init__(self, get_next_update_func, events_saver, combine_instantenious_snapshots = True) -> None:
        self._get_next_update_func = get_next_update_func
        self._get_snapshot_id = get_next_update_func
        self._combine_instantenious_snapshots = combine_instantenious_snapshots
        self._events_saver: EventsSaver = events_saver
    
    def state_updates(self, stream):
        for o in stream:
            key, ts, action, state = self._get_next_update_func(o)
            if key is not None:
                yield (key, ts, action, state)
    
    def snapshots(self, stream):
        snapshots_collection = {}
        updated_snapshots = set()
        last_ts = None
        for tpl in self.state_updates(stream):
            key = tpl[0]
            ts = tpl[1]
            action = tpl[2]
            state = tpl[3]
            snap_id = self._get_snapshot_id(state)
            if last_ts is not None:
                if not self._combine_instantenious_snapshots:
                    for k in updated_snapshots:
                        yield self.create_snapshot_event(last_ts, snapshots_collection[k]) #(last_ts, snapshots_collection[k])
                    updated_snapshots.clear()
                elif recon_lw.time_stamp_key(ts) != recon_lw.time_stamp_key(last_ts):
                    for k in updated_snapshots:
                        yield self.create_snapshot_event(last_ts, snapshots_collection[k]) #(last_ts, snapshots_collection[k])
                    updated_snapshots.clear()
                    
            if snap_id is None:
                continue
            if snap_id not in snapshots_collection:
                snapshots_collection[snap_id] = {}
            snapshot = snapshots_collection[snap_id]
            if action == 'c':
                if key in snapshot:
                    # Consistency error
                    err = self._events_saver.create_event("ObjectAlreadyPresent", 
                                                          "StateStreamError", False,
                                                          {'update': tpl})
                    self._events_saver.save_events([err])
                snapshot[key] = state
                updated_snapshots.add(snap_id)
                last_ts = ts
            elif action == 'u':
                if key not in snapshot:
                    # Consistency error
                    err = self._events_saver.create_event("ObjectDontExist", 
                                                          "StateStreamError", False,
                                                          {'update': tpl})
                    self._events_saver.save_events([err])
                snapshot[key] = state
                updated_snapshots.add(snap_id)
                last_ts = ts
            elif action == 'd':
                if key not in snapshot:
                    # Consistency error
                    err = self._events_saver.create_event("ObjectDontExist", 
                                                          "StateStreamError", False,
                                                          {'update': tpl})
                    self._events_saver.save_events([err])
                snapshot.pop(key)
                updated_snapshots.add(snap_id)
                last_ts = ts
        for k in updated_snapshots:
            yield self.create_snapshot_event(last_ts, snapshots_collection[k]) #(last_ts, snapshots_collection[k])
        updated_snapshots.clear()

    def create_snapshot_event(self, ts, snap_id, snapshot_source):
        return self._events_saver.create_event("Snapshot", 
                                               "Snapshot", True,
                                               {'snap_id': snap_id,
                                                'ts': ts,
                                                'sn': snapshot_source.copy()})

