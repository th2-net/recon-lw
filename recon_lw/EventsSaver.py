from recon_lw import recon_lw
from datetime import datetime, timedelta
from th2_data_services.data import Data
from pathlib import Path
import os

import pickle

class EventsSaver:
    def __init__(self, path):
        self._event_sequence = {"name": "recon_lw", "stamp": str(datetime.now().timestamp()), "n": 0}
        self._scopes_buffers = {}
        self._path = path
        #temp test
        self._files = {}

    def flush(self):
        for f in self._files.values():
            f.close()
        return
        for scope in self._scopes_buffers.keys():
            self.flush_scope(scope)

    def flush_scope(self, scope):
        if scope in self._scopes_buffers:
            ts_start = datetime.now().timestamp()
            events = Data(self._scopes_buffers[scope])
            events_file = Path(self._path) / (scope + "_scope_" + self._scopes_buffers[scope][0]["eventId"] + ".pickle")
            events.build_cache(events_file)
            self._scopes_buffers[scope].clear()
            ts_end = datetime.now().timestamp()
            print (datetime.now(), " Saved local events for ", scope, ", duration: ", ts_end - ts_start)

    def save_events(self, batch):
        for e in batch:
            scope = e["scope"] if "scope" in e else "default"
            #temp test

            #events_file = Path(self._path) / (scope + "_scope_" + self._scopes_buffers[scope][0]["eventId"] + ".pickle")
            if scope not in self._files:
                self._files[scope] = open(os.path.join(self._path, f"{scope}_scope_{self._event_sequence['stamp']}.pickle"), 'wb')
            
            pickle.dump(e, self._files[scope])
            continue
            if scope not in self._scopes_buffers:
                self._scopes_buffers[scope] = []
            self._scopes_buffers[scope].append(e)
            if len(self._scopes_buffers[scope]) > 50000:
                self.flush_scope(scope)

    def create_event(self, name, type, ok=True, body=None, parentId=None, attached_messages=None, ts = None):
        attached_messages = attached_messages or []
        if ts is None:
            ts = datetime.now()
        e = {"eventId": self._create_event_id(),
             "successful": ok,
             "eventName": name,
             "eventType": type,
             "body": body,
             "parentEventId": parentId,
             "startTimestamp": {"epochSecond": int(ts.timestamp()), "nano": ts.microsecond * 1000},
             "attachedMessageIds": attached_messages}
        return e

    def _create_event_id(self):
        self._event_sequence["n"] += 1
        return "{0}_{1}-{2}".format(self._event_sequence["name"],
                                    self._event_sequence["stamp"],
                                    self._event_sequence["n"])
