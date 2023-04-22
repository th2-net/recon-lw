from recon_lw import recon_lw
from datetime import datetime, timedelta
from th2_data_services.data import Data


class EventsSaver:
    def __init__(self, path):
        self._event_sequence = {"name": "recon_lw", "stamp": str(datetime.now().timestamp()), "n": 0}
        self._buffer = []
        self._path = path

    def save_events(self, batch):
        self._buffer.extend(batch)
        if len(self._buffer) > 50000:
            events = Data(self._buffer)
            events_file = self._path + "/" + self._buffer[0]["eventId"] + ".pickle"
            events.build_cache(events_file)
            self._buffer.clear()

    def create_event(self, name, type, ok=True, body=None, parentId=None):
        ts = datetime.now()
        e = {"eventId": self._create_event_id(),
             "successful": ok,
             "eventName": name,
             "eventType": type,
             "body": body,
             "parentEventId": parentId,
             "startTimestamp": {"epochSecond": int(ts.timestamp()), "nano": ts.microsecond * 1000},
             "attachedMessageIds": []}
        return e

    def _create_event_id(self):
        self._event_sequence["n"] += 1
        return "{0}_{1}-{2}".format(self._event_sequence["name"],
                                    self._event_sequence["stamp"],
                                    self._event_sequence["n"])
