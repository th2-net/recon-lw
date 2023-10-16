from sortedcontainers import SortedKeyList
from recon_lw import recon_lw


class SequenceCache:
    def __init__(self, horizon_delay_seconds):
        self._sequence = SortedKeyList(key=lambda item: item[0])
        self._times = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))
        self._duplicates = []#= SortedKeyList(key=lambda item: item[0])
        self._horizon_delay_seconds = horizon_delay_seconds
        self._debug = False

        self._time_indexes = {}
        self._objects = {}
        self._gaps = []
        self._last_processed_seq_num = 0
        self._last_processesd_seq_num_ts = None

    def get_next_gaps(self):
        if len(self._gaps) == 0:
            return []
        
        next_gaps = self._gaps
        self._gaps = []
        return next_gaps

    def add_object(self, seq_num: int, ts: dict, o: dict) -> None:
        if seq_num in self._objects:
            self._duplicates.append((seq_num,o, self._objects[seq_num]))
            return
        
        if ts["epochSecond"] not in self._time_indexes:
            self._time_indexes[ts["epochSecond"]] = [seq_num,seq_num]
        else:
            min_max = self._time_indexes[ts["epochSecond"]]
            if seq_num < min_max[0]:
                min_max[0] = seq_num
            elif seq_num > min_max[1]:
                min_max[1] = seq_num
        self._objects[seq_num] = o
        if self._last_processed_seq_num < seq_num:
            if self._last_processed_seq_num + 1 < seq_num:
                self._gaps.append({'s1': self._last_processed_seq_num + 1, 
                                   't1': self._last_processesd_seq_num_ts,
                                   's2': seq_num - 1,
                                   't2': ts})
            self._last_processed_seq_num = seq_num
            self._last_processesd_seq_num_ts = ts
        return
        seq_element = (seq_num, o)
        # gaps = sequence_cache["gaps"]
        gap = {"gap": True}
        if len(self._sequence) > 0:
            last_elem = self._sequence[-1]
            first_elem = self._sequence[0]
            if seq_num > last_elem[0]:
                self._sequence.add(seq_element)
                self._sequence.update([(i, gap) for i in range(last_elem[0] + 1, seq_num)])
                self._times.add((ts, seq_num))
            elif seq_num < first_elem[0]:
                #  radical difference means sequence Reset
                if first_elem[0] - seq_num > 500:
                    raise Exception(f'Stream break detected: got{seq_num} vs {first_elem[0]}')
                self._sequence.update([(i, gap) for i in range(seq_num + 1, first_elem[0])])
                self._sequence.add(seq_element)
                self._times.add((ts, seq_num))
            else:
                if "gap" in self._sequence[seq_num - first_elem[0]][1]:
                    del self._sequence[seq_num - first_elem[0]]
                    self._sequence.add(seq_element)
                    self._times.add((ts, seq_num))
                else:
                    self._duplicates.add((seq_num, o, self._sequence[seq_num - first_elem[0]][1]))
        else:
            self._sequence.add(seq_element)
            self._times.add((ts, seq_num))
    
    def yeild_objects(self, start,end):
        for seq_num in range(start, end+1):
            if seq_num in self._objects:
                yield (seq_num, self._objects.pop(seq_num))
            else:
                yield (seq_num,{"gap": True})

    def get_next_chunk(self, current_ts: dict) -> SortedKeyList:
        if current_ts is not None:
            horizon = current_ts["epochSecond"] - self._horizon_delay_seconds
            expiring_times = [t for t in self._time_indexes.keys() if t < horizon]
        else:
            expiring_times = list(self._time_indexes.keys())
        
        if len(expiring_times) == 0:
            return []
        start = None
        end = None
        for t in expiring_times:
            min_max = self._time_indexes.pop(t)
            if start is None or min_max[0] < start:
                start = min_max[0]
            if end is None or min_max[1] > end:
                end = min_max[1]
        
        return self.yeild_objects(start, end)
        
        if current_ts is not None:
            edge_timestamp = {"epochSecond": current_ts["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            horizon_edge = self._times.bisect_key_left(recon_lw.time_stamp_key(edge_timestamp))
            if horizon_edge < len(self._times):
                seq_index = self._times[horizon_edge][1]
                sub_seq = self._sequence.irange(None, (seq_index, None), (False, False))
                for i in range(0, horizon_edge):
                    self._times.pop(0)
            else:
                sub_seq = self._sequence
                self._times.clear()
            return sub_seq
        else:
            self._times.clear()
            return self._sequence

    def clear_processed_chunk(self, processed_len: int) -> None:
        return
        for i in range(0, processed_len):
            self._sequence.pop(0)

    def get_duplicates_collection(self):
        return self._duplicates
