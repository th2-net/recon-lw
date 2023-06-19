from sortedcontainers import SortedKeyList
from recon_lw import recon_lw


class SequenceCache:
    def __init__(self, horizon_delay_seconds):
        self._sequence = SortedKeyList(key=lambda item: item[0])
        self._times = SortedKeyList(key=lambda t: recon_lw.time_stamp_key(t[0]))
        self._duplicates = SortedKeyList(key=lambda item: item[0])
        self._horizon_delay_seconds = horizon_delay_seconds
        self._debug = False

    def add_object(self, seq_num: int, ts: dict, o: dict) -> None:
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

    def get_next_chunk(self, current_ts: dict) -> SortedKeyList:
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
        for i in range(0, processed_len):
            self._sequence.pop(0)

    def get_duplicates_collection(self):
        return self._duplicates
