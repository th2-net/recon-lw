from typing import Tuple, Iterator, Optional, List, Callable, \
    Iterable, Any

from sortedcontainers import SortedKeyList

from recon_lw.core._types import Th2Timestamp
from recon_lw.core.ts_converters import time_stamp_key

StreamsVal = Tuple[Th2Timestamp, Iterator, Optional[dict]]


class Streams(SortedKeyList):
    """
    Streams -- wrapper for SortedKeyList that provides type hints and
    methods to work with streams.
    Note:
        Default sort function sorts by Seconds precision.

    streams: [(Th2ProtobufTimestamp,
                    iterator for Data object,
                    First object from Data object or None), ...]
    """

    def __init__(self, iterable=None, key=None):
        # t[0] - Th2Timestamp
        self.default_sort_key_func = lambda t: time_stamp_key(t[0])

        if key is None:
            key = self.default_sort_key_func

        super().__init__(iterable, key)

    def __iter__(
            self
    ) -> Iterator[StreamsVal]:
        return super().__iter__()

    def add(self, value: StreamsVal):
        return super().add(value)

    def pop(self, index=-1) -> StreamsVal:
        return super().pop(index)

    def sync_streams(self, get_timestamp_func: Callable):
        """Yields synced by `get_timestamp_func` values from the streams.
        Almost the same as `get_next_batch` but yields all values from all
        streams. `get_next_batch` will return only the messages in the list.
        Args:
            get_timestamp_func: the function should take an element of any
                stream from streams inside this Streams object.
                It means that the function should be able to understand what
                element was passed (from which stream) and should return
                timestamp from it.
                If you use the `default_sort_key_func` for your `Streams` object,
                it means that your `get_timestamp_func` should return `Th2Timestamp`.
                Example:
                    your msg: {ts: {"epochSecond": 123, "nano": 55}, field1: 'a'}
                    your function should be: lambda m: m['ts']
        """
        # TODO
        #   We can add default `get_timestamp_func` using resolvers.get_timestamp

        while len(self) > 0:
            # TODO
            #   pop(0) complexity for list - O(n)
            #   It's better to use deque here
            #   https://wiki.python.org/moin/TimeComplexity
            #   or to have reversed sorting
            ts, next_stream_iterator, first_val_in_iterator = self.pop(0)
            try:
                if first_val_in_iterator is not None:
                    yield first_val_in_iterator
                o = next(next_stream_iterator)
                self.add((get_timestamp_func(o), next_stream_iterator, o))
            except StopIteration as e:
                continue

    def get_next_batch(self,
                       batch: List[Optional[dict]],
                       batch_len: int,  # buffer len
                       get_timestamp_func: Callable) -> int:
        """

        Args:
            streams: [(Th2ProtobufTimestamp,
                        iterator for Data object,
                        First object from Data object or None), ...]
            batch: buffer that will be populated with first_val_in_iterator
            batch_len: buffer len
            get_timestamp_func:

        Returns:

        """
        batch_pos = 0
        while batch_pos < batch_len and len(self) > 0:
            ts, iterator, first_val_in_iterator = self.pop(0)
            try:
                if first_val_in_iterator is not None:
                    batch[batch_pos] = first_val_in_iterator
                    batch_pos += 1
                o = next(iterator)
                self.add((get_timestamp_func(o), iterator, o))
            except StopIteration as e:
                continue

        return batch_pos

    def add_stream(self, iter_obj: Iterable):
        """Adds Iterable object to streams.

        Note:
            Data object from th2-data-services is Iterable.
        """
        ts0 = {"epochSecond": 0, "nano": 0}
        self.add((ts0, iter(iter_obj), None))
