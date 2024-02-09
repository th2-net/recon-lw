from abc import ABC, abstractmethod
from typing import Callable, Any, Tuple, Optional, Union

from sortedcontainers import SortedKeyList

from recon_lw.ts_converters import epoch_nano_str_to_ts, ts_to_epoch_nano_str, time_stamp_key

from recon_lw import recon_lw
from th2_data_services.utils import time as time_utils

from recon_lw.EventsSaver import IEventsSaver
from recon_lw._types import Th2Timestamp
from recon_lw.stream import Streams


class IInterpretHandler(ABC):
    @abstractmethod
    def handler(self, match: list, custom_settings, create_event,
                                 save_events) -> None:
        """
        Matched messages handling.

        The method should store some Recon Events to file.



        Args:
            match: The list of 3 elements. [o1, o2, tech]
                where
                    o1 - second stream order
                    o2 - first stream order or None
            custom_settings:
            create_event:
            save_events:

        Returns:

        """


# _SearchTsKeyFuncTyping = TypeVar('_SearchTsKeyFuncTyping', bound=Callable[[Any, Any], Tuple[Optional[Th2Timestamp], Any]])
class ISearchTsKeyHandler(ABC):
    @abstractmethod
    def handler(self,
                o: Union[dict, Any],
                settings: Union[dict, Any]) -> Tuple[Optional[Th2Timestamp], Any]:
        """
        The method should return message-timestamp and the matching key for the stream-1.

        For the first stream (left).

        `1 stream` is a stream of sequential state changes. It is a
        deterministic set of events that shows every change.
        For example:
            [1] OrderBook.
            Each record in such a stream is a complete description of the state
            of the order book at a certain point in time.
            This stream has no gaps. We know all the states of the OrderBook.

        Args:
            o:
            settings:

        Returns:
            ts1, key1

        """


class IStateTsKeyOrderHandler(ABC):
    @abstractmethod
    def handler(self,
                o: Union[dict, Any],
                settings: Union[dict, Any]
                ) -> Tuple[Optional[Th2Timestamp], Any, Optional[int]]:
        """
        The method should return message-timestamp, the matching key for the
        stream-2 and .

        For the second stream (right)

        `2nd stream` is the same thing, but received at a random point in time.
        For example:
            [1] MarketData snapshots.
            This is an OrderBook state stream aggregated over time. For example
            every 10 seconds. This stream does not contain all the states of the
            book. It only displays the status at the time the message was
            published.
            [2] Some API request that returns some current state.

        Args:
            o:
            settings:

        Returns:
            ts2, key2, order
        """


class LastStateMatcher:
    def __init__(self,
                 horizon_delay_seconds: int,
                 get_search_ts_key: Union[Callable, ISearchTsKeyHandler],  # For the first stream (left)
                 get_state_ts_key_order: Union[Callable, IStateTsKeyOrderHandler],  # For the second stream (right)
                 interpret_func: Union[Callable, IInterpretHandler],
                 custom_settings: Union[dict, Any],
                 create_event: Union[Callable, IEventsSaver],
                 send_events,
                 events_saver: Optional[IEventsSaver] = None,
                 ):
        """
        LastStateMatcher matches two non-equal streams.

        For each state (record) from aggregated `stream 2` (the stream contains
        only some system states) LastStateMatcher looking for a state (record)
        in `stream 1` (the stream contains all states of the system) by
        keys returned by `get_search_ts_key` and `get_state_ts_key_order`.

        Matched messages are compared in `interpret_func`.

        It matches 1 to 1.


        LastStateMatcher assumes that there are 2 unequal streams.

        `1 stream` is a stream of sequential state changes. It is a
        deterministic set of events that shows every change.
        For example:
            [1] OrderBook.
            Each record in such a stream is a complete description of the state
            of the order book at a certain point in time.
            This stream has no gaps. We know all the states of the OrderBook.

        `2nd stream` is the same thing, but received at a random point in time.
        For example:
            [1] MarketData snapshots.
            This is an OrderBook state stream aggregated over time. For example
            every 10 seconds. This stream does not contain all the states of the
            book. It only displays the status at the time the message was
            published.
            [2] Some API request that returns some current state.


        Args:
            horizon_delay_seconds:
            get_search_ts_key: For the first stream (left)
            get_state_ts_key_order: For the second stream (right)
            interpret_func:
            custom_settings: Any object that will be put to handler functions.
                TODO - user can manually provide them to these functions. What
                    is the idea to do it via this class?
            create_event:
            send_events:
        """
        self._search_time_index = SortedKeyList(key=lambda t: time_stamp_key(t[0]))

        # {key2 : { "prior_ts" : ts, "prior_obj" ; obj, SortedKeyList(timestamps)}  }
        self._state_cache = {}

        # sorted list of tuples(ts, key2, order, object)
        self._state_time_index = SortedKeyList(key=lambda t: f"{time_stamp_key(t[0])}_{t[1]}")

        # Note, we don't use a function to define the handler functions, because
        #   the function don't provide IDE type hinting.
        self._get_search_ts_key = get_search_ts_key.handler if isinstance(get_search_ts_key, ISearchTsKeyHandler) else get_search_ts_key
        self._get_state_ts_key_order = get_state_ts_key_order.handler if isinstance(get_state_ts_key_order, IStateTsKeyOrderHandler) else get_state_ts_key_order
        self._interpret_func = interpret_func.handler if isinstance(interpret_func, IInterpretHandler) else interpret_func

        # TODO -- _create_event
        #   1. we only put this function to _interpret_func
        #   2. We don't know it's interface, so we cannot use it inside this class.
        #
        #   Do we need to provide this parameter?
        #   Should we provide it to _interpret_func ?
        #   I don't know the interface of create_event function, when develop
        #   _interpret_func  ->  no IDE type hints.
        #
        #   If we want to have create_event as the class parameter self._create_event,
        #   there are two way
        #       1. Better way -- make this LastStateMatcher class as an Abstract one
        #        and mark all required functions as abstractmethods.
        #        The same as it was in the previous th2-check2-recon
        #       2. Another way -- is to Define Interfaces outside the class and just
        #        expect the subclasses of these interfaces.
        self._create_event = create_event

        # TODO
        #   All the same points for _send_events as for _create_event
        self._send_events = send_events
        self._horizon_delay_seconds = horizon_delay_seconds
        self._custom_settings = custom_settings
        self._debug = False
        self._events_saver = events_saver

    # def _get_handler_func(self, func, interface_cls: TypeGuard[_T]) -> _T:
    #     """That doesn't provide type hints ..."""
    #     return func.handler if isinstance(func, interface_cls) else func

    def _state_cache_add_item(self, ts, key2):
        if key2 not in self._state_cache:
            self._state_cache[key2] = {"prior_ts": None, "prior_o": None,
                                       "records_times": SortedKeyList(key=time_stamp_key)}
        self._state_cache[key2]["records_times"].add(ts)

    def _state_cache_delete_item(self, ts, key2, o):
        p_ts = self._state_cache[key2]["prior_ts"]
        if p_ts is None or time_utils.timestamp_delta_us(p_ts, ts) > 0:
            self._state_cache[key2]["prior_ts"] = ts
            self._state_cache[key2]["prior_o"] = o

        self._state_cache[key2]["records_times"].remove(ts)

    def process_objects_batch(self, batch: list) -> None:
        """

        Args:
            batch: list with objects from stream-1 (full stream) or stream-2
            (agg stream).
            There is no separation between streams. Both streams combined to 1
            stream. TODO -- Why?

        Returns:

        """
        stream_time = None
        for o in batch:
            ts1, key1 = self._get_search_ts_key(o, self._custom_settings)
            if key1 is not None:
                stream_time = ts1
                self._search_time_index.add((ts1, key1, o))
                continue
            ts2, key2, order = self._get_state_ts_key_order(o, self._custom_settings)
            if key2 is not None:
                stream_time = ts2
                index_key = f"{time_stamp_key(ts2)}_{key2}"
                i = self._state_time_index.bisect_key_left(index_key)
                current_len = len(self._state_time_index)
                next_key = None
                if i < current_len:
                    next_key = f"{time_stamp_key(self._state_time_index[i][0])}_{self._state_time_index[i][1]}"
                if i == current_len or index_key != next_key:
                    self._state_time_index.add([ts2, key2, order, o])
                    self._state_cache_add_item(ts2, key2)
                else:
                    rec = self._state_time_index[i]
                    if order >= rec[2]:
                        rec[2] = order
                        rec[3] = o

        #flush
        if stream_time is not None:
            self._flush(stream_time)

    def _flush(self, horizon_time):
        if horizon_time is not None:
            edge_timestamp = {"epochSecond": horizon_time["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            search_edge = self._search_time_index.bisect_key_left(
                time_stamp_key(edge_timestamp))
        else:
            search_edge = len(self._search_time_index)

        for n in range(search_edge):
            nxt = self._search_time_index.pop(0)
            ts1 = nxt[0]
            key1 = nxt[1]  # TODO -- that is actually key2
            o1 = nxt[2]  # second stream order

            if key1 not in self._state_cache:
                o2 = None
            else:
                records_times = self._state_cache[key1]["records_times"]
                i = records_times.bisect_key_right(
                    time_stamp_key(ts1))
                if i == 0:
                    o2 = self._state_cache[key1]["prior_o"]
                else:
                    ts2 = records_times[i-1]
                    sti = self._state_time_index.bisect_key_left(
                        f"{time_stamp_key(ts2)}_{key1}")
                    o2 = self._state_time_index[sti][3]
            tech = {"key1": key1,
                    "ts1": ts1,
                    "state_cache": self._state_cache.get(key1)}
            # TODO 1 -- what's the idea to pass
            #   self._custom_settings, self._create_event, self._send_events
            #   if the user provides self._interpret_func and can put them there manually?
            # TODO 2
            #   [o1, o2, tech] -- what is the idea to pass the list?
            #   It's more convenient to pass the object with fields.
            self._interpret_func([o1, o2, tech], self._custom_settings, self._create_event, self._send_events)

        if horizon_time is not None:
            edge_timestamp = {"epochSecond": horizon_time["epochSecond"] - self._horizon_delay_seconds,
                              "nano": 0}
            state_edge = self._state_time_index.bisect_key_left(f"{time_stamp_key(edge_timestamp)}_")
        else:
            state_edge = len(self._state_time_index)

        for n in range(state_edge):
            nxt_state = self._state_time_index.pop(0)
            self._state_cache_delete_item(nxt_state[0], nxt_state[1], nxt_state[3])

    def flush_all(self) -> None:
        self._flush(None)

    def execute_standalone(self,
                           streams: Streams,
                           get_timestamp: Callable,
                           buffer_len=100):

        message_buffer = [None] * buffer_len

        while len(streams) > 0:
            next_batch_len = streams.get_next_batch(message_buffer,
                                                     buffer_len, get_timestamp)
            buffer_to_process = message_buffer
            if next_batch_len < buffer_len:
                buffer_to_process = message_buffer[:next_batch_len]
            self.process_objects_batch(buffer_to_process)

        self.flush_all()
        self._events_saver.flush()
