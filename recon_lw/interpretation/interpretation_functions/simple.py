from recon_lw.interpretation.interpretation_functions.base import InterpretationFunctionProvider, InterpretationFunctionType
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.interpretation.interpretation_functions.event_name_provider import ReconEventNameProvider, \
    BasicReconEventNameProvider
from recon_lw.core.utility.counter import Counters
from recon_lw.interpretation.field_checker.base import FieldChecker
from recon_lw.interpretation.interpretation_functions.event_name_provider.base import ReconEventNameProviderProtocol
from recon_lw.matching.key_functions.base import KeyFunctionProvider
from recon_lw.interpretation.interpretation_functions.event_enhancement.enhancement_chain import ReconEventChainEnhancement
from recon_lw.core.type.types import Message
from typing import List, Optional, Union
from recon_lw.interpretation.interpretation_functions.event_handling_strategy.base import IEventHandlingStrategy, \
    SimpleMissEventHandlingStrategy, SimpleMatchEventHandlingStrategy


class BasicInterpretationFunctionProvider(InterpretationFunctionProvider):

    def __init__(
            self,
            recon_name: str,
            original_stream_adapter: Adapter,
            copy_stream_adapter: Adapter,
            fields_checker: FieldChecker,
            enrich_event_with_messages: bool = False,
            event_name_provider: Optional[Union[
                ReconEventNameProvider, ReconEventNameProviderProtocol]] = None,
            recon_event_chain_enhancement: Optional[
                ReconEventChainEnhancement] = None,
            match_event_handling_strategy: IEventHandlingStrategy = None,
            miss_event_handling_strategy: IEventHandlingStrategy = None,
    ):
        """Default interpretation function for 2 data streams.

        It takes matched messages and categorises them by 4 types:
          - miss left (orig) - there is a message in `stream2` stream with
                certain key and there is no message with the same key in
                `stream1` stream within horizon delay (match window).
          - miss right (copy) - there is a message in `stream1` stream with
                certain key and there is no message with the same key in
                `stream2` stream within horizon delay (match window).
          - match - there is a message in `stream1` and `stream2` stream with
                the same key and all fields to compare are equal.
          - match_diff - there is message in `stream1` and `stream2` stream
                with the same key and some fields are not equal.

        This interpretation function

        In result, pickle with events ( dict object ) will be published.

        Args:
            recon_name:
            original_stream_adapter:
            copy_stream_adapter:
            fields_checker:
            enrich_event_with_messages:
            event_name_provider:
            recon_event_chain_enhancement:
            match_event_handling_strategy:
            miss_event_handling_strategy:
        """
        # TODO
        #   1. We have to define single name-convention.
        #       Either Left/Right or Orig/Copy
        #       or maybe something else, because sometimes we can have more
        #       that 2 streams

        self._original_stream_adapter = original_stream_adapter
        self._copy_stream_adapter = copy_stream_adapter
        self._event_name_provider = event_name_provider
        self._fields_checker = fields_checker
        self._counters = Counters()
        self._recon_event_chain_enhancement = recon_event_chain_enhancement

        if event_name_provider is None:
            event_name_provider = BasicReconEventNameProvider(recon_name)

        if miss_event_handling_strategy is None:
            miss_event_handling_strategy = SimpleMissEventHandlingStrategy(
                recon_name=recon_name,
                event_name_provider=event_name_provider,
                fields_checker=fields_checker,
                counters=self._counters,
                recon_event_chain_enhancement=recon_event_chain_enhancement,
                enrich_event_with_messages=enrich_event_with_messages
            )

        if match_event_handling_strategy is None:
            match_event_handling_strategy = SimpleMatchEventHandlingStrategy(
                recon_name=recon_name,
                event_name_provider=event_name_provider,
                fields_checker=fields_checker,
                counters=self._counters,
                recon_event_chain_enhancement=recon_event_chain_enhancement,
                enrich_events_with_messages=enrich_event_with_messages
            )

        self.miss_event_handling_strategy = miss_event_handling_strategy
        self.match_event_handling_strategy = match_event_handling_strategy

    def provide(self) -> InterpretationFunctionType:
        def interpret(match_msgs: List[Message], _, event_sequence: dict):
            if match_msgs is None:
                return []
            original = match_msgs[0]
            copy = match_msgs[1]
            events = []

            if original is not None and copy is not None:
                events += self.match_event_handling_strategy(
                    match_msgs, event_sequence, False,
                    self._original_stream_adapter, self._copy_stream_adapter)
            elif original is None and copy is not None:
                events += self.miss_event_handling_strategy(
                    match_msgs, event_sequence, True,
                    self._original_stream_adapter, self._copy_stream_adapter)
            elif copy is None and original is not None:
                events += self.miss_event_handling_strategy(
                    match_msgs, event_sequence, False,
                    self._original_stream_adapter, self._copy_stream_adapter)

            return events

        return interpret


class BasicInterpretationFunctionProviderBuilder:
    # FIXME:
    #   Looks like a useless class.
    def __init__(self):
        self._recon_name = "any"
        self._original_stream_adapter = None
        self._copy_stream_adapter = None
        self._event_name_provider = None
        self._fields_checker = None
        self._counters = None
        self._original_stream_key_function = None
        self._copy_stream_key_function = None
        self._recon_event_chain_enhancement = None

    def with_original_stream_adapter(self, adapter: Adapter):
        self._original_stream_adapter = adapter
        return self

    def with_copy_stream_adapter(self, adapter: Adapter):
        self._copy_stream_adapter = adapter
        return self

    def with_event_name_provider(self, provider: ReconEventNameProvider):
        self._event_name_provider = provider
        return self

    def with_fields_checker(self, checker: FieldChecker):
        self._fields_checker = checker
        return self

    def with_counters(self, counters: Counters):
        self._counters = counters
        return self

    def with_original_stream_key_function(self, provider: KeyFunctionProvider):
        self._original_stream_key_function = provider
        return self

    def with_copy_stream_key_function(self, provider: KeyFunctionProvider):
        self._copy_stream_key_function = provider
        return self

    def with_recon_event_chain_enhancement(self, enhancement: ReconEventChainEnhancement):
        self._recon_event_chain_enhancement = enhancement
        return self

    def with_recon_name(self, name):
        self._recon_name = name
        return self

    def build(self) -> BasicInterpretationFunctionProvider:
        return BasicInterpretationFunctionProvider(
            recon_name=self._recon_name,
            original_stream_adapter=self._original_stream_adapter,
            copy_stream_adapter=self._copy_stream_adapter,
            event_name_provider=self._event_name_provider,
            fields_checker=self._fields_checker,
            recon_event_chain_enhancement=self._recon_event_chain_enhancement
        )
