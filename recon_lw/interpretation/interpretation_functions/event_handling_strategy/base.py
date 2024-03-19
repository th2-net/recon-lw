from typing import List, Dict, Iterator, Protocol, Union
from abc import ABC, abstractmethod

from recon_lw.interpretation.adapter import Adapter
from recon_lw.interpretation.check_rule import FieldCheckResult
from recon_lw.interpretation.field_checker import FieldChecker
from recon_lw.interpretation.interpretation_functions.event_enhancement import ReconEventChainEnhancement
from recon_lw.interpretation.interpretation_functions.event_name_provider import ReconEventNameProvider
from recon_lw.interpretation.interpretation_functions.event_name_provider.base import ReconEventNameProviderProtocol
from recon_lw.interpretation.interpretation_functions.event_type import ReconType
from recon_lw.core.type.types import Message
from recon_lw.core.utility import create_event, Counters


class IEventHandlingStrategy(Protocol):

    def __call__(self, match_msgs: List[Message], event_sequence: dict, is_copy: bool, orig_adapter: Adapter,
                     copy_adapter: Adapter) -> List[Dict]:
        pass


class SimpleMatchEventHandlingStrategy(IEventHandlingStrategy):

    def __init__(
            self,
            recon_name: str,
            event_name_provider: Union[ReconEventNameProvider, ReconEventNameProviderProtocol],
            fields_checker: FieldChecker,
            counters,
            recon_event_chain_enhancement: ReconEventChainEnhancement,
            enrich_events_with_messages: bool = False,
    ):
        self._event_name_provider = event_name_provider
        self._fields_checker = fields_checker
        self._counters = counters
        self._recon_event_chain_enhancement = recon_event_chain_enhancement
        self.recon_name = recon_name
        self.enrich_events_with_messages = enrich_events_with_messages

    def __call__(self, match_msgs: List[Message], event_sequence: dict, is_copy: bool, orig_adapter: Adapter,
                     copy_adapter: Adapter) -> List[Dict]:

        original = match_msgs[0]
        copy = match_msgs[1]
        if isinstance(self._event_name_provider, ReconEventNameProvider):
            name = self._event_name_provider.get_match_event_name()
        else:
            name = self._event_name_provider(ReconType.BasicReconMatch)

        body = {}
        diff_list = []
        differences: Iterator[FieldCheckResult] = self._fields_checker.compare(original, copy)
        status = True
        for fcr in differences:
            status = False
            diff_list.append(
                dict(field_name=fcr.field, expected=fcr.left_val, actual=fcr.right_val)
            )

        order_ids = orig_adapter.get_fields_group(original, "order_ids") \
                    or copy_adapter.get_fields_group(copy, "order_ids")

        if order_ids is not None:
            body["order_ids"] = order_ids

        if not status:
            if isinstance(self._event_name_provider, ReconEventNameProvider):
                name = self._event_name_provider.get_match_diff_event_name()
            else:
                name = self._event_name_provider(ReconType.BasicReconMatch, False)
            self._counters.match_fail += 1
            body['diff'] = diff_list
            if self.enrich_events_with_messages:
                body['messages'] = match_msgs
        else:
            self._counters.match_ok += 1

        event = create_event(
            recon_name=self.recon_name,
            name=name,
            type=ReconType.BasicReconMatch.value,
            event_sequence=event_sequence,
            body=body,
            ok=status
        )

        event["attachedMessageIds"] = [m['messageId'] for m in match_msgs if m is not None]
        if self._recon_event_chain_enhancement:
            self._recon_event_chain_enhancement.apply(event, original, orig_adapter)
        copy_adapter.on_message_exit(copy)
        orig_adapter.on_message_exit(original)

        return [event]


class SimpleMissEventHandlingStrategy(IEventHandlingStrategy):
    def __init__(self,
                 recon_name: str,
                 event_name_provider: Union[ReconEventNameProvider, ReconEventNameProviderProtocol],
                 fields_checker: FieldChecker,
                 counters,
                 recon_event_chain_enhancement: ReconEventChainEnhancement,
                 enrich_event_with_messages: bool = True
                 ):
        self._event_name_provider = event_name_provider
        self._fields_checker = fields_checker
        self._counters = counters
        self._recon_event_chain_enhancement = recon_event_chain_enhancement
        self.recon_name = recon_name
        self._enrich_event_with_messages = enrich_event_with_messages

    def __call__(self, match_msgs: List[Message], event_sequence: dict, is_copy: bool, orig_adapter: Adapter,
                     copy_adapter: Adapter):
        if is_copy:
            msg = match_msgs[1]
            adapter = copy_adapter
        else:
            msg = match_msgs[0]
            adapter = orig_adapter

        order_ids = adapter.get_fields_group(msg, "order_ids")
        match_key = adapter.on_message_exit(msg)

        event = self._get_miss_event(
            msg,
            match_key=match_key,
            recon_type=ReconType.BasicReconMissRight if is_copy else ReconType.BasicReconMissLeft,
            event_sequence=event_sequence,
            order_ids=order_ids,
            event_name_provider=self._event_name_provider,
            counters=self._counters
        )

        if self._recon_event_chain_enhancement:
            self._recon_event_chain_enhancement.apply(event, msg, orig_adapter)

        if self._enrich_event_with_messages:
            if is_copy:
                event['body']['messages'] = match_msgs[1:]
            else:
                event['body']['messages'] = [match_msgs[0]]
        adapter.on_message_exit(msg)

        return [event]

    def _get_miss_event(self, msg, event_name_provider: Union[ReconEventNameProvider, ReconEventNameProviderProtocol],
                        match_key,
                        recon_type: ReconType,
                        counters: Counters,
                        order_ids,
                        event_sequence):
        if recon_type == ReconType.BasicReconMissLeft:
            counters.no_left += 1
            if isinstance(event_name_provider, ReconEventNameProvider):
                name = event_name_provider.get_miss_copy_event_name()
            else:
                name = event_name_provider(ReconType.BasicReconMissLeft)
        elif recon_type == ReconType.BasicReconMissRight:
            counters.no_right += 1
            if isinstance(event_name_provider, ReconEventNameProvider):
                name = event_name_provider.get_miss_original_event_name()
            else:
                name = event_name_provider(ReconType.BasicReconMissRight)
        else:
            raise Exception('unexpected behaviour')

        body = {"key": match_key}

        if order_ids:
            body["order_ids"] = order_ids

        event = create_event(
            recon_name=self.recon_name,
            name=name,
            type=recon_type.value,
            ok=False,
            event_sequence=event_sequence,
            body=body,
        )
        event["attachedMessageIds"] = [msg["messageId"]]

        return event
