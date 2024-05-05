
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from recon_lw.core.EventsSaver import IEventsSaver
from recon_lw.matching.init_function import AbstractMatcherContext


class RuleContext:
    def __init__(self,
                 rule_root_event: dict,
                 event_saver: IEventsSaver,
                 event_sequence: Dict[str, Any]
                 ):
        self.rule_root_event = rule_root_event
        self.event_saver = event_saver
        self.event_sequence = event_sequence

    @staticmethod
    def from_dict(rule_context: dict):
        return RuleContext(
            rule_context['events_saver'],
            rule_context['event_sequence'],
            rule_context['event']
        )


class AbstractRule(ABC):

    def __init__(self):
        self.horizon_delay = None
        self.collect_func = None
        self.flush_func = None

        self.rule_context: Optional[RuleContext] = None
        self.matcher_context: Optional[AbstractMatcherContext] = None

        self.first_key_func = None
        self.second_key_func = None

    def set_rule_context(self, context: RuleContext):
        self.rule_context = context

    def get_root_event(self) -> Dict[str, Any]:
        return self.rule_context.rule_root_event

    def get_event_saver(self) -> IEventsSaver:
        return self.rule_context.event_saver

    def get_event_sequence(self) -> Dict[str, Any]:
        return self.rule_context.event_sequence

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

