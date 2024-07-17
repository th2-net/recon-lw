from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TYPE_CHECKING

from recon_lw.core.EventsSaver import IEventsSaver
from recon_lw.matching.init_function import AbstractMatcherContext
from recon_lw.matching.key_functions import KeyFunction

if TYPE_CHECKING:
    from recon_lw.matching.collect_matcher import CollectMatcher
    from recon_lw.matching.flush_function import FlushFunction


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

    def __init__(self,
                 # FixME:
                 #  Constructor is commented because it's required to update
                 #  many other places to apply this change.
                 #  This is required to do.

                 # name: str,
                 # # TODO -- perhaps we `horizon_delay` only for horizon recon
                 # #  and other recon types won't have such parameter.
                 # horizon_delay: int,
                 # collect_func: CollectMatcher,
                 # flush_func: FlushFunction,
                 # # TODO -- perhaps we matcher_context only for some subset of
                 # #  rules/recons only.
                 # matcher_context: AbstractMatcherContext,
                 # first_key_func: KeyFunction,
                 # second_key_func: KeyFunction,
                 ):
        self.name: str = None # name
        self.horizon_delay: int = None # horizon_delay
        self.collect_func: 'CollectMatcher' = None # collect_func  # 'rule_match_func'
        self.flush_func: 'FlushFunction' = None # flush_func
        self.matcher_context: Optional[AbstractMatcherContext] = None
        self.first_key_func: KeyFunction = None
        self.second_key_func: KeyFunction = None

        # rule_context will be added in execute_standalone
        self.rule_context: Optional[RuleContext] = None

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
        # FIXME:
        #   Probably we can define it here, and user don't need to do it.

        # FIXME:
        #   Also, what format should be here?
        pass
