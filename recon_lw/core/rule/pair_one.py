from typing import Optional, Callable, Dict, Any

from recon_lw.core.rule.base import AbstractRule
from recon_lw.matching.LastStateMatcher import LastStateMatcher
from recon_lw.matching.flush_function import DefaultFlushFunction
from recon_lw.matching.init_function import SimpleMatcherContext, MatcherContextProvider, DefaultMatcherContextProvider


class PairOneRule(AbstractRule):
    def __init__(self):
        super().__init__()
        self.live_orders_cache: Optional[LastStateMatcher] = None
        self.context: Optional[SimpleMatcherContext] = None
        self.pair_key_func = None
        self.one_key_func = None
        self._dict_config = None

    def to_dict(self) -> Dict[str, Any]:
        return self._dict_config

    @staticmethod
    def from_dict(name: str, config: dict) -> 'PairOneRule':
        rule = PairOneRule()

        rule.name = name
        rule.horizon_delay = config['horizon_delay']
        last_state_matcher = config.get('live_orders_cache')

        rule.collect_func = config.get('collect_func')

        if rule.collect_func is None:
            from recon_lw.matching.collect_matcher import DefaultCollectMatcher
            rule.collect_func = DefaultCollectMatcher(config['rule_match_func'], last_state_matcher)

        init_func = config.get('init_func', DefaultMatcherContextProvider())
        if isinstance(init_func, MatcherContextProvider):
            rule.context = init_func.get_context(rule)

        elif isinstance(init_func, Callable):
            init_func(config)
            rule.context = SimpleMatcherContext(
                match_index=config['match_index'],
                time_index=config['time_index'],
                message_cache=config['message_cache']
            )

        rule.flush_func = config.get('flush_func',
                                     DefaultFlushFunction(rule.context, config['interpret_func'], last_state_matcher))
        rule._dict_config = config

        rule.pair_key_func = config.get('pair_key_func')
        rule.one_key_func = config.get('one_key_func')

        return rule
