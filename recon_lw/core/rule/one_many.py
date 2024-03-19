from typing import Optional, Callable, Dict, Any

from recon_lw.core.cache.processor.base import CacheManager
from recon_lw.core.rule.base import AbstractRule
from recon_lw.matching.LastStateMatcher import LastStateMatcher
from recon_lw.matching.collect_matcher.base import CollectMatcher
from recon_lw.matching.flush_function import DefaultFlushFunction, FlushFunction
from recon_lw.matching.init_function import MatcherContextProvider, SimpleMatcherContext, DefaultMatcherContextProvider
from recon_lw.matching.key_functions import KeyFunction
from recon_lw.matching.stream_matcher import ReconMatcher


class OneManyRuleConfig(AbstractRule):
    def __init__(self):
        super().__init__()
        self.first_key_func: Optional[KeyFunction] = None
        self.second_key_func: Optional[KeyFunction] = None
        self.cache_manager: Optional[CacheManager] = None
        self._as_dict: dict = None

    def to_dict(self) -> Dict[str, Any]:
        return self._as_dict

    @staticmethod
    def from_dict(name: str, config: dict) -> 'OneManyRuleConfig':

        rule = OneManyRuleConfig()
        rule._as_dict = config

        rule.name = name
        rule.horizon_delay = config['horizon_delay']
        last_state_matcher = config.get('live_orders_cache')

        rule.collect_func = config.get('collect_func')

        if rule.collect_func is None:
            from recon_lw.matching.collect_matcher import DefaultCollectMatcher
            rule.collect_func = DefaultCollectMatcher(config['rule_match_func'], last_state_matcher)

        init_func = config.get('init_func', DefaultMatcherContextProvider())
        if isinstance(init_func, MatcherContextProvider):
            rule.matcher_context = init_func.get_context()

        elif isinstance(init_func, Callable):
            init_func(config)
            rule.context = SimpleMatcherContext(
                match_index=config['match_index'],
                time_index=config['time_index'],
                message_cache=config['message_cache']
            )

        rule.flush_func = config.get('flush_func',
                                     DefaultFlushFunction(config['interpret_func'],
                                                          last_state_matcher))

        return rule

    @staticmethod
    def from_params(
            name: str,
            horizon_delay: int,
            context_provider: MatcherContextProvider=None,
            collect_func: CollectMatcher=None,
            flush_func: FlushFunction=None,
            first_key_func: Callable = None,
            second_key_func: Callable = None,
            last_state_matcher: Optional[LastStateMatcher]=None,
            cache_manager: CacheManager=None
    ):
        rule = OneManyRuleConfig()
        rule.name = name
        rule.horizon_delay = horizon_delay
        rule.matcher_context = context_provider.get_context()
        rule.last_state_matcher = last_state_matcher
        rule.collect_func = collect_func
        rule.first_key_func = first_key_func
        rule.second_key_func = second_key_func
        rule.flush_func = flush_func
        rule.cache_manager = cache_manager

        return rule

    @staticmethod
    def from_defaults(
            name: str,
            horizon_delay: int,
            match_function: ReconMatcher,
            intepretation_function: Callable,
            first_key_func: Callable = None,
            second_key_func: Callable = None,
    ):

        from recon_lw.matching.init_function import DefaultMatcherContextProvider
        context_provider = DefaultMatcherContextProvider()

        from recon_lw.matching.collect_matcher import DefaultCollectMatcher
        collect_func = DefaultCollectMatcher(match_function)

        from recon_lw.matching.flush_function import DefaultFlushFunction
        flush_func = DefaultFlushFunction(intepretation_function)

        rule = OneManyRuleConfig()
        rule.name = name
        rule.horizon_delay = horizon_delay
        rule.matcher_context = context_provider.get_context()
        rule.last_state_matcher = None
        rule.collect_func = collect_func
        rule.first_key_func = first_key_func
        rule.second_key_func = second_key_func
        rule.flush_func = flush_func
        rule.cache_manager = None

        return rule