from typing import List, Set, Union

from recon_lw.interpretation.filter import Filter, FilterChain, FilterProtocol
from recon_lw.matching.key_functions import BasicCopyKeyFunctionProvider, BasicOriginalKeyFunctionProvider
from recon_lw.matching.matching_key_extractor import BasicSeparatorMatchingKeyExtractor


def default_key_function(filters: List[Union[Filter, FilterProtocol]], key_fields: Set[str], is_copy=False):
    chain = FilterChain()
    for filter in filters:
        chain.add_filter(filter)
    if is_copy:
        return BasicCopyKeyFunctionProvider(
            filter_chain=chain,
            matching_key=BasicSeparatorMatchingKeyExtractor(separator=':'),
            key_fields=key_fields
        )
    else:
        return BasicOriginalKeyFunctionProvider(
            filter_chain=chain,
            matching_key=BasicSeparatorMatchingKeyExtractor(separator=':'),
            key_fields=key_fields
        )