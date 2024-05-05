from recon_lw.matching.key_functions.base import KeyFunctionProvider
from recon_lw.interpretation.filter import FilterChain
from recon_lw.matching.matching_key_extractor.base import IMatchingKeyExtractor, \
    MatchingKeyExtractorProtocol
from typing import Set, List
from recon_lw.interpretation.adapter.base import Adapter
from recon_lw.core.type.types import KeyFunctionType, Message


class BasicCopyKeyFunctionProvider(KeyFunctionProvider):

    def __init__(self, filter_chain: FilterChain,
                 matching_key: MatchingKeyExtractorProtocol,
                 key_fields: Set[str]):
        super().__init__()
        self._filter_chain = filter_chain
        self._matching_key = matching_key
        self._key_fields = key_fields

    def provide(self, adapter: Adapter) -> KeyFunctionType:
        def key_function(message: Message):
            if not self._filter_chain.filter(message, adapter):
                adapter.on_message(message)
                mks = self._matching_key(adapter, message, self._key_fields)
                if len(mks) > 1:
                    raise SystemError(
                        f"Copy matching fun can have only single value, received {mks}"
                    )
                return mks[0]

        return key_function
