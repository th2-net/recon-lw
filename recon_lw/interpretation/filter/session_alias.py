from recon_lw.interpretation.filter.base import Filter
from typing import Set, Dict, Any
from recon_lw.interpretation.adapter.base import Adapter


class SessionAliasFilter(Filter):

    def __init__(self, whitelisted_aliases: Set[str]):
        self.whitelisted_aliases = whitelisted_aliases

    def filter(self, message: Dict[str, Any], adapter: Adapter) -> bool:
        session_id = adapter.get_root_message_field(message, 'session_id', True)
        return session_id not in self.whitelisted_aliases
