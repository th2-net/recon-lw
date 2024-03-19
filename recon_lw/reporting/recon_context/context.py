from typing import Dict

from th2_data_services.data_source.lwdp.resolver import MessageFieldResolver, EventFieldResolver

from recon_lw.interpretation.interpretation_functions import ReconType
from recon_lw.reporting.recon_metadata.base import ReconMetadata
from recon_lw.reporting.utils import get_recon_events


class ReconContext:
    def __init__(self,
                 recon_events_directory: str,
                 message_fields_resolver: MessageFieldResolver,
                 event_fields_resolver: EventFieldResolver
    ):
        self.recon_events_directory = recon_events_directory
        self._recons_metadata = {}
        self._recon_events: Dict[str, ReconMetadata] = None
        self.mft = message_fields_resolver
        self.eft = event_fields_resolver

    def get_efr(self):
        return self.eft

    def get_mft(self):
        return self.mft

    def get_recon_events(self, update_cache: bool = False):
        if self._recon_events and not update_cache:
            return self._recon_events
        self._recon_events = get_recon_events(self.recon_events_directory)
        return self._recon_events


    def update_recon_metadata(self, recon_metadata: ReconMetadata):
        self._recons_metadata[recon_metadata.recon_name] = recon_metadata

    def get_metadata(self)-> Dict[str, ReconMetadata]:
        return self._recons_metadata