from typing import Dict

from th2_data_services.data import Data
from th2_data_services.interfaces.utils import resolver as i_resolver
from recon_lw.reporting.recon_metadata.base import ReconMetadata
from recon_lw.reporting.utils import get_recon_events


class ReconContext:
    # TODO
    #   This class, probably, can be used not only by reporting, but by the
    #   whole framework.
    def __init__(self,
                 recon_events_directory: str,
                 message_fields_resolver: i_resolver.MessageFieldResolver,
                 event_fields_resolver: i_resolver.EventFieldResolver
                 ):
        """

        Args:
            recon_events_directory: Recon results directory.
                The path to folder with .pickle files.
            message_fields_resolver:
            event_fields_resolver:
        """
        self.recon_events_directory = recon_events_directory
        self._recons_metadata = {}
        self._recon_events: Dict[str, ReconMetadata] = None
        self.mfr = message_fields_resolver
        self.efr = event_fields_resolver

    def get_efr(self):
        return self.efr

    def get_mfr(self):
        return self.mfr

    def get_recon_events(self, update_cache: bool = False) -> Data:
        if self._recon_events and not update_cache:
            return self._recon_events
        self._recon_events = get_recon_events(self.recon_events_directory)
        return self._recon_events

    def update_recon_metadata(self, recon_metadata: ReconMetadata):
        self._recons_metadata[recon_metadata.recon_name] = recon_metadata

    def get_metadata(self) -> Dict[str, ReconMetadata]:
        return self._recons_metadata
