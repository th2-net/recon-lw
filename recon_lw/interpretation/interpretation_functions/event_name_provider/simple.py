from recon_lw.core.basic_recon_event.basic_recon_event import status_postfix, \
    ReconEventMatchStatus
from recon_lw.interpretation.interpretation_functions.event_name_provider import \
    ReconEventNameProvider


class BasicReconEventNameProvider(ReconEventNameProvider):
    def __init__(self, event_name_prefix: str) -> None:
        self.event_name_prefix = event_name_prefix

    def get_miss_original_event_name(self):
        return f"{self.event_name_prefix}[miss_original]"
        # FIXME:
        #   1. We use different naming in this file and in the ReconEventMatchStatus
        # return f"{self.event_name_prefix}{status_postfix[ReconEventMatchStatus.NO_ORIG]}"

    def get_miss_copy_event_name(self):
        return f"{self.event_name_prefix}[miss_copy]"
        # FIXME:
        #   1. We use different naming in this file and in the ReconEventMatchStatus
        # return f"{self.event_name_prefix}{status_postfix[ReconEventMatchStatus.NO_COPY]}"

    def get_match_event_name(self):
        return f"{self.event_name_prefix}{status_postfix[ReconEventMatchStatus.MATCH]}"

    def get_match_diff_event_name(self):
        return f"{self.event_name_prefix}{status_postfix[ReconEventMatchStatus.MATCH_DIFF_FOUND]}"
