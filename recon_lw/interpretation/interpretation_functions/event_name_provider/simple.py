from recon_lw.interpretation.interpretation_functions.event_name_provider import ReconEventNameProvider


class BasicReconEventNameProvider(ReconEventNameProvider):
    def __init__(self, event_name_prefix: str) -> None:
        self.event_name_prefix = event_name_prefix

    def get_miss_original_event_name(self):
        return f"{self.event_name_prefix}[miss_original]"

    def get_miss_copy_event_name(self):
        return f"{self.event_name_prefix}[miss_copy]"

    def get_match_event_name(self):
        return f"{self.event_name_prefix}[match]"

    def get_match_diff_event_name(self):
        return f"{self.event_name_prefix}[match[diff_found]]"
