from collections import defaultdict
from io import FileIO

from recon_lw.interpretation.interpretation_functions import ReconType
from recon_lw.reporting.missing_messages.categorizer.matcher_interface import MissCategorizer
from recon_lw.reporting.recon_context.context import ReconContext


class MissedMessageHandler:
    def __init__(self,
                 recon_context: ReconContext,
                 miss_categoriser: MissCategorizer):
        self.recon_context = recon_context
        self.efr = recon_context.get_efr()
        self.mfr = recon_context.get_mfr()
        self.miss_categoriser = miss_categoriser

    def write_to_file(self, file: FileIO):
        for e in self.recon_context.get_recon_events():
            if self.efr.get_status(e):
                continue
            type = self.efr.get_type(e)
            recon_name = e['reconName']
            attached = e["attachedMessageIds"]
            if type == ReconType.BasicReconMissLeft.value:
                print("\t\t NO_ORIG", recon_name, attached, e['body']['key'], file)
            elif type == ReconType.BasicReconMissRight.value:
                print("\t\t NO_COPY", recon_name, attached, e['body']['key'], file)

    def categorise_and_filter(self, messages):
        missed_message_ids = {}
        error_categories = defaultdict(int)
        for e in self.recon_context.get_recon_events():
            if self.efr.get_status(e):
                continue
            type = self.efr.get_type(e)
            recon_name = e['recon_name']
            attached = e["attachedMessageIds"]
            if type == ReconType.BasicReconMissLeft.value:
                error_kind = f"no_orig {recon_name}"
            elif type == ReconType.BasicReconMissRight.value:
                error_kind = f"no_copy {recon_name}"
            else:
                error_kind = None
            if error_kind:
                missed_message_ids[attached[0]] = error_kind
                error_categories[(error_kind,) + self.miss_categoriser(error_kind, e)] += 1
        return messages.filter(lambda m: m['messageId'] in missed_message_ids), error_categories

