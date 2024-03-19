from th2_data_services.utils.category import Category
from th2_data_services.utils.event_utils.totals import get_category_totals2

from recon_lw.reporting.recon_context.context import ReconContext


class EventStatisticsTableReport:

    def __init__(self, recon_context: ReconContext):
        self.recon_context = recon_context
        self.efr = recon_context.get_efr()

    def get_event_type_report_table(self):
        return get_category_totals2(
            self.recon_context.get_recon_events(),
            [
                Category("Event Type", self.efr.get_type),
                Category("Status", self.efr.get_status)
            ]
        ).sort_by(["Event Type", "Status"])

    def get_event_names_report_table(self):
        return get_category_totals2(
            self.recon_context.get_recon_events(),
            [
                Category("Event Name", self.efr.get_name),
                Category("Status", self.efr.get_status)
            ]
        ).sort_by(["Event Name", "Status"])