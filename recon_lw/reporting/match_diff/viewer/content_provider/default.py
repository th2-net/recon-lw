import pprint

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.viewer.types.category_table_view import CategoryTableView, \
    CategoryTableRow, MatchDiffExampleData
from recon_lw.reporting.match_diff.categorizer import EventCategory, \
    ReconErrorStatsContext
from recon_lw.reporting.match_diff.viewer.content_provider.base import IExampleContentProvider


class FullMessageExampleContentProvider(IExampleContentProvider):
    """
    This class returns messages content (body.fields) that will be shown
    in the examples.

    E.g.
    -----------------------
    Failed field: A  1 != 2
    ------------------------
    Content         |   Content             {   HERE!
    Example msg 1   |   Example msg 2       {
    """
    def get_example_content(
            self,
            err_ex_msg_ids: list[str],
            context: ReconErrorStatsContext,
            msgs_cache: dict[str, Message],
            category: EventCategory
    ) -> CategoryTableView:

        columns = []
        for msg_id in err_ex_msg_ids:
            msg = msgs_cache[msg_id]
            columns.append(MatchDiffExampleData(msg_id, pprint.pformat(msg)))

        return CategoryTableView(
            rows=[
                CategoryTableRow(columns)
            ],
            event_category=category
        )


DefaultExampleContentProvider = FullMessageExampleContentProvider
