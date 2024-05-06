from typing import Callable, Optional

from th2_data_services.data import Data

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.viewer.types.category_table_view import CategoryTableView, \
    MatchDiffExampleData, CategoryTableRow
from recon_lw.reporting.match_diff.categorizer import ReconErrorStatsContext, \
    EventCategory
from recon_lw.reporting.match_diff.viewer.content_provider.base import \
    IExampleContentProvider
from recon_lw.reporting.match_diff.viewer.utils import get_group_data_map, \
    get_group_from_id, get_msgs_by_id, sort_msgs_by_th2_timestamp

IdProvider = Callable[[Message], list[Optional[str]]]


class ErHistoryExampleContentProvider(IExampleContentProvider):
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

    def __init__(self,
                 data_objects: list[Data],
                 message_business_ids_provider: IdProvider, ):
        self.group_data_map = get_group_data_map(data_objects, 'default')
        self.ordid_provider: IdProvider = message_business_ids_provider

    def get_example_content(
            self,
            err_ex_msg_ids: list[str],
            context: ReconErrorStatsContext,
            msgs_cache: dict[str, Message],
            category: EventCategory
    ) -> CategoryTableView:
        """
        Builds history only for the First(Left) stream

        Args:
            err_ex_msg_ids:
            context:
            msgs_cache:
            category:

        Returns:

        """
        msg_id0 = err_ex_msg_ids[0]
        # FIXME:
        #   POSSIBLE BUG -- if the ID doesn't have group!!
        #       e.g. when this id is not Th2-id
        group = get_group_from_id(msg_id0)
        data_for_group = self.group_data_map.get(group,
                                                 self.group_data_map['default'])

        # msg_ord_ids -- like OrdId or ClordId
        msg0 = msgs_cache[msg_id0]
        msg_ord_ids = self.ordid_provider(msg0)

        columns = []

        if len(msg_ord_ids) > 0:
            matched_msgs = get_msgs_by_id(
                data_for_group,
                ids=msg_ord_ids,
                id_function=self.ordid_provider
            )
            sorted_msgs0 = sort_msgs_by_th2_timestamp(matched_msgs)

            columns.append(
                MatchDiffExampleData(msg_id0, str(sorted_msgs0)))

        return CategoryTableView(
            rows=[
                CategoryTableRow(columns)
            ],
            event_category=category
        )
