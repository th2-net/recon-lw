from abc import abstractmethod, ABC

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.viewer.types.category_table_view import CategoryTableView
from recon_lw.reporting.match_diff.categorizer import ReconErrorStatsContext, \
    EventCategory


class IExampleContentProvider(ABC):
    """
    This class should return messages content that will be shown in the examples.

    E.g.
    -----------------------
    Failed field: A  1 != 2
    ------------------------
    Content         |   Content             {   HERE!
    Example msg 1   |   Example msg 2       {
    """
    @abstractmethod
    def get_example_content(
            self,
            err_ex_msg_ids: list[str],
            context: ReconErrorStatsContext,
            msgs_cache: dict[str, Message],
            category: EventCategory
    ) -> CategoryTableView:
        """

        Args:
            err_ex_msg_ids:
            context:
            msgs_cache:
            category:

        Returns:

        """
