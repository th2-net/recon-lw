from dataclasses import dataclass

from recon_lw.reporting.match_diff.categorizer import EventCategory


@dataclass
class MatchDiffExampleData:
    message_id: str
    message_content: str


@dataclass
class CategoryTableRow:
    columns: list[MatchDiffExampleData]


class CategoryTableView:

    def __init__(
            self,
            rows: list[CategoryTableRow],
            event_category: EventCategory,
            header=None
    ):
        """
        This class used to draw error-example for category.

        Note:
            It's usually expected that every ContentRow within a single
            `EventCategoryTableView` object has an equal number of columns.
        """
        self.event_category = event_category

        self.rows: list[CategoryTableRow] = rows

    # TODO
    #   1. We will want to configure header somehow in the future.
    # def default_header(self):
    #     return f'''
    #       <tr>
    #         <td style="text-align: left;{category_style}" colspan=2><b style="font-size: 15px;">{category}</b></td>
    #       </tr>
    #     '''
