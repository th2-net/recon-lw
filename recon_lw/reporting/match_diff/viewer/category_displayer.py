import json
from itertools import chain
from typing import List, Tuple, Callable, Dict, Optional

from IPython.core.display import HTML, Markdown
from IPython.core.display_functions import display
from tabulate import tabulate
from th2_data_services.data import Data

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.categorizer.types.context import ReconErrorStatsContext
from recon_lw.reporting.match_diff.viewer.color_provider.base import ICategoryColorProvider, \
    ICategoryColorProviderProtocol
from recon_lw.reporting.match_diff.viewer.content_provider.base import IExampleContentProvider
from recon_lw.reporting.match_diff.viewer.style_provider.base import ErrorExamplesStyleProvider, \
    ErrorExamplesStyleProviderProtocol
from recon_lw.reporting.match_diff.viewer.types.types import MatchDiffExampleData
from recon_lw.reporting.match_diff.viewer.utils import get_group_data_map, get_group_from_id, \
    sort_msgs_by_th2_timestamp, get_msgs_by_id
from recon_lw.reporting.recon_context.context import ReconContext

MessageId = str
IdProvider = Callable[[Message], List[Optional[str]]]

class ErrorExampleDisplayer:
    def __init__(
            self,
            category_color_provider: ICategoryColorProviderProtocol,
            error_examples_styles_provider: ErrorExamplesStyleProviderProtocol
    ):
        self.category_color_provider = category_color_provider
        self.error_examples_styles_provider = error_examples_styles_provider
        self._uid = 1

    def apply_styles(self):
        display(HTML(self.error_examples_styles_provider()))

    def display_category(self, category: str, examples: List[Tuple[MatchDiffExampleData, MatchDiffExampleData]]) -> None:
        data = self._get_example_comparison_table(
            category,
            examples,
            self._uid
        )
        self._uid += 1
        display(HTML(data))

    def _get_example_comparison_table(self,
                                      category: str,
                                      examples: List[Tuple[MatchDiffExampleData, MatchDiffExampleData]],
                                      uid: int
    ):
        category_color = self.category_color_provider(category)
        category_style = f"background-color: {category_color}"

        content_header = f'''
        <table border="0" width="100%">
          <tr>
            <td style="text-align: left;{category_style}" colspan=2><b style="font-size: 15px;">{category}</b></td>
          </tr>
        '''

        content_footer = f"""
        </table>
        """

        items = (
            self._get_example_tr(
                self._get_example_td(example[0], f"colapsable-{uid}-{idx}"),
                self._get_example_td(example[1], f"colapsable-{uid}-{-idx}"),
            )
            for idx, example in enumerate(examples, start=1)
        )

        return "\n".join(chain((content_header,), items, (content_footer, )))

    @staticmethod
    def _get_example_td(example_data: MatchDiffExampleData, item_id: str):
        if isinstance(example_data.message_content, list):
            code_mc = ''
            for mc in example_data.message_content:
                code_mc += f'<div><code id="code">{json.dumps(mc, indent=4)}</code></div>'
        else:
            code_mc = f'<code id="code">{json.dumps(example_data.message_content, indent=4)}</code>'

        return f'''
        <td style="text-align: left; vertical-align: top">
          <div class="wrap-collabsible">
            <input id="{item_id}" class="toggle" type="checkbox">
            <label for="{item_id}" class="lbl-toggle">{example_data.message_id}</label>
            <div class="collapsible-content">
              <div class="content-inner">
                {code_mc}
              </div>
            </div>
          </div>
        </td>
        '''

    @staticmethod
    def _get_example_tr(td1, td2):
        return f'''
        <tr>
          {td1}
          {td2}
        </tr>
        '''

class MatchDiffViewer:
    def __init__(
            self,
            recon_stats_context: ReconErrorStatsContext,
            messages: Data,
            data_objects: List[Data],
            message_business_ids_provider: IdProvider,
            message_content_provider: IExampleContentProvider,
            recon_context: ReconContext,
            error_example_displayer: ErrorExampleDisplayer
    ):
        self.context = recon_stats_context
        self.events: List[dict] = recon_context.get_recon_events()
        self.messages = messages
        self.mfr = recon_context.get_mft()
        self.id_provider = message_business_ids_provider
        self.data_objects: List[Data] = data_objects
        self.content_provider: IExampleContentProvider = message_content_provider
        self._cache = None
        self.error_example_displayer = error_example_displayer

    def _get_cache(self) -> Dict[MessageId, Message]:
        if self._cache:
            return self._cache

        self._cache = {}

        for message in self.messages:
            id = self.mfr.get_id(message)
            if self.context.error_examples.is_id_affected(id):
                self._cache[id] = message
        return self._cache

    def display_report(self):
        for recon_name in self.context.error_examples.get_affected_recons():
            display(Markdown(f"### {recon_name}"))
            display(Markdown(f"#### {recon_name} full matches = {self.context.matches_stats.match_number(recon_name)}"))
            display(Markdown(f"#### {recon_name} fields with problems"))
            display(Markdown(f"#### {self.context.problem_fields.get_table(recon_name)}"))
            display(Markdown(f"#### {recon_name} matches with diffs"))

            group_data_map = get_group_data_map(self.data_objects, 'default')
            self.error_example_displayer.apply_styles()
            for category, items in self.context.error_examples.get_examples(recon_name).items():
                examples = []
                for i in items:
                    msg_id0 = i[0]
                    group = get_group_from_id(msg_id0)
                    data_for_group = group_data_map.get(group, group_data_map['default'])

                    msg0 = self._get_cache()[msg_id0]
                    msg_ids = self.id_provider(msg0)

                    sorted_msgs0 = None
                    message_content0 = None
                    if len(msg_ids) > 0:
                        matched_msgs = get_msgs_by_id(
                            data_for_group,
                            ids=msg_ids,
                            id_function=self.id_provider
                        )
                        sorted_msgs0 = sort_msgs_by_th2_timestamp(matched_msgs)
                        message_content0 = self.content_provider.get_example_content(msg_ids, sorted_msgs0)
                    if not sorted_msgs0:
                        message_content0 = self.content_provider.get_example_content(msg_ids, [i[0]])

                    msg_id1 = i[1]
                    group1 = get_group_from_id(msg_id1)
                    data_for_group1 = group_data_map.get(group1, group_data_map['default'])

                    msg1 = self._get_cache()[msg_id1]
                    msg_ids1 = self.id_provider(msg1)

                    sorted_msgs1 = None
                    message_content1 = None
                    if len(msg_ids1) > 0:
                        matched_msgs = get_msgs_by_id(
                            data_for_group1,
                            ids=msg_ids1,
                            id_function=self.id_provider
                        )
                        sorted_msgs1 = sort_msgs_by_th2_timestamp(matched_msgs)
                        message_content1 = self.content_provider.get_example_content(msg_ids1, sorted_msgs1)

                    if not sorted_msgs1:
                        message_content1 = self.content_provider.get_example_content(msg_ids1, [i[1]])

                    examples.append(
                        (
                            MatchDiffExampleData(i[0], message_content0),
                            MatchDiffExampleData(i[1], message_content1)
                        )
                    )

                self.error_example_displayer.display_category(category, list(examples))