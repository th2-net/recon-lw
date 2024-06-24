import json
from itertools import chain
from typing import List, Dict, Optional
try:
    from IPython.core.display import HTML, Markdown
    from IPython.core.display_functions import display
except ImportError:
    pass
from th2_data_services.data import Data

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.viewer.types.category_table_view import CategoryTableView, \
    MatchDiffExampleData
from recon_lw.reporting.match_diff.categorizer import EventCategory
from recon_lw.reporting.match_diff.categorizer.types.context import ReconErrorStatsContext
from recon_lw.reporting.match_diff.viewer.color_provider.base import \
    ICategoryColorProviderProtocol
from recon_lw.reporting.match_diff.viewer.content_provider.base import IExampleContentProvider
from recon_lw.reporting.match_diff.viewer.style_provider.base import \
    ErrorExamplesStyleProviderProtocol
from recon_lw.reporting.recon_context.context import ReconContext

MessageId = str



class ErrorExampleDisplayer:
    def __init__(
            self,
            category_color_provider: ICategoryColorProviderProtocol,
            error_examples_styles_provider: ErrorExamplesStyleProviderProtocol
    ):
        """Displays error examples collected by error categorizer
        (e.g. BasicErrorCategoriser) with default styles.


        Args:
            category_color_provider:
            error_examples_styles_provider:
        """
        self.category_color_provider = category_color_provider
        self.error_examples_styles_provider = error_examples_styles_provider
        self._uid = 1

    def apply_styles(self):
        """Applies CSS styles for HTML in Jupyter."""
        display(HTML(self.error_examples_styles_provider()))

    def get_styles(self):
        return self.error_examples_styles_provider()

    def display_category(
            self,
            example_content: CategoryTableView
    ) -> None:
        """Draws in HTML something like this:

        -----------------------                 {
        Failed field: A  1 != 2                 {  CATEGORY line
        ------------------------                {
        Content         |   Content             {  Category content
        Example msg 1   |   Example msg 2       {


        Args:
            example_content:

        Returns:

        TODO
            How this can look in the future:
            -----------------------
            field   |   Left    | Right |   ExecType    | Any other category params..
            A       |   1       |   2   |   F           | ...
            ------------------------
            Content         |   Content
            Example msg 1   |   Example msg 2


        """
        data = self._get_example_comparison_table(
            example_content.event_category,
            example_content,
            self._uid
        )
        self._uid += 1
        display(HTML(data))

    def get_category_example_html(self,
                                  example_content: CategoryTableView):
        data = self._get_example_comparison_table(
            example_content.event_category,
            example_content,
            self._uid
        )
        self._uid += 1
        return data

    def _get_example_comparison_table(
            self,
            category: EventCategory,
            table_view: CategoryTableView,
            uid: int
    ):
        """

        Every example object == example column in HTML.

        Args:
            category:
            table_view:
            uid:

        Returns:

        """
        category_color = self.category_color_provider(category)
        category_style = f"background-color: {category_color}"

        content_header = f'''
          <tr>
            <td style="text-align: left;{category_style}" colspan=2><b style="font-size: 15px;">{category}</b></td>
          </tr>
        '''

        content_footer = f"""
        </table>
        """

        items = (
            self._get_example_tr(
                *[
                    self._get_example_td(
                        example_data=match_diff_example_data,
                        item_id=f"collapsable-{uid}-{row_idx}.{cell_idx}"
                    )
                    for cell_idx, match_diff_example_data in
                    enumerate(column_examples_within_row.columns, start=1)
                ]
            )
            for row_idx, column_examples_within_row in
            enumerate(table_view.rows, start=1)
        )

        return "\n".join(chain(
            ('<table border="0" width="100%">',
             content_header,),
            items,
            (content_footer,)))

    @staticmethod
    def _get_example_td(example_data: MatchDiffExampleData, item_id: str):
        """
        <td>: The Table Data Cell element
        Every TD adds 1 column to table.

        Args:
            example_data:
            item_id:

        Returns:

        """
        if isinstance(example_data.message_content, list):
            code_mc = ''
            for mc in example_data.message_content:
                if isinstance(mc, dict):
                    code_mc += f'<div><code id="code">{json.dumps(mc, indent=4)}</code></div>'
                else:
                    code_mc += f'<div><code id="code">{mc}</code></div>'
        elif isinstance(example_data.message_content, dict):
            code_mc = f'<code id="code">{json.dumps(example_data.message_content, indent=4)}</code>'
        else:
            code_mc = f'<code id="code">{example_data.message_content}</code>'

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
    def _get_example_tr(*td_elements: str):
        """Returns table row.

        <th>: The Table Header element
        <tr>: The Table Row element

        Args:
            td_elements: html <td> strings
        """
        elements = "\n".join(td for td in td_elements)

        return f'''
        <tr>
          {elements}
        </tr>
        '''


class MatchDiffViewer:
    def __init__(
            self,
            recon_stats_context: ReconErrorStatsContext,
            messages: Data,
            data_objects: List[Data],
            message_content_provider: IExampleContentProvider,
            recon_context: ReconContext,
            error_example_displayer: ErrorExampleDisplayer
    ):
        # TODO
        #   1. looks strange that we need 2 almost the same objects
        #       - messages
        #       - data_objects
        #     Probably we can pass only one of them. E.g. data_objects
        """

        Args:
            recon_stats_context:
            messages: Data object with all messages that were used during
                reconciliation. It will search messages by IDs from this object.
            data_objects: List of Data objects that were used during
                reconciliation.
            message_content_provider:
                Function or `IExampleContentProvider` class that provides
                CategoryTableView

            recon_context:
            error_example_displayer:
        """
        self.context: ReconErrorStatsContext = recon_stats_context
        self.events: Data[dict] = recon_context.get_recon_events()
        self.messages: Data = messages
        self.mfr = recon_context.get_mfr()

        self.data_objects: List[Data] = data_objects
        self.content_provider: IExampleContentProvider = message_content_provider
        self._cache = None
        self.error_example_displayer: ErrorExampleDisplayer = error_example_displayer

    def _get_cache(self) -> Dict[MessageId, Message]:
        """
        Cache for example error messages.

        FIXME:
            There will memory problems, if we have too many messages.
        TODO -- Ilya did some new cache version.
            1. We need to migrate it here.
            2. Probably we don't need to have this cache if we put the messages
                to Recon result events.
        """
        if self._cache is not None:
            return self._cache

        self._cache = {}

        # FIXME:
        #   The problem here that we use self.mfr.get_id(message)
        #   But we can actually provide events. (any stream, not only messages)

        for message in self.messages:
            msg_id = self.mfr.get_id(message)
            if self.context.error_examples.is_id_affected(msg_id):
                self._cache[msg_id] = message
        return self._cache

    def display_report(self, out_categories_limit: Optional[int] = 5000):
        """

        Args:
            out_categories_limit: If provided, will be shown only this number
                of category examples. Use '-1' to have unlimited number of
                examples.
                - It was limited because, in most of the cases, if you have too
                many examples, you have bad categories.
                - Also, you should recognize that every shown example takes your
                RAM in the browser.

        Returns:

        """
        if out_categories_limit == -1:
            out_categories_limit = 999999999999999999
        categories_shown = 0

        affected_recons = self.context.error_examples.get_affected_recons()

        if not affected_recons:
            print('Warning: there are no any `affected_recons`. \n'
                  'That means that there are 0 element in the `ErrorExamples`. \n'
                  'It can happen because:\n'
                  '\t1. Your events have eventType that not matches with default types.\n'
                  "\t2. Your `ErrorCategoryStrategy.diff_category_extractor` haven't return `EventCategory`.")

        for recon_name in affected_recons:
            display(Markdown(f"### {recon_name}"))
            display(Markdown(f"#### {recon_name} full matches = {self.context.matches_stats.match_number(recon_name)}"))
            display(Markdown(f"#### {recon_name} fields with problems"))
            display(Markdown(f"#### {self.context.problem_fields.get_table(recon_name)}"))
            display(Markdown(f"#### {recon_name} matches with diffs"))

            # group_data_map = get_group_data_map(self.data_objects, 'default')
            self.error_example_displayer.apply_styles()
            for category, err_examples_ids in self.context.error_examples.get_examples(recon_name).items():
                if categories_shown >= out_categories_limit:
                    print("WARNING: out_categories_limit reached. \n"
                          " - in most of the cases, if you have too many examples, you have bad categories.\n"
                          " - Use '-1' to have unlimited number of examples.")
                    return
                categories_shown += 1

                rows = []

                for err_ex_msg_ids in err_examples_ids:
                    # TODO
                    #   1. It's strange that we don't provide the result recon event
                    #       to this function.
                    #   2. Cache should be moved outside, I think.
                    table_view = self.content_provider.get_example_content(
                        err_ex_msg_ids=err_ex_msg_ids,
                        context=self.context,
                        msgs_cache=self._get_cache(),
                        category=category)
                    rows.extend(table_view.rows)

                grouped_table_view = CategoryTableView(
                    rows=rows,
                    event_category=category)

                self.error_example_displayer.display_category(grouped_table_view)

    def as_html(self, out_categories_limit: Optional[int] = 5000, fp=None):
        """

        Args:
            out_categories_limit: If provided, will be shown only this number
                of category examples. Use '-1' to have unlimited number of
                examples.
            fp: file pointer, where html/styles must be saved
        Returns:

        """
        if out_categories_limit == -1:
            out_categories_limit = 999999999999999999
        categories_shown = 0

        affected_recons = self.context.error_examples.get_affected_recons()

        if not affected_recons:
            print('Warning: there are no any `affected_recons`. \n'
                  'That means that there are 0 element in the `ErrorExamples`. \n'
                  'It can happen because:\n'
                  '\t1. Your events have eventType that not matches with default types.\n'
                  "\t2. Your `ErrorCategoryStrategy.diff_category_extractor` haven't return `EventCategory`.")

        styles = self.error_example_displayer.get_styles()

        # fp.write(f'<style type="text/css">\n{styles}</styles>\n')
        fp.write(styles)

        for recon_name in affected_recons:
            self._write_header(3, f"{recon_name}", fp)
            self._write_header(4,
                               f"{recon_name} full matches = {self.context.matches_stats.match_number(recon_name)}",
                               fp)
            self._write_header(4, f"{recon_name} fields with problems", fp)
            self._write_header(4, f"{self.context.problem_fields.get_table(recon_name)}", fp)
            self._write_header(4, f"{recon_name} matches with diffs", fp)

            for category, err_examples_ids in self.context.error_examples.get_examples(recon_name).items():
                if categories_shown >= out_categories_limit:
                    print(f"WARNING: out_categories_limit reached for recon {recon_name}. \n"
                          " - in most of the cases, if you have too many examples, you have bad categories.\n"
                          " - Use '-1' to have unlimited number of examples.")
                    break
                categories_shown += 1

                rows = []

                for err_ex_msg_ids in err_examples_ids:
                    # TODO
                    #   1. It's strange that we don't provide the result recon event
                    #       to this function.
                    #   2. Cache should be moved outside, I think.
                    table_view = self.content_provider.get_example_content(
                        err_ex_msg_ids=err_ex_msg_ids,
                        context=self.context,
                        msgs_cache=self._get_cache(),
                        category=category)
                    rows.extend(table_view.rows)

                grouped_table_view = CategoryTableView(
                    rows=rows,
                    event_category=category)

                fp.write(self.error_example_displayer.get_category_example_html(grouped_table_view))
                fp.write('\n')

    def _write_header(self, header_size: int, text, fp):
        """
        Args:
            header_size (int): is the the size of the header which will ( 1-6 )
        """
        h = f"h{header_size}"
        fp.write(f"<{h}>{text}</{h}>\n")