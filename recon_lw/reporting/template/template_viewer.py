import os
from dataclasses import dataclass
from typing import List, Dict, Optional

from recon_lw.reporting.match_diff.viewer.color_provider.base import ICategoryColorProvider

try:
    import jinja2
    from jinja2 import Template
except ModuleNotFoundError:
    print("Jinja2 is required for report template generation.")

try:
    from IPython.core.display import HTML
    from IPython.core.display_functions import display
except ModuleNotFoundError:
    print("IPython to display report in Jupyter.")

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.categorizer import ReconErrorStatsContext, EventCategory
from recon_lw.reporting.match_diff.viewer.category_displayer import ErrorExampleDisplayer, MessageId
from recon_lw.reporting.match_diff.viewer.color_provider.default import DefaultCategoryColorProvider
from recon_lw.reporting.match_diff.viewer.content_provider.base import IExampleContentProvider
from recon_lw.reporting.match_diff.viewer.types.category_table_view import CategoryTableRow
from recon_lw.reporting.recon_context.context import ReconContext
from th2_data_services.data import Data
from typing import Callable


# Define the categories and their examples

@dataclass
class CategoryExamples:
    title: str
    category: EventCategory
    examples: List[CategoryTableRow]


@dataclass
class ReconInfo:
    recon_name: str
    matches: int
    problems: dict[str, int]
    examples: List[CategoryExamples]
    is_misses: bool = False


class TemplateViewer:
    def __init__(
            self,
            recon_stats_context: ReconErrorStatsContext,
            messages: Data,
            data_objects: List[Data],
            message_content_provider: IExampleContentProvider,
            recon_context: ReconContext,
            template_config_generator_function: Callable[[ReconInfo], dict],
            template: Template,
            report_match_diff: bool = False,
            report_miss: bool = False
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
        """
        self.context: ReconErrorStatsContext = recon_stats_context
        self.events: Data[dict] = recon_context.get_recon_events()
        self.messages: Data = messages
        self.mfr = recon_context.get_mfr()

        self.data_objects: List[Data] = data_objects
        self.content_provider: IExampleContentProvider = message_content_provider
        self._cache = None
        self.template_config_generator_function: Callable[
            [ReconInfo, ReconErrorStatsContext], dict] = template_config_generator_function
        self.template: Template = template
        self.report_match_diff = report_match_diff
        self.report_miss = report_miss

    def _get_cache(self) -> Dict[MessageId, Message]:
        """
        Cache for example error messages.

        """

        if self._cache is not None:
            return self._cache

        self._cache = {}

        for message in self.messages:
            msg_id = self.mfr.get_id(message)
            if self.context.error_examples.is_id_affected(msg_id):
                self._cache[msg_id] = message
            if self.context.misses_examples.is_id_affected(msg_id):
                self._cache[msg_id] = message

        return self._cache

    def _generate_html(self, out_categories_limit: Optional[int] = 5000):
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

        if self.report_match_diff:
            affected_recons = self.context.error_examples.get_affected_recons()
        elif self.report_miss:
            affected_recons = self.context.misses_examples.get_affected_recons()
        else:
            affected_recons = self.context.error_examples.get_affected_recons()

        if not affected_recons:
            print('Warning: there are no any `affected_recons`. \n'
                  'That means that there are 0 element in the `ErrorExamples`. \n'
                  'It can happen because:\n'
                  '\t1. Your events have eventType that not matches with default types.\n'
                  "\t2. Your `ErrorCategoryStrategy.diff_category_extractor` haven't return `EventCategory`.")

        recon_infos = []
        for recon_name in affected_recons:
            categories = []
            if self.report_match_diff:
                recon_examples = self.context.error_examples.get_examples(recon_name)
            elif self.report_miss:
                recon_examples = self.context.misses_examples.get_examples(recon_name)
            else:
                recon_examples = self.context.error_examples.get_examples(recon_name)
            for category, err_examples_ids in recon_examples.items():
                if categories_shown >= out_categories_limit:
                    print("WARNING: out_categories_limit reached. \n"
                          " - in most of the cases, if you have too many examples, you have bad categories.\n"
                          " - Use '-1' to have unlimited number of examples.")
                    break
                categories_shown += 1
                examples = []

                rows = []

                for err_ex_msg_ids in err_examples_ids:
                    # TODO
                    #   1. It's strange that we don't provide the result recon event
                    #       to this function.
                    #   2. Cache should be moved outside, I think.
                    table_view = self.content_provider.get_example_content(
                        err_ex_msg_ids=err_ex_msg_ids.message_ids,
                        context=self.context,
                        msgs_cache=self._get_cache(),
                        category=category,
                        event=err_ex_msg_ids.event
                    )
                    rows.extend(table_view.rows)

                categories.append(
                    CategoryExamples(
                        title=str(category.name),
                        category=category,
                        examples=rows
                    )
                )

            recon_info = ReconInfo(
                recon_name=recon_name,
                matches=self.context.matches_stats.match_number(recon_name),
                problems=self.context.problem_fields._problem_fields.get(recon_name, {}),
                examples=categories,
                is_misses=self.report_miss
            )
            recon_infos.append(recon_info)
        template_cfg = self.template_config_generator_function(recon_infos, self.context)
        rendered_html = self.template.render(**template_cfg)
        return rendered_html

    def display_report(self, out_categories_limit: Optional[int] = 5000):
        html = self._generate_html(out_categories_limit)
        display(HTML(html))

    def as_html(self, out_categories_limit: Optional[int] = 5000, fp=None):
        html = self._generate_html(out_categories_limit)
        fp.write(html)
        fp.write('\n')


def get_default_template_config(color_provider: ICategoryColorProvider = DefaultCategoryColorProvider(),
                                number_of_columns: int = 2):
    def template_config_generator_function(recon_infos: List[ReconInfo], context: ReconErrorStatsContext):
        configs = []
        uuid = 0

        for recon_info in recon_infos:
            categories = []
            for cat_example in recon_info.examples:
                examples = []
                for example in cat_example.examples:
                    for column in example.columns:
                        examples.append({'id': uuid, 'title': column.message_id, 'value': column.message_content})
                        uuid += 1
                categories.append(
                    {
                        'id': uuid,
                        'title': cat_example.title,
                        'color': color_provider.get_category_color(cat_example.category),
                        'examples': examples
                    }
                )
                uuid += 1
            configs.append(
                {
                    'table_data': {
                        'recon_name': recon_info.recon_name,
                        'matches': recon_info.matches,
                        'problems': recon_info.problems,
                    },
                    'categories': categories,
                    'examples_per_row': number_of_columns,
                    'is_misses': recon_info.is_misses
                }
            )

        return {
            'recon_configs': configs,
            'examples_per_row': number_of_columns
        }

    return template_config_generator_function


def get_default_template():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(current_dir, 'template_json_report')
    fs = jinja2.FileSystemLoader(searchpath=template_dir)
    env = jinja2.Environment(loader=fs)
    return env.get_template('default_template.jinja')
