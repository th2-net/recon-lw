from typing import Union

from recon_lw.core.basic_recon_event import BasicReconEvent
from recon_lw.interpretation.interpretation_functions import ReconType
from recon_lw.reporting.match_diff.categorizer import ErrorCategoriesStats, \
    MatchesStats, ProblemFields, ErrorExamples
from recon_lw.reporting.match_diff.categorizer.base import IErrorCategorizer
from recon_lw.reporting.match_diff.categorizer.event_category.base import \
    ErrorCategoryStrategy
from recon_lw.reporting.match_diff.categorizer.types.miss_categories_stats import MissCategoriesStats
from recon_lw.reporting.match_diff.categorizer.types.miss_examples import MissExamples
from recon_lw.reporting.recon_context.context import ReconContext


class BasicErrorCategorizer(IErrorCategorizer):
    def __init__(
            self,
            error_extractor_strategy: ErrorCategoryStrategy,
            recon_context: ReconContext,
            error_stats: ErrorCategoriesStats = ErrorCategoriesStats(),
            matches_stats: MatchesStats = MatchesStats(),
            miss_stats: MissCategoriesStats = MissCategoriesStats(),
            problem_fields: ProblemFields = ProblemFields(),
            error_examples: ErrorExamples = ErrorExamples(),
            miss_examples: MissExamples = MissExamples()
    ):
        """Categorizer which categorizes events basing on strategies for
        a different type of events.

        Args:
            error_extractor_strategy: `Strategy class` object that defines set
                of functions that return categories.
            recon_context: ReconContext object.
        """
        super().__init__(
            error_stats=error_stats,
            matches_stats=matches_stats,
            problem_fields=problem_fields,
            error_examples=error_examples,
            miss_examples=miss_examples,
            miss_stats=miss_stats
        )
        self.error_extractor_strategy = error_extractor_strategy
        self.efr = recon_context.get_efr()
        self.mfr = recon_context.get_mfr()

    def _get_attached_msg_ids(self, event):
        try:
            # FIXME:
            #   there is no guarantee that they will in this order.
            orig_msg_id, copy_msg_id = self.efr.get_attached_messages_ids(event)
        except ValueError:
            print(f"Warning: Cannot get attached_messages_ids from event. "
                  f"The number of IDs != 2, "
                  f"attached_messages_ids: {self.efr.get_attached_messages_ids(event)}")
            # TODO: what to do with multimatches
            return None, None

        return orig_msg_id, copy_msg_id

    def _get_attached_msg_ids_all(self, event):
        return self.efr.get_attached_messages_ids(event)

    def process_event(
        self,
        event: Union[BasicReconEvent, dict]
    ):
        # if isinstance(event, dict):
        #     event = BasicReconEvent.from_dict(event)
        # e_type = event.event_type
        # status = event.successful
        # recon_name = event.recon_name
        # body = event.body

        e_type = self.efr.get_type(event)
        # status = self.efr.get_status(event)
        recon_name = event["reconName"]
        body = event["body"]
        event_status = event['successful']

        body = body if body is not None else {}
        is_match = e_type == ReconType.BasicReconMatch.value
        diff = body.get('diff')
        is_diff = diff is not None and len(diff) > 0

        if is_match and not is_diff:
            # FIXME:
            #   there is no guarantee that they will in this order.
            orig_msg_id, copy_msg_id = self._get_attached_msg_ids(event)
            if orig_msg_id is None:
                return  # TODO: what to do with multimatches

            self._matches_stats.add_match(recon_name)
            return

        elif is_match and is_diff:
            # FIXME:
            #   there is no guarantee that they will in this order.
            orig_msg_id, copy_msg_id = self._get_attached_msg_ids(event)
            if orig_msg_id is None:
                return  # TODO: what to do with multimatches

            # FixME:
            #   The following peace of code doesn't make sense.
            #   We don't use `category` after that.
            # if orig_msg_id and copy_msg_id:
            #     category = self.error_extractor_strategy.match_diff_extractor(
            #         recon_name, orig_msg_id, copy_msg_id, event)
            #     recon_name = f"{recon_name} | [{category.name}]"

            # TODO:
            #  event['body']['diff'] -- diff here is actually `diffs` - list of diff

            for diff in event['body']['diff']:
                category = self.error_extractor_strategy.category_extractor.extract_diff_category(
                    recon_name, diff, event)
                if not category:
                    continue

                field = diff["field"]
                self._problem_fields.add_problem_field(recon_name, field)
                self._error_stats.add_error_category(recon_name, category)
                self._error_examples.add_error_example(
                    recon_name, category, event, event['attachedMessageIds'])

        else:
            miss_msgs = self._get_attached_msg_ids_all(event)
            category = self.error_extractor_strategy.category_extractor.extract_miss_category(recon_name, event)

            if category is not None:
                self._miss_stats.add_miss_category(recon_name, category)
                self._miss_examples.add_miss_example(recon_name, category, event, event['attachedMessageIds'])


BasicErrorCategoriser = BasicErrorCategorizer
