from typing import Union

from recon_lw.core.basic_recon_event import BasicReconEvent
from recon_lw.interpretation.interpretation_functions import ReconType
from recon_lw.reporting.match_diff.categorizer import ErrorCategoriesStats, \
    MatchesStats, ProblemFields, ErrorExamples
from recon_lw.reporting.match_diff.categorizer.base import IErrorCategorizer
from recon_lw.reporting.match_diff.categorizer.event_category.base import \
    ErrorCategoryStrategy
from recon_lw.reporting.recon_context.context import ReconContext


class BasicErrorCategorizer(IErrorCategorizer):
    def __init__(
            self,
            error_extractor_strategy: ErrorCategoryStrategy,
            recon_context: ReconContext,
            error_stats: ErrorCategoriesStats = ErrorCategoriesStats(),
            matches_stats: MatchesStats = MatchesStats(),
            problem_fields: ProblemFields = ProblemFields(),
            error_examples: ErrorExamples = ErrorExamples(),
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

        body = body if body is not None else {}
        is_match = e_type == ReconType.BasicReconMatch.value
        is_diff = body.get('diff') is not None

        if is_match and not is_diff:
            # FIXME:
            #   there is no guarantee that they will in this order.
            orig_msg_id, copy_msg_id = self._get_attached_msg_ids(event)
            if orig_msg_id is None:
                return  # TODO: what to do with multimatches

            if orig_msg_id and copy_msg_id:
                category = self.error_extractor_strategy.match_extractor(
                    recon_name, orig_msg_id, copy_msg_id, event)
                recon_name = f"{recon_name} | [{category.name}]"

            self._matches_stats.add_match(recon_name)

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
                category = self.error_extractor_strategy.diff_category_extractor(
                    recon_name, diff, event)
                if not category:
                    continue

                field = diff["field"]
                self._problem_fields.add_problem_field(recon_name, field)
                self._error_stats.add_error_category(recon_name, category)
                self._error_examples.add_error_example(
                    recon_name, category, event['attachedMessageIds'])

        # else:
        #   When NOT match (miss)
        # TODO -- probably it's better to add misses handling here


BasicErrorCategoriser = BasicErrorCategorizer
