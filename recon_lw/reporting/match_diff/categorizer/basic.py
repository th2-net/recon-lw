from recon_lw.interpretation.interpretation_functions import ReconType
from recon_lw.reporting.match_diff.categorizer.base import IErrorCategorizer
from recon_lw.reporting.match_diff.categorizer.event_category.base import ErrorCategoryStrategy
from recon_lw.reporting.recon_context.context import ReconContext


class BasicErrorCategoriser(IErrorCategorizer):
    def __init__(
            self,
            error_extractor_strategy: ErrorCategoryStrategy,
            recon_context: ReconContext
    ):
        super().__init__()
        self.error_extractor_strategy = error_extractor_strategy
        self.efr = recon_context.get_efr()
        self.mfr = recon_context.get_mft()


    def process_event(
            self,
            event: dict,
    ):
        e = event
        etype = self.efr.get_type(event)
        status = self.efr.get_status(event)
        recon_name = event["recon_name"]
        body = event["body"]
        body = body if body is not None else {}
        is_match = etype == ReconType.BasicReconMatch.value
        is_diff = body.get('diff') is not None

        if is_match and not is_diff:
            try:
                orig, copy = self.efr.get_attached_messages_ids(event)
            except ValueError:
                # TODO: what to do with multimatches
                return

            if orig and copy:
                category = self.error_extractor_strategy.match_extractor(recon_name, orig, copy, event)
                recon_name = f"{recon_name} | [{category.name}]"

            self._matches_stats.add_match(recon_name)

        if is_match and is_diff:
            try:
                orig, copy = self.efr.get_attached_messages_ids(event)
            except ValueError:
                # TODO: what to do with multimatches
                return

            if orig and copy:
                category = self.error_extractor_strategy.match_diff_extractor(recon_name, orig, copy, event)
                recon_name = f"{recon_name} | [{category.name}]"

            for diff in event['body']['diff']:
                category = self.error_extractor_strategy.diff_category_extractor(recon_name, diff, event)
                if not category:
                    continue

                field = diff["field_name"]
                self._problem_fields.add_problem_field(recon_name, field)
                self._error_stats.add_error_category(recon_name, category)
                self._error_examples.add_error_example(recon_name, category, event['attachedMessageIds'])