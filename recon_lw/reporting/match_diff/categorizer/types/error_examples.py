from collections import defaultdict
from typing import Optional, List

from recon_lw.reporting.match_diff.categorizer.event_category import EventCategory


class ErrorExamples:
    def __init__(self, category_example_limit=5):
        self._error_examples = defaultdict(lambda: defaultdict(list))
        self.category_example_limit = category_example_limit
        self._error_ids = []

    def add_error_example(self, recon_name: str, error_category: EventCategory, attached_ids: Optional[List[str]]):
        if attached_ids is not None:
            n = len(self._error_examples[recon_name][error_category])
            if n < self.category_example_limit:
                self._error_examples[recon_name][error_category].append(attached_ids)
                for attached_id in attached_ids:
                    self._error_ids.append(attached_id)

    def is_id_affected(self, message_id):
        return message_id in self._error_ids

    def get_affected_recons(self):
        return self._error_examples.keys()

    def get_examples(self, recon_name) -> dict:
        return self._error_examples[recon_name]