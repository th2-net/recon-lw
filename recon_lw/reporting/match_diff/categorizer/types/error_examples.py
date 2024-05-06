from collections import defaultdict
from typing import Optional, List, Dict

from recon_lw.reporting.match_diff.categorizer.event_category import EventCategory


recon_err_example_T = Dict[EventCategory, List[List[str]]]
examples_T = Dict[str, recon_err_example_T]


class ErrorExamples:
    def __init__(self, category_example_limit: int = 5):
        """
        Contains message Ids of error examples.

        Args:
            category_example_limit: The number of collected examples for every
                category is limited by this parameter.
        """
        self._error_examples: examples_T = defaultdict(lambda: defaultdict(list))
        self.category_example_limit = category_example_limit
        self._error_ids = set()

    def add_error_example(self,
                          recon_name: str,
                          error_category: EventCategory,
                          attached_ids: Optional[List[str]]):
        if attached_ids is not None:
            n = len(self._error_examples[recon_name][error_category])
            if n < self.category_example_limit:
                self._error_examples[recon_name][error_category].append(attached_ids)
                for attached_id in attached_ids:
                    self._error_ids.add(attached_id)

    def is_id_affected(self, message_id):
        return message_id in self._error_ids

    def get_affected_recons(self):
        return self._error_examples.keys()

    def get_examples(self, recon_name) -> recon_err_example_T:
        """Returns {EventCategory: [ [id1, id2], [id3, id4, id5], ... ]}"""
        return self._error_examples[recon_name]
