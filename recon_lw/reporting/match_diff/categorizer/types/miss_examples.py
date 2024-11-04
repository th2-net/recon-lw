
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Dict

from recon_lw.reporting.match_diff.categorizer.event_category import EventCategory


Event = dict[str, any]


@dataclass
class ExamplesWithEvent:
    message_ids: List[str]
    event: Event


recon_err_example_T = Dict[EventCategory, List[ExamplesWithEvent]]
examples_T = Dict[str, recon_err_example_T]


class MissExamples:
    def __init__(self, category_example_limit: int = 5):
        self._miss_examples: examples_T = defaultdict(lambda: defaultdict(list))
        self.category_example_limit = category_example_limit
        self._error_ids = set()

    def add_miss_example(self,
                         recon_name: str,
                         event_category: EventCategory,
                         event: dict,
                         attached_ids: Optional[List[str]]):
        if attached_ids is not None:
            n = len(self._miss_examples[recon_name][event_category])
            if n < self.category_example_limit:
                self._miss_examples[recon_name][event_category].append(ExamplesWithEvent(attached_ids, event))
                for attached_id in attached_ids:
                    self._error_ids.add(attached_id)

    def is_id_affected(self, message_id):
        return message_id in self._error_ids

    def get_affected_recons(self):
        return self._miss_examples.keys()

    def get_examples(self, recon_name) -> recon_err_example_T:
        """Returns {EventCategory: [ [id1, id2], [id3, id4, id5], ... ]}"""
        return self._miss_examples[recon_name]