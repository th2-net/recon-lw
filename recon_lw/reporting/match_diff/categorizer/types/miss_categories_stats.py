from collections import defaultdict
from typing import Dict

import tabulate

from recon_lw.reporting.match_diff.categorizer.event_category import EventCategory


class MissCategoriesStats:
    def __init__(self, miss_categories: Dict[EventCategory, int] = None):
        if not miss_categories:
            miss_categories = defaultdict(lambda: defaultdict(lambda: 0))
        self.miss_categories = miss_categories

    def add_miss_category(self, recon_name, miss_category: EventCategory):
        if miss_category is None:
            return None
        self.miss_categories[recon_name][miss_category] += 1

    def _get_sorted_error_categories(self, recon_name):
        return [
            (k.name, v) for k, v in sorted(
                self.miss_categories[recon_name].items(), key=lambda x: x[1],
                reverse=True
            )
        ]

    def get_recon_names(self):
        return self.miss_categories.keys()

    def get_table_stats(self, recon_name: str):
        return tabulate.tabulate(
            self._get_sorted_error_categories(recon_name),
            headers=['category', 'count'],
            tablefmt='html'
        )