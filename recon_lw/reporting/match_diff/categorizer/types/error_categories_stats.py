from collections import defaultdict
from typing import Dict

import tabulate

from recon_lw.reporting.match_diff.categorizer.event_category.base import \
    EventCategory


class ErrorCategoriesStats:
    def __init__(self, error_categories: Dict[EventCategory, int] = None):
        if not error_categories:
            error_categories = defaultdict(lambda: defaultdict(lambda: 0))
        self.error_categories = error_categories

    def add_error_category(self, recon_name, error_category: EventCategory):
        self.error_categories[recon_name][error_category] += 1

    def _get_sorted_error_categories(self, recon_name):
        return [
            (k, v) for k, v in sorted(
                self.error_categories[recon_name].items(), key=lambda x: x[1],
                reverse=True
            )
        ]

    def get_table_stats(self, recon_name: str):
        return tabulate.tabulate(
            self._get_sorted_error_categories(recon_name),
            headers=['category', 'count'],
            tablefmt='html'
        )
