from typing import Dict, Tuple

import tabulate
from IPython.core.display_functions import display

Count = int
MissCategory = Tuple

class MissingMessagesDisplayer:
    def __init__(self,
         missed_messages_categories: Dict[MissCategory, Count]
    ):
        self.classes = missed_messages_categories

    def display(self, missed_messages):

        classes = self.classes

        simple_misses_table = []
        for miss_category, miss_counter in classes.items():
            if miss_category[1:] != (None, None):
                error_kind, miss_issue, miss_commentary = miss_category
                simple_misses_table.append((error_kind, miss_issue, miss_commentary, miss_counter))
            else:
                simple_misses_table.append((miss_category[0], "UNCATEGORIZED", '', miss_counter))
        simple_misses_table.append(('total', '', '', sum(classes.values())))


        display(
            tabulate.tabulate(
                simple_misses_table,
                headers=['recon', 'miss_issue', 'miss_commentary', 'count'],
                tablefmt='html'
            )
        )


