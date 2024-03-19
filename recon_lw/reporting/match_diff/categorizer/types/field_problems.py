from collections import defaultdict

import tabulate


class ProblemFields:
    def __init__(self):
        self._problem_fields = defaultdict(lambda: defaultdict(lambda: 0))

    def add_problem_field(self, recon_name: str, problem_field: str):
        self._problem_fields[recon_name][problem_field] += 1

    def _get_sorted_problem_fields(self, recon_name: str):
        return [
            (k, v)
            for k, v in sorted(
                self._problem_fields[recon_name].items(), key=lambda x: x[1], reverse=True
            )
        ]

    def get_table(self, recon_name: str):
        return tabulate.tabulate(self._get_sorted_problem_fields(recon_name), headers=["field", "count"], tablefmt='html')