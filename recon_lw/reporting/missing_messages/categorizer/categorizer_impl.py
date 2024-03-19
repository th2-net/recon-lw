from typing import List, Dict

from recon_lw.reporting.missing_messages.categorizer.matcher_interface import MissCategorizer
from recon_lw.reporting.missing_messages.categorizer.rule import MissCategorizationRule


class SimpleMissesCategorizer(MissCategorizer):
    def __init__(self, rules: Dict[str, List[MissCategorizationRule]]):
        self.rules = rules

    def __call__(self, recon_error, miss_event):
        rules_list = self.rules.get(recon_error, [])

        for rule in rules_list:
            if rule.handler(miss_event):
                return rule.ticket, rule.comment
        return None, None