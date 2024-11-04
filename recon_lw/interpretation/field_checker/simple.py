from typing import Iterator, Dict
from recon_lw.interpretation.check_rule import IFieldCheckRule
from recon_lw.interpretation.check_rule import FieldCheckResult
from recon_lw.interpretation.check_rule.base import IFieldCheckRuleProtocol
from recon_lw.interpretation.field_checker.base import FieldChecker


class SimpleFieldChecker(FieldChecker):
    def __init__(self, rules: Dict[str, IFieldCheckRuleProtocol], publish_matches: bool = False):
        super().__init__(rules)
        self.publish_matches = publish_matches

    def compare(self, msg1, msg2) -> Iterator[FieldCheckResult]:
        for field, rule in self.rules.items():
            check_rule_result = rule(field, msg1, msg2)

            if check_rule_result.result is False or self.publish_matches:
                yield check_rule_result
