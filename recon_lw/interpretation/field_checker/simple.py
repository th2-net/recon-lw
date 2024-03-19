from typing import Iterator, Dict
from recon_lw.interpretation.check_rule import IFieldCheckRule
from recon_lw.interpretation.check_rule import FieldCheckResult
from recon_lw.interpretation.check_rule.base import IFieldCheckRuleProtocol
from recon_lw.interpretation.field_checker.base import FieldChecker


class SimpleFieldChecker(FieldChecker):
    def __init__(self, rules: Dict[str, IFieldCheckRuleProtocol]):
        super().__init__(rules)

    def compare(self, msg1, msg2) -> Iterator[FieldCheckResult]:
        for field, rule in self.rules.items():
            check_rule_result = rule(field, msg1, msg2)

            if check_rule_result.result is False:
                yield check_rule_result
