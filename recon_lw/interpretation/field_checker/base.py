from abc import ABC, abstractmethod
from dataclasses import Field
from typing import Dict, Iterator, Protocol
from recon_lw.interpretation.check_rule import IFieldCheckRule
from recon_lw.interpretation.check_rule.base import FieldCheckResult, IFieldCheckRuleProtocol


class FieldChecker(ABC):

    def __init__(self, rules: Dict[str, IFieldCheckRuleProtocol]):
        self.rules = rules

    def __call__(self, msg1, msg2):
        return self.compare(msg1, msg2)

    @abstractmethod
    def compare(self, msg1, msg2) -> Iterator[FieldCheckResult]:
        pass

class FieldCheckerProtocol(Protocol):
    def __call__(self, msg1, msg2):
        pass
