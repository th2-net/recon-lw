from abc import ABC, abstractmethod
from typing import Protocol

from recon_lw.interpretation.check_rule.check_result import FieldCheckResult
from dataclasses import dataclass


class IFieldCheckRule(ABC):
    """
    Abstract base class representing a field check rule.

    This class defines the interface for field check rules, which are used to
    compare fields between two messages.
    """

    def __call__(self, field, msg1, msg2):
        return self.handler(field, msg1, msg2)

    @abstractmethod
    def handler(self, field, msg1, msg2) -> FieldCheckResult:
        pass

class IFieldCheckRuleProtocol(Protocol):
    def __call__(self, field, msg1, msg2):
        pass


@dataclass
class FieldToCheck:
    """
    Data class representing a field to check.

    This class holds information about a field that needs to be checked between
    two messages.

    Attributes:
       field (str): The name of the field to check.
       field_checker (IFieldCheckRule): The field check rule associated with the field.
       field_description (str): A description of the field (optional).

    """
    field: str
    field_checker: IFieldCheckRule
    field_description: str = ''