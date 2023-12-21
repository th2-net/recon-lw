from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Tuple, Optional, Dict

from template.adapters.base_adapter import IBaseAdapter


@dataclass
class FieldCheckResult:
    field: str
    left_val: Any
    right_val: Any
    result: Any
    check_comment: Optional[str] = None


class IFieldCheckRule(ABC):
    def __call__(self, field, msg1, msg2):
        return self.handler(field, msg1, msg2)

    @abstractmethod
    def handler(self, field, msg1, msg2) -> FieldCheckResult:
        pass


class IAdapterFieldCheckRule(IFieldCheckRule, ABC):
    def __init__(self, stream1_adapter: IBaseAdapter,
                 stream2_adapter: IBaseAdapter):
        self.stream1_adapter = stream1_adapter
        self.stream2_adapter = stream2_adapter

    def get_field_values(self, field, msg1, msg2):
        v1 = self.stream1_adapter.get(msg1, field)
        v2 = self.stream2_adapter.get(msg2, field)
        return v1, v2


class EqualFieldCheckRule(IAdapterFieldCheckRule):
    def handler(self, field, msg1, msg2) -> FieldCheckResult:
        v1, v2 = self.get_field_values(field, msg1, msg2)

        return FieldCheckResult(
            field=field,
            left_val=v1,
            right_val=v2,
            result=v1 == v2,
            check_comment='Equal comparison'
        )


@dataclass
class FieldToCheck:
    field: str
    check_rule: IFieldCheckRule


def get_simple_fields_checker(
        fields_to_check: Dict[str, FieldToCheck]
) -> Callable[[dict, dict], Iterator[FieldCheckResult]]:
    def simple_fields_checker(msg1, msg2):
        for field, ftc in fields_to_check.items():
            check_rule_result = ftc.check_rule(field, msg1, msg2)

            if check_rule_result.result is False:
                yield check_rule_result

    return simple_fields_checker
