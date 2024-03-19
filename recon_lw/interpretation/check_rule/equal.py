from recon_lw.interpretation.check_rule.adapter import IAdapterFieldCheckRule
from recon_lw.interpretation.check_rule.check_result import FieldCheckResult


class EqualFieldCheckRule(IAdapterFieldCheckRule):
    """
    A field check rule that checks if two fields are equal.

    This rule compares the values of a field between two messages and returns
    whether they are equal.

    Methods:
        handler: Method to implement the field check logic.
    """

    def handler(self, field, msg1, msg2) -> FieldCheckResult:
        v1, v2 = self.get_field_values(field, msg1, msg2)

        return FieldCheckResult(
            field=field,
            left_val=v1,
            right_val=v2,
            result=v1 == v2,
            check_comment='Equal comparison'
        )
