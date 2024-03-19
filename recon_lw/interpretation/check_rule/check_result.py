from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class FieldCheckResult:
    """
   Data class representing the result of a field check.

   This class holds information about the comparison result of a field between
   two messages.

   Attributes:
       field (str): The name of the field that was checked.
       left_val (Any): The value of the field in the left message.
       right_val (Any): The value of the field in the right message.
       result (Any): The result of the field check.
       check_comment (Optional[str]): An optional comment about the check result.

   """
    field: str
    left_val: Any
    right_val: Any
    result: Any
    check_comment: Optional[str] = None
