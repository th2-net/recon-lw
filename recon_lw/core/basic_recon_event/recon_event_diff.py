from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class ReconEventDiff:
    field: str

    # TODO
    #   expected -- can be a string or a dict expected['message']
    #   We have to define the format
    expected: Any
    actual: Any

    # TODO - add compare_comment field

    def to_dict(self):
        return asdict(self)

# class ReconEventDiff:
#     def __init__(self, field, expected, actual):
#
#         self.field = field
#         self.expected = expected if expected is not None else not_exists
#         self.actual = actual if actual is not None else not_exists
#
#     def to_dict(self):
#         return {
#             'field': self.field,
#             'expected': self.expected,
#             'actual': self.actual,
#             # TODO - add compare_comment field
#         }