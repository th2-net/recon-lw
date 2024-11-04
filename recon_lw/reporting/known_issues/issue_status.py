from enum import Enum


class IssueStatus(Enum):
    DRAFT = ("Draft", 0)
    UNCATEGORIZED = ("Uncategorized", 0)
    UNDER_INVESTIGATION = ("UnderInvestigation", 0)
    FOR_REVIEW = ("ForReview", 1)
    CLOSED = ("Closed", 2)
    APPROVED = ("Approved", 3)
