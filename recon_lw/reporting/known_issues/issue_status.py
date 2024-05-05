from enum import Enum


class IssueStatus(Enum):
    FOR_REVIEW = "ForReview"
    CLOSED = "Closed"
    APPROVED = "Approved"
    DRAFT = "Draft"
