from abc import ABC, abstractmethod
from typing import Protocol, Optional

from recon_lw.reporting.known_issues import Issue, UNCATEGORIZED_ISSUE


class KnownIssueProtocol(Protocol):
    def __call__(self, event: dict, recon_name: str) -> Issue:
        pass


class KnownIssueHandler(ABC):
    def __call__(self, event: dict, recon_name: str) -> Issue:
        return self.handle(event, recon_name)

    @abstractmethod
    def handle(self, event: dict, recon_name: str) -> Issue:
        pass


class KnownIssues:
    def __init__(self, known_issues: dict[str, list[KnownIssueProtocol]] = {}):
        """
        Args:
            known_issues:
                example:
                    known_issues={
                        "stream1_vs_stream2 | field 'field1' '10' != '100": Issue(
                            code='ISSUE-121',
                            description='Invalid field1 value for mt2 in stream2.',
                            status=IssueStatus.APPROVED,
                            status_update_date='19.03.2024'
                        )
                    }
        """
        self.issues: dict[str, KnownIssueHandler] = known_issues

    def find_known_issue(
            self, category: str, event: dict, recon_name: str
    ) -> Optional[Issue]:
        handlers = self.issues.get(category, [])
        issue = None
        for handler in handlers:
            issue = handler(event, recon_name)
            if issue is not None:
                break
        if issue is None:
            return UNCATEGORIZED_ISSUE
        return issue