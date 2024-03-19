from typing import Optional

from recon_lw.reporting.known_issues.issue_status import IssueStatus
from datetime import datetime

class Issue:
    def __init__(
            self,
            code: str,
            description: str,
            status: IssueStatus,
            status_update_date: str,
            expected_fix_version: Optional[str] = None,
            status_reason: Optional[str]=None,
            is_wip: bool = False,
            **kwargs
    ):
        self.status_update_date = status_update_date
        self.expected_fix_version = expected_fix_version
        self.status_reason = status_reason
        self.code = code
        self.description = description
        self.status = status
        self.is_wip = is_wip

    def _rep(self):
        if self.expected_fix_version is not None:
            expected_fix = f'[Expected fix: {self.expected_fix_version}]'
        else:
            expected_fix = ''

        if self.status_reason is not None:
            status_reason = f'[Status reason: {self.status_reason}]'
        else:
            status_reason = ''

        if self.is_wip:
            res = f"{self.code} {expected_fix} {status_reason} - " \
                f"{self.description}"
        else:
            res = f"{self.code} [{self.status}, {self.status_update_date}]{expected_fix} {status_reason} - " \
                    f"{self.description}"

        if self.status in {IssueStatus.CLOSED, IssueStatus.DRAFT}:
            if self.status == IssueStatus.DRAFT and self.is_wip:
                return res
            return f"! {res}"
        return res

    def __str__(self):
        return self._rep()

    def __repr__(self):
        return self._rep()

    def __add__(self, other):
        return f"{self}{other}"

    def __radd__(self, other):
        return f"{other}{self}"

