from dataclasses import dataclass, field
from typing import Optional

from recon_lw.reporting.known_issues.issue import Issue
from recon_lw.reporting.missing_messages.categorizer.matcher_interface import MissMatcher


@dataclass
class MissCategorizationRule:
    ticket: Issue
    handler: MissMatcher
    comment: Optional[str] = field(default=None)