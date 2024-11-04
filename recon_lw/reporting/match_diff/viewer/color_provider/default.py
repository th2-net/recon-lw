from recon_lw.reporting.known_issues import IssueStatus
from recon_lw.reporting.match_diff.categorizer import EventCategory
from recon_lw.reporting.match_diff.viewer.color_provider.base import ICategoryColorProvider


class DefaultCategoryColorProvider(ICategoryColorProvider):
    def get_category_color(self, category: EventCategory) -> str:
        issue = category.issue
        if issue is None:
            return 'red'

        status = issue.status
        is_wip = issue.is_wip

        if status in (IssueStatus.UNDER_INVESTIGATION, IssueStatus.DRAFT):
            return 'purple'

        if status == IssueStatus.CLOSED:
            return 'green'

        if status == IssueStatus.UNCATEGORIZED:
            return 'red'

        if status == IssueStatus.FOR_REVIEW:
            return 'purple'

        if status == IssueStatus.APPROVED:
            return 'green'