from typing import Dict, Optional, List

from recon_lw.reporting.known_issues.known_issues import KnownIssues
from recon_lw.reporting.match_diff.categorizer.event_category.base import EventCategory, \
    ICategoryExtractor


class BasicCategoryExtractor(ICategoryExtractor):
    def __init__(self,
                 known_issues: Optional[KnownIssues] = None,
                 text_fields_masked_values: Optional[List[str]] = None,
                 list_fields_masked_values: Optional[List[str]] = None,
                 additional_field_aliases=None,
                 known_issue_text_fields_masked_values: Optional[List[str]] = None
                 ):
        # TODO
        #   Slava Ermakov
        #       known_issues -- it's better to have separate class for it
        #           I have a prototype from one of the projects.
        #
        """Default implementation of `DiffCategoryExtractor`.

        This handler will be executed only for [match][diff_found] cases.

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
            text_fields_masked_values: List of field names to mask.
                Add your field name here if you want to change field values
                to "TEXT VALUE" in the category.
            list_fields_masked_values: List of field names to mask.
                Add your field name here if you want to change field values
                to "LIST VALUE" in the category.
            additional_field_aliases:
        """
        if known_issues is None:
            known_issues = KnownIssues()
        if text_fields_masked_values is None:
            text_fields_masked_values = []
        if list_fields_masked_values is None:
            list_fields_masked_values = []
        if additional_field_aliases is None:
            additional_field_aliases = {}
        if known_issue_text_fields_masked_values is None:
            known_issue_text_fields_masked_values = []
        self.known_issues = known_issues
        self.text_fields_masked_values = set(text_fields_masked_values)
        self.list_fields = set(list_fields_masked_values)
        self.additional_fields_aliases = additional_field_aliases
        self.known_issues_text_fields_masked_values = known_issue_text_fields_masked_values

    def extract_diff_category(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        """
        This handler will be executed only for [match][diff_found] cases.

        Args:
            recon_name:
            diff: dict representation of `ReconEventDiff`
            event:

        Returns: EventCategory

        """
        event_status = event['successful']
        if event_status:
            return None
        expected = diff["expected"]
        actual = diff["actual"]
        field = diff["field"]

        expected = self._primify(expected)
        actual = self._primify(actual)

        expected = self._transform_ne(expected)
        actual = self._transform_ne(actual)

        if field in self.text_fields_masked_values:
            expected = self._apply_masked_value(expected)
            actual = self._apply_masked_value(actual)

        elif field in self.list_fields:
            expected = "LIST VALUE"
            actual = "LIST VALUE"

        cat = f"{recon_name}: field {field} {expected} != {actual}"
        if field in self.known_issues_text_fields_masked_values:
            expected_masked = self._apply_masked_value(expected)
            actual_masked = self._apply_masked_value(actual)
            known_issue_cat = f"{recon_name}: field {field} {expected_masked} != {actual_masked}"
        else:
            known_issue_cat = cat

        additional_fields_info = event['body'].get('additional_fields_info')
        if additional_fields_info:
            additional_info = " | ".join(
                self._get_additional_info_formatted(key, values)
                for key, values in additional_fields_info.items())

            cat = f"{cat} | {additional_info}"
            known_issue_cat = f"{known_issue_cat} | {additional_info}"

        issue = self.known_issues.find_known_issue(known_issue_cat, event, recon_name)
        cat += f" | {issue}"
        return EventCategory(cat, issue, field)

    def extract_miss_category(self, recon_name: str, event: dict) -> EventCategory:
        """
        This handler will be executed only for [miss_orig] and [miss_copy] cases.

        Args:
            recon_name:
            diff: dict representation of `ReconEventDiff`
            event:

        Returns: EventCategory

        """
        event_status = event['successful']
        if event_status:
            return None
        event_name = event['eventName']
        recon_name = event['reconName']

        cat = f"{recon_name}: {event_name}"
        issue = self.known_issues.find_known_issue(cat, event, recon_name)
        cat += f" | {issue}"
        return EventCategory(cat, issue)

    def _transform_ne(self, val):
        if val == "'_NE_'":
            return "__NOT_EXISTS__"
        else:
            return val

    def _apply_masked_value(self, val):
        if val not in {"__NOT_EXISTS__", "''"} and not isinstance(val, bool):
            return "TEXT VALUE"
        else:
            return val

    def _get_additional_info_formatted(self, key, values):
        alias = self.additional_fields_aliases.get(key)
        if alias:
            key = alias

        if values[0] == values[1]:
            return f"{key}='{values[0]}'"
        else:
            return f"{key}='{values[0]}'!='{values[1]}"

    def _primify(self, str) -> str:
        return f"'{str}'"


class BasicEventCategoryExtractor(ICategoryExtractor):
    def extract_diff_category(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        return EventCategory(recon_name)

    def extract_miss_category(self, recon_name: str, event: dict) -> EventCategory:
        return EventCategory(recon_name)