from typing import Dict, Optional, List

from recon_lw.reporting.known_issues.issue import Issue
from recon_lw.reporting.match_diff.categorizer.event_category.base import IDiffCategoryExtractor, EventCategory, \
    IEventCategoryExtractor


class BasicDiffCategoryExtractor(IDiffCategoryExtractor):
    def __init__(self,
                 known_issues: Optional[Dict[str, Issue]] = None,
                 text_fields_masked_values: Optional[List[str]] = None,
                 list_fields_masked_values: Optional[List[str]] = None,
                 additional_field_aliases=None
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
            known_issues = {}
        if text_fields_masked_values is None:
            text_fields_masked_values = []
        if list_fields_masked_values is None:
            list_fields_masked_values = []
        if additional_field_aliases is None:
            additional_field_aliases = {}
        self.known_issues = known_issues
        self.text_fields_masked_values = set(text_fields_masked_values)
        self.list_fields = set(list_fields_masked_values)
        self.additional_fields_aliases = additional_field_aliases

    def extract_category(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        """
        This handler will be executed only for [match][diff_found] cases.

        Args:
            recon_name:
            diff: dict representation of `ReconEventDiff`
            event:

        Returns:

        """
        expected = diff["expected"]
        actual = diff["actual"]
        field = diff["field"]

        if isinstance(expected, dict):
            # TODO
            #   1. expected['message'] -- we have to describe this format
            cat = f"{recon_name}: {field}: {expected['message']}"
            # TODO - later there will  `known_issues` class that will
            #   have special method to find matched category (not only by the name)
            #   but also by category parameters
            issue = self.known_issues.get(cat)
            if issue:
                cat += f" | {issue}"

            return EventCategory(cat)

        if isinstance(actual, dict):
            raise NotImplementedError

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
        additional_fields_info = event['body'].get('additional_fields_info')
        if additional_fields_info:
            additional_info = " | ".join(
                self._get_additional_info_formatted(key, values)
                for key, values in additional_fields_info.items())

            cat = f"{cat} | {additional_info}"
        issue = self.known_issues.get(cat)
        if issue:
            cat += f" | {issue}"
        return EventCategory(cat)

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


class BasicEventCategoryExtractor(IEventCategoryExtractor):
    def extract_category(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        return EventCategory(recon_name)
