from typing import Dict

from recon_lw.reporting.known_issues.issue import Issue
from recon_lw.reporting.match_diff.categorizer.event_category.base import IDiffCategoryExtractor, EventCategory, \
    IEventCategoryExtractor


class BasicDiffCategoryExtractor(IDiffCategoryExtractor):
    def __init__(self,
                 known_issues: Dict[str, Issue],
                 text_fields_masked_values=None,
                 list_fields_masked_values=None,
                 additional_field_aliases=None
    ):
        if known_issues is None:
            known_issues = []
        if text_fields_masked_values is None:
            text_fields_masked_values = []
        if list_fields_masked_values is None:
            list_fields_masked_values = []
        if additional_field_aliases is None:
            additional_field_aliases = {}
        self.known_issues = known_issues
        self.text_fields_masked_values = text_fields_masked_values
        self.list_fields = list_fields_masked_values
        self.additional_fields_aliases = additional_field_aliases

    def extract_category(self, recon_name: str, diff: dict, event: dict) -> EventCategory:
        expected = diff["expected"]
        actual = diff["actual"]

        field = diff["field_name"]
        if isinstance(expected, dict):
            cat = f"{recon_name}: {field}: {expected['message']}"
            issue = self.known_issues.get(cat)

            if issue:
                cat += f" | {issue}"

            return EventCategory(cat)

        if isinstance(actual, dict):
            return None

        expected = self._primify(expected)
        actual = self._primify(actual)

        field = diff["field_name"]

        if field in self.text_fields_masked_values:

            if expected not in ("__NOT_EXISTS__", "''") and not isinstance(expected, bool):
                expected = "TEXT VALUE"

            if actual not in ("__NOT_EXISTS__", "''") and not isinstance(actual, bool):
                actual = "TEXT_VALUE"

        elif field in self.list_fields:
            expected = "LIST VALUE"
            actual = "LIST VALUE"

        cat = f"{recon_name}: field {field} {expected} != {actual}"
        additional_fields_info = event['body'].get('additional_fields_info')
        if additional_fields_info:
            additional_info = " | ".join(self._get_additional_info_formatted(key, values) for key, values in additional_fields_info.items())

            cat = f"{cat} | {additional_info}"
        issue = self.known_issues.get(cat)
        if issue:
            cat += f" | {issue}"
        return EventCategory(cat)


    def _get_additional_info_formatted(self, key, values):
        alias = self.additional_fields_aliases.get(key)
        if alias:
            key = alias

        if values[0] == values[1]:
            return f"{key}='{values[0]}'"
        else:
            return f"{key}='{values[0]}'!='{values[1]}"

    def _primify(self, str):
        return f"'{str}'"

class BasicEventCategoryExtractor(IEventCategoryExtractor):
    def extract_category(self, recon_name: str, orig, copy, event: dict) -> EventCategory:
        return EventCategory(recon_name)