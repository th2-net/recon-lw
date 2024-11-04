from typing import Callable

import json
import re
from collections import OrderedDict
from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.categorizer import ReconErrorStatsContext, EventCategory
from recon_lw.reporting.match_diff.viewer.content_provider.base import IExampleContentProvider
from recon_lw.reporting.match_diff.viewer.types.category_table_view import CategoryTableView, MatchDiffExampleData, \
    CategoryTableRow


class HighlightingContentProvider(IExampleContentProvider):
    def __init__(self,
        adapter_name_to_paths: dict[str, list[str]],
        highlight_color: str = 'red',
        get_body_field: Callable[[dict], dict]=lambda x: x['body']['fields']
    ):
        """
        Initialize the content provider with regex-enabled path patterns.

        Args:
            adapter_name_to_paths: Dictionary mapping adapter names to lists of path patterns.
                                 Patterns can include regular expressions.
        """
        self.adapter_name_to_paths = {
            name: [re.compile(self._convert_to_regex(pattern)) for pattern in patterns]
            for name, patterns in adapter_name_to_paths.items()
        }

        self.highlight_color = highlight_color
        self.get_body_field = get_body_field

    def _convert_to_regex(self, pattern: str) -> str:
        """
        Converts a path pattern to a regex pattern.

        Args:
            pattern: Path pattern that may contain regex special characters

        Returns:
            String representation of the regex pattern
        """
        if pattern.startswith('^') and pattern.endswith('$'):
            return pattern
        escaped = re.escape(pattern)
        return f"^{escaped}$"

    def _should_highlight(self, path: str, patterns: list[re.Pattern]) -> bool:
        """
        Check if a path should be highlighted based on patterns.

        Args:
            path: Field path to check
            patterns: List of compiled regex patterns

        Returns:
            Boolean indicating if the path should be highlighted
        """
        return any(pattern.match(path) for pattern in patterns)

    def _highlight_field(self, field_path: str, value: str) -> tuple[str, str]:
        """
        Creates a highlighted version of the field path and value.

        Args:
            field_path: The full path to the field
            value: The field value

        Returns:
            Tuple of (highlighted_path, highlighted_value)
        """
        return f"<strong class=\"highlight-red\">{field_path}</strong>", f"<strong class=\"highlight-red\">{value}</strong>"

    def _get_flattened_paths(self, obj: dict, prefix: str = '') -> list[str]:
        """
        Get all paths in the original object in order.

        Args:
            obj: Dictionary to process
            prefix: Current path prefix

        Returns:
            List of paths in original order
        """
        paths = []

        def _recurse(curr_obj, curr_prefix=''):
            if isinstance(curr_obj, dict):
                for key, value in curr_obj.items():
                    path = f"{curr_prefix}{key}" if curr_prefix else key
                    if isinstance(value, (dict, list)):
                        _recurse(value, f"{path}.")
                    else:
                        paths.append(path)
            elif isinstance(curr_obj, list):
                for i, item in enumerate(curr_obj):
                    path = f"{curr_prefix}{i}"
                    if isinstance(item, (dict, list)):
                        _recurse(item, f"{path}.")
                    else:
                        paths.append(path)

        _recurse(obj)
        return paths

    def _reconstruct_message(self, original: dict, flat_dict: dict, patterns: list[re.Pattern]) -> OrderedDict:
        """
        Reconstructs a message with highlighted fields, preserving original order.

        Args:
            original: Original message structure
            flat_dict: Flattened dictionary of fields
            patterns: List of compiled regex patterns to match against

        Returns:
            OrderedDict with fields in original order, with highlighting applied
        """
        original_paths = self._get_flattened_paths(original)

        result = OrderedDict()

        for path in original_paths:
            if path in flat_dict:
                if self._should_highlight(path, patterns):
                    highlighted_path, highlighted_value = self._highlight_field(
                        path,
                        str(flat_dict[path])
                    )
                    result[highlighted_path] = highlighted_value
                else:
                    result[path] = flat_dict[path]

        return result

    def _pretty_print_json(self, data: dict) -> str:
        """
        Converts dictionary to a pretty-printed JSON string while preserving HTML tags.

        Args:
            data: Dictionary to convert

        Returns:
            Pretty-printed JSON string with preserved HTML formatting
        """

        class HTMLPreservingJSONEncoder(json.JSONEncoder):
            def encode(self, obj):
                if isinstance(obj, str) and ('<strong>' in obj or '</strong>' in obj):
                    return f'"{obj}"'
                return super().encode(obj)

        json_str = json.dumps(data, indent=2, cls=HTMLPreservingJSONEncoder)
        return json_str.replace('\\"', '"')

    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for key, value in d.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                items.extend(self.flatten_dict(value, new_key, sep).items())
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}{sep}{i}", sep).items())
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
            else:
                items.append((new_key, value))

        return dict(items)

    def get_example_content(
            self,
            err_ex_msg_ids: list[str],
            context: ReconErrorStatsContext,
            msgs_cache: dict[str, Message],
            event: dict[str, Message],
            category: EventCategory
    ) -> CategoryTableView:
        columns = []
        patterns = self.adapter_name_to_paths.get(category.field, [])

        for msg_id in err_ex_msg_ids:
            msg = msgs_cache[msg_id]
            flat = self.flatten_dict(self.get_body_field(msg), sep='.')

            highlighted_msg = self._reconstruct_message(self.get_body_field(msg), flat, patterns)
            pretty_json = self._pretty_print_json(highlighted_msg)
            columns.append(MatchDiffExampleData(msg_id, pretty_json))

        return CategoryTableView(
            rows=[CategoryTableRow(columns)],
            event_category=category
        )