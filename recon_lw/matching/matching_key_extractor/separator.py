from typing import Any, Dict, Set, List
from recon_lw.matching.matching_key_extractor.base import IMatchingKeyExtractor

from recon_lw.interpretation.adapter.base import Adapter


class BasicSeparatorMatchingKeyExtractor(IMatchingKeyExtractor):

    def __init__(self, separator: str):
        self.separator = separator

    def extract(self, adapter: Adapter, message: Dict[str, Any], fields: Set[str]) -> List[str]:
        def scale_item(val, count):
            if len(val) == count:
                for x in val:
                    yield x
            else:
                v = val[0]
                for _ in range(count):
                    yield v

        items = {}
        max_count = 1
        for field in fields:
            val = adapter.get(message, field, strict=True)
            if not isinstance(val, list):
                val = [val]
            items[field] = val
            l = len(val)
            if l != 1 and max_count != 1 and l != max_count:
                raise SystemError(
                    f"Diff found {max_count} != {l} | {adapter.__class__.__name__} "
                    f"| {field} | {message}"
                )

            max_count = max(max_count, len(val))

        z = list(
            zip(
                *(scale_item([str(x) for x in items[field]], max_count) for field in
                  fields)
            )
        )

        result = [self.separator.join(chunks) for chunks in z]
        return result
