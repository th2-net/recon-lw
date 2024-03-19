from typing import List, Any

from recon_lw.core.type.types import Message
from recon_lw.reporting.match_diff.viewer.content_provider.base import IExampleContentProvider


class DefaultExampleContentProvider(IExampleContentProvider):
    def get_example_content(self, ids: List[str], messages: List[Message]) -> List[Any]:
        body = messages[0].get('body')
        if isinstance(body, list):
            body = body[0]
        return [body.get('fields')]
