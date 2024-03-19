from typing import Dict

from recon_lw.reporting.missing_messages.categorizer.matcher_interface import MissMatcher


class SimpleMatcher(MissMatcher):
    def __init__(self, field: str, **kwargs: Dict):
        self.field = field
        self.conditions = kwargs

    def __call__(self, event):
        values = event['body'][self.field]
        for key, value in self.conditions.items():
            if values.get(key) != value:
                return False
        return True

class SimpleMatcherFlat(MissMatcher):
    def __init__(self, **kwargs: Dict):
        self.conditions = kwargs

    def __call__(self, event):
        values = event['body']
        for key, value in self.conditions.items():
            if values.get(key) != value:
                return False
        return True

class MessageMatcherFlat(MissMatcher):
    def __init__(self, **kwargs: Dict):
        self.conditions = kwargs