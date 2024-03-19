from base import Converter
import re
from typing import Pattern, Any

from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class RegexConverter(Converter):
    def __init__(self, regex: Pattern[str]):
        self.regex = re.compile(regex)

    def convert(self, message: Message, field: str, val: Any, adapter: Adapter):
        match = self.regex.match(val)
        return match