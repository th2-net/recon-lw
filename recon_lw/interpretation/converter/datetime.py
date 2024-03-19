from recon_lw.interpretation.converter.base import Converter
import datetime
from recon_lw.core.type.types import Message
from recon_lw.interpretation.adapter.base import Adapter


class DateTimeConverter(Converter):
    def __init__(self, fmt='%Y-%m-%d %H:%M:%S'):
        self.fmt = fmt

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        return datetime.datetime.strptime(val, self.fmt)


class DateConverter(Converter):
    def __init__(self, fmt='%Y-%m-%d'):
        self.fmt = fmt

    def convert(self, message: Message, field: str, val: str, adapter: Adapter):
        return datetime.date.strftime(val, self.fmt)
