from base import Filter
from typing import Set

from recon_lw.interpretation.adapter import Adapter
from recon_lw.core.type.types import Message


class AmendRejectFilter(Filter):
    def __init__(self,
                 message_types=None,
                 rej_code_field_name='reject_code',
                 rej_text_field_name='reject_text',
                 invalid_rej_codes: Set[str]=None,
                 invalid_reason_codes: Set[str]=None
                 ):

        if invalid_rej_codes is None:
            invalid_rej_codes = { '0', '1' }
        self.invalid_rej_codes = invalid_rej_codes

        if message_types is None:
            message_types = {'OrderCancelReject'}
        self.message_types = message_types

        if invalid_reason_codes is None:
            invalid_reason_codes = {'1000'}

        self.invalid_reason_codes = invalid_reason_codes

        self.rej_code_field_name = rej_code_field_name
        self.rej_text_field_name = rej_text_field_name

    def filter(self, message: Message, adapter: Adapter) -> bool:
        mt = adapter.get_message_type(message)
        if mt in self.message_types:
            rej_code = adapter.get(message, self.rej_code_field_name)

            if rej_code in self.invalid_rej_codes:
                return False

        rej_text = adapter.get(message, self.rej_text_field_name)

        for invalid_reason_code in self.invalid_reason_codes:
            if invalid_reason_code in rej_text:
                return False

        return True