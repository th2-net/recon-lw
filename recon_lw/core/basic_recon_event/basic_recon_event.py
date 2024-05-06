"""This event class should be used if you want to use basic
implementations of Reporting."""

import re
from datetime import datetime
from enum import Enum
from typing import List, Optional

from recon_lw.core.basic_recon_event.recon_event_diff import ReconEventDiff
from recon_lw.core.utility import create_event_id

not_exists = '_NE_'


class ReconEventMatchStatus(Enum):
    MATCH = 1
    MATCH_DIFF_FOUND = 2
    NO_COPY = 10
    NO_ORIG = 11


def _get_event_status(event_name: str) -> ReconEventMatchStatus:
    # extracts values enclosed in square brackets
    pattern = r'\[([^\]]+)\]'
    values = re.findall(pattern, event_name)

    if 'match' in values:
        if 'diff found' in values:
            return ReconEventMatchStatus.MATCH_DIFF_FOUND
        return ReconEventMatchStatus.MATCH
    else:
        if 'no_copy' in values:
            return ReconEventMatchStatus.NO_COPY
        if 'no_orig' in values:
            return ReconEventMatchStatus.NO_ORIG
        raise Exception('wrong event name, impossible to extract status')


status_postfix = {
    ReconEventMatchStatus.MATCH: '[match]',
    ReconEventMatchStatus.MATCH_DIFF_FOUND: '[match][diff_found]',
    ReconEventMatchStatus.NO_COPY: '[no_copy]',
    ReconEventMatchStatus.NO_ORIG: '[no_orig]',
}


def _get_postfix_for_status(status: ReconEventMatchStatus) -> str:
    return status_postfix[status]


class BasicReconEvent:
    def __init__(
            self,
            name,
            event_type,
            recon_name: str,
            ok: bool = True,
            body=None,  # TODO -- (defined format is required) can have event['body'].get('additional_fields_info')
            parent_id=None,
            match_status: ReconEventMatchStatus = None,
            event_id=None,
            event_sequence=None,
            attached_messages=None,
            start_timestamp=None,
            matching_key=None,
            diffs: Optional[List[ReconEventDiff]]=None):
        """

        Args:
            name:
            event_type:
            ok:
            body:
            parent_id:
            match_status:  Match status  # TODO -- rename
            event_id:
            event_sequence:
            attached_messages:
            start_timestamp:
            matching_key:
            diffs:

        Returns:

        """
        self.recon_name = recon_name
        ts = datetime.now()  # TODO -- probably we want UTC time
        if event_sequence is not None:
            self.event_id = create_event_id(event_sequence)
        elif event_id is not None:
            self.event_id = event_id
        self.successful = ok
        self.event_name = name
        self.event_type = event_type
        self.body = body
        self.parent_event_id = parent_id

        self.start_timestamp = start_timestamp if start_timestamp is not None else \
            {"epochSecond": int(ts.timestamp()), "nano": ts.microsecond * 1000}
        if attached_messages is None:
            self.attached_message_ids = []
        else:
            self.attached_message_ids = attached_messages

        if match_status is None:
            self.match_status = _get_event_status(self.event_name)
        else:
            self.match_status = match_status
            postfix = _get_postfix_for_status(self.match_status)
            self.event_name = f'{self.event_name}_{postfix}'

        if diffs is None:
            diffs = []
        self.diffs: List[ReconEventDiff] = diffs
        self.matching_key = matching_key

    @property
    def body(self):
        full_body = {
            **self._body,
            "diff": list(map(lambda d: d.to_dict(), self.diffs)),
            "key": self.matching_key,
        }
        return full_body

    @body.setter
    def body(self, body):
        self._body = body

    def convert_to_dict(self) -> dict:
        e = {"eventId": self.event_id,
             "successful": self.successful,
             "eventName": self.event_name,
             "eventType": self.event_type,
             "reconName": self.recon_name,
             "body": self.body,
             "parentEventId": self.parent_event_id,
             "startTimestamp": self.start_timestamp,
             "attachedMessageIds": self.attached_message_ids
             }
        return e

    @classmethod
    def from_dict(cls, e: dict) -> "BasicReconEvent":
        body = e['body']
        key = body.get('key')
        diffs = body.get('diff', [])

        return cls(
            name=e['eventName'],
            event_type=e['eventType'],
            recon_name=e['reconName'],
            ok=e['successful'],
            body=e['body'],
            parent_id=e['parentEventId'],
            event_id=e['eventId'],
            attached_messages=e['attachedMessageIds'],
            start_timestamp=e['startTimestamp'],
            diffs=diffs,
            matching_key=key,
        )
