from dataclasses import dataclass
from typing import List, Callable
from collections import defaultdict
from typing import Tuple, Any, Generator, Iterator, Callable

from recon_lw import recon_lw
from template.adapters.base_adapter import IBaseAdapter
from template.fields_checker import FieldCheckResult
from template.recon_event_types import ReconType


@dataclass
class Counters:
    match_ok: int = 0
    match_fail: int = 0
    no_right: int = 0
    no_left: int = 0


def get_interpret_func(
        # match_msgs,
        orig_adapter,
        copy_adapter,
        event_name_prefix,
        fields_checker: Callable,
        counters: Counters,
        first_key_func,
        second_key_func,
):
    def interpret_func(match_msgs: List[dict], _, event_sequence: dict):
        """

        Args:
            match_msgs: list of matched messages
            _:
            event_sequence:

        Returns:

        """
        return compare_2_msgs(
            match_msgs=match_msgs,
            event_sequence=event_sequence,
            orig_adapter=orig_adapter,
            copy_adapter=copy_adapter,
            fields_checker=fields_checker,
            event_name_prefix=event_name_prefix,
            counters=counters,
            first_key_func=first_key_func,
            second_key_func=second_key_func
        )

    return interpret_func


def _get_miss_event(msg, event_name_prefix,
                    match_key,
                    recon_type: ReconType, counters: Counters,
                    order_ids,
                    event_sequence):
    if recon_type == ReconType.BasicReconMissLeft:
        counters.no_left += 1
        name = f"{event_name_prefix}_[no_left]"
    elif recon_type == ReconType.BasicReconMissRight:
        counters.no_right += 1
        name = f"{event_name_prefix}_[no_right]"
    else:
        raise Exception('unexpected behaviour')

    body = {"key": match_key}

    if order_ids:
        body["order_ids"] = order_ids

    event = recon_lw.create_event(
        name=name,
        type=recon_type.value,
        ok=False,
        event_sequence=event_sequence,
        body=body,
    )
    event["attachedMessageIds"] = [msg["messageId"]]

    return event


def compare_2_msgs(
        match_msgs,
        event_sequence: dict,
        orig_adapter: IBaseAdapter,
        copy_adapter: IBaseAdapter,
        fields_checker: Callable[[dict, dict], Iterator[FieldCheckResult]],
        event_name_prefix,
        counters: Counters,
        first_key_func,
        second_key_func,
):
    msg1 = match_msgs[0]
    msg2 = match_msgs[1]
    events = []
    if msg1 is not None and msg2 is not None:
        name = f"{event_name_prefix}_[match]"

        body = {}
        diff_list = []
        # field, orig_value, copy_value
        differences: Iterator[FieldCheckResult] = fields_checker(msg1, msg2)
        status = True

        if differences:
            status = False
            for fcr in differences:
                diff_list.append(
                    dict(field=fcr.field, expected=fcr.left_val,
                         actual=fcr.right_val)
                )

        order_ids = orig_adapter.get_fields_group(
            msg1, "order_ids"
        ) or copy_adapter.get_fields_group(msg2, "order_ids")

        if order_ids:
            body["order_ids"] = order_ids

        if not status:
            name = f"{name}[diff_found]"
            counters.match_fail += 1
            body["diff"] = diff_list
        else:
            counters.match_ok += 1

        event = recon_lw.create_event(
            name=name,
            type=ReconType.BasicReconMatch.value,
            event_sequence=event_sequence,
            ok=status,
            body=body,
        )
        event["attachedMessageIds"] = [m["messageId"] for m in match_msgs if
                                       m is not None]
        events.append(event)
        orig_adapter.on_message_exit(msg1)
        copy_adapter.on_message_exit(msg2)

    elif msg1 is not None:
        # counters["no_right"] += 1
        # body = {"key": first_key_func(msg1)}
        # # TODO -- get_fields_group что это ???
        # order_ids = orig_adapter.get_fields_group(msg1, "order_ids")
        #
        # if order_ids:
        #     body["order_ids"] = order_ids
        #
        # name = f"{event_name_prefix}_[no_right]"
        # event = recon_lw.create_event(
        #     name=name,
        #     type=ReconType.BasicReconMissLeft.value,
        #     ok=False,
        #     event_sequence=event_sequence,
        #     body=body,
        # )
        # event["attachedMessageIds"] = [msg1["messageId"]]

        order_ids = orig_adapter.get_fields_group(msg1, "order_ids")
        match_key = first_key_func(msg1)
        event = _get_miss_event(msg2, event_name_prefix,
                                match_key=match_key,
                                recon_type=ReconType.BasicReconMissLeft,
                                counters=counters,
                                order_ids=order_ids,
                                event_sequence=event_sequence)
        events.append(event)
        orig_adapter.on_message_exit(msg1)

    elif msg2 is not None:
        # counters["no_left"] += 1
        #
        # body = {"key": second_key_func(msg2)}
        # order_ids = copy_adapter.get_fields_group(msg2, "order_ids")
        #
        # if order_ids:
        #     body["order_ids"] = order_ids
        #
        # name = f"{event_name_prefix}_[no_left]"
        # event = recon_lw.create_event(
        #     name=name,
        #     type=ReconType.BasicReconMissRight.value,
        #     ok=False,
        #     event_sequence=event_sequence,
        #     body=body,
        # )
        # event["attachedMessageIds"] = [m["messageId"] for m in match_msgs if
        #                                m is not None]

        order_ids = copy_adapter.get_fields_group(msg2, "order_ids")
        match_key = second_key_func(msg2)
        event = _get_miss_event(msg2, event_name_prefix,
                                match_key=match_key,
                                recon_type=ReconType.BasicReconMissRight,
                                counters=counters,
                                order_ids=order_ids,
                                event_sequence=event_sequence)
        events.append(event)
        copy_adapter.on_message_exit(msg2)

    return events
