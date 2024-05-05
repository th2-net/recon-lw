from recon_lw.core.EventsSaver import EventsSaver, IEventsSaver
from typing import Union, Callable, Any

from recon_lw.core.rule import AbstractRule
from recon_lw.core.rule.base import RuleContext
from recon_lw.matching.collect_matcher import CollectMatcher

from recon_lw.core.utility import *
from recon_lw.matching.flush_function import FlushFunction
from recon_lw.matching.old.matching import init_matcher, collect_matcher, flush_matcher


def execute_standalone(
        message_pickle_path,
        sessions_list,
        result_events_path,
        rules: Dict[str, Dict[str, Union[Dict[str, Any], AbstractRule]]],
        data_objects=None,
        buffer_len=100,
        events_saver: Optional[IEventsSaver] = None,
):
    """Entrypoint for recon-lw.

    It generates ReconEvents and stores them in the `result_events_path` file
    to disc in pickle format.

    It matches messages 1 to 1 or 1 to many  (depends on `rule_match_func` param
    in the config).
    When messages were matched or unmatched, the list of [msg1, *msgs2] will be
    passed to interp_func.
    msg1 and *msgs2 can be None if no match message was found.
    It's not possible case when all msgs are None.

    If you provide data_objects, message_pickle_path -- will be ignored.


    Args:
        message_pickle_path:
        sessions_list:
        result_events_path:
        rules_settings_dict: { ReconRuleName: {}, ... }
        data_objects:

    Returns:

    """
    box_ts = datetime.now()
    if events_saver is None:
        events_saver = EventsSaver(result_events_path)

    event_sequence = EventSequence(name="recon_lw", timestamp=str(box_ts.timestamp()), n=0).to_dict()
    root_event = create_event("recon_lw " + box_ts.isoformat(), "Microservice", event_sequence)

    events_saver.save_events([root_event])
    new_rules_settings_dict = {}
    for rule_key, rule_settings in rules.items():
        if isinstance(rule_settings, dict):
            new_rules_settings_dict[rule_key] = preprocess_rule_config_dict(
                rule_key,
                event_sequence,
                root_event,
                rule_settings,
                events_saver
            )
        elif isinstance(rule_settings, AbstractRule):
            new_rules_settings_dict[rule_key] = preprocess_rule_config_object(
                rule_key,
                event_sequence,
                root_event,
                rule_settings,
                events_saver
            )
        else:
            raise SystemError("Invalid rule settings type.")

    events_saver.save_events(
        [
            r.rule_context.rule_root_event if isinstance(r, AbstractRule) else r["rule_root_event"]
                for r in new_rules_settings_dict.values()
        ]
    )
    if data_objects:
        streams = open_streams(message_pickle_path, data_objects=data_objects)
    else:
        if sessions_list is not None and len(sessions_list):
            sessions_set = set(sessions_list)
            streams = open_streams(message_pickle_path,
                                   lambda n: n[:n.rfind('_')] in sessions_set)
        else:
            streams = open_streams(message_pickle_path)

    message_buffer = [None] * buffer_len

    while len(streams) > 0:
        next_batch_len = streams.get_next_batch(message_buffer, buffer_len,
                                        lambda m: m["timestamp"])
        buffer_to_process = message_buffer
        if next_batch_len < buffer_len:
            buffer_to_process = message_buffer[:next_batch_len]

        for rule_settings in new_rules_settings_dict.values():
            if isinstance(rule_settings, AbstractRule):
                rule_settings.collect_func.collect_matches(buffer_to_process, rule_settings)
                ts = buffer_to_process[len(buffer_to_process) - 1]["timestamp"]
                rule_settings.flush_func(
                    ts,
                    rule_settings,
                    lambda ev_batch: events_saver.save_events(ev_batch)
                )
            else:
                rule_settings["collect_func"](buffer_to_process, rule_settings)
                ts = buffer_to_process[len(buffer_to_process) - 1]["timestamp"]
                rule_settings["flush_func"](ts, rule_settings, event_sequence,
                                            lambda ev_batch: events_saver.save_events(ev_batch))
    # final flush
    for rule_settings in new_rules_settings_dict.values():
        if isinstance(rule_settings, AbstractRule):
            rule_settings.flush_func(
                None,
                rule_settings,
                lambda ev_batch: events_saver.save_events(ev_batch)
            )
        else:
            rule_settings["flush_func"](None, rule_settings, event_sequence,
                                        lambda ev_batch: events_saver.save_events(ev_batch))
    # one final flush
    events_saver.flush()

    # TODO
    #   Probably It's a good idea to return ReconContext here.


def preprocess_rule_config_object(
    rule_key: str,
    event_sequence: dict,
    root_event: dict,
    rule: AbstractRule,
    events_saver: IEventsSaver
) -> AbstractRule:
    rule_root_event = create_event(rule_key, "LwReconRule",
                                   event_sequence,
                                   parentId=root_event["eventId"])

    rule.set_rule_context(RuleContext(rule_root_event, events_saver, event_sequence))
    return rule

def preprocess_rule_config_dict(
    rule_key: str,
    event_sequence: dict,
    root_event: dict,
    rule: dict,
    events_saver: IEventsSaver
) -> dict:
    rule_root_event = create_event(rule_key, "LwReconRule",
                                   event_sequence,
                                   parentId=root_event["eventId"])

    rule["rule_root_event"] = rule_root_event
    rule["events_saver"] = events_saver
    rule["event_sequence"] = event_sequence

    if "init_func" not in rule:
        rule["init_func"] = init_matcher\

    if "collect_func" not in rule:
        rule["collect_func"] = collect_matcher
    if "flush_func" not in rule:
        rule["flush_func"] = flush_matcher
    rule["init_func"](rule)
    return rule


#