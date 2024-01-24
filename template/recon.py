from th2_data_services.config import options
from th2_data_services.data import Data

import sys
from th2_data_services.data_source import (
    lwdp,
)  # Required. Will initialize options resolvers

from template.download_data import get_messages
from template.rules.rule1 import rule1

efr = options.EVENT_FIELDS_RESOLVER
mfr = options.MESSAGE_FIELDS_RESOLVER

from recon_lw import recon_lw


def all_recons(
        events_dir,
        metadata,
        config,
):
    """
    Entry point to run your rules.

    Args:
        events_dir:
        metadata:
        config:

    Returns:

    """
    enabled_recons = config["recons"]

    rule1_cfg = (
        {}
        if "Rule1" not in enabled_recons
        else rule1("Rule1", metadata)
    )

    rules = rule1_cfg

    recon_lw.execute_standalone(
        message_pickle_path=None,
        sessions_list=None,
        result_events_path=events_dir,
        rules_settings_dict=rules,
        data_objects=get_messages()
    )

    return metadata
