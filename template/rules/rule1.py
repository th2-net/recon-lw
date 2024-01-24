from recon_lw import recon_lw
from template.adapters.stream1_adapter import Stream1Adapter
from template.adapters.stream2_adapter import Stream2Adapter
from template.fields_checker import EqualFieldCheckRule, \
    FieldToCheck, get_simple_fields_checker
from template.interpret_functions import Counters, get_interpret_func
from template.matching_functions import get_simple_matching_func, \
    basic_matching_key_fun


# Filter example
# def get_fix_filter(
#         check_exec_type=True,
#         prefix="fix_",
#         blacklisted_exec_types=None,
# ):
#     if not blacklisted_exec_types:
#         blacklisted_exec_types = {"Done"}
#
#     def fix_filter(m, adapter: Adapter):
#         if prefix and not m["sessionId"].startswith(prefix):
#             return False
#
#         if m["messageType"] != "ExecutionReport":
#             return False
#
#         if adapter.get(m, "poss_dup") == "True":
#             return False
#
#         if check_exec_type:
#             exec_type = adapter.get(m, "orderbook_execution_type")
#             if exec_type in blacklisted_exec_types:
#                 return False
#
#         return True
#
#     return fix_filter


def rule1(recon_name, metadata):
    stream1_adapter = Stream1Adapter()
    stream2_adapter = Stream2Adapter()

    equal_field_checker_rule = EqualFieldCheckRule(
        stream1_adapter=stream1_adapter,
        stream2_adapter=stream2_adapter
    )

    fields_to_check = {
        ftc.field: ftc
        for ftc in [
            # field name as described in the adapters
            FieldToCheck('order_id', equal_field_checker_rule),
            FieldToCheck('clordid', equal_field_checker_rule),
        ]
    }

    fields_to_compare = list(fields_to_check.keys())

    metadata["recons"][recon_name] = {"matchingFields": fields_to_compare}

    fix_fields_checker = get_simple_fields_checker(
        fields_to_check
    )

    counters = Counters()

    stream1_key_fun = basic_matching_key_fun(
        is_orig=True,
        adapter=stream1_adapter,
        fields=["exec_id", "dc_target"],
        # filter_fun=get_fix_filter(
        #     True, True, prefix=None, blacklisted_exec_types={"Rejected"}
        # ),
        alias_categorizer=get_alias_category,
        alias_category={AliasCategory.FIX_OE, AliasCategory.BIN_OE},
    )
    stream2_key_fun = basic_matching_key_fun(
        is_orig=False,
        adapter=stream2_adapter,
        fields=["exec_id", "target_comp_id"],
        # filter_fun=get_fix_filter(False, False, prefix=None),
        alias_categorizer=get_alias_category,
        alias_category=AliasCategory.FIX_DC,
    )

    rules = {
        recon_name: {
            "first_key_func": stream1_key_fun,
            "second_key_func": stream2_key_fun,
            "interpret_func": get_interpret_func(
                orig_adapter=stream1_adapter,
                copy_adapter=stream2_adapter,
                event_name_prefix="YOUR_PREFIX_NAME",
                fields_checker=fix_fields_checker,
                counters=counters,
                first_key_func=stream1_key_fun,
                second_key_func=stream2_key_fun,
            ),
            "horizon_delay": 180,
            "rule_match_func": recon_lw.one_many_match,
        }
    }
    return rules
