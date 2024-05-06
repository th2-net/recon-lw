from typing import List, Callable, Dict

from th2_data_services.data import Data
from th2_data_services.utils.converters import Th2TimestampConverter

from recon_lw.core.type.types import Message

from th2_data_services.config import options as o


def get_group_data_map(datas_list: List[Data], default: str) -> Dict[str, Data]:
    """

    Args:
        datas_list:
        default:

    Returns:
        {group-name: Data object}

    """
    return {do.metadata.get('group', default): do for do in datas_list}


def get_msgs_by_id(data: Data, ids: list,
                   id_function: Callable[[Message], List[str]]):
    ids = set(ids)

    res = []
    for m in data:
        m_ids = id_function(m)
        for id in m_ids:
            if id in ids:
                res.append(m)
    return res


def get_group_from_id(msg_id: str):
    return msg_id.split(':', 2)[1]


def get_timestamp_ns(m):
    return Th2TimestampConverter.to_microseconds(o.emfr.get_timestamp(m))


def sort_msgs_by_th2_timestamp(msgs: List[Message]):
    return sorted(msgs, key=get_timestamp_ns)
