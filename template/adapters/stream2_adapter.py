from typing import Dict

from template.adapters.base_adapter import IBaseAdapter, FieldGetterFunc
from th2_data_services.data_source import lwdp
from th2_data_services.config.config import options as o


def get_field_func(field):
    def get_field(msg):
        return o.emfr.get_fields(msg)[field]

    return get_field


class Stream2Adapter(IBaseAdapter):
    def init_mapping(self) -> Dict[str, FieldGetterFunc]:
        return {
            'field1': get_field_func('field1')
        }
