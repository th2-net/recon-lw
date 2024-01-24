from typing import Dict

from template.adapters.base_adapter import IBaseAdapter, FieldGetterFunc
from th2_data_services.data_source import lwdp
from th2_data_services.config.config import options as o


def get_field_func(field):
    def get_field(msg):
        return o.emfr.get_fields(msg)[field]

    return get_field


class Stream1Adapter(IBaseAdapter):

    def init_mapping(self) -> Dict[str, FieldGetterFunc]:
        """
        Recommendations:
            1. Use pairwise names like `msgField1_msgField2` if you have
            different names for the same field in the messages
            # TODO -- probably it's better to use some Enum here instead

        """
        return {
            'field1': get_field_func('field1'),
            'msgField1_msgField2': get_field_func('field1'),
        }

    def get_fields_group(self, m, group_name):
        if group_name == "order_ids":
            return {
                "OrderID": self.get(m, "order_id"),
                "ClOrdID": self.get(m, "clordid"),
            }

    def on_message(self, m):
        pass

    def on_message_exit(self, m):
        pass
