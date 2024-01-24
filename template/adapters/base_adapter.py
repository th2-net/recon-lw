from abc import abstractmethod, ABC
from typing import Callable, Dict, Any

Msg = dict
FieldVal = Any
FieldGetterFunc = Callable[[Msg], FieldVal]


# class a:
#
#
#     def get(self, field) -> FieldGetterFunc:
#         return self.mapping[field]


class IBaseAdapter(ABC):
    covered_fields = set()
    body_field = "body"

    def __init__(self):
        self.mapping = self.init_mapping()

    @abstractmethod
    def init_mapping(self) -> Dict[str, FieldGetterFunc]:
        """
        Examples:
            {field1: get_field1_func}

        Returns:

        """

    @abstractmethod
    def get_fields_group(self, m, group_name):
        return None

    @abstractmethod
    def on_message(self, m):
        """This is triggered when the message arrives in the recon

        Was done mostly for ack_handler
        So that you can track the state

        return None in your Adapter if it's not required.
        """
        pass

    @abstractmethod
    def on_message_exit(self, m):
        """This is triggered after the message has been processed

        Was done mostly for ack_handler
        So that you can track the state

        return None in your Adapter if it's not required.
        """
        pass

    def get(self, item, field, strict=False):
        actual_field = self.mapping[field]
        if isinstance(actual_field, Callable):
            val = actual_field(item)
        else:
            val = item[self.body_field].get(actual_field, self.NE)

        if strict and val == self.NE:
            raise KeyError(field)

        if val != self.NE:
            val = str(val)

        return val

    def basic_conv_handler(self, item, field, converter):
        val = item[self.body_field].get(field, self.NE)
        if val != self.NE:
            val = str(val)
        return converter(item, field, val)

    def build_base_handler(self, field):
        def handler(m, _):
            val = m[self.body_field].get(field, self.NE)
            if val != self.NE:
                val = str(val)
            return val

        return handler

    def build_conv_handler(self, field, converter):
        def handler(m, _):
            val = m[self.body_field].get(field, self.NE)
            if val in {self.NE, None}:
                return val

            val = str(val)

            return converter(m, field, val)

        return handler

    def get_simple_conv_handler(self, field, converter, pass_NE=False):
        def simple_conv(item, field, val):
            if val in {None, self.NE} and not pass_NE:
                return val
            return converter(val)

        def fun(item, _):
            return self.basic_conv_handler(
                item=item, field=field, converter=simple_conv
            )

        return fun

    def get_dict_handler(self, field, mapping):
        def dict_converter(item, field, val):
            result = mapping.get(val, b"404")
            if result == b"404" and val not in {None, self.NE}:
                if val == '300' or val == 300:
                    raise KeyError(
                        f"Uncovered value {val} for field {field} in mapping {mapping}")
                return f"Unknown value {val}"

            if result == b"404":
                return val
            return result

        def fun(item, _):
            return self.basic_conv_handler(
                item=item, field=field, converter=dict_converter
            )

        return fun

    def build_default_value_handler(self, field, default):
        base_handler = self.build_base_handler(field)

        def handler(m, _):
            val = base_handler(m, _)
            if val == self.NE:
                return default

            return val

        return handler

    def build_constant_handler(self, value):
        def handler(m, _):
            return value

        return handler

    def build_conditional_masking_handler(
            self, field_or_handler, condition, mask_value
    ):
        if isinstance(field_or_handler, str):
            base_handler = self.build_base_handler(field_or_handler)
        else:
            base_handler = field_or_handler

        def handler(m, _):
            if condition(m):
                return mask_value

            val = base_handler(m, _)
            return val

        return handler

    NE = "_NE_"
    "Not exists"


class CompoundAdapter(IBaseAdapter):
    def __init__(self, *adapters):
        super().__init__()
        self.adapters = adapters

    def init_mapping(self) -> Dict[str, FieldGetterFunc]:
        pass

    def get_adapter(self, m):
        for condition, adapter in self.adapters:
            if condition(m):
                return adapter

        raise SystemError(f"Can't handle {m} - no matching adapter")

    def get(self, item, field, strict=False):
        handler = self.get_adapter(item)
        return handler.get(item, field, strict)

    def on_message(self, m):
        handler = self.get_adapter(m)
        return handler.on_message(m)

    def on_message_exit(self, m):
        handler = self.get_adapter(m)
        return handler.on_message_exit(m)

    def get_fields_group(self, m, group_name):
        handler = self.get_adapter(m)
        return handler.get_fields_group(m, group_name)
