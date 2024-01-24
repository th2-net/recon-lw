from collections import defaultdict


def aggregate_list_field(
        m,
        body_field,
        field,
        keys=None,
        values=None,
        sep="/",
        mem_field="",
        result_type=dict,
        keys_remap=None,
):
    """
    keys = [a,b,c]c
    values = [d,e,f]

    field = field

    body = {
        field.0.a : "1",
        field.0.b : "2",
        field.0.c : "3",
        field.0.d : "4",
        field.0.e : "5",
        field.0.f : "6",
        field.1.a : "01",
        field.1.b : "02",
        field.1.c : "03",
        field.1.d : "04",
        field.1.e : "05",
        field.1.f : "06",
    }

    result = {
        1/2/3: 4/5/6
        01/02/03: 04/05/06
    }


    :param m:
    :param body_field:
    :param field:
    :param keys:
    :param values:
    :param sep:
    :param mem_field:
    :return:
    """

    body = m[body_field]

    result = body.get(mem_field)
    if result:
        return result

    buffer = defaultdict(dict)
    field += "."
    for k, v in body.items():
        if k.startswith(field):
            # NoPartyIDs.2.PartyID
            _, n, sub_field = k.split(".", maxsplit=2)
            if keys_remap:
                sub_field = keys_remap.get(sub_field, sub_field)
                if not sub_field:
                    continue
            buffer[n][sub_field] = v

    if result_type == dict:
        result = {}
        for _, v in buffer.items():
            key = sep.join([v[k] for k in keys])
            value = sep.join([v[k] for k in values])

            if key in result:
                raise ValueError(f"Duplicated key = {key}, message - {m}")

            result[key] = value
    elif result_type == list:
        result = list(buffer.values())

    m[body_field][mem_field] = result

    return result


def get_list_handler(adapter, field_name, keys_remap=None):
    def handler(m, _):
        data = aggregate_list_field(
            m=m,
            body_field=adapter.body_field,
            field=field_name,
            mem_field=f"_{field_name}_cache",
            result_type=list,
            keys_remap=keys_remap
        )
        return data

    return handler


def get_list_size_handler(adapter, field_name):
    list_handler = get_list_handler(adapter, field_name)

    def handler(m, _):
        val = list_handler(m, _)
        if val and val != adapter.NE:
            return len(val)

        return adapter.NE

    return handler
