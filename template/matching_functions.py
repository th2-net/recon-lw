from typing import Callable

from template.adapters.base_adapter import IBaseAdapter


# TODO - move to another place
class _AnyValBase:
    def __eq__(self, other):
        return True


AnyVal = _AnyValBase()


def get_refdata_field(field_name, security_id, refdata):
    return refdata[security_id][field_name]


def check_message_field(field, check_fun):
    def decorator(fun):
        def wrapper(m, *args, **kwargs):
            if check_fun(m[field]):
                return fun(m)

        return wrapper

    return decorator


def check_session_prefix(prefix):
    return check_message_field(
        field="sessionId", check_fun=lambda s: s.startswith(prefix)
    )


def check_session(session):
    return check_message_field(field="sessionId",
                               check_fun=lambda s: s == session)


def get_simple_matching_func(

) -> Callable:
    def simple_matching_func(m):
        """Should return matching key"""

        return 123

    return simple_matching_func


def get_matching_key(adapter: IBaseAdapter, item, *fields, sep=":"):
    def scale_item(val, count):
        if len(val) == count:
            for x in val:
                yield x
        else:
            v = val[0]
            for _ in range(count):
                yield v

    result = []
    items = {}
    max_count = 1
    for field in fields:
        val = adapter.get(item, field, strict=True)
        if not isinstance(val, list):
            val = [val]
        items[field] = val
        l = len(val)
        if l != 1 and max_count != 1 and l != max_count:
            raise SystemError(
                f"Diff found {max_count} != {l} | {adapter.__class__.__name__} "
                f"| {field} | {item}"
            )

        max_count = max(max_count, len(val))

    z = list(
        zip(
            *(scale_item([str(x) for x in items[field]], max_count) for field in
              fields)
        )
    )

    result = [sep.join(chunks) for chunks in z]
    return result


def basic_matching_key_fun(
        is_orig, adapter: IBaseAdapter, fields,
        filter_fun=None, session=None,
        alias_categorizer=None, alias_category=None
):
    if not filter_fun:
        def filter_fun(x, adapter_):
            return True

    def fun_orig(m):
        if filter_fun(m, adapter):
            adapter.on_message(m)
            mks = get_matching_key(adapter, m, *fields)
            return mks

    def fun_copy(m):
        if filter_fun(m, adapter):
            adapter.on_message(m)
            mks = get_matching_key(adapter, m, *fields)
            if len(mks) > 1:
                raise SystemError(
                    f"Copy matching fun can have only single value, received {mks}"
                )
            return mks[0]

    if is_orig:
        fun = fun_orig
    else:
        fun = fun_copy

    session_checks = []

    if session:
        if isinstance(session, set):
            session_checks.append(lambda alias: alias in session)
        else:
            session_checks.append(lambda alias: alias == session)

    if alias_category:
        if isinstance(alias_category, set):
            session_checks.append(
                lambda alias: alias_categorizer(alias) in alias_category)
        else:
            session_checks.append(
                lambda alias: alias_categorizer(alias) == alias_category)

    fun = check_message_field("sessionId", lambda alias: all(
        c(alias) for c in session_checks))(fun)

    return fun
