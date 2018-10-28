import attr

import ibis.expr.rules as rlz


def Argument(validator, *args, **kwargs):
    if isinstance(validator, type) or (
        isinstance(validator, tuple) and all(
            isinstance(t, type) for t in validator
        )
    ):
        return attr.ib(
            *args, validator=attr.validators.instance_of(validator), **kwargs
        )
    elif isinstance(validator, rlz.validator):
        return attr.ib(
            *args,
            converter=lambda arg, *args: validator(arg, *args),
            **kwargs
        )
    return attr.ib(*args, validator=validator, **kwargs)
