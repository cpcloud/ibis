import functools

from collections import OrderedDict

import pytest

import ibis
from ibis.compat import wrapped


def struct_test(f):
    @wrapped(f)
    @functools.wraps(f)
    def wrapper(backend, *args, **kwargs):
        if not backend.supports_structs:
            pytest.skip('Backend {} does not support structs'.format(backend))
        return f(backend, *args, **kwargs)
    return wrapper


def direct_struct_operation_test(f):
    @wrapped(f)
    @functools.wraps(struct_test(f))
    def wrapper(backend, *args, **kwargs):
        if not backend.supports_structs_outside_of_select:
            pytest.skip(
                'Backend {} does not support operations directly on '
                'structs as column cells'.format(backend)
            )
        return f(backend, *args, **kwargs)
    return wrapper


# @tu.skipif_unsupported
@direct_struct_operation_test
def test_struct_literal(backend, con):
    const = OrderedDict([('a', 1), ('b', 'abc')])
    expr = ibis.literal(const)
    result = con.execute(expr)
    assert result == {'a': 1, 'b': 'abc'}


# @tu.skipif_unsupported
@direct_struct_operation_test
def test_struct_literal_field_access(backend, con):
    const = OrderedDict([('a', 1), ('b', 'abc')])
    expr = ibis.literal(const).b
    assert con.execute(expr) == 'abc'
