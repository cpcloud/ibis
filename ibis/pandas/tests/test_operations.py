import operator

import pytest

import pandas as pd
import pandas.util.testing as tm

import ibis
from ibis.pandas.api import connect


@pytest.fixture
def df():
    return pd.DataFrame({
        'a': [1, 2, 3],
        'b': list('abc'),
        'c': [4.0, 5.0, 6.0],
        'd': pd.date_range('now', periods=3).values
    })


@pytest.fixture
def client(df):
    return connect({'df': df})


@pytest.fixture
def t(client):
    return client.table('df')


def test_table_column(t, df):
    expr = t.a
    result = expr.execute()
    tm.assert_series_equal(result, df.a)


def test_literal(client):
    assert client.execute(ibis.literal(1)) == 1


@pytest.mark.parametrize(
    'op',
    [
        operator.add,
        operator.sub,
        operator.mul,
        operator.truediv,
        operator.eq,
        operator.ne,
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.pow,
    ]
)
def test_binary_operations(t, df, op):
    expr = op(t.c, t.a)
    result = expr.execute()
    tm.assert_series_equal(result, op(df.c, df.a))
