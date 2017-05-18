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
        'd': pd.date_range('now', periods=3).values,
        'e': list('dad')
    })


@pytest.fixture
def df1():
    return pd.DataFrame({'key': list('abcd'), 'value': [3, 4, 5, 6]})


@pytest.fixture
def df2():
    return pd.DataFrame({'key': list('ac'), 'other_value': [4.0, 6.0]})


@pytest.fixture
def client(df, df1, df2):
    return connect({'df': df, 'df1': df1, 'df2': df2})


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


@pytest.mark.parametrize(
    'how',
    [
        'inner',
        'left',
        'outer',

        pytest.mark.xfail('right', raises=KeyError),

        pytest.mark.xfail('semi', raises=NotImplementedError),
        pytest.mark.xfail('anti', raises=NotImplementedError),
    ]
)
def test_join(client, how):
    df1 = client.table('df1')
    df2 = client.table('df2')
    expr = df1.join(df2, df1.key == df2.key, how=how)
    result = expr.execute()
    left_df = client.dictionary['df1']
    right_df = client.dictionary['df2']
    expected = pd.merge(left_df, right_df, how=how, on='key')
    tm.assert_frame_equal(result, expected)


def test_selection(t, df):
    expr = t[((t.b == 'a') | (t.a == 3)) & (t.e == 'd')]
    result = expr.execute()
    expected = df[((df.b == 'a') | (df.a == 3)) & (df.e == 'd')]
    tm.assert_frame_equal(result, expected)


def test_group_by(t, df):
    expr = t.group_by(t.e).aggregate(avg_a=t.a.mean(), sum_c=t.c.sum())
    result = expr.execute()
    expected = df.groupby('e').agg(
        {'a': 'mean', 'c': 'sum'}
    ).reset_index().rename(columns={'a': 'avg_a', 'c': 'sum_c'})
    tm.assert_frame_equal(result, expected)


def test_filtered_aggregation(t, df):
    expr = t.a.mean(where=(t.b == 'a') | (t.b == 'c'))
    result = expr.execute()
    expected = df.loc[(df.b == 'a') | (df.b == 'c'), 'a'].mean()
    assert float(result) == float(expected)
