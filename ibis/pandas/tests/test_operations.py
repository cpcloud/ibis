import operator
import datetime

import pytest

import pandas as pd
import pandas.util.testing as tm

import ibis
from ibis import literal as L
from ibis.pandas.api import connect, execute


@pytest.fixture
def df():
    return pd.DataFrame({
        'a': [1, 2, 3],
        'b': list('abc'),
        'c': [4.0, 5.0, 6.0],
        'd': pd.date_range('now', periods=3).values,
        'e': list('dad'),
        'f': ['1.0', '2', '3.234'],
        'g': list(map(str, range(1, 4))),
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


@pytest.mark.parametrize('from_', list('ac'))
@pytest.mark.parametrize(
    ('to', 'expected'),
    [
        ('double', 'float64'),
        ('float', 'float32'),
        ('int8', 'int8'),
        ('int16', 'int16'),
        ('int32', 'int32'),
        ('int64', 'int64'),
        ('string', 'object'),
    ],
)
def test_cast_numeric(t, df, from_, to, expected):
    c = t[from_].cast(to)
    result = c.execute()
    assert str(result.dtype) == expected


@pytest.mark.parametrize('from_', list('fg'))
@pytest.mark.parametrize(
    ('to', 'expected'),
    [
        ('double', 'float64'),
        ('string', 'object'),
    ]
)
def test_cast_string(t, df, from_, to, expected):
    c = t[from_].cast(to)
    result = c.execute()
    assert str(result.dtype) == expected


@pytest.mark.parametrize('from_', ['d'])
@pytest.mark.parametrize(
    ('to', 'expected'),
    [
        ('string', 'object'),
        ('int64', 'int64'),
        pytest.mark.xfail(('double', 'float64'), raises=TypeError),
    ]
)
def test_cast_timestamp(t, df, from_, to, expected):
    c = t[from_].cast(to)
    result = c.execute()
    assert str(result.dtype) == expected


@pytest.mark.xfail
def test_cast_date(t, df, from_, to, expected):
    assert False


@pytest.mark.parametrize(
    ('case_func', 'expected_func'),
    [
        (lambda v: v.strftime('%Y%m%d'), lambda vt: vt.strftime('%Y%m%d')),

        (lambda v: v.year(), lambda vt: vt.year),
        (lambda v: v.month(), lambda vt: vt.month),
        (lambda v: v.day(), lambda vt: vt.day),
        (lambda v: v.hour(), lambda vt: vt.hour),
        (lambda v: v.minute(), lambda vt: vt.minute),
        (lambda v: v.second(), lambda vt: vt.second),
        (lambda v: v.millisecond(), lambda vt: int(vt.microsecond / 1e3)),
    ] + [
        (
            operator.methodcaller('strftime', pattern),
            operator.methodcaller('strftime', pattern),
        ) for pattern in [
            '%Y%m%d %H',
            'DD BAR %w FOO "DD"',
            'DD BAR %w FOO "D',
            'DD BAR "%w" FOO "D',
            'DD BAR "%d" FOO "D',
            'DD BAR "%c" FOO "D',
            'DD BAR "%x" FOO "D',
            'DD BAR "%X" FOO "D',
        ]
    ]
)
def test_timestamp_functions(case_func, expected_func):
    v = L('2015-09-01 14:48:05.359').cast('timestamp')
    vt = datetime.datetime(
        year=2015, month=9, day=1,
        hour=14, minute=48, second=5, microsecond=359000
    )
    result = case_func(v)
    expected = expected_func(vt)
    assert execute(result) == expected


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
    result = expr.execute()[['avg_a', 'sum_c']]
    expected = df.groupby('e').agg(
        {'a': 'mean', 'c': 'sum'}
    ).reset_index().rename(
        columns={'a': 'avg_a', 'c': 'sum_c'}
    )[['avg_a', 'sum_c']]
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError)
def test_group_by_with_having(t, df):
    expr = t.group_by(t.e).having(t.c.sum() == 5).aggregate(
        avg_a=t.a.mean(),
        sum_c=t.c.sum(),
    )
    result = expr.execute()[['avg_a', 'sum_c']]

    expected = df.groupby('e').agg(
        {'a': 'mean', 'c': 'sum'}
    ).reset_index().rename(columns={'a': 'avg_a', 'c': 'sum_c'})
    expected = expected.loc[expected.sum_c == 5, ['avg_a', 'sum_c']]

    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    'reduction',
    ['mean', 'sum', 'count', 'std', 'var']
)
@pytest.mark.parametrize(
    'where',
    [
        lambda t: (t.b == 'a') | (t.b == 'c'),
        lambda t: (t.e == 'd') & ((t.a == 1) | (t.a == 3)),
        lambda t: None,
    ]
)
def test_aggregation(t, df, reduction, where):
    func = getattr(t.a, reduction)
    mask = where(t)
    expr = func(where=mask)
    result = expr.execute()

    df_mask = where(df)
    expected_func = getattr(
        df.loc[df_mask if df_mask is not None else slice(None), 'a'],
        reduction,
    )
    expected = expected_func()
    assert result == expected
