import pytest


@pytest.mark.parametrize(
    'column',
    [
        'string_col',
        'double_col',
        'date_string_col',
        pytest.param('timestamp_col', marks=pytest.mark.skip(reason='hangs')),
    ],
)
@pytest.mark.xfail_unsupported
def test_distinct_column(backend, alltypes, df, column):
    expr = alltypes[column].distinct()
    result = expr.execute()
    expected = df[column].unique()
    assert set(result) == set(expected)
