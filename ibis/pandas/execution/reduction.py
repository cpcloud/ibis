import operator

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy, SeriesGroupBy

import ibis.common as com
import ibis.expr.operations as ops

from ibis.compat import functools
from ibis.pandas.dispatch import execute_node


@execute_node.register(ops.Reduction, SeriesGroupBy, type(None))
def execute_reduction_series_groupby(
    op, data, mask, aggcontext=None, **kwargs
):
    return aggcontext.agg(data, type(op).__name__.lower())


variance_ddof = {
    'pop': 0,
    'sample': 1,
}


@execute_node.register(ops.Variance, SeriesGroupBy, type(None))
def execute_reduction_series_groupby_var(
    op, data, _, aggcontext=None, **kwargs
):
    return aggcontext.agg(data, 'var', ddof=variance_ddof[op.how])


@execute_node.register(ops.StandardDev, SeriesGroupBy, type(None))
def execute_reduction_series_groupby_std(
    op, data, _, aggcontext=None, **kwargs
):
    return aggcontext.agg(data, 'std', ddof=variance_ddof[op.how])


@execute_node.register(
    (ops.CountDistinct, ops.HLLCardinality), SeriesGroupBy, type(None))
def execute_count_distinct_series_groupby(
    op, data, _, aggcontext=None, **kwargs
):
    return aggcontext.agg(data, 'nunique')


@execute_node.register(ops.Arbitrary, SeriesGroupBy, type(None))
def execute_arbitrary_series_groupby(op, data, _, aggcontext=None, **kwargs):
    if op.how not in {'first', 'last'}:
        raise com.OperationNotDefinedError(
            'Arbitrary {!r} is not supported'.format(op.how))
    return aggcontext.agg(data, op.how)


def _filtered_reduction(mask, method, data):
    return method(data[mask[data.index]])


@execute_node.register(ops.Reduction, SeriesGroupBy, SeriesGroupBy)
def execute_reduction_series_gb_mask(
    op, data, mask, aggcontext=None, **kwargs
):
    method = operator.methodcaller(type(op).__name__.lower())
    return aggcontext.agg(
        data,
        functools.partial(_filtered_reduction, mask.obj, method)
    )


@execute_node.register(
    (ops.CountDistinct, ops.HLLCardinality),
    SeriesGroupBy,
    SeriesGroupBy
)
def execute_count_distinct_series_groupby_mask(
    op, data, mask, aggcontext=None, **kwargs
):
    return aggcontext.agg(
        data,
        functools.partial(_filtered_reduction, mask.obj, pd.Series.nunique)
    )


@execute_node.register(ops.Variance, SeriesGroupBy, SeriesGroupBy)
def execute_var_series_groupby_mask(op, data, mask, aggcontext=None, **kwargs):
    return aggcontext.agg(
        data,
        lambda x, mask=mask.obj, ddof=variance_ddof[op.how]: (
            x[mask[x.index]].var(ddof=ddof)
        )
    )


@execute_node.register(ops.StandardDev, SeriesGroupBy, SeriesGroupBy)
def execute_std_series_groupby_mask(op, data, mask, aggcontext=None, **kwargs):
    return aggcontext.agg(
        data,
        lambda x, mask=mask.obj, ddof=variance_ddof[op.how]: (
            x[mask[x.index]].std(ddof=ddof)
        )
    )


@execute_node.register(ops.Count, DataFrameGroupBy, type(None))
def execute_count_frame_groupby(op, data, _, **kwargs):
    result = data.size()
    # FIXME(phillipc): We should not hard code this column name
    result.name = 'count'
    return result


@execute_node.register(ops.Reduction, pd.Series, (pd.Series, type(None)))
def execute_reduction_series_mask(op, data, mask, aggcontext=None, **kwargs):
    operand = data[mask] if mask is not None else data
    return aggcontext.agg(operand, type(op).__name__.lower())


@execute_node.register(
    (ops.CountDistinct, ops.HLLCardinality),
    pd.Series,
    (pd.Series, type(None))
)
def execute_count_distinct_series_mask(
    op, data, mask, aggcontext=None, **kwargs
):
    return aggcontext.agg(data[mask] if mask is not None else data, 'nunique')


@execute_node.register(ops.Arbitrary, pd.Series, (pd.Series, type(None)))
def execute_arbitrary_series_mask(op, data, mask, aggcontext=None, **kwargs):
    if op.how == 'first':
        index = 0
    elif op.how == 'last':
        index = -1
    else:
        raise com.OperationNotDefinedError(
            'Arbitrary {!r} is not supported'.format(op.how))

    data = data[mask] if mask is not None else data
    return data.iloc[index]


@execute_node.register(ops.StandardDev, pd.Series, (pd.Series, type(None)))
def execute_standard_dev_series(op, data, mask, aggcontext=None, **kwargs):
    return aggcontext.agg(
        data[mask] if mask is not None else data,
        'std',
        ddof=variance_ddof[op.how]
    )


@execute_node.register(ops.Variance, pd.Series, (pd.Series, type(None)))
def execute_variance_series(op, data, mask, aggcontext=None, **kwargs):
    return aggcontext.agg(
        data[mask] if mask is not None else data,
        'var',
        ddof=variance_ddof[op.how]
    )


@execute_node.register((ops.Any, ops.All), pd.Series)
def execute_any_all_series(op, data, aggcontext=None, **kwargs):
    return aggcontext.agg(data, type(op).__name__.lower())


@execute_node.register(ops.NotAny, pd.Series)
def execute_notany_series(op, data, aggcontext=None, **kwargs):
    return ~aggcontext.agg(data, 'any')


@execute_node.register(ops.NotAll, pd.Series)
def execute_notall_series(op, data, aggcontext=None, **kwargs):
    return ~aggcontext.agg(data, 'all')


@execute_node.register(ops.Count, pd.DataFrame, type(None))
def execute_count_frame(op, data, _, **kwargs):
    return len(data)
