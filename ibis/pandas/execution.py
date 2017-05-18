import numbers
import operator
import datetime

from functools import reduce

import six

import numpy as np
import pandas as pd

import ibis.expr.types as ir
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops

from ibis.pandas.dispatch import execute, execute_node


integer_types = six.integer_types + (np.integer,)

scalar_types = (
    numbers.Real, datetime.datetime, datetime.date, np.number, np.bool_,
    np.datetime64, np.timedelta64,
) + six.string_types


@execute_node.register(ir.Literal)
def execute_node_literal(op):
    return op.value


@execute_node.register(ir.Literal, object)
def execute_node_literal_object(op, _, scope=None):
    return op.value


@execute.register(ir.Literal)
def execute_literal(literal):
    return literal.value


_IBIS_TYPE_TO_PANDAS_TYPE = {
    dt.float: np.float32,
    dt.double: np.float64,
    dt.int8: np.int8,
    dt.int16: np.int16,
    dt.int32: np.int32,
    dt.int64: np.int64,
    dt.string: str,
    dt.timestamp: 'datetime64[ns]',
}


def ibis_type_to_pandas_type(ibis_type):
    return _IBIS_TYPE_TO_PANDAS_TYPE[ibis_type]


@execute_node.register(ops.Cast, pd.Series, dt.DataType)
def execute_cast_series_generic(op, data, type, scope=None):
    return data.astype(ibis_type_to_pandas_type(type))


@execute_node.register(ops.Cast, pd.Series, dt.Date)
def execute_cast_series_date(op, data, type, scope=None):
    return data.dt.date


_LITERAL_CAST_TYPES = {
    dt.double: float,
    dt.float: float,
    dt.int64: int,
    dt.int32: int,
    dt.int16: int,
    dt.int8: int,
    dt.string: str,
    dt.timestamp: pd.Timestamp,
}


@execute_node.register(ops.Cast, scalar_types, dt.DataType)
def execute_cast_string_literal(op, data, type, scope=None):
    try:
        return _LITERAL_CAST_TYPES[type](data)
    except KeyError:
        raise TypeError(
            "Don't know how to cast {!r} to type {}".format(data, type)
        )


@execute_node.register(ops.TableColumn, pd.DataFrame)
def execute_table_column_dataframe(op, data, scope=None):
    return data[op.name]


@execute_node.register(ops.Selection, pd.DataFrame)
def execute_selection_dataframe(op, data, scope=None):
    selections = op.selections
    predicates = op.predicates
    sort_keys = op.sort_keys

    result = data

    if selections:
        old_names = [s.op().name for s in selections]
        new_names = [
            getattr(s, '_name', old_name) or old_name
            for old_name, s in zip(old_names, selections)
        ]
        result = result[old_names].rename(dict(zip(old_names, new_names)))

    if predicates:
        where = reduce(operator.and_, (execute(p, scope) for p in predicates))
        result = result.loc[where]

    if sort_keys:
        return result.sort_values(sort_keys)
    return result


def reduction_to_lambda(expr):
    op = expr.op()

    def reduction(
        df,
        column=op.args[0]._name,
        method_name=type(op).__name__.lower(),
        new_name=expr._name,
    ):
        method = getattr(df[column], method_name)
        return method().rename(new_name)
    return reduction


@execute_node.register(ops.Aggregation, pd.DataFrame)
def execute_aggregation_dataframe(op, data, scope=None):
    assert op.agg_exprs
    assert op.by

    if op.having:
        raise NotImplementedError('having expressions not yet implemented')

    if op.sort_keys:
        raise NotImplementedError(
            'sorting on aggregations not yet implemented'
        )

    predicates = op.predicates
    if predicates:
        data = data.loc[
            reduce(operator.and_, (execute(p, scope) for p in predicates))
        ]

    gb = data.groupby([execute(by, scope) for by in op.by])

    aggs = [reduction_to_lambda(agg_expr) for agg_expr in op.agg_exprs]
    pieces = [func(gb) for func in aggs]
    agg_df = pd.concat(pieces, axis=1)

    return agg_df.reset_index()


@execute_node.register(ops.Reduction, pd.Series, (pd.Series, type(None)))
def execute_reduction_series_mask(op, data, mask, scope=None):
    operand = data[mask] if mask is not None else data
    return getattr(operand, type(op).__name__.lower())()


@execute_node.register(ops.StandardDev, pd.Series, (pd.Series, type(None)))
def execute_standard_dev_series(op, data, mask, scope=None):
    return (data[mask] if mask is not None else data).std()


@execute_node.register(ops.Variance, pd.Series, (pd.Series, type(None)))
def execute_variance_series(op, data, mask, scope=None):
    return (data[mask] if mask is not None else data).var()


@execute_node.register((ops.Any, ops.All), pd.Series)
def execute_any_all_series(op, data, scope=None):
    return getattr(data, type(op).__name__.lower())()


@execute_node.register(ops.Not, (bool, np.bool_))
def execute_not_bool(op, data, scope=None):
    return not data


_JOIN_TYPES = {
    ops.LeftJoin: 'left',
    ops.InnerJoin: 'inner',
    ops.OuterJoin: 'outer',
}


@execute_node.register(ops.Join, pd.DataFrame, pd.DataFrame)
def execute_join(op, left, right, scope=None):
    try:
        how = _JOIN_TYPES[type(op)]
    except KeyError:
        raise NotImplementedError('{} not supported'.format(type(op).__name__))

    left_on = []
    right_on = []

    for predicate in op.predicates:
        pred_op = predicate.op()
        if not isinstance(pred_op, ops.Equals):
            raise TypeError(
                'Only equality join predicates supported with pandas'
            )
        left_key = pred_op.left._name
        right_key = pred_op.right._name
        left_on.append(left_key)
        right_on.append(right_key)

    return pd.merge(left, right, how=how, left_on=left_on, right_on=right_on)


_BINARY_OPERATIONS = {
    ops.Greater: operator.gt,
    ops.Less: operator.lt,
    ops.LessEqual: operator.le,
    ops.GreaterEqual: operator.ge,
    ops.Equals: operator.eq,
    ops.NotEquals: operator.ne,

    ops.And: operator.and_,
    ops.Or: operator.or_,
    ops.Xor: operator.xor,

    ops.Add: operator.add,
    ops.Subtract: operator.sub,
    ops.Multiply: operator.mul,
    ops.Divide: operator.truediv,
    ops.FloorDivide: operator.floordiv,
    ops.Modulus: operator.mod,
    ops.Power: operator.pow,
}


@execute_node.register(
    ops.BinaryOp, pd.Series, (pd.Series, numbers.Real) + six.string_types
)
def execute_binary_operation_series(op, left, right, scope=None):
    return _BINARY_OPERATIONS[type(op)](left, right)


@execute_node.register(ops.Not, pd.Series)
def execute_not_series(op, data, scope=None):
    return ~data


@execute_node.register(ops.Strftime, pd.Timestamp, six.string_types)
def execute_strftime_timestamp_str(op, data, format_string, scope=None):
    return data.strftime(format_string)


@execute_node.register(ops.Strftime, pd.Series, six.string_types)
def execute_strftime_series_str(op, data, format_string, scope=None):
    return data.dt.strftime(format_string)


@execute_node.register(ops.ExtractTimestampField, pd.Timestamp)
def execute_extract_timestamp_field_timestamp(op, data, scope=None):
    field_name = type(op).__name__.lower().replace('extract', '')
    return getattr(data, field_name)


@execute_node.register(ops.ExtractMillisecond, pd.Timestamp)
def execute_extract_millisecond_timestamp(op, data, scope=None):
    return int(data.microsecond // 1000.0)


@execute_node.register(ops.ExtractTimestampField, pd.Series)
def execute_extract_timestamp_field_series(op, data, scope=None):
    field_name = type(op).__name__.lower().replace('extract', '')
    return getattr(data.dt, field_name)


@execute_node.register(ops.NullIfZero, pd.Series)
def execute_null_if_zero_series(op, data, scope=None):
    result = data.copy()
    result[result == 0] = np.nan
    return result


@execute_node.register(ops.StringLength, pd.Series)
def execute_string_length_series(op, data, scope=None):
    return data.str.len()


@execute_node.register(
    ops.Substring,
    pd.Series,
    (pd.Series,) + integer_types,
    (pd.Series,) + integer_types
)
def execute_string_substring(op, data, start, length, scope=None):
    return data.str[start:start + length]


@execute_node.register(ops.Strip, pd.Series)
def execute_string_strip(op, data, scope=None):
    return data.str.strip()


@execute_node.register(ops.LStrip, pd.Series)
def execute_string_lstrip(op, data, scope=None):
    return data.str.lstrip()


@execute_node.register(ops.RStrip, pd.Series)
def execute_string_rstrip(op, data, scope=None):
    return data.str.rstrip()


@execute_node.register(
    ops.LPad,
    pd.Series,
    (pd.Series,) + integer_types,
    (pd.Series,) + six.string_types
)
def execute_string_lpad(op, data, length, pad, scope=None):
    return data.str.pad(length, side='left', fillchar=pad)


@execute_node.register(
    ops.RPad,
    pd.Series,
    (pd.Series,) + integer_types,
    (pd.Series,) + six.string_types
)
def execute_string_rpad(op, data, length, pad, scope=None):
    return data.str.pad(length, side='right', fillchar=pad)


@execute_node.register(ops.Reverse, pd.Series)
def execute_string_reverse(op, data, scope=None):
    return data.str[::-1]


@execute_node.register(ops.Lowercase, pd.Series)
def execute_string_lower(op, data, scope=None):
    return data.str.lower()


@execute_node.register(ops.Uppercase, pd.Series)
def execute_string_upper(op, data, scope=None):
    return data.str.upper()


@execute_node.register(ops.Capitalize, pd.Series)
def execute_string_capitalize(op, data, scope=None):
    return data.str.capitalize()


@execute_node.register(ops.Repeat, pd.Series, (pd.Series,) + integer_types)
def execute_string_repeat(op, data, times, scope=None):
    return data.str.repeat(times)


@execute_node.register(
    ops.StringFind,
    pd.Series,
    (pd.Series,) + six.string_types,
    (pd.Series, type(None)) + integer_types,
    (pd.Series, type(None)) + integer_types,
)
def execute_string_contains(op, data, needle, start, end, scope=None):
    return data.str.find(needle, start, end)


@execute_node.register(
    ops.Between,
    pd.Series,
    (pd.Series, numbers.Real, str, datetime.datetime),
    (pd.Series, numbers.Real, str, datetime.datetime)
)
def execute_between(op, data, lower, upper, scope=None):
    return data.between(lower, upper)


@execute_node.register(ops.Union, pd.DataFrame, pd.DataFrame)
def execute_union_dataframe_dataframe(op, left, right, scope=None):
    return pd.concat([left, right], axis=0)


# Core execution

def find_data(expr):
    stack = [expr]
    seen = set()
    data = dict()

    while stack:
        e = stack.pop()
        node = e.op()

        if node not in seen:
            seen.add(node)

            if hasattr(node, 'source'):
                data[e] = node.source.dictionary[node.name]
            elif isinstance(node, ir.Literal):
                data[e] = node.value

            stack.extend(arg for arg in node.args if isinstance(arg, ir.Expr))
    return data


@execute.register(ir.Expr, dict)
def execute_with_scope(expr, scope):
    if expr in scope:
        return scope[expr]

    op = expr.op()
    args = op.args

    evaluated_arguments = [
        execute(arg, scope) if hasattr(arg, 'op') else arg
        for arg in args if isinstance(
            arg, (ir.Expr, ir.Node, dt.DataType, type(None))
        )
    ] or [scope.get(arg, arg) for arg in args]

    return execute_node(op, *evaluated_arguments, scope=scope)


@execute.register(ir.Expr)
def execute_without_scope(expr):
    scope = find_data(expr)
    if not scope:
        raise ValueError('No data sources found')
    return execute(expr, scope)
