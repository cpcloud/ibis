import numbers
import operator

from functools import reduce

import six

import pandas as pd

import ibis.expr.types as ir
import ibis.expr.operations as ops

from ibis.pandas.dispatch import execute, execute_node


@execute_node.register(ir.Literal)
def execute_node_literal(op):
    return op.value


@execute_node.register(ir.Literal, object)
def execute_node_literal_object(op, _, scope=None):
    return op.value


@execute.register(ir.Literal)
def execute_literal(literal):
    return literal.value


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

    predicates = op.predicates
    if predicates:
        data = data.loc[
            reduce(operator.and_, (execute(p, scope) for p in predicates))
        ]

    gb = data.groupby([execute(by, scope) for by in op.by])

    aggs = [reduction_to_lambda(agg_expr) for agg_expr in op.agg_exprs]
    pieces = [func(gb) for func in aggs]
    agg_df = pd.concat(pieces, axis=1)

    having = op.having
    if having:
        agg_df = agg_df[
            reduce(operator.and_, (execute(h, scope) for h in having))
        ]

    sort_keys = op.sort_keys
    if sort_keys:
        agg_df = agg_df.sort_values(sort_keys)
    return agg_df.reset_index()


@execute_node.register(ops.Reduction, pd.Series)
def execute_reduction_series(op, data, scope=None):
    return getattr(data, type(op).__name__.lower())()


@execute_node.register(ops.Reduction, pd.Series, pd.Series)
def execute_reduction_series_mask(op, data, mask, scope=None):
    return getattr(data[mask], type(op).__name__.lower())()


@execute_node.register(ops.StandardDev, pd.Series)
def execute_standard_dev_series(op, data, scope=None):
    return data.std()


@execute_node.register(ops.StandardDev, pd.Series, pd.Series)
def execute_standard_dev_series_mask(op, data, mask, scope=None):
    return data[mask].std()


@execute_node.register(ops.Variance, pd.Series)
def execute_variance_series(op, data, scope=None):
    return data.var()


@execute_node.register(ops.Variance, pd.Series, pd.Series)
def execute_variance_series_mask(op, data, mask, scope=None):
    return data[mask].var()


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

    ops.Add: operator.add,
    ops.Subtract: operator.sub,
    ops.Multiply: operator.mul,
    ops.Divide: operator.truediv,
    ops.Power: operator.pow,
}


@execute_node.register(
    ops.BinaryOp, pd.Series, (pd.Series, numbers.Real) + six.string_types
)
def execute_binary_operation_series(op, left, right, scope=None):
    return _BINARY_OPERATIONS[type(op)](left, right)


def hashable(obj):
    try:
        hash(obj)
    except TypeError:
        return False
    else:
        return True


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
        for arg in args if isinstance(arg, (ir.Expr, ir.Node))
    ] or [
        scope.get(arg, arg) for arg in args if hashable(arg)
    ]
    return execute_node(op, *evaluated_arguments, scope=scope)


@execute.register(ir.Expr)
def execute_without_scope(expr):
    if isinstance(expr.op(), ir.Literal):
        return execute(expr.op())

    scope = find_data(expr)
    if not scope:
        raise ValueError('No data sources found')
    return execute(expr, scope)
