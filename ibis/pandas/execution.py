import numbers
import operator

import six

import toolz

import pandas as pd

import ibis.expr.types as ir
import ibis.expr.operations as ops

from ibis.pandas.dispatch import execute, execute_node


@execute_node.register(ir.Literal)
def execute_node_literal(op):
    return op.value


@execute_node.register(ops.TableColumn, pd.DataFrame)
def execute_table_column_dataframe(op, data, scope=None):
    return data[op.name]


@execute_node.register(ops.Selection, pd.DataFrame)
def execute_selection_dataframe(op, data, scope=None):
    pass


@execute_node.register(ops.Aggregation, pd.DataFrame)
def execute_aggregation_dataframe(op, data, scope=None):
    pass


@execute_node.register(ops.Reduction, pd.Series)
def execute_reduction_series(op, data, scope=None):
    return getattr(data, type(op).__name__)()


@execute_node.register(ops.Reduction, pd.Series, pd.Series)
def execute_masked_reduction_series(op, data, mask, scope=None):
    return getattr(data[mask], type(op).__name__)()


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

    while stack:
        e = stack.pop()
        node = e.op()

        if node not in seen:
            seen.add(node)

            if hasattr(node, 'source'):
                yield {e: node.source.dictionary[node.name]}

            stack.extend(arg for arg in node.args if isinstance(arg, ir.Expr))


@execute.register(ir.Expr, dict)
def execute_with_scope(expr, scope):
    if expr in scope:
        return scope[expr]

    op = expr.op()
    args = getattr(op, 'inputs', []) or op.args

    evaluated_arguments = [
        execute(arg, scope) if hasattr(arg, 'op') else arg
        for arg in args if isinstance(arg, (ir.Expr, ir.Node))
    ] or [
        scope.get(arg, arg) for arg in args if hashable(arg)
    ]
    return execute_node(op, *evaluated_arguments, scope=scope)


@execute.register(ir.Expr)
def execute_without_scope(expr):
    scope = toolz.merge(find_data(expr))
    if not scope:
        raise ValueError('No data sources found')
    return execute(expr, scope)


@execute.register(ir.ScalarExpr)
def execute_literal(literal):
    return execute_node(literal.op())
