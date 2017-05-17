import toolz

import pandas as pd

import ibis.expr.types as ir
import ibis.expr.operations as ops

from ibis.pandas.dispatch import execute, execute_node


@execute_node.register(ops.TableColumn, pd.DataFrame)
def execute_table_column_dataframe(op, data, scope=None):
    return data[op.name]


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
    return execute(expr, toolz.merge(find_data(expr)))
