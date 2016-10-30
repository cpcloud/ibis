from toolz import identity, curry, flip, compose
from operator import getitem, methodcaller

from ibis.compat import lzip
import ibis.common as com
import ibis.expr.analysis as L
import ibis.expr.analytics as analytics

import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.expr.format

import ibis.sql.transforms as transforms
import ibis.util as util
import ibis

from multipledispatch import dispatch



@dispatch(object, object)
def adapt(expr, dialect):
    """Non-table expressions need to be adapted to some well-formed table
    expression, along with a way to adapt the results to the desired
    arity (whether array-like or scalar, for example)

    Canonical case is scalar values or arrays produced by some reductions
    (simple reductions, or distinct, say)
    """
    raise com.TranslationError(
        'No adapt rule for expressions of type {0} found'.format(
            type(expr).__name__
        )
    )


@adapt.register(ir.TableExpr, object)
def adapt_table_expr(expr, dialect):
    return expr, identity


@adapt.register(ir.ScalarExpr, object)
def adapt_scalar_expr(expr, dialect):
    name = 'tmp'

    if L.is_scalar_reduce(expr):
        result, name = L.reduction_to_aggregation(expr, default_name='tmp')
    else:
        base_table = ir.find_base_table(expr)
        if base_table is None:
            # expr with no table refs
            result = expr.name(name)
        else:
            raise NotImplementedError(expr._repr())
    return result, compose(flip(getitem, 0), flip(getitem, name))


@adapt.register(ir.AnalyticExpr, object)
def adapt_analytic_expr(expr, dialect):
    return expr.to_aggregation(), identity


@adapt.register(ir.ExprList, object)
def adapt_expr_list(expr, dialect):
    exprs = expr.exprs()

    is_aggregation = True
    any_aggregation = False

    for x in exprs:
        if not L.is_scalar_reduce(x):
            is_aggregation = False
        else:
            any_aggregation = True

    if is_aggregation:
        table = ir.find_base_table(exprs[0])
        return table.aggregate(exprs), identity
    elif not any_aggregation:
        return expr, identity

    raise NotImplementedError(expr._repr())


@adapt.register(ir.ArrayExpr, object)
def adapt_array_expr(expr, dialect):
    op = expr.op()
    name = 'tmp'

    if isinstance(op, ops.TableColumn):
        name = op.name
        table_expr = op.table[[name]]
    else:
        # Something more complicated.
        base_table = L.find_source_table(expr)

        if isinstance(op, ops.DistinctArray):
            expr = op.arg
            try:
                name = expr.get_name()
            except Exception:
                pass
            method = methodcaller('distinct')
        else:
            method = identity

        table_expr = method(base_table[expr.name(name)])

    result_handler = flip(getitem, name)

    return table_expr, result_handler
