"""The pandas backend is a departure from the typical ibis backend in that it
doesn't compile to anything, and the execution of the ibis expression
is under the purview of ibis itself rather than executing SQL on a server.

Design
------
The pandas backend uses a technique called `multiple dispatch
<https://en.wikipedia.org/wiki/Multiple_dispatch>`_, implemented in a
third-party open source library called `multipledispatch
<https://github.com/mrocklin/multipledispatch>`_.

Multiple dispatch is a generalization of standard single-dispatch runtime
polymorphism to multiple arguments.

Compilation
-----------
This is a no-op because we execute ibis expressions directly.

Execution
---------
Execution is divided into different dispatched functions, each arising from
different a use case.

A top level dispatched function named ``execute`` with two signatures exists
to provide a single API for executing an ibis expression.

The general flow of execution is:

::
       If the current operation is in scope:
           return it
       Else:
           execute the arguments of the current node

       execute the current node with its executed arguments

Specifically, execute is comprised of a series of steps that happen at
different times during the loop.

1. ``pre_execute``
------------------

Second, at the beginning of the main execution loop, ``pre_execute`` is called.
This function serves a similar purpose to ``data_preload``, the key difference
being that ``pre_execute`` is called *every time* there's a call to execute.

By default this function does nothing.

2. ``execute_node``
-------------------

Finally, when an expression is ready to be evaluated we call
:func:`~ibis.pandas.core.execute` on the expressions arguments and then
:func:`~ibis.pandas.dispatch.execute_node` on the expression with its
now-materialized arguments.
"""

from __future__ import absolute_import

import collections
import numbers
import datetime

import six

import numpy as np

import toolz

import ibis
import ibis.common as com

import ibis.expr.types as ir
import ibis.expr.operations as ops
import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.window as win

from ibis.compat import functools
from ibis.client import find_backends

import ibis.pandas.aggcontext as agg_ctx
from ibis.pandas.dispatch import (
     execute_node, pre_execute, post_execute, execute_literal
)


integer_types = six.integer_types + (np.integer,)
floating_types = numbers.Real,
numeric_types = integer_types + floating_types
boolean_types = bool, np.bool_
fixed_width_types = numeric_types + boolean_types
temporal_types = (
    datetime.datetime, datetime.date, datetime.timedelta, datetime.time,
    np.datetime64, np.timedelta64,
)
scalar_types = fixed_width_types + temporal_types
simple_types = scalar_types + six.string_types

_VALID_INPUT_TYPES = (
    ibis.client.Client, ir.Expr, dt.DataType, type(None), win.Window
) + scalar_types


def execute_with_scope(expr, scope, context=None, **kwargs):
    """Execute an expression `expr`, with data provided in `scope`.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The expression to execute.
    scope : collections.Mapping
        A dictionary mapping :class:`~ibis.expr.operations.Node` subclass
        instances to concrete data such as a pandas DataFrame.
    context : Optional[ibis.pandas.aggcontext.AggregationContext]

    Returns
    -------
    result : scalar, pd.Series, pd.DataFrame
    """
    op = expr.op()

    # Call pre_execute, to allow clients to intercept the expression before
    # computing anything *and* before associating leaf nodes with data. This
    # allows clients to provide their own scope.
    clients = list(find_backends(expr))
    scope = toolz.merge(
        scope,
        factory=type(scope)
    )
    new_scope = toolz.merge(
        scope,
        *map(
            functools.partial(pre_execute, op, scope=scope, **kwargs),
            clients
        ),
        factory=type(scope)
    )
    result = execute_zigzag(
        expr, new_scope, context=context, clients=clients, **kwargs)
    return post_execute(
        op, result, scope=scope, context=context, clients=clients, **kwargs)


def execute_zigzag(expr, scope, context=None, clients=None, **kwargs):
    # base case: our op has been computed (or is a leaf data node), so
    # return the corresponding value
    op = expr.op()
    if op in scope:
        return scope[op]

    if context is None:
        context = agg_ctx.Summarize()

    new_expr, new_scope = execute_until_type_change(
        expr, scope, context=context, **kwargs
    )

    if clients is None:
        clients = list(find_backends(new_expr))

    pre_executor = functools.partial(
        pre_execute, new_expr.op(), scope=scope, **kwargs
    )
    new_scope = toolz.merge(
        new_scope,
        *map(pre_executor, clients),
        factory=type(new_scope)
    )

    return execute_zigzag(
        new_expr, new_scope, context=context, clients=clients, **kwargs)


def type_sort_key(t):
    """Key function used to sort a list of type objects

    Parameters
    ----------
    t : object

    Returns
    -------
    name : str
        The name of the type `t`
    """
    return t.__class__.__name__


def is_computable_arg(op, arg):
    """Is `arg` a valid input to an ``execute_node`` rule?

    Parameters
    ----------
    arg : object
        Any Python object
    """
    # TODO(phillipc): Add an "_inputs" or "_required_args" property instead of
    # this curried function
    return (
        isinstance(op, (ops.ValueList, ops.WindowOp)) or
        isinstance(arg, _VALID_INPUT_TYPES)
    )


def execute_until_type_change(expr, scope, context=None, **kwargs):
    """Recursively compute `expr` until there's a change in the type of the
    result.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
    scope : Mapping[Union[ibis.expr.operation.sNode, object], object]
    context : Optional[ibis.pandas.aggcontext.AggregationContext]
    kwargs : Dict[str, object]
    """
    op = expr.op()
    factory = type(scope)

    if op in scope:
        return expr, scope
    elif isinstance(op, ops.Literal):
        return expr, {
            op: execute_literal(
                op, op.value, expr.type(),
                context=context, **kwargs
            )
        }

    args = op.inputs
    is_computable_argument = functools.partial(is_computable_arg, op)
    computable_args = list(filter(is_computable_argument, args))

    # recursively compute the op's arguments
    exprs_and_scopes = [
        execute_until_type_change(arg, scope, context=context, **kwargs)
        if hasattr(arg, 'op') else (arg, factory([(arg, arg)]))
        for arg in computable_args
    ]

    if not exprs_and_scopes:
        raise com.UnboundExpressionError(
            'Unable to find data for expression:\n{}'.format(repr(expr))
        )

    new_exprs, new_scopes = zip(*exprs_and_scopes)
    assert len(computable_args) == len(new_exprs)

    new_scope = toolz.merge(new_scopes, factory=factory)

    old_exprs = {e.op() if hasattr(e, 'op') else e for e in computable_args}
    new_exprs = frozenset(new_scope.keys())

    type_changed = not old_exprs.issubset(new_exprs)
    if type_changed:
        return expr, new_scope

    # Compute our op with its computed arguments
    data = [
        new_scope[arg.op()] if hasattr(arg, 'op') else arg
        for arg in computable_args
    ]
    result = execute_node(op, *data, scope=scope, context=context, **kwargs)
    return expr, factory([(op, result)])


def execute(expr, params=None, scope=None, context=None, **kwargs):
    """Execute an expression against data that are bound to it. If no data
    are bound, raise an Exception.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        The expression to execute
    params : Mapping[Expr, object]
    scope : Mapping[ibis.expr.operations.Node, object]
    context : Optional[ibis.pandas.aggcontext.AggregationContext]

    Returns
    -------
    result : scalar, pd.Series, pd.DataFrame

    Raises
    ------
    ValueError
        * If no data are bound to the input expression
    """
    factory = collections.OrderedDict

    if scope is None:
        scope = factory()

    if params is None:
        params = factory()

    params = {k.op() if hasattr(k, 'op') else k: v for k, v in params.items()}

    new_scope = toolz.merge(scope, params, factory=factory)

    # By default, our aggregate functions are N -> 1
    return execute_with_scope(
        expr,
        new_scope,
        context=context if context is not None else agg_ctx.Summarize(),
        **kwargs
    )
