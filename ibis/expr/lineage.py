import queue as q
from itertools import chain
from toolz import identity
from collections import deque

import ibis.expr.types as ir
import ibis.expr.operations as ops


def roots(expr, types=(ops.PhysicalTable,)):
    """Yield every node of a particular type on which an expression depends.

    Parameters
    ----------
    expr : Expr
        The expression to analyze
    types : tuple(type), optional, default (:mod:`ibis.expr.operations.PhysicalTable`,)
        The node types to traverse

    Yields
    ------
    table : Expr
        Unique node types on which an expression depends

    Notes
    -----
    If your question is: "What nodes of type T does `expr` depend on?", then
    you've come to the right place. By default, we yield the physical tables
    that an expression depends on.
    """
    seen = set()

    stack = list(reversed(expr.op().root_tables()))

    while stack:
        table = stack.pop()

        if table not in seen:
            seen.add(table)
            yield table

        # flatten and reverse so that we traverse in preorder
        stack.extend(reversed(list(chain.from_iterable(
            arg.op().root_tables() for arg in table.flat_args()
            if isinstance(arg, types)
        ))))


class Container(object):

    __slots__ = 'data',

    def __init__(self, data):
        self.data = deque(data or [])

    def append(self, item):
        self.data.append(item)

    def __len__(self):
        return len(self.data)

    def get(self):
        raise NotImplementedError('Child classes must implement get')

    @property
    def visitor(self):
        raise NotImplementedError('Child classes must implement visitor')

    def extend(self, items):
        return self.data.extend(items)


class Stack(Container):

    """Wrapper around a list to provide a common API for graph traversal
    """

    __slots__ = 'data',

    def get(self):
        return self.data.pop()

    @property
    def visitor(self):
        return reversed


class Queue(Container):

    """Wrapper around a queue.Queue to provide a common API for graph traversal
    """

    __slots__ = 'data',

    def get(self):
        return self.data.popleft()

    @property
    def visitor(self):
        return identity


def _get_args(op, name=None):
    """Hack to get relevant arguments for lineage computation.

    We need a better way to determine the relevant arguments of an expression.
    """
    # Could use multipledispatch here to avoid the pasta
    if isinstance(op, ops.Selection):
        result = op.selections

        # if Selection.selections is always columnar, could use an
        # OrderedDict to prevent scanning the whole thing
        if name is not None:
            return [col for col in result if col._name == name]
        return result
    elif isinstance(op, ops.Aggregation):
        result = list(chain(op.by, op.agg_exprs))
        if name is not None:
            return [col for col in result if col._name == name]
        return result
    else:
        return op.args


def traverse(expr, container=Stack, node_types=(ir.Node,)):
    c = container([expr])

    seen = set()

    # while we haven't visited everything
    while c:
        # import pdb
        # pdb.set_trace()
        node = c.get()

        if node not in seen:
            seen.add(node)
            if isinstance(node.op(), node_types):
                yield node

        # add our dependencies to the container if they match our name
        # and are ibis expressions
        c.extend(
            arg for arg in c.visitor(_get_args(node.op()))
            if isinstance(arg, ir.Expr)
        )



def lineage(expr, container=Stack, types=(ir.Expr,)):
    """Yield the path of the expression tree that comprises a column
    expression.

    Parameters
    ----------
    expr : Expr
        An ibis expression. It must be an instance of
        :class:`ibis.expr.types.ArrayExpr`.
    container : Container, {Stack, Queue}
        Stack for depth-first traversal, and Queue for breadth-first.
        Depth-first will reach root table nodes before continuing on to other
        columns in a column that is derived from multiple column. Breadth-
        first will traverse all columns at each level before reaching root
        tables.

    Yields
    ------
    node : Expr
        A column and its dependencies
    """
    if not isinstance(expr, ir.ArrayExpr):
        raise TypeError('Input expression must be an instance of ArrayExpr')

    c = container([(expr, expr._name)])

    seen = set()

    # while we haven't visited everything
    while c:
        node, name = c.get()

        if node not in seen:
            seen.add(node)
            yield node

        # add our dependencies to the container if they match our name
        # and are ibis expressions
        c.extend(
            (arg, getattr(arg, '_name', name))
            for arg in c.visitor(_get_args(node.op(), name))
            if isinstance(arg, types)
        )
