import collections

from itertools import chain
from typing import (  # noqa: F401
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from toolz import identity, compose

import ibis.expr.types as ir
import ibis.expr.operations as ops


def roots(
    expr: ir.Expr,
    types: Sequence[Type[ops.PhysicalTable]] = (ops.PhysicalTable,),
) -> Iterator[ops.TableNode]:
    """Yield every node of a particular type on which an expression depends.

    Parameters
    ----------
    expr : Expr
        The expression to analyze
    types : tuple(type), optional, default
            (:mod:`ibis.expr.operations.PhysicalTable`,)
        The node types to traverse

    Yields
    ------
    PhysicalTable
        Unique node types on which an expression depends

    Notes
    -----
    If your question is: "What nodes of type T does `expr` depend on?", then
    you've come to the right place. By default, we yield the physical tables
    that an expression depends on.

    """
    seen = set()  # type: Set[ops.PhysicalTable]

    tuple_of_types = tuple(types)
    stack = [
        arg
        for arg in reversed(expr.op().root_tables())
        if isinstance(arg, tuple_of_types)
    ]

    while stack:
        table = stack.pop()

        if table not in seen:
            seen.add(table)
            yield table

        # flatten and reverse so that we traverse in preorder
        stack.extend(
            reversed(
                list(
                    chain.from_iterable(
                        arg.op().root_tables()
                        for arg in table.flat_args()
                        if isinstance(arg, tuple_of_types)
                    )
                )
            )
        )


T = TypeVar('T')
U = TypeVar('U')


class Container(Generic[T]):
    __slots__ = ('data',)

    def __init__(self, data: Iterable[T]) -> None:
        self.data = collections.deque(self.visitor(data))

    def append(self, item: T) -> None:
        self.data.append(item)

    def __len__(self) -> int:
        return len(self.data)

    def get(self) -> T:
        raise NotImplementedError(
            'Child class {!r} must implement get'.format(type(self).__name__)
        )

    @property
    def visitor(self) -> Callable[[Iterable[U]], Iterable[U]]:
        raise NotImplementedError(
            'Child class {!r} must implement visitor'.format(
                type(self).__name__
            )
        )

    def extend(self, items: Iterable[T]) -> None:
        self.data.extend(items)


class Stack(Container[T]):
    """Wrapper around a deque to provide convenient depth-first traversal."""

    __slots__ = ('data',)

    def get(self) -> T:
        return self.data.pop()

    @property
    def visitor(self) -> Callable[[Iterable[U]], Iterable[U]]:
        return compose(reversed, list)


class Queue(Container[T]):
    """Wrapper around a deque to provide convenient breadth-first traversal."""

    __slots__ = ('data',)

    def get(self) -> T:
        return self.data.popleft()

    @property
    def visitor(self) -> Callable[[Iterable[U]], Iterable[U]]:
        return identity


def _get_args(op: ops.Node, name: Optional[str]) -> List[ir.Expr]:
    """Hack to get relevant arguments for lineage computation.

    We need a better way to determine the relevant arguments of an expression.

    """
    # Could use multipledispatch here to avoid the pasta
    if isinstance(op, ops.Selection):
        assert name is not None, 'name is None'
        result = op.selections

        # if Selection.selections is always columnar, could use an
        # OrderedDict to prevent scanning the whole thing
        return [col for col in result if col._name == name]
    elif isinstance(op, ops.Aggregation):
        assert name is not None, 'name is None'
        return [col for col in chain(op.by, op.metrics) if col._name == name]
    else:
        return op.args


LineagePair = Tuple[ir.Expr, Optional[str]]


def lineage(
    expr: ir.ColumnExpr,
    container: Type[Container[LineagePair]] = Stack[LineagePair],
) -> Iterator[ir.Expr]:
    """Yield the path of the expression tree that comprises a column
    expression.

    Parameters
    ----------
    expr
        An ibis expression. It must be an instance of
        :class:`ibis.expr.types.ColumnExpr`.
    container
        Stack for depth-first traversal, Queue for breadth-first.
        Depth-first will reach root table nodes before continuing on to other
        columns in a column that is derived from multiple column. Breadth-
        first will traverse all columns at each level before reaching root
        tables.

    Yields
    ------
    Expr
        A column and its dependencies

    """
    if not isinstance(expr, ir.ColumnExpr):
        raise TypeError('Input expression must be an instance of ColumnExpr')

    c = container([(expr, expr._name)])

    seen = set()  # type: Set[ir.Expr]

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
            if isinstance(arg, ir.Expr)
        )


# these could be callables instead
proceed = True
halt = False

Output = TypeVar('Output')


def traverse(
    fn: Callable[[ir.Expr], Tuple[bool, Output]],
    expr: ir.Expr,
    type: Type[ir.Expr] = ir.Expr,
    container: Type[Container[ir.Expr]] = Stack[ir.Expr],
) -> Iterator[Output]:
    """Utility for generic expression tree traversal

    Parameters
    ----------
    fn
        This function will be applied on each expressions, it must
        return a tuple. The first element of the tuple controls the
        traversal, and the second is the result if its not None.
    expr
        The traversable expression or a list of expressions.
    type
        Only the instances if this type are traversed.
    container
        Defines the traversing order. Defaults to Stack, providing depth-first
        traversal.

    """
    args = expr if isinstance(expr, collections.Iterable) else [expr]
    todo = container(arg for arg in args if isinstance(arg, type))
    seen = set()  # type: Set[ops.Node]

    while todo:
        expr = todo.get()
        op = expr.op()
        if op in seen:
            continue
        else:
            seen.add(op)

        control, result = fn(expr)
        if result is not None:
            yield result

        if control is not halt:
            if control is proceed:
                args = op.flat_args()
            else:
                raise TypeError(
                    'First item of the returned tuple must be True or False'
                )

            todo.extend(
                arg for arg in todo.visitor(args) if isinstance(arg, type)
            )
