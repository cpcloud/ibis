"""User API for grouped data operations."""

from __future__ import absolute_import

from typing import Any, List, Optional, Union

import types

import ibis.expr.analysis as L
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.expr.window as _window
import ibis.util as util

import toolz


def _resolve_exprs(table, exprs):
    exprs = util.promote_list(exprs)
    return table._resolve(exprs)


InputExpr = Union[ir.ValueExpr, List[ir.ValueExpr]]


_function_types = tuple(
    filter(
        None,
        (
            types.BuiltinFunctionType,
            types.BuiltinMethodType,
            types.FunctionType,
            types.LambdaType,
            types.MethodType,
            getattr(types, 'UnboundMethodType', None),
        ),
    )
)


def _get_group_by_key(table: ir.TableExpr, value: Any) -> ir.ValueExpr:
    if isinstance(value, str):
        return table[value]
    if isinstance(value, _function_types):
        return value(table)
    return value


class GroupedTableExpr:
    """Helper intermediate construct."""

    def __init__(
        self,
        table: ir.TableExpr,
        by: List[ir.ColumnExpr],
        having: Optional[List[ir.BooleanScalar]] = None,
        order_by: Optional[List[ir.ColumnExpr]] = None,
        window: Optional[_window.Window] = None,
        **expressions: ir.ValueExpr
    ) -> None:
        self.table = table
        self.by = util.promote_list(by if by is not None else []) + [
            _get_group_by_key(table, v).name(k)
            for k, v in sorted(expressions.items(), key=toolz.first)
        ]
        self._order_by = (
            order_by if order_by is not None else []
        )  # type: List[ir.ColumnExpr]
        self._having = (
            having if having is not None else []
        )  # type: List[ir.BooleanScalar]
        self._window = window

    def __getitem__(self, args):
        # Shortcut for projection with window functions
        return self.projection(list(args))

    def __getattr__(self, attr: str) -> 'GroupedColumn':
        if hasattr(self.table, attr):
            return self._column_wrapper(attr)
        raise AttributeError("GroupBy has no attribute {!r}".format(attr))

    def _column_wrapper(self, attr: str) -> 'GroupedColumn':
        col = self.table[attr]
        if isinstance(col, ir.NumericColumn):
            return GroupedNumbers(col, self)
        else:
            return GroupedColumn(col, self)

    def aggregate(self, metrics=None, **kwargs) -> ir.TableExpr:
        return self.table.aggregate(
            metrics, by=self.by, having=self._having, **kwargs
        )

    def having(
        self, expr: Union[List[ir.BooleanScalar], ir.BooleanScalar]
    ) -> 'GroupedTableExpr':
        """Add a post-aggregation result filter.

        This exists for composability with the group_by API.

        Parameters
        ----------
        expr

        Returns
        -------
        GroupedTableExpr

        """
        exprs = util.promote_list(expr)
        new_having = self._having + exprs
        return GroupedTableExpr(
            self.table,
            self.by,
            having=new_having,
            order_by=self._order_by,
            window=self._window,
        )

    def order_by(
        self, expr: Union[ir.ColumnExpr, List[ir.ColumnExpr]]
    ) -> 'GroupedTableExpr':
        """
        Expressions to use for ordering data for a window function
        computation. Ignored in aggregations.

        Parameters
        ----------
        expr : value expression or list of value expressions

        Returns
        -------
        grouped : GroupedTableExpr
        """
        exprs = util.promote_list(expr)
        new_order = self._order_by + exprs
        return GroupedTableExpr(
            self.table,
            self.by,
            having=self._having,
            order_by=new_order,
            window=self._window,
        )

    def mutate(
        self,
        exprs: Optional[Union[ir.ValueExpr, List[ir.ValueExpr]]] = None,
        **kwargs: ir.ValueExpr
    ) -> ir.TableExpr:
        """Returns a table projection with analytic / window functions applied.

        Parameters
        ----------
        exprs
        kwargs

        Returns
        -------
        TableExpr

        Examples
        --------
        >>> import ibis
        >>> t = ibis.table([
        ...     ('foo', 'string'),
        ...     ('bar', 'string'),
        ...     ('baz', 'double'),
        ... ], name='t')
        >>> t
        UnboundTable[table]
          name: t
          schema:
            foo : string
            bar : string
            baz : float64
        >>> expr = (t.group_by('foo')
        ...          .order_by(ibis.desc('bar'))
        ...          .mutate(qux=lambda x: x.baz.lag(),
        ...                  qux2=t.baz.lead()))
        >>> print(expr)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
        ref_0
        UnboundTable[table]
          name: t
          schema:
            foo : string
            bar : string
            baz : float64
        Selection[table]
          table:
            Table: ref_0
          selections:
            Table: ref_0
            qux = WindowOp[float64*]
              qux = Lag[float64*]
                baz = Column[float64*] 'baz' from table
                  ref_0
                offset:
                  None
                default:
                  None
              <ibis.expr.window.Window object at 0x...>
            qux2 = WindowOp[float64*]
              qux2 = Lead[float64*]
                baz = Column[float64*] 'baz' from table
                  ref_0
                offset:
                  None
                default:
                  None
              <ibis.expr.window.Window object at 0x...>

        """
        all_exprs = [] if exprs is None else util.promote_list(exprs)

        kwd_names = list(kwargs.keys())
        kwd_values = list(kwargs.values())
        kwd_values = self.table._resolve(kwd_values)

        all_exprs.extend(
            v.name(k) for k, v in sorted(zip(kwd_names, kwd_values))
        )

        return self.projection([self.table] + all_exprs)

    def projection(self, exprs: List[ir.ValueExpr]) -> ir.TableExpr:
        """Project expressions in `exprs` to a new ``TableExpr``."""
        w = self._get_window()
        exprs = self.table._resolve(exprs)
        windowed_exprs = [L.windowize_function(expr, w=w) for expr in exprs]
        return self.table.projection(windowed_exprs)

    def _get_window(self) -> _window.Window:
        if self._window is None:
            groups = self.by
            sorts = self._order_by
            preceding, following = None, None
        else:
            w = self._window
            groups = w.group_by + self.by
            sorts = w.order_by + self._order_by
            preceding, following = w.preceding, w.following

        sorts = [ops.to_sort_key(self.table, k) for k in sorts]

        groups = _resolve_exprs(self.table, groups)

        return _window.window(
            preceding=preceding,
            following=following,
            group_by=groups,
            order_by=sorts,
        )

    def over(self, window: _window.Window) -> 'GroupedTableExpr':
        """Add a window clause to an analytic expression."""
        return GroupedTableExpr(
            self.table,
            self.by,
            having=self._having,
            order_by=self._order_by,
            window=window,
        )

    def count(self, metric_name: str = 'count') -> ir.TableExpr:
        """Compute group sizes.

        Parameters
        ----------
        metric_name
            Name to use for the row count metric

        Returns
        -------
        TableExpr
            The aggregated table

        """
        metric = self.table.count().name(metric_name)
        return self.table.aggregate([metric], by=self.by, having=self._having)

    size = count


def _group_agg_dispatch(name: str):
    def wrapper(self: 'GroupedColumn', *args, **kwargs) -> ir.TableExpr:
        column = self.column
        f = getattr(column, name)
        metric = f(*args, **kwargs)
        alias = '{}({})'.format(name, column.get_name())
        return self.parent.aggregate(metric.name(alias))

    wrapper.__name__ = name
    return wrapper


class GroupedColumn:
    __slots__ = 'column', 'parent'

    def __init__(
        self, column: ir.ColumnExpr, parent: 'GroupedTableExpr'
    ) -> None:
        self.column = column
        self.parent = parent

    size = count = _group_agg_dispatch('count')
    min = _group_agg_dispatch('min')
    max = _group_agg_dispatch('max')
    approx_nunique = _group_agg_dispatch('approx_nunique')
    approx_median = _group_agg_dispatch('approx_median')
    group_concat = _group_agg_dispatch('group_concat')

    def summary(self, exact_nunique: bool = False) -> ir.TableExpr:
        metric = self.column.summary(exact_nunique=exact_nunique)
        return self.parent.aggregate(metric)


class GroupedNumbers(GroupedColumn):
    __slots__ = ()

    mean = _group_agg_dispatch('mean')
    sum = _group_agg_dispatch('sum')

    def summary(self, exact_nunique: bool = False) -> ir.TableExpr:
        metric = self.column.summary(exact_nunique=exact_nunique)
        return self.parent.aggregate(metric)
