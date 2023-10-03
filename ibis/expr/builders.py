from __future__ import annotations

import contextlib
import functools
import math
import operator
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import ibis.expr.types as ir
from ibis import util
from ibis.common.annotations import annotated, attribute
from ibis.common.deferred import Deferred, deferrable
from ibis.common.exceptions import IbisInputError
from ibis.common.grounds import Concrete
from ibis.common.typing import VarTuple  # noqa: TCH001  # noqa: TCH001
from ibis.expr.operations.core import Value
from ibis.expr.operations.relations import Relation  # noqa: TCH001
from ibis.expr.types.relations import bind_expr

if TYPE_CHECKING:
    from typing_extensions import Self

    from ibis.common.deferred import Resolver


class Builder(Concrete):
    pass


def _find_in_tables(key, tables):
    keys = {ops.TableColumn(table, key) for table in tables if key in table.schema}
    if len(keys) > 1:
        raise com.RelationError(f"Ambiguous reference to column name {key!r}")
    elif not keys:
        raise com.RelationError(f"Column {key!r} not found in any joined tables")
    return keys.pop()


def _resolve(tables, deferred):
    keys = set()

    for table in tables:
        with contextlib.suppress(com.IbisError):
            keys.add(deferred.resolve(table.to_expr()))

    if len(keys) > 1:
        # XXX: better error message
        raise com.RelationError("Deferred expression is ambiguous")
    elif not keys:
        # XXX: better error message
        raise com.RelationError("Deferred expression cannot be resolved")
    return keys.pop().op()


def _clean_join_predicates(tables, predicates):
    import ibis.expr.analysis as an
    import ibis.expr.types as ir

    result = []

    *all_but_last, last = tables
    for pred in predicates:
        if isinstance(pred, tuple):  # t.join(s, [("a", "b")])
            if len(pred) != 2:
                raise com.ExpressionError("Join key tuple must be length 2")
            lk, rk = pred
            lk = _find_in_tables(lk, all_but_last)
            rk = _find_in_tables(rk, [last])
            pred = ops.Equals(lk, rk)
        elif isinstance(pred, str):  # t.join(s, ["a"])
            lk = _find_in_tables(pred, all_but_last)
            rk = _find_in_tables(pred, [last])
            pred = ops.Equals(lk, rk)
        elif pred is True or pred is False:  # t.join(s, True)
            pred = ops.Literal(pred, dtype="bool")
        elif isinstance(pred, Value):  # t.join(s, arbitrary_predicate_expression)
            pass
        elif isinstance(pred, Deferred):
            # t.join(s, t.a == _.b)
            #
            # deferred is resolved against the left set of tables
            #
            # the right isn't included because it should already be present in
            # the deferred expression as a **non**-deferred expression
            pred = _resolve(all_but_last, pred)
        elif not isinstance(pred, ir.Expr):
            raise NotImplementedError(type(pred))
        else:
            pred = pred.op()

        assert isinstance(pred, ops.Node), type(pred)

        if not pred.dtype.is_boolean():
            raise com.ExpressionError("Join predicate must be a boolean expression")

        result.extend(an.flatten_predicate(pred))

    assert all(isinstance(pred, ops.Node) for pred in result)

    return functools.reduce(ops.And, result)


class JoinFragment(Concrete):
    right: ops.Relation
    how: str
    predicate: ops.Value


class JoinBuilder(Builder):
    first: ops.Relation
    rest: VarTuple[JoinFragment] = ()

    def __getattr__(self, name: str):
        return getattr(self.finish(), name)

    def __getitem__(self, key):
        if isinstance(key, (tuple, list)):
            return self.select(*key)
        return self.finish().__getitem__(key)

    def finish(self) -> ir.Table:
        rest, hows, predicates = [], [], []

        for obj in self.rest:
            rest.append(obj.right)
            hows.append(obj.how)
            predicates.append(obj.predicate)

        return ops.JoinProjection(
            table=self.first,
            # select everything, because the user asked us to
            selections=tuple(
                ops.TableColumn(table, name)
                for table in (self.first, *rest)
                for name in table.schema.names
            ),
            rest=rest,
            hows=hows,
            predicates=predicates,
        ).to_expr()

    def __repr__(self):
        return repr(self.finish())

    @attribute
    def tables(self) -> VarTuple[ops.Relation]:
        return (self.first, *map(operator.attrgetter("right"), self.rest))

    def join(self, right, predicates, how: str = "inner") -> Self:
        predicate = _clean_join_predicates(
            tables=(*self.tables, right), predicates=util.promote_list(predicates)
        )
        return self.__class__(
            self.first,
            rest=(*self.rest, JoinFragment(right=right, how=how, predicate=predicate)),
        )

    def select(self, col, *cols):
        import ibis.expr.analysis as an

        rest, hows, predicates = [], [], []

        for obj in self.rest:
            rest.append(obj.right)
            hows.append(obj.how)
            predicates.append(obj.predicate)

        newcols = []
        tables = self.tables
        table_set = frozenset(tables)

        for expr in (col, *cols):
            if isinstance(expr, str):
                new_op = _find_in_tables(expr, tables)
            elif isinstance(expr, Deferred):
                new_op = _resolve(tables, expr)
            else:
                new_op = expr.op()
                for col in an.find_toplevel_columns(new_op):
                    if col.table not in table_set:
                        new_base_col = _find_in_tables(col.name, tables)
                        new_op = an.sub_for(new_op, {col: new_base_col})

            newcols.append(new_op.to_expr())

        return ops.JoinProjection(
            table=self.first,
            selections=newcols,
            rest=rest,
            hows=hows,
            predicates=predicates,
        ).to_expr()


class CaseBuilder(Builder):
    results: VarTuple[Value] = ()
    default: Optional[ops.Value] = None


@deferrable(repr="<case>")
def _finish_searched_case(cases, results, default) -> ir.Value:
    """Finish constructing a SearchedCase expression.

    This is split out into a separate function to allow for deferred arguments
    to resolve.
    """
    return ops.SearchedCase(cases=cases, results=results, default=default).to_expr()


class SearchedCaseBuilder(Builder):
    """A case builder, used for constructing `ibis.case()` expressions."""

    cases: VarTuple[Union[Resolver, ops.Value[dt.Boolean]]] = ()
    results: VarTuple[Union[Resolver, ops.Value]] = ()
    default: Optional[Union[Resolver, ops.Value]] = None

    def when(self, case_expr: Any, result_expr: Any) -> Self:
        """Add a new condition and result to the `CASE` expression.

        Parameters
        ----------
        case_expr
            Predicate expression to use for this case.
        result_expr
            Value when the case predicate evaluates to true.
        """
        return self.copy(
            cases=self.cases + (case_expr,), results=self.results + (result_expr,)
        )

    def else_(self, result_expr: Any) -> Self:
        """Add a default value for the `CASE` expression.

        Parameters
        ----------
        result_expr
            Value to use when all case predicates evaluate to false.
        """
        return self.copy(default=result_expr)

    def end(self) -> ir.Value | Deferred:
        """Finish the `CASE` expression."""
        return _finish_searched_case(self.cases, self.results, self.default)


class SimpleCaseBuilder(Builder):
    """A case builder, used for constructing `Column.case()` expressions."""

    base: ops.Value
    cases: VarTuple[ops.Value] = ()
    results: VarTuple[ops.Value] = ()
    default: Optional[ops.Value] = None

    def when(self, case_expr: Any, result_expr: Any) -> Self:
        """Add a new condition and result to the `CASE` expression.

        Parameters
        ----------
        case_expr
            Expression to equality-compare with base expression. Must be
            comparable with the base.
        result_expr
            Value when the case predicate evaluates to true.
        """
        if not isinstance(case_expr, ir.Value):
            case_expr = ibis.literal(case_expr)
        if not isinstance(result_expr, ir.Value):
            result_expr = ibis.literal(result_expr)

        if not rlz.comparable(self.base, case_expr.op()):
            raise TypeError(
                f"Base expression {rlz._arg_type_error_format(self.base)} and "
                f"case {rlz._arg_type_error_format(case_expr)} are not comparable"
            )
        return self.copy(
            cases=self.cases + (case_expr,), results=self.results + (result_expr,)
        )

    def else_(self, result_expr: Any) -> Self:
        """Add a default value for the `CASE` expression.

        Parameters
        ----------
        result_expr
            Value to use when all case predicates evaluate to false.
        """
        return self.copy(default=result_expr)

    def end(self) -> ir.Value:
        """Finish the `CASE` expression."""
        if (default := self.default) is None:
            default = ibis.null().cast(rlz.highest_precedence_dtype(self.results))
        return ops.SimpleCase(
            cases=self.cases, results=self.results, default=default, base=self.base
        ).to_expr()


RowsWindowBoundary = ops.WindowBoundary[dt.Integer]
RangeWindowBoundary = ops.WindowBoundary[dt.Numeric | dt.Interval]


class WindowBuilder(Builder):
    """An unbound window frame specification.

    Notes
    -----
    This class is patterned after SQL window frame clauses.

    Using `None` for `preceding` or `following` indicates an unbounded frame.

    Use 0 for `CURRENT ROW`.
    """

    how: Literal["rows", "range"] = "rows"
    start: Optional[RangeWindowBoundary] = None
    end: Optional[RangeWindowBoundary] = None
    groupings: VarTuple[Union[str, Resolver, ops.Value]] = ()
    orderings: VarTuple[Union[str, Resolver, ops.Value]] = ()
    max_lookback: Optional[ops.Value[dt.Interval]] = None

    def _maybe_cast_boundary(self, boundary, dtype):
        if boundary.dtype == dtype:
            return boundary
        value = ops.Cast(boundary.value, dtype)
        return boundary.copy(value=value)

    def _maybe_cast_boundaries(self, start, end):
        if start and end:
            dtype = dt.higher_precedence(start.dtype, end.dtype)
            start = self._maybe_cast_boundary(start, dtype)
            end = self._maybe_cast_boundary(end, dtype)
        return start, end

    def _determine_how(self, start, end):
        if start and not start.dtype.is_integer():
            return self.range
        elif end and not end.dtype.is_integer():
            return self.range
        else:
            return self.rows

    def _validate_boundaries(self, start, end):
        start_, end_ = -math.inf, math.inf
        if start and isinstance(lit := start.value, ops.Literal):
            start_ = -lit.value if start.preceding else lit.value
        if end and isinstance(lit := end.value, ops.Literal):
            end_ = -lit.value if end.preceding else lit.value

        if start_ > end_:
            raise IbisInputError(
                "Window frame's start point must be greater than its end point"
            )

    @annotated
    def rows(
        self, start: Optional[RowsWindowBoundary], end: Optional[RowsWindowBoundary]
    ):
        self._validate_boundaries(start, end)
        start, end = self._maybe_cast_boundaries(start, end)
        return self.copy(how="rows", start=start, end=end)

    @annotated
    def range(
        self, start: Optional[RangeWindowBoundary], end: Optional[RangeWindowBoundary]
    ):
        self._validate_boundaries(start, end)
        start, end = self._maybe_cast_boundaries(start, end)
        return self.copy(how="range", start=start, end=end)

    @annotated
    def between(
        self, start: Optional[RangeWindowBoundary], end: Optional[RangeWindowBoundary]
    ):
        self._validate_boundaries(start, end)
        start, end = self._maybe_cast_boundaries(start, end)
        method = self._determine_how(start, end)
        return method(start, end)

    def group_by(self, expr) -> Self:
        return self.copy(groupings=self.groupings + util.promote_tuple(expr))

    def order_by(self, expr) -> Self:
        return self.copy(orderings=self.orderings + util.promote_tuple(expr))

    def lookback(self, value) -> Self:
        return self.copy(max_lookback=value)

    @annotated
    def bind(self, table: Relation):
        groupings = bind_expr(table.to_expr(), self.groupings)
        orderings = bind_expr(table.to_expr(), self.orderings)
        if self.how == "rows":
            return ops.RowsWindowFrame(
                table=table,
                start=self.start,
                end=self.end,
                group_by=groupings,
                order_by=orderings,
                max_lookback=self.max_lookback,
            )
        elif self.how == "range":
            return ops.RangeWindowFrame(
                table=table,
                start=self.start,
                end=self.end,
                group_by=groupings,
                order_by=orderings,
            )
        else:
            raise ValueError(f"Unsupported `{self.how}` window type")


class LegacyWindowBuilder(WindowBuilder):
    def _is_negative(self, value):
        if value is None:
            return False
        if isinstance(value, ir.Scalar):
            value = value.op().value
        return value < 0

    def preceding_following(self, preceding, following, how=None) -> Self:
        preceding_tuple = has_preceding = False
        following_tuple = has_following = False
        if preceding is not None:
            preceding_tuple = isinstance(preceding, tuple)
            has_preceding = True
        if following is not None:
            following_tuple = isinstance(following, tuple)
            has_following = True

        if (preceding_tuple and has_following) or (following_tuple and has_preceding):
            raise IbisInputError(
                "Can only specify one window side when you want an off-center window"
            )
        elif preceding_tuple:
            start, end = preceding
            if end is None:
                raise IbisInputError("preceding end point cannot be None")
            elif self._is_negative(end):
                raise IbisInputError("preceding end point must be non-negative")
            elif self._is_negative(start):
                raise IbisInputError("preceding start point must be non-negative")
            between = (
                None if start is None else ops.WindowBoundary(start, preceding=True),
                ops.WindowBoundary(end, preceding=True),
            )
        elif following_tuple:
            start, end = following
            if start is None:
                raise IbisInputError("following start point cannot be None")
            elif self._is_negative(start):
                raise IbisInputError("following start point must be non-negative")
            elif self._is_negative(end):
                raise IbisInputError("following end point must be non-negative")
            between = (
                ops.WindowBoundary(start, preceding=False),
                None if end is None else ops.WindowBoundary(end, preceding=False),
            )
        elif has_preceding and has_following:
            between = (
                ops.WindowBoundary(preceding, preceding=True),
                ops.WindowBoundary(following, preceding=False),
            )
        elif has_preceding:
            if self._is_negative(preceding):
                raise IbisInputError("preceding end point must be non-negative")
            between = (ops.WindowBoundary(preceding, preceding=True), None)
        elif has_following:
            if self._is_negative(following):
                raise IbisInputError("following end point must be non-negative")
            between = (None, ops.WindowBoundary(following, preceding=False))

        if how is None:
            return self.between(*between)
        elif how == "rows":
            return self.rows(*between)
        elif how == "range":
            return self.range(*between)
        else:
            raise ValueError(f"Invalid window frame type: {how}")
