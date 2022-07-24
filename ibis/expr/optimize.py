from __future__ import annotations

import enum
from functools import reduce
from typing import Iterable, Iterator, Sequence

from matchpy import (
    CustomConstraint,
    ManyToOneReplacer,
    Operation,
    Pattern,
    ReplacementRule,
    Wildcard,
)

import ibis
import ibis.common.exceptions as exc
import ibis.expr.analysis as an
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir

_ = Wildcard.dot("_")
name = Wildcard.dot("name")
dtype = Wildcard.dot("dtype")
operand = Wildcard.dot("operand")

left = Wildcard.dot("left")
right = Wildcard.dot("right")

table = Wildcard.dot("table")
table1 = Wildcard.dot("table1")
table2 = Wildcard.dot("table2")

selections = Wildcard.star("selections")
selections1 = Wildcard.star("selections1")
selections2 = Wildcard.star("selections2")

predicates = Wildcard.star("predicates")
predicates1 = Wildcard.star("predicates1")
predicates2 = Wildcard.star("predicates2")

sort_keys = Wildcard.star("sort_keys")
sort_keys1 = Wildcard.star("sort_keys1")
sort_keys2 = Wildcard.star("sort_keys2")

exprs_star0 = Wildcard.star("exprs_star0")
exprs_star1 = Wildcard.star("exprs_star1")
exprs_star2 = Wildcard.star("exprs_star2")

by = Wildcard.star("by")
metrics = Wildcard.star("metrics")
having = Wildcard.star("having")
where = Wildcard.dot("where")

true = ops.Literal.pattern(True, dtype=dtype)
TRUE = ops.Literal(True, dtype=dt.boolean)

false = ops.Literal.pattern(False, dtype=dtype)
FALSE = ops.Literal(False, dtype=dt.boolean)


_REPLACER = ManyToOneReplacer()
_COLLAPSER = ManyToOneReplacer()


def _replace_child(
    exprs: Iterable[ops.Value], table: ops.TableNode
) -> ops.NodeList:
    """Replace child tables of `exprs` with `table`."""
    return ops.NodeList(
        *(
            an.sub_for(
                expr,
                {
                    child: table
                    for child in an.find_immediate_parent_tables(expr)
                },
            )
            for expr in exprs
        )
    )


def opt_pass(replacer):
    """Define an optimization pass.

    Most rules can be part of the generic `rule` optimization pass.

    In the case of collapsing projections, we want only those rules to fire to
    avoid revisiting already-optimized expressions so we have a separate pass
    that run after the generic pass.
    """

    def rule(pattern: Operation, *constraints):
        def wrapper(fn):
            replacer.add(
                ReplacementRule(
                    Pattern(pattern, *map(CustomConstraint, constraints)),
                    fn,
                ),
            )
            return fn

        return wrapper

    return rule


generic = opt_pass(_REPLACER)
collapse = opt_pass(_COLLAPSER)


@generic(ops.And.pattern(_, false))
@generic(ops.Not.pattern(true))
def _always_false(**_):
    """Always-false expressions

    x AND false -> false
       not true -> false
    """
    return FALSE


@generic(ops.And.pattern(operand, true))
@generic(ops.And.pattern(operand, operand))
@generic(ops.Or.pattern(operand, false))
@generic(ops.Or.pattern(operand, operand))
def _logical_redundant(operand, **_):
    """Redundant logical operations.

    x AND true -> x
       x AND x -> x
    x OR false -> x
        x OR x -> x
    """
    return operand


@generic(ops.Or.pattern(_, true))
@generic(ops.Not.pattern(false))
def _always_true(**_):
    """Always true expressions.

    x OR true -> true
    not false -> true
    """
    return TRUE


@generic(ops.Not.pattern(ops.NotEquals.pattern(left, right)))
def _not_not_equals(left, right):
    """Logical negation of != becomes ==."""
    return ops.Equals(left, right)


@generic(ops.Not.pattern(ops.Equals.pattern(left, right)))
def _not_equals(left, right):
    """Logical negation of == becomes !=."""
    return ops.NotEquals(left, right)


@generic(
    ops.Filter.pattern(
        table,
        ops.NodeList.pattern(
            exprs_star0,
            ops.Equals.pattern(operand, operand),
            exprs_star1,
        ),
    ),
    lambda operand: not isinstance(operand.output_dtype, dt.Floating),
)
@generic(
    ops.Filter.pattern(
        table, ops.NodeList.pattern(exprs_star0, true, exprs_star1)
    )
)
def _useless_predicate(table, exprs_star0, exprs_star1, **_):
    """Remove useless predicates from a `Filter` operation.

    If `operand` is a floating point value then this optimization cannot be
    performed due to the possibility of NaNs, since NaN == NaN is always false.
    """
    # empty selections are a no-op on a projection
    # all columns from the child are projected
    return ops.Filter(table, ops.NodeList(*exprs_star0, *exprs_star1))


@generic(
    ops.Filter.pattern(
        ops.Filter.pattern(table, ops.NodeList.pattern(predicates1)),
        ops.NodeList.pattern(predicates2),
    )
)
def _compose_filters(table, predicates1, predicates2):
    """Merge two filter operations into one, combining predicates."""
    return ops.Filter(
        table, ops.NodeList(*predicates1, *_replace_child(predicates2, table))
    )


@generic(
    ops.TableColumn.pattern(table, name),
    lambda table, name: (
        isinstance(table, ops.Projection)
        and table.selections[table.schema._name_locs[name]].inlinable
    ),
)
def _inline_single_column(table, name):
    """Return the column underlying a projected TableColumn."""
    return table.selections[table.schema._name_locs[name]]


@generic(
    ops.Projection.pattern(
        ops.Projection.pattern(table, ops.NodeList.pattern(selections1)),
        ops.NodeList.pattern(selections2),
    ),
)
def _compose_projections(table, selections1, selections2):
    """Return only the final projection from a composition of projections."""
    return ops.Projection(table, selections2 or selections1)


@generic(ops.Projection.pattern(table, ops.NodeList.pattern()))
@generic(ops.Filter.pattern(table, ops.NodeList.pattern()))
@generic(ops.SortBy.pattern(table, ops.NodeList.pattern()))
@generic(
    ops.Selection.pattern(
        table,
        selections=ops.NodeList.pattern(),
        predicates=ops.NodeList.pattern(),
        sort_keys=ops.NodeList.pattern(),
    )
)
def _empty_rel(table):
    """Remove unnecessary projection, filter and sort operations."""
    return table


def _selections_are_table_columns(table, selections):
    """Check whether `selections` are redundant."""
    try:
        return (
            # 1. the number of selections is equal to the number of child table
            #    columns
            len(selections) == len(table.schema)
            # 2. all selections are table columns whose child table
            #    is the child of the projection
            and all(
                (
                    isinstance(sel, ops.TableColumn)
                    and an.find_first_base_table(sel).equals(table)
                )
                for sel in selections
            )
        )
    except exc.IntegrityError:
        return False


@generic(
    ops.Projection.pattern(table, ops.NodeList.pattern(selections)),
    _selections_are_table_columns,
)
@generic(ops.Projection.pattern(table, ops.NodeList.pattern(table)))
def _collapse_projection(table, **_):
    """Remove projections that are strict reprojections from the child."""
    return table


@generic(
    ops.UnionAll.pattern(
        ops.Filter.pattern(table, ops.NodeList.pattern(predicates1)),
        ops.Filter.pattern(table, ops.NodeList.pattern(predicates2)),
    )
)
def _union_all_to_or(table, predicates1, predicates2):
    """Turn a UNION ALL of two filters with the same child table into a single
    filter with the original predicates OR'd."""
    return ops.Filter(
        table,
        ops.NodeList(
            ops.Or(reduce(ops.And, predicates1), reduce(ops.And, predicates2))
        ),
    )


@generic(ops.Union.pattern(table, table))
@generic(ops.Intersection.pattern(table, table))
def _union_intersection_no_op(table):
    """Remove unnecessary UNIONs and INTERSECTs."""
    return table


@generic(ops.Filter.pattern(ops.Difference.pattern(table, table), _))
def _useless_filter(table, **_):
    """Filtering an empty difference operation is useless."""
    return ops.Filter(table, [ibis.literal(False)])


@generic(
    ops.Difference.pattern(
        ops.Filter.pattern(table1, ops.NodeList.pattern(predicates)),
        table2,
    )
)
@generic(
    ops.Difference.pattern(
        table1,
        ops.Filter.pattern(table2, ops.NodeList.pattern(predicates)),
    )
)
def _distribute_difference_filter(table1, predicates, table2):
    """Turn an difference of a table with a filtered table into an
    difference of two tables filtered."""

    table = ops.Difference(table1, table2)
    return ops.Filter(table, _replace_child(predicates, table))


@generic(
    ops.Intersection.pattern(
        ops.Filter.pattern(table1, ops.NodeList.pattern(predicates)),
        table2,
    )
)
@generic(
    ops.Intersection.pattern(
        table1,
        ops.Filter.pattern(table2, ops.NodeList.pattern(predicates)),
    )
)
def _distribute_intersection_filter(table1, predicates, table2):
    """Turn an intersection of a table with a filtered table into an
    intersection of two tables filtered."""
    table = ops.Intersection(table1, table2)
    return ops.Filter(table, _replace_child(predicates, table))


@generic(
    ops.NodeList.pattern(
        exprs_star0, ops.And.pattern(left, right), exprs_star1
    )
)
def _flatten_predicates(exprs_star0, left, right, exprs_star1):
    return ops.NodeList(*exprs_star0, left, right, *exprs_star1)


@enum.unique
class _Side(enum.Enum):
    LEFT = enum.auto()
    RIGHT = enum.auto()
    BOTH = enum.auto()


def _partition_predicate(
    rel1: ops.TableNode,
    rel2: ops.TableNode,
    predicates: Sequence[ops.BooleanValue],
) -> Iterator[_Side]:
    """Determine the origin table of each join predicate in `predicates`."""
    for operand_tables in map(
        frozenset,
        map(an.find_immediate_parent_tables, predicates),
    ):
        is_right_subset = operand_tables.issubset(
            an.find_immediate_parent_tables(rel2)
        )
        if not is_right_subset and operand_tables.issubset(
            an.find_immediate_parent_tables(rel1)
        ):
            yield _Side.LEFT
        elif is_right_subset:
            yield _Side.RIGHT
        else:
            yield _Side.BOTH


def _can_partition_predicate(predicates: Sequence[ops.BooleanValue]) -> bool:
    """Check whether join predicate partitioning is possible.

    Partitioning is possible if any predicate has less than 2
    child tables.

    One child table means at least a column reference.
    Zero child tables means a constant-equivalent.
    """
    return any(
        ntables < 2
        for ntables in map(an.count_immediate_parent_tables, predicates)
    )


@generic(
    ops.InnerJoin.pattern(table1, table2, ops.NodeList.pattern(predicates)),
    _can_partition_predicate,
)
def _filter_inner_join(table1, table2, predicates):
    """Push join predicates to inputs.

    A join predicate can be pushed towards an input if it contains 0 or 1 child
    table references.

    In the case where all predicates can be pushed into inputs the
    resulting join type is a cross join.
    """
    partitions = {_Side.LEFT: [], _Side.RIGHT: [], _Side.BOTH: []}

    # bucket the predicates
    for side, operand in zip(
        _partition_predicate(table1, table2, predicates), predicates
    ):
        partitions[side].append(operand)

    left = ops.Filter(table1, ops.NodeList(*partitions.pop(_Side.LEFT)))
    right = ops.Filter(table2, ops.NodeList(*partitions.pop(_Side.RIGHT)))

    both = partitions.pop(_Side.BOTH)
    op_class = ops.InnerJoin if both else ops.CrossJoin

    # replace old tables with new pushed down tables in remaining predicates
    substitutions = {table1: left, table2: right}
    predicates = ops.NodeList(
        *(an.sub_for(node, substitutions) for node in both)
    )
    return op_class(left=left, right=right, predicates=predicates)


def _no_scalar_subqueries(table, selections, predicates):
    import ibis.expr.lineage as lin

    def fn(op):
        is_correlated = isinstance(op, ops.TableArrayView)
        return lin.halt if is_correlated else lin.proceed, is_correlated

    return not any(lin.traverse(fn, predicates))


@collapse(
    ops.Filter.pattern(
        ops.Projection.pattern(table, ops.NodeList.pattern(selections)),
        ops.NodeList.pattern(predicates),
    ),
    _no_scalar_subqueries,
)
@collapse(
    ops.Projection.pattern(
        ops.Filter.pattern(table, ops.NodeList.pattern(predicates)),
        ops.NodeList.pattern(selections),
    ),
    _no_scalar_subqueries,
)
def _collapse_proj_filt(table, predicates, selections):
    """Combine projection -> filter or filter -> projection into a single
    selection."""
    selections = _replace_child(selections, table)
    predicates = _replace_child(predicates, table)
    return ops.Selection(table, selections=selections, predicates=predicates)


@collapse(
    ops.Projection.pattern(
        ops.SortBy.pattern(table, ops.NodeList.pattern(sort_keys)),
        ops.NodeList.pattern(selections),
    )
)
def _collapse_proj_sort(table, selections, sort_keys):
    return ops.Selection(
        table,
        selections=_replace_child(selections, table),
        sort_keys=sort_keys,
    )


@collapse(
    ops.Selection.pattern(
        table=ops.SortBy.pattern(table, ops.NodeList.pattern(sort_keys1)),
        selections=ops.NodeList.pattern(selections),
        predicates=ops.NodeList.pattern(predicates),
        sort_keys=ops.NodeList.pattern(sort_keys2),
    )
)
def _collapse_selection_sort(
    table, sort_keys1, selections, predicates, sort_keys2
):
    sort_keys = ops.NodeList(
        *sort_keys1,
        *_replace_child(sort_keys2, table),
    )
    res = ops.Selection(
        table=table,
        selections=_replace_child(selections, table),
        predicates=_replace_child(predicates, table),
        sort_keys=sort_keys,
    )
    return res


@collapse(
    ops.Selection.pattern(
        table=ops.Projection.pattern(table, ops.NodeList.pattern(selections1)),
        selections=ops.NodeList.pattern(selections2),
        predicates=ops.NodeList.pattern(predicates),
        sort_keys=ops.NodeList.pattern(sort_keys),
    )
)
def _collapse_selection_projection(
    table, selections1, selections2, predicates, sort_keys
):
    """Collapse projection into an existing selection."""
    selections = _replace_child(selections2, table) or selections1
    return ops.Selection(
        table,
        selections=selections,
        predicates=_replace_child(predicates, table),
        sort_keys=sort_keys,
    )


@collapse(
    ops.Selection.pattern(
        table=ops.Filter.pattern(table, ops.NodeList.pattern(predicates1)),
        selections=ops.NodeList.pattern(selections),
        predicates=ops.NodeList.pattern(predicates2),
        sort_keys=ops.NodeList.pattern(sort_keys),
    )
)
def _collapse_selection_filter(
    table, predicates1, selections, predicates2, sort_keys
):
    """Collapse filters into an existing selection."""
    selections = _replace_child(selections, table)
    predicates = ops.NodeList(
        *predicates1,
        *_replace_child(predicates2, table),
    )
    return ops.Selection(
        table,
        selections=selections,
        predicates=predicates,
        sort_keys=sort_keys,
    )


@collapse(
    ops.Selection.pattern(
        ops.Selection(
            table=table,
            selections=ops.NodeList.pattern(selections1),
            predicates=ops.NodeList.pattern(predicates1),
            sort_keys=ops.NodeList.pattern(sort_keys1),
        ),
        selections=ops.NodeList.pattern(selections2),
        predicates=ops.NodeList.pattern(predicates2),
        sort_keys=ops.NodeList.pattern(sort_keys2),
    )
)
def _collapse_selects(
    table,
    selections1,
    predicates1,
    sort_keys1,
    selections2,
    predicates2,
    sort_keys2,
):
    selections = _replace_child(selections2, table) or selections1
    predicates = ops.NodeList(
        *predicates1,
        *_replace_child(predicates2, table),
    )
    sort_keys = ops.NodeList(*sort_keys1, *_replace_child(sort_keys2, table))
    return ops.Selection(
        table,
        selections=selections,
        predicates=predicates,
        sort_keys=sort_keys,
    )


collapse_projections = _COLLAPSER.replace
generic = _REPLACER.replace


def optimize(expr: ir.Expr) -> ir.Expr:
    """Optimize an expression.

    Parameters
    ----------
    expr
        Expression to optimize

    Returns
    -------
    Expr
        Optimized expression

    Examples
    --------
    >>> import ibis
    >>> from ibis import _
    >>> from ibis.expr.optimize import optimize
    >>> t = ibis.table(dict(a="string", b="float64"), name="t")
    >>> expr = t.mutate(c=_.b + 1.0).select([_.c])
    >>> expr
    r0 := UnboundTable: t
      a string
      b float64

    r1 := Projection[r0]
      selections:
        a: r0.a
        b: r0.b
        c: r0.b + 1.0

    Projection[r1]
      selections:
        c: r1.c
    >>> optimize(expr)
    r0 := UnboundTable: t
      a string
      b float64

    Projection[r0]
      selections:
        c: r0.b + 1.0
    """
    op = expr.op()
    op = generic(op)
    op = collapse_projections(op)
    return op.to_expr()
