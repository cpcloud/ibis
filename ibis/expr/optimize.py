from __future__ import annotations

import itertools

from matchpy import CustomConstraint, Operation, Pattern, Wildcard, replace_all

import ibis.expr.analysis as an
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir

_ = Wildcard.dot('_')
dtype = Wildcard.dot("dtype")
x = Wildcard.dot('x')
y = Wildcard.dot('y')
operand = Wildcard.dot('operand')

left = Wildcard.dot('left')
right = Wildcard.dot('right')

table = Wildcard.dot('table')

selections = Wildcard.plus('selections')
sels1 = Wildcard.plus('sels1')
sels2 = Wildcard.plus('sels2')

predicates = Wildcard.plus('predicates')
preds1 = Wildcard.plus('preds1')
preds2 = Wildcard.plus('preds2')

sort_keys = Wildcard.plus('sort_keys')

exprs00 = Wildcard.star("exprs00")
exprs01 = Wildcard.star("exprs01")

true = ops.Literal.pattern(True, dtype=dtype)
false = ops.Literal.pattern(False, dtype=dtype)
zero = ops.Literal.pattern(0, dtype=dtype)
one = ops.Literal.pattern(1, dtype=dtype)


AND_RULES = (
    #  ReplacementRule(Pattern(And()), lambda: true),
    #  ReplacementRule(Pattern(And(expr, expr)), lambda expr: expr),
    #  ReplacementRule(
    #      Pattern(And(exprs0, true, exprs1)),
    #      lambda exprs0, exprs1: And(*exprs0, *exprs1),
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs1, true, exprs0)),
    #      lambda exprs1, exprs0: And(*exprs1, *exprs0),
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs0, false, exprs1)),
    #      lambda **_: false,
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs1, false, exprs0)),
    #      lambda **_: false,
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs0, null, exprs1)),
    #      lambda **_: null,
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs1, null, exprs0)),
    #      lambda **_: null,
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs1, expr, exprs00, expr, exprs0)),
    #      lambda exprs1, expr, exprs00, exprs0: And(
    #          *exprs1,
    #          expr,
    #          *exprs00,
    #          *exprs0,
    #      ),
    #  ),
    #  ReplacementRule(
    #      Pattern(And(exprs0, expr, exprs00, expr, exprs1)),
    #      lambda exprs0, expr, exprs00, exprs1: And(
    #          *exprs0,
    #          expr,
    #          *exprs00,
    #          *exprs1,
    #      ),
    #  ),
)

OR_RULES = (
    # Or identity
    #  ReplacementRule(Pattern(Or()), lambda: false),
    #  ReplacementRule(Pattern(Or(expr, expr)), lambda expr: expr),
    #  ReplacementRule(
    #      Pattern(Or(exprs0, false, exprs1)),
    #      lambda exprs0, exprs1: Or(*exprs0, *exprs1),
    #  ),
    #  ReplacementRule(
    #      Pattern(Or(exprs1, false, exprs0)),
    #      lambda exprs1, exprs0: Or(*exprs1, *exprs0),
    #  ),
    #  ReplacementRule(
    #      Pattern(Or(exprs0, null, exprs1)),
    #      lambda exprs0, exprs1: Or(*exprs0, *exprs1),
    #  ),
    #  ReplacementRule(
    #      Pattern(Or(exprs1, null, exprs0)),
    #      lambda exprs1, exprs0: Or(*exprs1, *exprs0),
    #  ),
    #  ReplacementRule(Pattern(Or(exprs0, true, exprs1)), lambda **_: true),
    #  ReplacementRule(Pattern(Or(exprs1, true, exprs0)), lambda **_: true),
)

COMPARISON_RULES = (
    #  # a > b => b < a
    #  (
    #      Pattern(ops.Greater.pattern(left, right)),
    #      lambda left, right: ops.Less(right, left),
    #  ),
    #  # a >= b => b <= a
    #  (
    #      Pattern(ops.GreaterEqual.pattern(left, right)),
    #      lambda left, right: ops.LessEqual(right, left),
    #  ),
)

LOGICAL_RULES = (
    #  # not (a != b) => a == b
    #  ReplacementRule(
    #      Pattern(Not(Ne(expr1, expr2))),
    #      lambda expr1, expr2: Eq(expr1, expr2),
    #  ),
    #  # not (a == b) => a != b
    #  ReplacementRule(
    #      Pattern(Not(Eq(expr1, expr2))),
    #      lambda expr1, expr2: Ne(expr1, expr2),
    #  ),
    #  # not (not a) => a
    #  ReplacementRule(Pattern(Not(Not(expr))), lambda expr: expr),
    #  # not True => False
    #  ReplacementRule(Pattern(Not(true)), lambda: false),
    #  # not False => True
    #  ReplacementRule(Pattern(Not(false)), lambda: true),
    #  # not None => None
    #  ReplacementRule(Pattern(Not(null)), lambda: null),
)


RULES = ()


def rule(pattern: Operation | Pattern, *constraints):
    def wrapper(fn):
        global RULES
        RULES += ((Pattern(pattern, *map(CustomConstraint, constraints)), fn),)
        return fn

    return wrapper


@rule(
    ops.Filter.pattern(
        table,
        predicates=(exprs00, ops.Equals.pattern(operand, operand), exprs01),
    ),
    lambda operand: not isinstance(operand.output_dtype, dt.Floating),
)
@rule(ops.Filter.pattern(table, predicates=(exprs00, true, exprs01)))
def _useless_predicate(table, exprs00, exprs01, **_):
    # empty selections are a no-op on a projection
    # all columns from the child are projected
    return ops.Filter(table=table, predicates=(*exprs00, *exprs01))


@rule(ops.Filter.pattern(ops.Filter.pattern(table, preds1), preds2))
def _compose_filters(table, preds1, preds2):
    preds1 = tuple(itertools.chain.from_iterable(preds1))
    preds2 = tuple(itertools.chain.from_iterable(preds2))
    return ops.Filter(
        table=table,
        predicates=(
            preds1
            + tuple(
                an.sub_for(pred, {child: table for child in children})
                for pred, children in zip(
                    preds2, map(an.find_immediate_parent_tables, preds2)
                )
            )
        ),
    )


def _is_projection_subset(table, sels1, sels2):
    sels1 = tuple(itertools.chain.from_iterable(sels1))
    sels2 = tuple(itertools.chain.from_iterable(sels2))
    sub = frozenset(
        an.sub_for(sel, {child: table for child in children})
        for sel, children in zip(
            sels2,
            map(an.find_immediate_parent_tables, sels2),
        )
    )
    breakpoint()
    return sub.issubset(sels1)


@rule(
    ops.Projection.pattern(ops.Projection.pattern(table, sels1), sels2),
    _is_projection_subset,
)
def _compose_projections(table, sels2, **_):
    sels2 = tuple(itertools.chain.from_iterable(sels2))
    new_sels = tuple(
        an.sub_for(sel, {child: table for child in children})
        for sel, children in zip(
            sels2,
            map(an.find_immediate_parent_tables, sels2),
        )
    )
    return ops.Projection(table, new_sels)


@rule(ops.Projection.pattern(table, ()))
@rule(ops.Filter.pattern(table, ()))
@rule(ops.SortBy.pattern(table, ()))
def _empty_rel(table):
    return table


def selections_are_table_columns(table, selections):
    ncolumns = len(table.schema)
    selections = tuple(itertools.chain.from_iterable(selections))
    return (
        len(selections) == ncolumns
        and sum(
            (
                isinstance(sel, ops.TableColumn)
                and an.find_first_base_table(sel).equals(table)
            )
            for sel in selections
        )
        == ncolumns
    )


@rule(ops.Projection.pattern(table, selections), selections_are_table_columns)
def _collapse_projections(table, selections):
    return table


RELATION_RULES = [
    # diff(select(t, p), select(s, p)) => select(diff(t, s), p)
    #      ReplacementRule(
    #          Pattern(Difference(Select(rel1, predicate), Select(rel2, predicate))),  # noqa: E501
    #          lambda rel1, rel2, predicate: Select(
    #              Difference(rel1, rel2),
    #              predicate,
    #          ),
    #      ),
    #      # diff(select(rel1), rel2) => select(diff(t, s), p)
    #      ReplacementRule(
    #          Pattern(Difference(Select(rel1, predicate), rel2)),
    #          lambda rel1, predicate, rel2: Select(
    #              Difference(rel1, rel2),
    #              predicate,
    #          ),
    #      ),
    #      # union(select(t, p), select(s, p)) => select(union(t, s), p)
    #      ReplacementRule(
    #          Pattern(Union(Select(rel1, predicate), Select(rel2, predicate))),  # noqa: E501
    #          lambda predicate, rel1, rel2: Select(
    #              Union(rel1, rel2),
    #              predicate,
    #          ),
    #      ),
    #      # intersection(select(t, p), select(s, p)) => select(intersection(t, s), p)  # noqa: E501
    #      ReplacementRule(
    #          Pattern(
    #              Intersection(
    #                  Select(rel1, predicate),
    #                  Select(rel2, predicate),
    #              )
    #          ),
    #          lambda predicate, rel1, rel2: Select(
    #              Intersection(rel1, rel2),
    #              predicate,
    #          ),
    #      ),
    #      # intersect(select(rel1), rel2) => select(intersect(t, s), p)
    #      ReplacementRule(
    #          Pattern(Intersection(Select(predicate, rel1), rel2)),
    #          lambda predicate, rel1, rel2: Select(
    #              Intersection(rel1, rel2),
    #              predicate,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(Intersection(rel1, Select(rel2, predicate))),
    #          lambda rel1, predicate, rel2: Select(
    #              Intersection(rel1, rel2),
    #              predicate,
    #          ),
    #      ),
    #      # avoid unions by turning them into a filter with ORs where possible
    #      ReplacementRule(
    #          Pattern(Union(Select(rel, predicate1), Select(rel, predicate2))),  # noqa: E501
    #          lambda predicate1, rel1, predicate2, rel2: Select(
    #              rel, Or(predicate1, predicate2)
    #          ),
    #      ),
    #      # project before filter if the filter predicate refers to a subset of the  # noqa: E501
    #      # projection columns
    #      ReplacementRule(
    #          Pattern(
    #              Project(Select(rel, predicate), Exprs(exprs1)),
    #              CustomConstraint(
    #                  lambda exprs1, predicate, rel: predicate.is_subset_of(exprs1)  # noqa: E501
    #              ),
    #          ),
    #          lambda exprs1, predicate, rel: Select(
    #              Project(rel, Exprs(*exprs1)),
    #              predicate,
    #          ),
    #      ),
    #      # turn a read -> filter -> project into a project of an optimized read  # noqa: E501
    #      # the projection is necessary because we're using expressions
    #      ReplacementRule(
    #          Pattern(Project(Select(Read(table), predicate), Exprs(exprs1))),
    #          lambda table, exprs1, predicate: Project(
    #              OptimizedRead(table, Refs(*find_refs(exprs1)), predicate),
    #              Exprs(*exprs1),
    #          ),
    #      ),
    #      # turn a read -> project -> filter into a project of an optimized read  # noqa: E501
    #      # the projection is necessary because we're using expressions
    #      ReplacementRule(
    #          Pattern(Select(Project(Read(table), Exprs(exprs1)), predicate)),
    #          lambda table, exprs1, predicate: Project(
    #              OptimizedRead(table, Refs(*find_refs(exprs1)), predicate),
    #              Exprs(*exprs1),
    #          ),
    #      ),
    #      # a project of an optimized read of the same columns is redundant
    #      ReplacementRule(
    #          Pattern(
    #              Project(OptimizedRead(rel, Refs(exprs1), predicate), Exprs(exprs1))  # noqa: E501
    #          ),
    #          lambda rel, exprs1, predicate: OptimizedRead(
    #              rel,
    #              Refs(*exprs1),
    #              predicate,
    #          ),
    #      ),
    #      # fuse read -> project into a projected read
    #      ReplacementRule(
    #          Pattern(Project(Read(table), Exprs(exprs1))),
    #          lambda table, exprs1: Project(
    #              ProjectedRead(table, Refs(*find_refs(exprs1))),
    #              Exprs(*exprs1),
    #          ),
    #      ),
    #      # fuse read -> project into a projected read
    #      ReplacementRule(
    #          Pattern(Project(Read(table), Refs(exprs1))),
    #          lambda table, exprs1: ProjectedRead(table, Refs(*exprs1)),
    #      ),
    #      # remove redundant projections from a projected read
    #      ReplacementRule(
    #          Pattern(Project(ProjectedRead(table, Refs(exprs1)), Exprs(exprs1))),  # noqa: E501
    #          lambda table, exprs1: ProjectedRead(table, Refs(*exprs1)),
    #      ),
    #      ReplacementRule(
    #          Pattern(Project(ProjectedRead(table, Exprs(exprs1)), Refs(exprs1))),  # noqa: E501
    #          lambda table, exprs1: ProjectedRead(table, Refs(*exprs1)),
    #      ),
    #      ReplacementRule(
    #          Pattern(Select(Read(table), predicate)),
    #          lambda table, predicate: SelectedRead(
    #              table,
    #              predicate,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(
    #              Join(rel1, rel2, predicate),
    #              CustomConstraint(can_replace_join),
    #          ),
    #          select_join_replacement,
    #      ),
    #      ReplacementRule(
    #          Pattern(OptimizedRead(rel, refs, true)),
    #          lambda rel, refs: ProjectedRead(rel, refs),
    #      ),
]
#
#
#  def partition_predicate(*, predicate, rel1, rel2):
#      if not isinstance(predicate, And):
#          return And(), And(), predicate
#
#      assert isinstance(predicate, And), f"{type(predicate).__name__}"
#      rel1_refs = frozenset(find_refs(rel1))
#      rel2_refs = frozenset(find_refs(rel2))
#      rel1_operands = []
#      rel2_operands = []
#      remaining = []
#      for operand in predicate:
#          operand_refs = frozenset(find_refs(operand))
#          # if operand contains only references to rel
#          if operand_refs <= rel1_refs and not operand_refs <= rel2_refs:
#              rel1_operands.append(operand)
#          elif operand_refs <= rel2_refs:
#              rel2_operands.append(operand)
#          else:
#              remaining.append(operand)
#      return And(*rel1_operands), And(*rel2_operands), And(*remaining)


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
    """
    return replace_all(expr.op(), RULES).to_expr()
