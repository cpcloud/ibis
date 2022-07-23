from __future__ import annotations

import itertools

from matchpy import CustomConstraint, Pattern, Wildcard, replace_all

import ibis.expr.analysis as an
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir

_ = Wildcard.dot()
dtype = Wildcard.dot("dtype")
x = Wildcard.dot('x')
y = Wildcard.dot('y')

left = Wildcard.dot('left')
right = Wildcard.dot('right')

rel = Wildcard.dot('rel')
table = Wildcard.dot('table')

selections = Wildcard.dot('selections')
predicates = Wildcard.dot('predicates')
preds1 = Wildcard.dot('preds1')
preds2 = Wildcard.dot('preds2')
sort_keys = Wildcard.dot('sort_keys')

exprs00 = Wildcard.star("exprs00")
exprs01 = Wildcard.star("exprs01")

true = ops.Literal.pattern(True, dtype=dtype)
false = ops.Literal.pattern(False, dtype=dtype)
zero = ops.Literal.pattern(0, dtype=dtype)
one = ops.Literal.pattern(1, dtype=dtype)


#  class Ref(Operation):
#      name = "#"
#      arity = Arity.polyadic
#      infix = False
#
#      def __str__(self) -> str:
#          return ".".join(map(str, self.operands))
#
#
#  Read = Operation.new("R", Arity.unary, "Read")
#
#  # Rel, Refs
#  ProjectedRead = Operation.new("R#", Arity.binary, "ProjectedRead")
#
#  # Rel, predicate
#  SelectedRead = Operation.new("R>", Arity.binary, "SelectedRead")
#
#  # Rel, Refs, predicate
#  OptimizedRead = Operation.new("R+", Arity.ternary, "OptimizedRead")
#
#  # Left, right, predicate
#  Join = Operation.new("⋈", Arity.ternary, "Join")
#
#  # Rel, Exprs
#  Project = Operation.new("Π", Arity.binary, "Project")
#
#  # Rel, predicate
#  Select = Operation.new("σ", Arity.binary, "Select")
#
#  # At least one rel
#  Union = Operation.new(
#      "⋃",
#      Arity(min_count=1, fixed_size=False),
#      "Union",
#      commutative=True,
#      associative=True,
#      one_identity=True,
#  )
#
#  # At least one rel
#  Intersection = Operation.new(
#      "⋂",
#      Arity(min_count=1, fixed_size=False),
#      "Intersection",
#      commutative=True,
#      associative=True,
#      one_identity=True,
#  )

#  Refs = Operation.new("#*", Arity(min_count=1, fixed_size=False), "Refs")
#  Exprs = Operation.new("@", Arity(min_count=1, fixed_size=False), "Exprs")


AND_RULES = (
    #  # Or identity
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

MUL_RULES = (
    #      # int * 1
    #      ReplacementRule(Pattern(Mul(int_, one_int)), lambda int_: int_),
    #      ReplacementRule(Pattern(Mul(one_int, int_)), lambda int_: int_),
    #      # int * 0
    #      ReplacementRule(Pattern(Mul(int_, zero_int)), lambda **_: zero_int),
    #      ReplacementRule(Pattern(Mul(zero_int, int_)), lambda **_: zero_int),
    #      # float * 1
    #      ReplacementRule(Pattern(Mul(float_, one_float)), lambda float_: float_),  # noqa: E501
    #      ReplacementRule(Pattern(Mul(one_float, float_)), lambda float_: float_),  # noqa: E501
    #      # float * 0
    #      ReplacementRule(Pattern(Mul(float_, zero_float)), lambda **_: zero_float),  # noqa: E501
    #      ReplacementRule(Pattern(Mul(zero_float, float_)), lambda **_: zero_float),  # noqa: E501
    #      # int * 1.0
    #      ReplacementRule(
    #          Pattern(Mul(int_, one_float)),
    #          lambda int_: FloatLiteral(int_.value),
    #      ),
    #      ReplacementRule(
    #          Pattern(Mul(one_float, int_)),
    #          lambda int_: FloatLiteral(int_.value),
    #      ),
    #      # int * 0.0
    #      ReplacementRule(Pattern(Mul(int_, zero_float)), lambda **_: zero_float),  # noqa: E501
    #      ReplacementRule(Pattern(Mul(zero_float, int_)), lambda **_: zero_float),  # noqa: E501
    #      # int * float
    #      ReplacementRule(
    #          Pattern(Mul(int_, float_)),
    #          lambda int_, float_: FloatLiteral(int(int_) * float(float_)),
    #      ),
    #      ReplacementRule(
    #          Pattern(Mul(float_, int_)),
    #          lambda float_, int_: FloatLiteral(float(float_) * int(int_)),
    #      ),
)

ADD_RULES = (
    #      ReplacementRule(
    #          Pattern(Add(int1_, int2_)),
    #          lambda int1_, int2_: IntLiteral(int1_.value + int2_.value),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(float1_, float2_)),
    #          lambda float1_, float2_: FloatLiteral(float1_.value + float2_.value),  # noqa: E501
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(float_, int_)),
    #          lambda float_, int_: FloatLiteral(float_.value + int_.value),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(int_, float_)),
    #          lambda int_, float_: FloatLiteral(int_.value + float_.value),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(exprs0, int1_, int2_, exprs1)),
    #          lambda exprs0, int1_, int2_, exprs1: Add(
    #              *exprs0,
    #              IntLiteral(int1_.value + int2_.value),
    #              *exprs1,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(exprs1, int1_, int2_, exprs0)),
    #          lambda exprs0, int1_, int2_, exprs1: Add(
    #              *exprs1,
    #              IntLiteral(int1_.value + int2_.value),
    #              *exprs0,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(exprs0, float1_, float2_, exprs1)),
    #          lambda exprs0, float1_, float2_, exprs1: Add(
    #              *exprs0,
    #              FloatLiteral(float1_.value + float2_.value),
    #              *exprs1,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(exprs1, float1_, float2_, exprs0)),
    #          lambda exprs0, float1_, float2_, exprs1: Add(
    #              *exprs1,
    #              FloatLiteral(float1_.value + float2_.value),
    #              *exprs0,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(exprs0, int_, float_, exprs1)),
    #          lambda exprs0, int_, float_, exprs1: Add(
    #              *exprs0,
    #              FloatLiteral(int_.value + float_.value),
    #              *exprs1,
    #          ),
    #      ),
    #      ReplacementRule(
    #          Pattern(Add(exprs1, float_, int_, exprs0)),
    #          lambda exprs0, float_, int_, exprs1: Add(
    #              *exprs1,
    #          FloatLiteral(float_.value + int_.value),
    #          *exprs0,
    #      ),
    #  ),
)


#  def can_replace_join(predicate, rel1, rel2):
#      rel1_predicates, rel2_predicates, _ = partition_predicate(
#          predicate=predicate,
#          rel1=rel1,
#          rel2=rel2,
#      )
#      return rel1_predicates or rel2_predicates
#
#
#  def select_join_replacement(predicate, rel1, rel2):
#      rel1_predicates, rel2_predicates, remaining = itertools.starmap(
#          And,
#          partition_predicate(
#              predicate=predicate,
#              rel1=rel1,
#              rel2=rel2,
#          ),
#      )
#      return Join(
#          Select(rel1, rel1_predicates),
#          Select(rel2, rel2_predicates),
#          remaining,
#      )


#  def refs_are_subset(rel, exprs2, exprs1):
#      return frozenset(exprs2).issubset(exprs1)


RELATION_RULES = [
    # empty selections are a no-op on a projection
    (
        Pattern(ops.Projection.pattern(table, ())),
        lambda table: table,
    ),
    (
        Pattern(ops.Filter.pattern(table, ())),
        lambda table: table,
    ),
    (
        Pattern(ops.SortBy.pattern(table, ())),
        lambda table: table,
    ),
    # all columns from the child are projected
    (
        Pattern(
            ops.Projection.pattern(table, selections),
            CustomConstraint(
                lambda table, selections: (
                    sum(
                        getattr(sel, "table", sel).equals(table)
                        for sel in selections
                    )
                    == len(table.schema)
                )
            ),
        ),
        lambda table, **_: table,
    ),
    (
        Pattern(
            ops.Filter.pattern(
                table,
                predicates=(exprs00, ops.Equals.pattern(x, x), exprs01),
            ),
            CustomConstraint(
                lambda x: not isinstance(x.output_dtype, dt.Floating)
            ),
        ),
        lambda table, exprs00, exprs01, **_: ops.Filter(
            table,
            predicates=(*exprs00, *exprs01),
        ),
    ),
    (
        Pattern(
            ops.Filter.pattern(table, predicates=(exprs00, true, exprs01))
        ),
        lambda table, exprs00, exprs01, **_: ops.Filter(
            table,
            predicates=(*exprs00, *exprs01),
        ),
    ),
    (
        Pattern(ops.Filter.pattern(ops.Filter.pattern(table, preds1), preds2)),
        lambda table, preds1, preds2: ops.Filter(
            table,
            preds1
            + tuple(
                an.sub_for(pred, dict(zip(children, itertools.repeat(table))))
                for pred, children in zip(
                    preds2, map(an.find_immediate_parent_tables, preds2)
                )
            ),
        ),
    )
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
    #      # compose filters
    #      ReplacementRule(
    #          Pattern(Select(Select(rel, predicate1), predicate2)),
    #          lambda rel, predicate1, predicate2: Select(
    #              rel, And(predicate1, predicate2)
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
    #      # collapse repeated projections
    #      ReplacementRule(
    #          Pattern(Project(Project(rel, expr), expr)),
    #          lambda rel, expr: Project(rel, expr),
    #      ),
    #      # remove the child projection if the parent columns are a subset of the  # noqa: E501
    #      # child columns
    #      ReplacementRule(
    #          Pattern(
    #              Project(Project(rel, Refs(exprs1)), Refs(exprs2)),
    #              CustomConstraint(
    #                  lambda rel, exprs1, exprs2: frozenset(exprs2).issubset(exprs1)  # noqa: E501
    #              ),
    #          ),
    #          lambda rel, exprs1, exprs2: Project(rel, Refs(*exprs2)),
    #      ),
    #      # remove the child projection if the parent expressions are a subset of the  # noqa: E501
    #      # child expressions
    #      ReplacementRule(
    #          Pattern(
    #              Project(Project(rel, Exprs(exprs1)), Exprs(exprs2)),
    #              CustomConstraint(
    #                  lambda rel, exprs1, exprs2: frozenset(exprs2).issubset(exprs1)  # noqa: E501
    #              ),
    #          ),
    #          lambda rel, exprs1, exprs2: Project(rel, Exprs(*exprs2)),
    #      ),
    #      ReplacementRule(
    #          Pattern(
    #              Project(Project(rel, Refs(exprs1)), Exprs(exprs2)),
    #              CustomConstraint(
    #                  lambda rel, exprs1, exprs2: frozenset(exprs2).issubset(exprs1)  # noqa: E501
    #              ),
    #          ),
    #          lambda rel, exprs1, exprs2: Project(rel, Refs(*exprs2)),
    #      ),
    #      ReplacementRule(
    #          Pattern(
    #              Project(Project(rel, Exprs(exprs1)), Refs(exprs2)),
    #              CustomConstraint(
    #                  lambda rel, exprs1, exprs2: frozenset(exprs2).issubset(exprs1)  # noqa: E501
    #              ),
    #          ),
    #          lambda rel, exprs1, exprs2: Project(rel, Refs(*exprs2)),
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


RULES = (
    #  AND_RULES
    #  + OR_RULES
    #  + COMPARISON_RULES
    #  + LOGICAL_RULES
    #  + ADD_RULES
    #  + MUL_RULES
    RELATION_RULES
)


def optimize(expr) -> ir.Expr:
    """Optimize an expression."""
    return replace_all(expr.op(), RULES).to_expr()


def popt(expr):
    opt_expr = optimize(expr)
    print("============ UNOPT ===============")
    print(expr)
    print("============= OPT ================")
    print(opt_expr)
    print("----------------------------------")
    print()


if __name__ == "__main__":
    import ibis
    from ibis import _

    t = ibis.table(dict(a="string", b="float64"), name="t")

    # project nothing new => t
    popt(t.projection([]))

    # project all columns from t => t
    popt(t[[t[col] for col in t.columns]])
    popt(t[list(t.columns)])

    # can't optimize => no optimization
    popt(t.filter([t.a == "1"]))

    # useless predicate iff dtype is not floating (because of NaNs) => t
    popt(t.filter([t.a == t.a]))

    # useless predicate => t
    popt(t.filter([ibis.literal(True)]))

    # a == a => True
    # True => True
    # True & True => True
    # => drop the predicate
    popt(t.filter([t.a == t.a, ibis.literal(True)]))
    popt(t.filter((t.a == t.a) & ibis.literal(True)))

    # b is float64, so because of nans we can't remove the x == x
    # predicate
    popt(t.filter((t.b == t.b) & ibis.literal(True)))

    # collapse filters that share the same child
    popt(t.filter(t.a == "1").filter(_.b == 2.0).filter(_.a < "b"))
