from __future__ import annotations

import collections
import itertools
from abc import abstractmethod

from public import public

import ibis.common.exceptions as com
import ibis.expr.rules as rlz
import ibis.expr.schema as sch
import ibis.util as util
from ibis.common.annotations import attribute, immutable_property
from ibis.expr.deferred import Deferred
from ibis.expr.operations.core import Named, Node, NodeList, Value
from ibis.expr.operations.generic import TableColumn
from ibis.expr.operations.logical import Equals, ExistsSubquery, NotExistsSubquery

_table_names = (f'unbound_table_{i:d}' for i in itertools.count())


@public
def genname():
    return next(_table_names)


@public
class TableNode(Node):
    def order_by(self, sort_exprs):
        this = dict(table=self)
        child = rlz.sort_key_from(rlz.ref("table"), this=this)
        keys = rlz.nodes_of(child, sort_exprs, this=this)
        return SortBy(self, keys)

    @property
    @abstractmethod
    def schema(self) -> sch.Schema:
        """Return a schema."""

    def to_expr(self):
        import ibis.expr.types as ir

        return ir.Table(self)


@public
class PhysicalTable(TableNode, Named):
    pass


@public
class UnboundTable(PhysicalTable):
    schema = rlz.instance_of(sch.Schema)
    name = rlz.optional(rlz.instance_of(str), default=genname)


@public
class DatabaseTable(PhysicalTable):
    name = rlz.instance_of(str)
    schema = rlz.instance_of(sch.Schema)
    source = rlz.client

    def change_name(self, new_name):
        return type(self)(new_name, self.args[1], self.source)


@public
class SQLQueryResult(TableNode):
    """A table sourced from the result set of a select query."""

    query = rlz.instance_of(str)
    schema = rlz.instance_of(sch.Schema)
    source = rlz.client


@public
class InMemoryTable(PhysicalTable):
    name = rlz.instance_of(str)
    schema = rlz.instance_of(sch.Schema)

    @property
    @abstractmethod
    def data(self) -> util.ToFrame:
        """Return the data of an in-memory table."""

    def has_resolved_name(self):
        return True

    def resolve_name(self):
        return self.name


# TODO(kszucs): desperately need to clean this up, the majority of this
# functionality should be handled by input rules for the Join class
def _clean_join_predicates(left, right, predicates):
    import ibis.expr.types as ir

    result = []

    for pred in predicates:
        if isinstance(pred, tuple):
            if len(pred) != 2:
                raise com.ExpressionError('Join key tuple must be ' 'length 2')
            lk, rk = pred
            lk = left.to_expr()._ensure_expr(lk)
            rk = right.to_expr()._ensure_expr(rk)
            yield Equals(lk.op(), rk.op())
        elif isinstance(pred, str):
            yield Equals(TableColumn(left, pred), TableColumn(right, pred))
        elif isinstance(pred, Value):
            pred = pred.to_expr()
        elif isinstance(pred, Deferred):
            # resolve deferred expressions on the left table
            pred = pred.resolve(left.to_expr())
        elif not isinstance(pred, ir.Expr):
            raise NotImplementedError(f"Predicate with type {type(pred)} not supported")

        if not isinstance(pred, ir.BooleanValue):
            raise com.ExpressionError("Join predicate must be a boolean value")

        result.append(pred)

    return tuple(result)


@public
class Join(TableNode):
    left = rlz.table
    right = rlz.table
    predicates = rlz.optional(
        rlz.nodes_of(
            rlz.one_of(
                (
                    rlz.eq_pair(
                        rlz.column_from(rlz.ref("left")),
                        rlz.column_from(rlz.ref("right")),
                    ),
                    rlz.instance_of(str),
                    rlz.boolean,
                )
            )
        ),
        default=NodeList(),
    )

    @property
    def schema(self):
        # For joins retaining both table schemas, merge them together here
        return self.left.schema.append(self.right.schema)


@public
class InnerJoin(Join):
    pass


@public
class LeftJoin(Join):
    pass


@public
class RightJoin(Join):
    pass


@public
class OuterJoin(Join):
    pass


@public
class AnyInnerJoin(Join):
    pass


@public
class AnyLeftJoin(Join):
    pass


@public
class LeftSemiJoin(Join):
    @property
    def schema(self):
        return self.left.schema


@public
class LeftAntiJoin(Join):
    @property
    def schema(self):
        return self.left.schema


@public
class CrossJoin(Join):
    pass


@public
class AsOfJoin(Join):
    # TODO(kszucs): convert to proper predicate rules
    by = rlz.optional(lambda x, this: x, default=())
    tolerance = rlz.optional(rlz.interval)

    def __init__(self, left, right, by, predicates, **kwargs):
        by = _clean_join_predicates(left, right, util.promote_list(by))
        super().__init__(left=left, right=right, by=by, predicates=predicates, **kwargs)


@public
class SetOp(TableNode):
    left = rlz.table
    right = rlz.table
    distinct = rlz.optional(rlz.instance_of(bool), default=False)

    def __init__(self, left, right, **kwargs):
        if left.schema != right.schema:
            raise com.RelationError('Table schemas must be equal for set operations')
        super().__init__(left=left, right=right, **kwargs)

    @property
    def schema(self):
        return self.left.schema


@public
class Union(SetOp):
    """Union of two relations.

    Removes duplicates.
    """


@public
class UnionAll(SetOp):
    """Union of two relations.

    Preserves duplicates.
    """


@public
class Intersection(SetOp):
    """Intersection of two relations.

    Removes duplicates.
    """


@public
class IntersectionAll(SetOp):
    """Intersection of two relations.

    Preserves duplicates.
    """


@public
class Difference(SetOp):
    """Difference of two relations.

    Removes duplicates.
    """


@public
class DifferenceAll(SetOp):
    """Difference of two relations.

    Preserves duplicates.
    """


@public
class Limit(TableNode):
    table = rlz.table
    n = rlz.instance_of(int)
    offset = rlz.instance_of(int)

    @property
    def schema(self):
        return self.table.schema


@public
class SelfReference(TableNode):
    table = rlz.table

    @property
    def schema(self):
        return self.table.schema


@public
class Projection(TableNode):
    table = rlz.table
    selections = rlz.nodes_of(
        rlz.one_of(
            (
                rlz.table,
                rlz.column_from(rlz.ref("table")),
                rlz.function_of(rlz.ref("table")),
                rlz.any,
            )
        )
    )

    @attribute.default
    def schema(self):
        schema_dict = {}

        for sel in self.selections:
            if isinstance(sel, Value):
                schema_dict[sel.name] = sel.output_dtype
            elif isinstance(sel, TableNode):
                schema_dict.update(sel.schema.items())

        return sch.schema(schema_dict)


@public
class Filter(TableNode):
    table = rlz.table
    predicates = rlz.nodes_of(rlz.boolean)

    @immutable_property
    def schema(self):
        return self.table.schema


@public
class SortBy(TableNode):
    table = rlz.table
    sort_keys = rlz.nodes_of(rlz.sort_key_from(rlz.ref("table")))

    @immutable_property
    def schema(self):
        return self.table.schema


@public
class Selection(TableNode):
    table = rlz.table
    selections = rlz.optional(
        rlz.nodes_of(
            rlz.one_of(
                (
                    rlz.table,
                    rlz.column_from(rlz.ref("table")),
                    rlz.function_of(rlz.ref("table")),
                    rlz.any,
                )
            )
        ),
        default=NodeList(),
    )
    predicates = rlz.optional(rlz.nodes_of(rlz.boolean), default=NodeList())
    sort_keys = rlz.optional(
        rlz.nodes_of(rlz.sort_key_from(rlz.ref("table"))),
        default=NodeList(),
    )

    @immutable_property
    def schema(self):
        # Resolve schema and initialize
        if not self.selections:
            return self.table.schema

        assert self.selections

        schema_dict = {}

        for sel in self.selections:
            if isinstance(sel, Value):
                schema_dict[sel.name] = sel.output_dtype
            elif isinstance(sel, TableNode):
                schema_dict.update(sel.schema.items())

        return sch.Schema.from_dict(schema_dict)


@public
class Aggregation(TableNode):

    """
    metrics : per-group scalar aggregates
    by : group expressions
    having : post-aggregation predicate

    TODO: not putting this in the aggregate operation yet
    where : pre-aggregation predicate
    """

    table = rlz.table
    by = rlz.optional(
        rlz.nodes_of(
            rlz.one_of(
                (
                    rlz.function_of(rlz.ref("table")),
                    rlz.column_from(rlz.ref("table")),
                    rlz.column(rlz.any),
                )
            )
        ),
        default=(),
    )
    metrics = rlz.optional(
        rlz.nodes_of(
            rlz.one_of(
                (
                    rlz.function_of(
                        rlz.ref("table"),
                        output_rule=rlz.one_of((rlz.reduction, rlz.scalar(rlz.any))),
                    ),
                    rlz.reduction,
                    rlz.scalar(rlz.any),
                    rlz.nodes_of(rlz.scalar(rlz.any)),  # TODO(kszucs): ???
                )
            ),
            flatten=True,
        ),
        default=(),
    )
    having = rlz.optional(
        rlz.nodes_of(
            rlz.one_of(
                (
                    rlz.function_of(
                        rlz.ref("table"), output_rule=rlz.scalar(rlz.boolean)
                    ),
                    rlz.scalar(rlz.boolean),
                )
            ),
        ),
        default=(),
    )

    @immutable_property
    def schema(self) -> sch.Schema:
        return sch.Schema.from_tuples(
            (expr.name, expr.output_dtype) for expr in self.by + self.metrics
        )


@public
class AggregateSelection(Aggregation):
    """Combined aggregation."""

    predicates = rlz.optional(rlz.nodes_of(rlz.boolean), default=NodeList())
    sort_keys = rlz.optional(
        rlz.nodes_of(rlz.sort_key_from(rlz.ref("table"))),
        default=NodeList(),
    )


@public
class Distinct(TableNode):
    """Distinct is a table-level unique-ing operation.

    In SQL, you might have:

    SELECT DISTINCT foo
    FROM table

    SELECT DISTINCT foo, bar
    FROM table
    """

    table = rlz.table

    @property
    def schema(self):
        return self.table.schema


@public
class FillNa(TableNode):
    """Fill null values in the table."""

    table = rlz.table
    replacements = rlz.one_of(
        (
            rlz.numeric,
            rlz.string,
            rlz.instance_of(collections.abc.Mapping),
        )
    )

    def __init__(self, table, replacements, **kwargs):
        super().__init__(
            table=table,
            replacements=(
                replacements
                if not isinstance(replacements, collections.abc.Mapping)
                else util.frozendict(replacements)
            ),
            **kwargs,
        )

    @property
    def schema(self):
        return self.table.schema


@public
class DropNa(TableNode):
    """Drop null values in the table."""

    table = rlz.table
    how = rlz.isin({'any', 'all'})
    subset = rlz.optional(rlz.nodes_of(rlz.column_from(rlz.ref("table"))))

    @property
    def schema(self):
        return self.table.schema


@public
class View(PhysicalTable):
    """A view created from an expression."""

    child = rlz.table
    name = rlz.instance_of(str)

    @property
    def schema(self):
        return self.child.schema


@public
class SQLStringView(PhysicalTable):
    """A view created from a SQL string."""

    child = rlz.table
    name = rlz.instance_of(str)
    query = rlz.instance_of(str)

    @attribute.default
    def schema(self):
        # TODO(kszucs): avoid converting to expression
        backend = self.child.to_expr()._find_backend()
        return backend._get_schema_using_query(self.query)


def _dedup_join_columns(expr, suffixes: tuple[str, str]):
    op = expr.op()
    left = op.left.to_expr()
    right = op.right.to_expr()

    right_columns = frozenset(right.columns)
    overlap = frozenset(column for column in left.columns if column in right_columns)
    equal = set()

    if isinstance(op, InnerJoin) and util.all_of(op.predicates, Equals):
        # For inner joins composed exclusively of equality predicates, we can
        # avoid renaming columns with colliding names if their values are
        # guaranteed to be equal due to the predicate. Here we collect a set of
        # colliding column names that are known to have equal values between
        # the left and right tables in the join.
        tables = {op.left, op.right}
        for pred in op.predicates:
            if (
                isinstance(pred.left, TableColumn)
                and isinstance(pred.right, TableColumn)
                and {pred.left.table, pred.right.table} == tables
                and pred.left.name == pred.right.name
            ):
                equal.add(pred.left.name)

    if not overlap:
        return expr

    left_suffix, right_suffix = suffixes

    # Rename columns in the left table that overlap, unless they're known to be
    # equal to a column in the right
    left_projections = [
        left[column].name(f"{column}{left_suffix}")
        if column in overlap and column not in equal
        else left[column]
        for column in left.columns
    ]

    # Rename columns in the right table that overlap, dropping any columns that
    # are known to be equal to those in the left table
    right_projections = [
        right[column].name(f"{column}{right_suffix}")
        if column in overlap
        else right[column]
        for column in right.columns
        if column not in equal
    ]
    return expr.projection(left_projections + right_projections)


public(ExistsSubquery=ExistsSubquery, NotExistsSubquery=NotExistsSubquery)
