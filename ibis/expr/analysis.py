from __future__ import annotations

import functools
import operator
from collections import Counter

import toolz

import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis import util
from ibis.common.exceptions import IbisTypeError
from ibis.expr.rules import Shape
from ibis.expr.window import window

# ---------------------------------------------------------------------
# Some expression metaprogramming / graph transformations to support
# compilation later


def sub_for(node, substitutions):
    """Substitute subexpressions in `expr` with expression to expression
    mapping `substitutions`.

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        An Ibis expression
    substitutions : List[Tuple[ibis.expr.types.Expr, ibis.expr.types.Expr]]
        A mapping from expression to expression. If any subexpression of `expr`
        is equal to any of the keys in `substitutions`, the value for that key
        will replace the corresponding expression in `expr`.

    Returns
    -------
    ibis.expr.types.Expr
        An Ibis expression
    """
    assert isinstance(node, ops.Node), type(node)

    def fn(node):
        try:
            return substitutions[node]
        except KeyError:
            if isinstance(node, ops.TableNode):
                return lin.halt
            return lin.proceed

    return substitute(fn, node)


class ScalarAggregate:
    def __init__(self, expr):
        assert isinstance(expr, ir.Expr)
        self.expr = expr
        self.tables = []

    def get_result(self):
        expr = self.expr
        subbed_expr = self._visit(expr)

        table = self.tables[0]
        for other in self.tables[1:]:
            table = table.cross_join(other)

        return table.projection([subbed_expr])

    def _visit(self, expr):
        assert isinstance(expr, ir.Expr), type(expr)

        if is_scalar_reduction(expr.op()) and not has_multiple_bases(
            expr.op()
        ):
            # An aggregation unit
            if not expr.has_name():
                expr = expr.name('tmp')
            agg_expr = reduction_to_aggregation(expr.op())
            self.tables.append(agg_expr)
            return agg_expr[expr.get_name()]
        elif not isinstance(expr, ir.Expr):
            return expr

        node = expr.op()
        # TODO(kszucs): use the substitute() utility instead
        new_args = (
            self._visit(arg.to_expr()) if isinstance(arg, ops.Node) else arg
            for arg in node.args
        )
        new_node = node.__class__(*new_args)
        new_expr = new_node.to_expr()

        if expr.has_name():
            new_expr = new_expr.name(name=expr.get_name())

        return new_expr


def has_multiple_bases(node):
    assert isinstance(node, ops.Node), type(node)
    return len(find_immediate_parent_tables(node)) > 1


def reduction_to_aggregation(node):
    tables = find_immediate_parent_tables(node)

    # TODO(kszucs): remove to_expr() conversions
    if len(tables) == 1:
        (table,) = tables
        agg = table.to_expr().aggregate([node.to_expr()])
    else:
        agg = ScalarAggregate(node.to_expr()).get_result()

    return agg


def find_immediate_parent_tables(node):
    """Find every first occurrence of a :class:`ibis.expr.types.Table`
    object in `expr`.

    Parameters
    ----------
    expr : ir.Expr

    Yields
    ------
    e : ir.Expr

    Notes
    -----
    This function does not traverse into Table objects. This means that the
    underlying PhysicalTable of a Selection will not be yielded, for example.

    Examples
    --------
    >>> import ibis, toolz
    >>> t = ibis.table([('a', 'int64')], name='t')
    >>> expr = t.mutate(foo=t.a + 1)
    >>> result = find_immediate_parent_tables(expr)
    >>> len(result)
    1
    >>> result[0]
    r0 := UnboundTable[t]
      a int64
    Selection[r0]
      selections:
        r0
        foo: r0.a + 1
    """
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(node))

    def finder(node):
        if isinstance(node, ops.TableNode):
            return lin.halt, node
        else:
            return lin.proceed, None

    return list(toolz.unique(lin.traverse(finder, node)))


def substitute(fn, node):
    """Substitute expressions with other expressions."""

    assert isinstance(node, ops.Node), type(node)

    result = fn(node)
    if result is lin.halt:
        return node
    elif result is not lin.proceed:
        assert isinstance(result, ops.Node), type(result)
        return result

    new_args = []
    for arg in node.args:
        if isinstance(arg, tuple):
            arg = tuple(
                substitute(fn, x) if isinstance(arg, ops.Node) else x
                for x in arg
            )
        elif isinstance(arg, ops.Node):
            arg = substitute(fn, arg)
        new_args.append(arg)

    try:
        return node.__class__(*new_args)
    except IbisTypeError:
        return node


def substitute_parents(node):
    """
    Rewrite the input expression by replacing any table expressions part of a
    "commutative table operation unit" (for lack of scientific term, a set of
    operations that can be written down in any order and still yield the same
    semantic result)
    """
    assert isinstance(node, ops.Node), type(node)

    def fn(node):
        if isinstance(node, ops.Selection):
            # stop substituting child nodes
            return lin.halt
        elif isinstance(node, ops.TableColumn):
            # For table column references, in the event that we're on top of a
            # projection, we need to check whether the ref comes from the base
            # table schema or is a derived field. If we've projected out of
            # something other than a physical table, then lifting should not
            # occur
            table = node.table

            if isinstance(table, ops.Selection):
                for val in table.selections:
                    if (
                        isinstance(val, ops.PhysicalTable)
                        and node.name in val.schema
                    ):
                        return ops.TableColumn(val, node.name)

        # keep looking for nodes to substitute
        return lin.proceed

    return substitute(fn, node)


def get_mutation_exprs(
    exprs: list[ir.Expr], table: ir.Table
) -> list[ir.Expr | None]:
    """Given the list of exprs and the underlying table of a mutation op,
    return the exprs to use to instantiate the mutation."""
    # The below logic computes the mutation node exprs by splitting the
    # assignment exprs into two disjoint sets:
    # 1) overwriting_cols_to_expr, which maps a column name to its expr
    # if the expr contains a column that overwrites an existing table column.
    # All keys in this dict are columns in the original table that are being
    # overwritten by an assignment expr.
    # 2) non_overwriting_exprs, which is a list of all exprs that do not do
    # any overwriting. That is, if an expr is in this list, then its column
    # name does not exist in the original table.
    # Given these two data structures, we can compute the mutation node exprs
    # based on whether any columns are being overwritten.
    # TODO issue #2649
    overwriting_cols_to_expr: dict[str, ir.Expr | None] = {}
    non_overwriting_exprs: list[ir.Expr] = []
    table_schema = table.schema()
    for expr in exprs:
        expr_contains_overwrite = False
        if isinstance(expr, ir.Value) and expr.get_name() in table_schema:
            overwriting_cols_to_expr[expr.get_name()] = expr
            expr_contains_overwrite = True

        if not expr_contains_overwrite:
            non_overwriting_exprs.append(expr)

    columns = table.columns
    if overwriting_cols_to_expr:
        return [
            overwriting_cols_to_expr.get(column, table[column])
            for column in columns
            if overwriting_cols_to_expr.get(column, table[column]) is not None
        ] + non_overwriting_exprs

    table_expr: ir.Expr = table
    return [table_expr] + exprs


# TODO(kszucs): rewrite to receive and return an ops.Node
def windowize_function(expr, w=None):
    assert isinstance(expr, ir.Expr), type(expr)

    def _windowize(op, w):
        if not isinstance(op, ops.Window):
            walked = _walk(op, w)
        else:
            window_arg, window_w = op.args
            walked_child = _walk(window_arg, w)

            if walked_child is not window_arg:
                walked = ops.Window(walked_child, window_w)
            else:
                walked = op

        if isinstance(op, (ops.Analytic, ops.Reduction)):
            if w is None:
                w = window()
            return walked.to_expr().over(w).op()
        elif isinstance(op, ops.Window):
            if w is not None:
                return walked.to_expr().over(w.combine(op.window)).op()
            else:
                return walked
        else:
            return walked

    def _walk(op, w):
        # TODO(kszucs): rewrite to use the substitute utility
        windowed_args = []
        for arg in op.args:
            if not isinstance(arg, ops.Value):
                windowed_args.append(arg)
                continue

            new_arg = _windowize(arg, w)
            windowed_args.append(new_arg)

        return type(op)(*windowed_args)

    return _windowize(expr.op(), w).to_expr()


def simplify_aggregation(agg):
    def _pushdown(nodes):
        subbed = []
        for node in nodes:
            subbed.append(sub_for(node, {agg.table: agg.table.table}))

        # TODO(kszucs): perhaps this validation could be omitted
        if subbed:
            valid = shares_all_roots(subbed, agg.table.table)
        else:
            valid = True

        return valid, subbed

    if isinstance(agg.table, ops.Selection) and not agg.table.selections:
        metrics_valid, lowered_metrics = _pushdown(agg.metrics)
        by_valid, lowered_by = _pushdown(agg.by)
        having_valid, lowered_having = _pushdown(agg.having)

        if metrics_valid and by_valid and having_valid:
            return ops.Aggregation(
                agg.table.table,
                lowered_metrics,
                by=lowered_by,
                having=lowered_having,
                predicates=agg.table.predicates,
                sort_keys=agg.table.sort_keys,
            )

    return agg


class Projector:

    """
    Analysis and validation of projection operation, taking advantage of
    "projection fusion" opportunities where they exist, i.e. combining
    compatible projections together rather than nesting them. Translation /
    evaluation later will not attempt to do any further fusion /
    simplification.
    """

    def __init__(self, parent, proj_exprs):
        # TODO(kszucs): rewrite projector to work with operations exclusively
        proj_exprs = util.promote_list(proj_exprs)
        self.parent = parent
        self.input_exprs = proj_exprs
        self.resolved_exprs = [parent._ensure_expr(e) for e in proj_exprs]
        self.clean_exprs = list(map(windowize_function, self.resolved_exprs))

    def get_result(self):
        return ops.Projection(self.parent, self.clean_exprs)


def find_first_base_table(node):
    def predicate(node):
        if isinstance(node, ops.TableNode):
            return lin.halt, node
        else:
            return lin.proceed, None

    try:
        return next(lin.traverse(predicate, node))
    except StopIteration:
        return None


def _find_projections(node):
    assert isinstance(node, ops.Node), type(node)

    if isinstance(
        node,
        (
            ops.Projection,
            ops.Filter,
            ops.SortBy,
            ops.SelfReference,
        ),
    ):
        return lin.proceed, node
    elif isinstance(node, ops.Join):
        return lin.proceed, None
    elif isinstance(node, ops.TableNode):
        return lin.halt, node
    else:
        return lin.proceed, None


def shares_all_roots(exprs, parents):
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(exprs))
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(parents))

    # unique table dependencies of exprs and parents
    exprs_deps = set(lin.traverse(_find_projections, exprs))
    parents_deps = set(lin.traverse(_find_projections, parents))
    return exprs_deps <= parents_deps


def shares_some_roots(exprs, parents):
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(exprs))
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(parents))

    # unique table dependencies of exprs and parents
    exprs_deps = set(lin.traverse(_find_projections, exprs))
    parents_deps = set(lin.traverse(_find_projections, parents)) | set(
        util.promote_list(parents)
    )
    return bool(exprs_deps & parents_deps)


def flatten_predicate(node):
    """Yield the expressions corresponding to the `And` nodes of a predicate.

    Parameters
    ----------
    expr : ir.BooleanColumn

    Returns
    -------
    exprs : List[ir.BooleanColumn]

    Examples
    --------
    >>> import ibis
    >>> t = ibis.table([('a', 'int64'), ('b', 'string')], name='t')
    >>> filt = (t.a == 1) & (t.b == 'foo')
    >>> predicates = flatten_predicate(filt)
    >>> len(predicates)
    2
    >>> predicates[0]
    r0 := UnboundTable[t]
      a int64
      b string
    r0.a == 1
    >>> predicates[1]
    r0 := UnboundTable[t]
      a int64
      b string
    r0.b == 'foo'
    """
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(node))

    def predicate(node):
        if isinstance(node, ops.And):
            return lin.proceed, None
        else:
            return lin.halt, node

    return list(lin.traverse(predicate, node))


def is_analytic(node):
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(node))

    def predicate(node):
        if isinstance(node, (ops.Reduction, ops.Analytic)):
            return lin.halt, True
        else:
            return lin.proceed, None

    return any(lin.traverse(predicate, node))


def is_reduction(node):
    """
    Check whether an expression contains a reduction or not

    Aggregations yield typed scalar expressions, since the result of an
    aggregation is a single value. When creating an table expression
    containing a GROUP BY equivalent, we need to be able to easily check
    that we are looking at the result of an aggregation.

    As an example, the expression we are looking at might be something
    like: foo.sum().log10() + bar.sum().log10()

    We examine the operator DAG in the expression to determine if there
    are aggregations present.

    A bound aggregation referencing a separate table is a "false
    aggregation" in a GROUP BY-type expression and should be treated a
    literal, and must be computed as a separate query and stored in a
    temporary variable (or joined, for bound aggregations with keys)

    Parameters
    ----------
    expr : ir.Expr

    Returns
    -------
    check output : bool
    """
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(node))

    def predicate(node):
        if isinstance(node, ops.Reduction):
            return lin.halt, True
        elif isinstance(node, ops.TableNode):
            # don't go below any table nodes
            return lin.halt, None
        else:
            return lin.proceed, None

    return any(lin.traverse(predicate, node))


def is_scalar_reduction(node):
    assert isinstance(node, ops.Node), type(node)
    return node.output_shape is Shape.SCALAR and is_reduction(node)


_ANY_OP_MAPPING = {
    ops.Any: ops.UnresolvedExistsSubquery,
    ops.NotAny: ops.UnresolvedNotExistsSubquery,
}


def find_predicates(node, flatten=True):
    # TODO(kszucs): consider to remove flatten argument and compose with
    # flatten_predicates instead
    assert isinstance(node, ops.Node), type(node)

    def predicate(node):
        assert isinstance(node, ops.Node), type(node)
        if isinstance(node, ops.Value) and isinstance(
            node.output_dtype, dt.Boolean
        ):
            if flatten and isinstance(node, ops.And):
                return lin.proceed, None
            else:
                return lin.halt, node
        return lin.proceed, None

    return list(lin.traverse(predicate, node))


def find_subqueries(node: ops.Node) -> Counter:
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(node))

    counts = Counter()

    def finder(node: ops.Node):
        if isinstance(node, ops.Join):
            return [node.left, node.right], None
        elif isinstance(node, ops.PhysicalTable):
            return lin.halt, None
        elif isinstance(node, ops.SelfReference):
            return lin.proceed, None
        elif isinstance(node, (ops.Selection, ops.Aggregation)):
            counts[node] += 1
            return [node.table], None
        elif isinstance(node, ops.TableNode):
            counts[node] += 1
            return lin.proceed, None
        elif isinstance(node, ops.TableColumn):
            return node.table not in counts, None
        else:
            return lin.proceed, None

    # keep duplicates so we can determine where an expression is used
    # more than once
    list(lin.traverse(finder, node, dedup=False))

    return counts


# TODO(kszucs): move to types/logical.py
def _make_any(
    expr,
    any_op_class: type[ops.Any] | type[ops.NotAny],
):
    assert isinstance(expr, ir.Expr)

    tables = find_immediate_parent_tables(expr.op())
    predicates = find_predicates(expr.op(), flatten=True)

    if len(tables) > 1:
        op = _ANY_OP_MAPPING[any_op_class](
            tables=[t.to_expr() for t in tables],
            predicates=predicates,
        )
    else:
        op = any_op_class(expr)
    return op.to_expr()


# TODO(kszucs): use substitute instead
@functools.singledispatch
def _rewrite_filter(op, **kwargs):
    raise NotImplementedError(type(op))


@_rewrite_filter.register(ops.Reduction)
def _rewrite_filter_reduction(op, name: str | None = None, **kwargs):
    """Turn a reduction inside of a filter into an aggregate."""
    # TODO: what about reductions that reference a join that isn't visible at
    # this level? Means we probably have the wrong design, but will have to
    # revisit when it becomes a problem.

    # TODO(kszucs): may need to wrap with Alias
    if name is not None:
        # expr = expr.name(name)
        op = ops.Alias(op, name=name)
    aggregation = reduction_to_aggregation(op)
    return ops.TableArrayView(aggregation)


@_rewrite_filter.register(ops.Any)
@_rewrite_filter.register(ops.TableColumn)
@_rewrite_filter.register(ops.Literal)
@_rewrite_filter.register(ops.ExistsSubquery)
@_rewrite_filter.register(ops.NotExistsSubquery)
@_rewrite_filter.register(ops.Window)
def _rewrite_filter_subqueries(op, **kwargs):
    """Don't rewrite any of these operations in filters."""
    return op


@_rewrite_filter.register(ops.Alias)
def _rewrite_filter_alias(op, name: str | None = None, **kwargs):
    """Rewrite filters on aliases."""
    return _rewrite_filter(
        op.arg,
        name=name if name is not None else op.name,
        **kwargs,
    )


@_rewrite_filter.register(ops.Value)
def _rewrite_filter_value(op, name: str | None = None, **kwargs):
    """Recursively apply filter rewriting on operations."""

    visited = [
        _rewrite_filter(arg, **kwargs) if isinstance(arg, ops.Node) else arg
        for arg in op.args
    ]
    if all(map(operator.is_, visited, op.args)):
        return op

    new_op = op.__class__(*visited)
    if op.has_resolved_name():
        return ops.Alias(new_op, name=op.resolve_name())
    else:
        return ops.Alias(new_op, name="tmp")
