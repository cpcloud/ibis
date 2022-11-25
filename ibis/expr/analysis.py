from __future__ import annotations

import functools
import operator
from collections import Counter
from typing import Callable, Iterator, Mapping

import toolz

import ibis.common.graph as g
import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis import util
from ibis.expr.rules import Shape
from ibis.expr.window import window

# ---------------------------------------------------------------------
# Some expression metaprogramming / graph transformations to support
# compilation later


def sub_for(node: ops.Node, substitutions: Mapping[ops.node, ops.Node]) -> ops.Node:
    """Substitute operations in `node` with nodes in `substitutions`.

    Parameters
    ----------
    node
        An Ibis operation
    substitutions
        A mapping from node to node. If any subnode of `node` is equal to any
        of the keys in `substitutions`, the value for that key will replace the
        corresponding node in `node`.

    Returns
    -------
    Node
        An Ibis expression
    """
    assert isinstance(node, ops.Node), type(node)

    def fn(node):
        try:
            return substitutions[node]
        except KeyError:
            if isinstance(node, ops.TableNode):
                return g.halt
            return g.proceed

    return substitute(fn, node)


def sub_immediate_parents(op: ops.Node, table: ops.TableNode) -> ops.Node:
    """Replace immediate parent tables in `op` with `table`."""
    return sub_for(op, {base: table for base in find_immediate_parent_tables(op)})


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

        if is_scalar_reduction(expr.op()) and not has_multiple_bases(expr.op()):
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
    tables = set(find_immediate_parent_tables(node))

    # TODO(kszucs): remove to_expr() conversions
    ntables = len(tables)
    assert 0 <= ntables <= 1, ntables
    if ntables == 1:
        table = tables.pop()
        agg = table.to_expr().aggregate([node.to_expr()])
    else:
        agg = ScalarAggregate(node.to_expr()).get_result()

    return agg


def find_immediate_parent_tables(node: ops.Node) -> Iterator[ops.TableNode]:
    """Find every first occurrence of a :class:`ibis.expr.types.Table` object
    in `expr`.

    Parameters
    ----------
    node
        An ibis node

    Returns
    ------
    Iterator[TableNode]
        Set of immediate child tables

    Notes
    -----
    This function does not traverse into `TableNode` objects. This means that
    the underlying `PhysicalTable` of a `Node` will not be yielded, for
    example.

    Examples
    --------
    >>> import toolz
    >>> import ibis
    >>> t = ibis.table([('a', 'int64')], name='t')
    >>> expr = t.mutate(foo=t.a + 1)
    >>> result = toolz.count(find_immediate_parent_tables(expr))
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
            return g.halt, node
        else:
            return g.proceed, None

    return toolz.unique(g.traverse(finder, node))


def count_immediate_parent_tables(node: ops.Node) -> int:
    """Count every first occurrence of a `TableNode` object in `node`."""
    assert all(isinstance(arg, ops.Node) for arg in util.promote_list(node))

    def finder(node):
        halt = is_table = isinstance(node, ops.TableNode)
        return not halt, is_table

    return sum(g.traverse(finder, node))


def substitute(fn: Callable[[ops.Node], bool], node: ops.Node) -> ops.Node:
    """Substitute expressions with other expressions."""

    assert isinstance(node, ops.Node), type(node)

    result = fn(node)
    if result is g.halt:
        return node
    elif result is not g.proceed:
        assert isinstance(result, ops.Node), type(result)
        return result

    new_args = []
    for arg in node.args:
        if isinstance(arg, tuple):
            arg = tuple(
                substitute(fn, x) if isinstance(arg, ops.Node) else x for x in arg
            )
        elif isinstance(arg, ops.Node):
            arg = substitute(fn, arg)
        new_args.append(arg)

    try:
        return node.__class__(*new_args)
    except TypeError:
        return node


def substitute_parents(node):
    """Rewrite the input expression by replacing any table expressions part of
    a "commutative table operation unit" (for lack of scientific term, a set of
    operations that can be written down in any order and still yield the same
    semantic result)"""
    assert isinstance(node, ops.Node), type(node)

    def fn(node):
        if isinstance(node, ops.Selection):
            # stop substituting child nodes
            return g.halt
        elif isinstance(node, ops.TableColumn):
            # For table column references, in the event that we're on top of a
            # projection, we need to check whether the ref comes from the base
            # table schema or is a derived field. If we've projected out of
            # something other than a physical table, then lifting should not
            # occur
            table = node.table

            if isinstance(table, ops.Selection):
                for val in table.selections:
                    if isinstance(val, ops.PhysicalTable) and node.name in val.schema:
                        return ops.TableColumn(val, node.name)

        # keep looking for nodes to substitute
        return g.proceed

    return substitute(fn, node)


def substitute_unbound(node):
    """Rewrite the input expression by replacing any table expressions with an
    equivalent unbound table."""
    assert isinstance(node, ops.Node), type(node)

    def fn(node, *args, **kwargs):
        if isinstance(node, ops.DatabaseTable):
            return ops.UnboundTable(name=node.name, schema=node.schema)
        else:
            return node.__class__(*args, **kwargs)

    return node.substitute(fn)


def get_mutation_exprs(exprs: list[ir.Expr], table: ir.Table) -> list[ir.Expr | None]:
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

    return [table[column] for column in table.columns] + exprs


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

        if isinstance(walked, (ops.Analytic, ops.Reduction)):
            if w is None:
                w = window()
            return walked.to_expr().over(w).op()
        elif isinstance(walked, ops.Window):
            if w is not None:
                return walked.to_expr().over(w.combine(walked.window)).op()
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
            valid_lowered_sort_keys = frozenset(lowered_metrics).union(lowered_by)
            return ops.Aggregation(
                agg.table.table,
                lowered_metrics,
                by=lowered_by,
                having=lowered_having,
                predicates=agg.table.predicates,
                # only the sort keys that exist as grouping keys or metrics can
                # be included
                sort_keys=[
                    key
                    for key in agg.table.sort_keys
                    if key.expr in valid_lowered_sort_keys
                ],
            )

    return agg


def find_first_base_table(node: ops.Node) -> ops.Node | None:
    try:
        return next(find_immediate_parent_tables(node))
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
            ops.SetOp,
        ),
    ):
        return g.proceed, node
    elif isinstance(node, ops.Join):
        return g.proceed, None
    elif isinstance(node, ops.TableNode):
        return g.halt, node
    else:
        return g.proceed, None


def shares_all_roots(exprs, parents):
    # unique table dependencies of exprs and parents
    exprs_deps = set(g.traverse(_find_projections, exprs))
    parents_deps = set(g.traverse(_find_projections, parents))
    return exprs_deps <= parents_deps


def shares_some_roots(exprs, parents):
    # unique table dependencies of exprs and parents
    exprs_deps = set(g.traverse(_find_projections, exprs))
    parents_deps = set(g.traverse(_find_projections, parents)) | set(
        util.promote_list(parents)
    )
    return bool(exprs_deps & parents_deps)


def is_analytic(node):
    def predicate(node):
        if isinstance(node, (ops.Reduction, ops.Analytic)):
            return g.halt, True
        else:
            return g.proceed, None

    return any(g.traverse(predicate, node))


def is_reduction(node):
    """Check whether an expression contains a reduction or not.

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

    def predicate(node):
        if isinstance(node, ops.Reduction):
            return g.halt, True
        elif isinstance(node, ops.TableNode):
            # don't go below any table nodes
            return g.halt, None
        else:
            return g.proceed, None

    return any(g.traverse(predicate, node))


def is_scalar_reduction(node):
    assert isinstance(node, ops.Node), type(node)
    return node.output_shape is Shape.SCALAR and is_reduction(node)


_ANY_OP_MAPPING = {
    ops.Any: ops.UnresolvedExistsSubquery,
    ops.NotAny: ops.UnresolvedNotExistsSubquery,
}


def find_predicates(node):
    # TODO(kszucs): consider to remove flatten argument and compose with
    # flatten_predicates instead
    assert isinstance(node, ops.Node), type(node)

    def predicate(node: ops.Node):
        assert isinstance(node, ops.Node), type(node)
        if isinstance(node, ops.Value) and node.output_dtype.is_boolean():
            return g.halt, node
        return g.proceed, None

    return g.traverse(predicate, node)


def find_subqueries(node: ops.Node) -> Counter[ops.Node, int]:
    counts = Counter()

    def finder(node: ops.Node) -> tuple[bool, ops.Node | None]:
        if isinstance(node, ops.Join):
            return [node.left, node.right], None
        elif isinstance(node, ops.PhysicalTable):
            return g.halt, None
        elif isinstance(node, ops.SelfReference):
            return g.proceed, None
        elif isinstance(
            node,
            (ops.Projection, ops.Filter, ops.SortBy, ops.Aggregation),
        ):
            counts[node] += 1
            return [node.table], None
        elif isinstance(node, ops.TableNode):
            counts[node] += 1
            return g.proceed, None
        elif isinstance(node, ops.TableColumn):
            return node.table not in counts, None
        else:
            return g.proceed, None

    # keep duplicates so we can determine where an expression is used
    # more than once
    counts.update(g.traverse(finder, node, dedup=False))
    return counts


# TODO(kszucs): move to types/logical.py
def _make_any(expr, any_op_class: type[ops.Any] | type[ops.NotAny]):
    assert isinstance(expr, ir.Expr)

    tables = frozenset(find_immediate_parent_tables(expr.op()))
    predicates = find_predicates(expr.op())

    if len(tables) > 1:
        opclass = _ANY_OP_MAPPING[any_op_class]
        op = opclass(
            tables=[t.to_expr() for t in tables],
            predicates=list(predicates),
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

    if name is not None:
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
def _rewrite_filter_value(op, **kwargs):
    """Recursively apply filter rewriting on operations."""

    visited = [
        _rewrite_filter(arg, **kwargs) if isinstance(arg, ops.Node) else arg
        for arg in op.args
    ]
    if all(map(operator.is_, visited, op.args)):
        return op

    return op.__class__(*visited)


@_rewrite_filter.register(ops.NodeList)
def _rewrite_filter_value_list(op, **kwargs):
    visited = [
        _rewrite_filter(arg, **kwargs) if isinstance(arg, ops.Node) else arg
        for arg in op.args
    ]

    if all(map(operator.is_, visited, op.args)):
        return op

    return op.__class__(*visited)
