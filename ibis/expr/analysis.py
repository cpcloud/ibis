import functools
import operator
from typing import Any

import toolz

import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.util
from ibis.common import ExpressionError, IbisTypeError, RelationError
from ibis.expr.window import window

# ---------------------------------------------------------------------
# Some expression metaprogramming / graph transformations to support
# compilation later


def sub_for(expr, substitutions):
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
    mapping = {k.op(): v for k, v in substitutions}
    substitutor = Substitutor()
    return substitutor.substitute(expr, mapping)


class Substitutor:
    def __init__(self):
        """Initialize the Substitutor class.

        Notes
        -----
        We need a new cache per substitution call, otherwise we leak state
        across calls and end up incorrectly reusing other substitions' cache.

        """
        cache = toolz.memoize(key=lambda args, kwargs: args[0].op())
        self.substitute = cache(self._substitute)

    def _substitute(self, expr, mapping):
        """Substitute expressions with other expressions.

        Parameters
        ----------
        expr : ibis.expr.types.Expr
        mapping : Mapping[ibis.expr.operations.Node, ibis.expr.types.Expr]

        Returns
        -------
        ibis.expr.types.Expr
        """
        node = expr.op()
        try:
            return mapping[node]
        except KeyError:
            if node.blocks():
                return expr

            new_args = list(node.args)
            unchanged = True
            for i, arg in enumerate(new_args):
                if isinstance(arg, ir.Expr):
                    new_arg = self.substitute(arg, mapping)
                    unchanged = unchanged and new_arg is arg
                    new_args[i] = new_arg
            if unchanged:
                return expr
            try:
                new_node = type(node)(*new_args)
            except IbisTypeError:
                return expr

            try:
                name = expr.get_name()
            except ExpressionError:
                name = None
            return expr._factory(new_node, name=name)


class ScalarAggregate:
    def __init__(self, expr, memo=None, default_name='tmp'):
        self.expr = expr
        self.memo = memo or {}
        self.tables = []
        self.default_name = default_name

    def get_result(self):
        expr = self.expr
        subbed_expr = self._visit(expr)

        try:
            name = subbed_expr.get_name()
            named_expr = subbed_expr
        except ExpressionError:
            name = self.default_name
            named_expr = subbed_expr.name(self.default_name)

        return (
            functools.reduce(ir.TableExpr.cross_join, self.tables).projection(
                [named_expr]
            ),
            name,
        )

    def _visit(self, expr):
        if is_scalar_reduction(expr) and not has_multiple_bases(expr):
            # An aggregation unit
            key = expr.op()
            if key not in self.memo:
                agg_expr, name = reduction_to_aggregation(expr)
                self.memo[key] = agg_expr, name
                self.tables.append(agg_expr)
            else:
                agg_expr, name = self.memo[key]
            return agg_expr[name]
        elif not isinstance(expr, ir.Expr):
            return expr

        node = expr.op()
        subbed_args = []
        for arg in node.args:
            if ibis.util.is_iterable(arg):
                subbed_arg = list(map(self._visit, arg))
            else:
                subbed_arg = self._visit(arg)
            subbed_args.append(subbed_arg)

        subbed_node = type(node)(*subbed_args)
        if isinstance(expr, ir.ValueExpr):
            result = expr._factory(subbed_node, name=expr._name)
        else:
            result = expr._factory(subbed_node)

        return result


def has_multiple_bases(expr):
    return toolz.count(find_immediate_parent_tables(expr)) > 1


def reduction_to_aggregation(expr, default_name='tmp'):
    tables = list(find_immediate_parent_tables(expr))

    try:
        name = expr.get_name()
        named_expr = expr
    except ExpressionError:
        name = default_name
        named_expr = expr.name(default_name)

    if len(tables) == 1:
        table, = tables
        return table.aggregate([named_expr]), name
    else:
        return ScalarAggregate(expr, None, default_name).get_result()


def find_immediate_parent_tables(expr):
    """Find every first occurrence of a :class:`ibis.expr.types.TableExpr`
    object in `expr`.

    Parameters
    ----------
    expr : ir.Expr

    Yields
    ------
    e : ir.Expr

    Notes
    -----
    This function does not traverse into TableExpr objects. This means that the
    underlying PhysicalTable of a Selection will not be yielded, for example.

    Examples
    --------
    >>> import ibis, toolz
    >>> t = ibis.table([('a', 'int64')], name='t')
    >>> expr = t.mutate(foo=t.a + 1)
    >>> result = list(find_immediate_parent_tables(expr))
    >>> len(result)
    1
    >>> result[0]  # doctest: +NORMALIZE_WHITESPACE
    ref_0
    UnboundTable[table]
      name: t
      schema:
        a : int64
    Selection[table]
      table:
        Table: ref_0
      selections:
        Table: ref_0
        foo = Add[int64*]
          left:
            a = Column[int64*] 'a' from table
              ref_0
          right:
            Literal[int8]
              1
    """

    def finder(expr):
        if isinstance(expr, ir.TableExpr):
            return lin.halt, expr
        else:
            return lin.proceed, None

    return lin.traverse(finder, expr)


def _base_table(table_node):
    # Find the aggregate or projection root. Not proud of this
    if table_node.blocks():
        return table_node
    else:
        return _base_table(table_node.table.op())


def has_reduction(expr):
    """Does `expr` contain a reduction?

    Parameters
    ----------
    expr : ibis.expr.types.Expr
        An ibis expression

    Returns
    -------
    truth_value : bool
        Whether or not there's at least one reduction in `expr`

    Notes
    -----
    The ``isinstance(op, ops.TableNode)`` check in this function implies
    that we only examine every non-table expression that precedes the first
    table expression.
    """

    def fn(expr):
        op = expr.op()
        if isinstance(op, ops.TableNode):  # don't go below any table nodes
            return lin.halt, None
        if isinstance(op, ops.Reduction):
            return lin.halt, True
        return lin.proceed, None

    reduction_status = lin.traverse(fn, expr)
    return any(reduction_status)


def windowize_function(expr, w=None):
    def _windowize(x, w):
        if not isinstance(x.op(), ops.WindowOp):
            walked = _walk(x, w)
        else:
            window_arg, window_w = x.op().args
            walked_child = _walk(window_arg, w)

            if walked_child is not window_arg:
                walked = x._factory(
                    ops.WindowOp(walked_child, window_w), name=x._name
                )
            else:
                walked = x

        op = walked.op()
        if isinstance(op, ops.AnalyticOp) or getattr(op, '_reduction', False):
            if w is None:
                w = window()
            return walked.over(w)
        elif isinstance(op, ops.WindowOp):
            if w is not None:
                return walked.over(w)
            else:
                return walked
        else:
            return walked

    def _walk(x, w):
        op = x.op()

        unchanged = True
        windowed_args = []
        for arg in op.args:
            if not isinstance(arg, ir.ValueExpr):
                windowed_args.append(arg)
                continue

            new_arg = _windowize(arg, w)
            unchanged = unchanged and arg is new_arg
            windowed_args.append(new_arg)

        if not unchanged:
            new_op = type(op)(*windowed_args)
            return x._factory(new_op, name=x._name)
        else:
            return x

    return _windowize(expr, w)


def _maybe_resolve_exprs(table, exprs):
    try:
        return table._resolve(exprs)
    except (AttributeError, IbisTypeError):
        return None


class ExprValidator:
    def __init__(self, exprs):
        self.parent_exprs = exprs

        self.roots = []
        for expr in self.parent_exprs:
            self.roots.extend(expr._root_tables())

    def has_common_roots(self, expr):
        return self.validate(expr)

    def validate(self, expr):
        op = expr.op()
        if isinstance(op, ops.TableColumn):
            if self._among_roots(op.table.op()):
                return True
        elif isinstance(op, ops.Selection):
            if self._among_roots(op):
                return True

        expr_roots = expr._root_tables()
        for root in expr_roots:
            if not self._among_roots(root):
                return False
        return True

    def _among_roots(self, node):
        return self.roots_shared(node) > 0

    def roots_shared(self, node):
        return sum(root.is_ancestor(node) for root in self.roots)

    def shares_some_roots(self, expr):
        expr_roots = expr._root_tables()
        return any(self._among_roots(root) for root in expr_roots)

    def shares_one_root(self, expr):
        expr_roots = expr._root_tables()
        total = sum(self.roots_shared(root) for root in expr_roots)
        return total == 1

    def shares_multiple_roots(self, expr):
        expr_roots = expr._root_tables()
        total = sum(self.roots_shared(expr_roots) for root in expr_roots)
        return total > 1

    def validate_all(self, exprs):
        for expr in exprs:
            self.assert_valid(expr)

    def assert_valid(self, expr):
        if not self.validate(expr):
            msg = self._error_message(expr)
            raise RelationError(msg)

    def _error_message(self, expr):
        return (
            'The expression %s does not fully originate from '
            'dependencies of the table expression.' % repr(expr)
        )


def fully_originate_from(exprs, parents):
    def finder(expr):
        op = expr.op()

        if isinstance(expr, ir.TableExpr):
            return lin.proceed, expr.op()
        return lin.halt if op.blocks() else lin.proceed, None

    # unique table dependencies of exprs and parents
    exprs_deps = set(lin.traverse(finder, exprs))
    parents_deps = set(lin.traverse(finder, parents))
    return exprs_deps <= parents_deps


class FilterValidator(ExprValidator):

    """
    Filters need not necessarily originate fully from the ancestors of the
    table being filtered. The key cases for this are

    - Scalar reductions involving some other tables
    - Array expressions involving other tables only (mapping to "uncorrelated
      subqueries" in SQL-land)
    - Reductions or array expressions like the above, but containing some
      predicate with a record-specific interdependency ("correlated subqueries"
      in SQL)
    """

    def validate(self, expr):
        op = expr.op()

        if isinstance(expr, ir.BooleanColumn) and isinstance(
            op, ops.TableColumn
        ):
            return True

        is_valid = True

        if isinstance(op, ops.Contains):
            value_valid = super().validate(op.value)
            is_valid = value_valid
        else:
            roots_valid = []
            for arg in op.flat_args():
                if isinstance(arg, ir.ScalarExpr):
                    # arg_valid = True
                    pass
                elif isinstance(arg, ir.TopKExpr):
                    # TopK not subjected to further analysis for now
                    roots_valid.append(True)
                elif isinstance(arg, (ir.ColumnExpr, ir.AnalyticExpr)):
                    roots_valid.append(self.shares_some_roots(arg))
                elif isinstance(arg, ir.Expr):
                    raise NotImplementedError(repr((type(expr), type(arg))))
                else:
                    # arg_valid = True
                    pass

            is_valid = any(roots_valid)

        return is_valid


def find_source_table(expr):
    """Find the first table expression observed for each argument that the
    expression depends on

    Parameters
    ----------
    expr : ir.Expr

    Returns
    -------
    table_expr : ir.TableExpr

    Examples
    --------
    >>> import ibis
    >>> t = ibis.table([('a', 'double'), ('b', 'string')], name='t')
    >>> expr = t.mutate(c=t.a + 42.0)
    >>> expr  # doctest: +NORMALIZE_WHITESPACE
    ref_0
    UnboundTable[table]
      name: t
      schema:
        a : float64
        b : string
    Selection[table]
      table:
        Table: ref_0
      selections:
        Table: ref_0
        c = Add[float64*]
          left:
            a = Column[float64*] 'a' from table
              ref_0
          right:
            Literal[float64]
              42.0
    >>> find_source_table(expr)
    UnboundTable[table]
      name: t
      schema:
        a : float64
        b : string
    >>> left = ibis.table([('a', 'int64'), ('b', 'string')])
    >>> right = ibis.table([('c', 'int64'), ('d', 'string')])
    >>> result = left.inner_join(right, left.a == right.c)
    >>> find_source_table(result)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    NotImplementedError: More than one base table not implemented
    """

    def finder(expr):
        if isinstance(expr, ir.TableExpr):
            return lin.halt, expr
        else:
            return lin.proceed, None

    first_tables = lin.traverse(finder, expr.op().flat_args())
    options = list(toolz.unique(first_tables, key=operator.methodcaller('op')))

    if len(options) > 1:
        raise NotImplementedError('More than one base table not implemented')

    return options[0]


def flatten_predicate(expr):
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
    >>> predicates[0]  # doctest: +NORMALIZE_WHITESPACE
    ref_0
    UnboundTable[table]
      name: t
      schema:
        a : int64
        b : string
    Equals[boolean*]
      left:
        a = Column[int64*] 'a' from table
          ref_0
      right:
        Literal[int64]
          1
    >>> predicates[1]  # doctest: +NORMALIZE_WHITESPACE
    ref_0
    UnboundTable[table]
      name: t
      schema:
        a : int64
        b : string
    Equals[boolean*]
      left:
        b = Column[string*] 'b' from table
          ref_0
      right:
        Literal[string]
          foo
    """

    def predicate(expr):
        if isinstance(expr.op(), ops.And):
            return lin.proceed, None
        else:
            return lin.halt, expr

    return list(lin.traverse(predicate, expr, type=ir.BooleanColumn))


def is_analytic(expr: Any, exclude_windows: bool = False) -> bool:
    if not isinstance(expr, ir.Expr):
        return False
    op = expr.op()
    if isinstance(op, ops.WindowOp) and exclude_windows:
        return False
    return isinstance(op, (ops.Reduction, ops.AnalyticOp)) or any(
        map(is_analytic, op.args)
    )


def is_reduction(expr):
    """Check whether an expression is a reduction or not

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

    def has_reduction(op):
        if getattr(op, '_reduction', False):
            return True

        for arg in op.args:
            if isinstance(arg, ir.ScalarExpr) and has_reduction(arg.op()):
                return True

        return False

    return has_reduction(expr.op() if isinstance(expr, ir.Expr) else expr)


def is_scalar_reduction(expr):
    return isinstance(expr, ir.ScalarExpr) and is_reduction(expr)
