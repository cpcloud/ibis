import functools
import operator
import os
import itertools
import warnings
import webbrowser

from typing import Iterable, Sequence, Tuple

import ibis
import ibis.util as util
import ibis.common as com
import ibis.config as config


# TODO move methods containing ops import to api.py

class Expr:
    """Base expression class"""

    def _type_display(self):
        return type(self).__name__

    def __init__(self, arg):
        # TODO: all inputs must inherit from a common table API
        self._arg = arg

    def __repr__(self):
        if not config.options.interactive:
            return self._repr()

        try:
            result = self.execute()
        except com.TranslationError as e:
            output = ('Translation to backend failed\n'
                      'Error message: {0}\n'
                      'Expression repr follows:\n{1}'
                      .format(e.args[0], self._repr()))
            return output
        else:
            return repr(result)

    def __hash__(self):
        return hash(self._key)

    def __bool__(self):
        raise ValueError("The truth value of an Ibis expression is not "
                         "defined")

    __nonzero__ = __bool__

    def _repr(self, memo=None):
        from ibis.expr.format import ExprFormatter
        return ExprFormatter(self, memo=memo).get_result()

    @property
    def _safe_name(self):
        """Get the name of an expression `expr`, returning ``None`` if the
        expression has no name.

        Returns
        -------
        Optional[str]
        """
        try:
            return self.get_name()
        except (com.ExpressionError, AttributeError):
            return None

    @property
    def _key(self):
        """Key suitable for hashing an expression.

        Returns
        -------
        Tuple[Type[Expr], Optional[str], ibis.expr.operations.Node]
            A tuple of hashable objects uniquely identifying this expression.
        """
        return type(self), self._safe_name, self.op()

    def _repr_png_(self):
        if not ibis.options.graphviz_repr:
            return None
        try:
            import ibis.expr.visualize as viz
        except ImportError:
            return None
        else:
            try:
                return viz.to_graph(self).pipe(format='png')
            except Exception:
                # Something may go wrong, and we can't error in the notebook
                # so fallback to the default text representation.
                return None

    def visualize(self, format='svg'):
        """Visualize an expression in the browser as an SVG image.

        Parameters
        ----------
        format : str, optional
            Defaults to ``'svg'``. Some additional formats are
            ``'jpeg'`` and ``'png'``. These are specified by the ``graphviz``
            Python library.

        Notes
        -----
        This method opens a web browser tab showing the image of the expression
        graph created by the code in :module:`ibis.expr.visualize`.

        Raises
        ------
        ImportError
            If ``graphviz`` is not installed.
        """
        import ibis.expr.visualize as viz
        path = viz.draw(viz.to_graph(self), format=format)
        webbrowser.open('file://{}'.format(os.path.abspath(path)))

    def pipe(self, f, *args, **kwargs):
        """Generic composition function to enable expression pipelining.

        Parameters
        ----------
        f : function or (function, arg_name) tuple
          If the expression needs to be passed as anything other than the first
          argument to the function, pass a tuple with the argument name. For
          example, (f, 'data') if the function f expects a 'data' keyword
        args : positional arguments
        kwargs : keyword arguments

        Examples
        --------
        >>> import ibis
        >>> t = ibis.table([('a', 'int64'), ('b', 'string')], name='t')
        >>> f = lambda a: (a + 1).name('a')
        >>> g = lambda a: (a * 2).name('a')
        >>> result1 = t.a.pipe(f).pipe(g)
        >>> result1  # doctest: +NORMALIZE_WHITESPACE
        ref_0
        UnboundTable[table]
          name: t
          schema:
            a : int64
            b : string
        a = Multiply[int64*]
          left:
            a = Add[int64*]
              left:
                a = Column[int64*] 'a' from table
                  ref_0
              right:
                Literal[int8]
                  1
          right:
            Literal[int8]
              2
        >>> result2 = g(f(t.a))  # equivalent to the above
        >>> result1.equals(result2)
        True

        Returns
        -------
        result : result type of passed function
        """
        if isinstance(f, tuple):
            f, data_keyword = f
            kwargs = kwargs.copy()
            kwargs[data_keyword] = self
            return f(*args, **kwargs)
        else:
            return f(self, *args, **kwargs)

    __call__ = pipe

    def op(self):
        return self._arg

    @property
    def _factory(self):
        def factory(arg, name=None):
            return type(self)(arg, name=name)
        return factory

    def execute(self, limit='default', params=None, **kwargs):
        """
        If this expression is based on physical tables in a database backend,
        execute it against that backend.

        Parameters
        ----------
        limit : integer or None, default 'default'
          Pass an integer to effect a specific row limit. limit=None means "no
          limit". The default is whatever is in ibis.options.

        Returns
        -------
        result : expression-dependent
          Result of compiling expression and executing in backend
        """
        from ibis.client import execute
        return execute(self, limit=limit, params=params, **kwargs)

    def compile(self, limit=None, params=None):
        """
        Compile expression to whatever execution target, to verify

        Returns
        -------
        compiled : value or list
           query representation or list thereof
        """
        from ibis.client import compile
        return compile(self, limit=limit, params=params)

    def verify(self):
        """
        Returns True if expression can be compiled to its attached client
        """
        try:
            self.compile()
        except Exception:
            return False
        else:
            return True

    def equals(self, other, cache=None):
        if type(self) != type(other):
            return False
        return self._arg.equals(other._arg, cache=cache)

    def _root_tables(self):
        return self.op().root_tables()


class ExprList(Expr):
    def _type_display(self):
        return ', '.join(expr._type_display() for expr in self.exprs())

    def exprs(self):
        return self.op().exprs

    def names(self):
        return [x.get_name() for x in self.exprs()]

    def types(self):
        return [x.type() for x in self.exprs()]

    def schema(self):
        import ibis.expr.schema as sch
        return sch.Schema(self.names(), self.types())

    def rename(self, f):
        import ibis.expr.operations as ops
        new_exprs = [x.name(f(x.get_name())) for x in self.exprs()]
        return ops.ExpressionList(new_exprs).to_expr()

    def prefix(self, value):
        return self.rename(lambda x: value + x)

    def suffix(self, value):
        return self.rename(lambda x: x + value)

    def concat(self, *others):
        """
        Concatenate expression lists

        Returns
        -------
        combined : ExprList
        """
        import ibis.expr.operations as ops
        exprs = list(self.exprs())
        for o in others:
            if not isinstance(o, ExprList):
                raise TypeError(o)
            exprs.extend(o.exprs())
        return ops.ExpressionList(exprs).to_expr()


# ---------------------------------------------------------------------
# Helper / factory functions


class ValueExpr(Expr):

    """
    Base class for a data generating expression having a fixed and known type,
    either a single value (scalar)
    """

    def __init__(self, arg, dtype, name=None):
        super().__init__(arg)
        self._name = name
        self._dtype = dtype

    def equals(self, other, cache=None):
        return (
            isinstance(other, ValueExpr) and
            self._name == other._name and
            self._dtype == other._dtype and
            super().equals(other, cache=cache)
        )

    def has_name(self):
        if self._name is not None:
            return True
        return self.op().has_resolved_name()

    def get_name(self):
        if self._name is not None:
            # This value has been explicitly named
            return self._name

        # In some but not all cases we can get a name from the node that
        # produces the value
        return self.op().resolve_name()

    def name(self, name):
        return self._factory(self._arg, name=name)

    def type(self):
        return self._dtype

    @property
    def _factory(self):
        def factory(arg, name=None):
            return type(self)(arg, dtype=self.type(), name=name)
        return factory


class ScalarExpr(ValueExpr):

    def _type_display(self):
        return str(self.type())


class ColumnExpr(ValueExpr):

    def _type_display(self):
        return '{}*'.format(self.type())

    def parent(self):
        return self._arg

    def to_projection(self):
        """
        Promote this column expression to a table projection
        """
        roots = self._root_tables()
        if len(roots) > 1:
            raise com.RelationError('Cannot convert array expression '
                                    'involving multiple base table references '
                                    'to a projection')

        table = TableExpr(roots[0])
        return table.projection([self])


class AnalyticExpr(Expr):

    @property
    def _factory(self):
        def factory(arg):
            return type(self)(arg)
        return factory

    def _type_display(self):
        return str(self.type())

    def type(self):
        return 'analytic'


def _resolve_predicates(table, predicates):
    import ibis.expr.analysis as L
    if isinstance(predicates, Expr):
        preds = L.flatten_predicate(predicates)
    else:
        preds = predicates
    pred_gen = map(functools.partial(bind_expr, table), util.to_tuple(preds))
    resolved_predicates = tuple(
        pred.to_filter()
        if isinstance(pred, AnalyticExpr)
        else pred
        for pred in pred_gen
    )
    return resolved_predicates


def clean_predicates(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        import ibis.expr.analysis as L
        predicates = kwargs.pop('predicates', ())
        if isinstance(predicates, Expr):
            predicates = L.flatten_predicate(predicates)
        else:
            predicates = util.to_tuple(predicates)
        kwargs['predicates'] = predicates
        return method(*args, **kwargs)
    return wrapper


class TableExpr(Expr):
    @property
    def _factory(self):
        def factory(arg):
            return TableExpr(arg)
        return factory

    def _type_display(self):
        return 'table'

    def _is_valid(self, exprs):
        try:
            self._assert_valid(util.to_tuple(exprs))
        except com.RelationError:
            return False
        else:
            return True

    def _assert_valid(self, exprs):
        from ibis.expr.analysis import ExprValidator
        ExprValidator((self,)).validate_all(exprs)

    def __contains__(self, name):
        return name in self.schema()

    def __getitem__(self, what):
        if isinstance(what, (str, int)):
            return self.get_column(what)

        if isinstance(what, slice):
            step = what.step
            if step is not None and step != 1:
                raise ValueError('Slice step can only be 1')
            start = what.start or 0
            stop = what.stop

            if stop is None or stop < 0:
                raise ValueError('End index must be a positive number')

            if start < 0:
                raise ValueError('Start index must be a positive number')

            return self.limit(stop - start, offset=start)

        what = bind_expr(self, what)

        if isinstance(what, AnalyticExpr):
            what = what._table_getitem()

        if isinstance(what, (list, tuple, TableExpr)):
            # Projection case
            return self.projection(what)
        elif isinstance(what, BooleanColumn):
            # Boolean predicate
            return self.filter((what,))
        elif isinstance(what, ColumnExpr):
            # Projection convenience
            return self.projection(what)
        else:
            raise NotImplementedError(
                'Selection rows or columns with {} objects is not '
                'supported'.format(type(what).__name__)
            )

    def __len__(self):
        raise com.ExpressionError('Use .count() instead')

    def __setstate__(self, instance_dictionary):
        self.__dict__ = instance_dictionary

    def __getattr__(self, key):
        try:
            schema = self.schema()
        except com.IbisError:
            raise AttributeError(key)

        if key not in schema:
            raise AttributeError(key)

        try:
            return self.get_column(key)
        except com.IbisTypeError:
            raise AttributeError(key)

    def __dir__(self):
        attrs = dir(type(self))
        if self._is_materialized():
            attrs = frozenset(attrs + self.schema().names)
        return sorted(attrs)

    def _resolve(self, exprs: Iterable) -> Tuple[Expr, ...]:
        # Stash this helper method here for now
        return tuple(map(self._ensure_expr, util.to_tuple(exprs)))

    def _ensure_expr(self, expr):
        if isinstance(expr, str):
            return self[expr]
        elif isinstance(expr, int):
            return self[self.schema().name_at_position(expr)]
        elif not isinstance(expr, Expr):
            return expr(self)
        else:
            return expr

    def _get_type(self, name):
        return self._arg.get_type(name)

    def get_columns(self, iterable):
        """
        Get multiple columns from the table

        Examples
        --------
        >>> import ibis
        >>> table = ibis.table(
        ...    [
        ...        ('a', 'int64'),
        ...        ('b', 'string'),
        ...        ('c', 'timestamp'),
        ...        ('d', 'float'),
        ...    ],
        ...    name='t'
        ... )
        >>> a, b, c = table.get_columns(['a', 'b', 'c'])

        Returns
        -------
        columns : list of column/array expressions
        """
        return [self.get_column(x) for x in iterable]

    def get_column(self, name):
        """
        Get a reference to a single column from the table

        Returns
        -------
        column : array expression
        """
        import ibis.expr.operations as ops
        ref = ops.TableColumn(self, name)
        return ref.to_expr()

    @property
    def columns(self):
        return self.schema().names

    def schema(self):
        """
        Get the schema for this table (if one is known)

        Returns
        -------
        schema : Schema
        """
        if not self._is_materialized():
            raise com.IbisError('Table operation is not yet materialized')
        return self.op().schema

    def _is_materialized(self):
        # The operation produces a known schema
        return self.op().has_schema()

    def group_by(self, by=None, **additional_grouping_expressions):
        """Create an intermediate grouped table expression.

        This returns an object that is pending some group operation to be
        applied with it.

        Examples
        --------
        >>> import ibis
        >>> pairs = [('a', 'int32'), ('b', 'timestamp'), ('c', 'double')]
        >>> t = ibis.table(pairs)
        >>> b1, b2 = t.a, t.b
        >>> result = t.group_by([b1, b2]).aggregate(sum_of_c=t.c.sum())

        Notes
        -----
        group_by and groupby are equivalent, with `groupby` being provided for
        ease-of-use for pandas users.

        Returns
        -------
        grouped_expr : GroupedTableExpr
        """
        from ibis.expr.groupby import GroupedTableExpr
        return GroupedTableExpr(self, by, **additional_grouping_expressions)

    groupby = group_by

    def distinct(self) -> 'TableExpr':
        """Compute set of unique rows/tuples occurring in this table."""
        import ibis.expr.operations as ops
        return ops.Distinct(self).to_expr()

    def limit(self, n: int, offset: int = 0) -> 'TableExpr':
        """Select the first `n` rows of a table.

        This operation is not deterministic unless the query contains an
        ``ORDER BY``.

        Parameters
        ----------
        n
            Number of rows to include
        offset
            Number of rows to skip first

        Returns
        -------
        TableExpr

        """
        import ibis.expr.operations as ops
        op = ops.Limit(self, n, offset=offset)
        return op.to_expr()

    def head(self, n: int = 5) -> 'TableExpr':
        """Select the first `n` rows of a table, with `n` defaulting to 5.

        Parameters
        ----------
        n : int
            Number of rows to include, defaults to 5

        Returns
        -------
        TableExpr

        See Also
        --------
        ibis.expr.types.TableExpr.limit

        """
        return self.limit(n)

    def count(self) -> 'IntegerScalar':
        """Compute the number of rows in the table expression.

        Returns
        -------
        IntegerScalar

        """
        import ibis.expr.operations as ops
        return ops.Count(self, None).to_expr().name('count')

    def info(self, buf=None) -> None:
        """Similar to pandas DataFrame.info.

        Show column names, types, and null counts. Output to stdout by default.

        """
        metrics = [self.count().name('nrows')]
        for col in self.columns:
            metrics.append(self[col].count().name(col))
        metrics = self.aggregate(metrics).execute().loc[0]
        names = ['Column', '------'] + self.columns
        types = ['Type', '----'] + [repr(x) for x in self.schema().types]
        counts = ['Non-null #', '----------'] + [str(x) for x in metrics[1:]]
        col_metrics = util.adjoin(2, names, types, counts)
        result = 'Table rows: {}\n\n{}'.format(metrics[0], col_metrics)
        print(result, file=buf)

    def drop(self, fields: Sequence[str]) -> 'TableExpr':
        if not fields:
            return self

        fields = set(fields)
        to_project = []
        for name in self.schema():
            if name in fields:
                fields.remove(name)
            else:
                to_project.append(name)

        if fields > 0:
            raise KeyError('Fields not in table: {}'.format(fields))
        return self.projection(to_project)

    def aggregate(
        self, metrics=None, by=None, having=None, **kwds
    ) -> 'TableExpr':
        """Aggregate a table with a given set of reductions.

        Grouping expressions (`by`) and post-aggregation filters (`having`) are
        allowed.

        Parameters
        ----------
        table : ir.TableExpr
        metrics : Optional[Union[ir.ValueExpr, Sequence[ir.ValueExpr]]]
        by : Optional[ir.ValueExpr]
            Grouping expressions
        having : Optional[Union[ir.BooleanScalar, Sequence[ir.BooleanScalar]]]
            Post-aggregation filters

        Returns
        -------
        ir.TableExpr

        """
        metrics = () if metrics is None else util.to_tuple(metrics)
        metrics += tuple(
            self._ensure_expr(v).name(k) for k, v in sorted(kwds.items())
        )
        table_op = self.op()
        op = table_op.aggregate(self, metrics, by=by, having=having)
        return op.to_expr()

    def filter(self, predicates) -> 'TableExpr':
        """Select rows from table based on boolean expressions.

        Parameters
        ----------
        predicates : boolean array expressions, or list thereof

        Returns
        -------
        TableExpr

        """
        import ibis.expr.analysis as L
        resolved_predicates = _resolve_predicates(self, predicates)
        return L.apply_filter(self, resolved_predicates)

    def sort_by(self, sort_exprs):
        """Sort table by the indicated column expressions and sort orders.

        Parameters
        ----------
        sort_exprs : sorting expressions
          Must be one of:
            - Column name or expression
            - Sort key, e.g. desc(col)
            - (column name, True (ascending) / False (descending))

        Examples
        --------
        >>> import ibis
        >>> t = ibis.table([('a', 'int64'), ('b', 'string')])
        >>> ab_sorted = t.sort_by([('a', True), ('b', False)])
        >>> b_sorted = t.sort_by(('b', False))
        >>> a_sorted = t.sort_by('a')  # defaults to ascending

        Returns
        -------
        TableExpr

        """
        # XXX: We should not allow so much variation in input. How about use of
        # asc, desc on strings, just column name, and column expression
        if isinstance(sort_exprs, tuple):
            sort_exprs = (sort_exprs,)
        elif isinstance(sort_exprs, list):
            sort_exprs = util.to_tuple(sort_exprs)
        elif isinstance(sort_exprs, str):
            sort_exprs = ((sort_exprs, True),)
        result = self.op().sort_by(self, sort_exprs)
        return result.to_expr()

    def union(self, other: 'TableExpr', distinct: bool = False) -> 'TableExpr':
        """Form the table set union of two table expressions.

        `self` and `other` must have identical schemas.

        Parameters
        ----------
        right : TableExpr
        distinct : boolean, default False
            Only union distinct rows not occurring in the calling table (this
            can be very expensive, be careful)

        Returns
        -------
        TableExpr

        """
        import ibis.expr.operations as ops
        return ops.Union(self, other, distinct=distinct).to_expr()

    def materialize(self) -> 'TableExpr':
        """Force schema resolution for a joined table.

        This will select all fields from all tables.

        Returns
        -------
        TableExpr

        """
        if self._is_materialized():
            return self

        import ibis.expr.operations as ops
        return ops.MaterializedJoin(self).to_expr()

    def add_column(self, expr, name=None):
        """
        Add indicated column expression to table, producing a new table. Note:
        this is a shortcut for performing a projection having the same effect.

        Returns
        -------
        modified_table : TableExpr
        """
        warnings.warn('add_column is deprecated, use mutate(name=expr, ...)',
                      DeprecationWarning)
        if name is not None:
            return self.mutate(**{name: expr})
        else:
            return self.mutate(expr)

    def mutate(self, new_columns=None, **mutations):
        """Convenience function to add columns to `self`.

        Parameters
        ----------
        exprs : list, default None
          List of named expressions to add as columns
        mutations : keywords for new columns

        Returns
        -------
        mutated : TableExpr

        Examples
        --------
        Using keywords arguments to name the new columns

        >>> import ibis
        >>> table = ibis.table([
        ...     ('foo', 'double'), ('bar', 'double')],
        ...     name='t'
        ... )
        >>> expr = table.mutate(qux=table.foo + table.bar, baz=5)
        >>> expr  # doctest: +NORMALIZE_WHITESPACE
        ref_0
        UnboundTable[table]
          name: t
          schema:
            foo : float64
            bar : float64
        <BLANKLINE>
        Selection[table]
          table:
            Table: ref_0
          selections:
            Table: ref_0
            baz = Literal[int8]
              5
            qux = Add[float64*]
              left:
                foo = Column[float64*] 'foo' from table
                  ref_0
              right:
                bar = Column[float64*] 'bar' from table
                  ref_0

        Using the :meth:`ibis.expr.types.Expr.name` method to name the new
        columns

        >>> new_columns = [ibis.literal(5).name('baz',),
        ...                (table.foo + table.bar).name('qux')]
        >>> expr2 = table.mutate(new_columns)
        >>> expr.equals(expr2)
        True

        """
        exprs = () if new_columns is None else util.to_tuple(new_columns)
        exprs += tuple(
            (v(self) if util.is_function(v) else as_value_expr(v)).name(k)
            for k, v in sorted(mutations.items(), key=operator.itemgetter(0))
        )

        has_replacement = False
        for expr in exprs:
            if expr.get_name() in self:
                has_replacement = True

        if has_replacement:
            by_name = {x.get_name(): x for x in exprs}
            used = set()
            proj_exprs = []
            for c in self.columns:
                if c in by_name:
                    proj_exprs.append(by_name[c])
                    used.add(c)
                else:
                    proj_exprs.append(c)

            for x in exprs:
                if x.get_name() not in used:
                    proj_exprs.append(x)

            return self.projection(tuple(proj_exprs))
        else:
            return self.projection((self,) + exprs)

    def projection(self, exprs):
        """
        Compute new table expression with the indicated column expressions from
        this table.

        Parameters
        ----------
        exprs : column expression, or string, or list of column expressions and
          strings. If strings passed, must be columns in the table already

        Returns
        -------
        projection : TableExpr

        Notes
        -----
        Passing an aggregate function to this method will broadcast the
        aggregate's value over the number of rows in the table. See the
        examples section for more details.

        Examples
        --------
        Simple projection

        >>> import ibis
        >>> fields = [('a', 'int64'), ('b', 'double')]
        >>> t = ibis.table(fields, name='t')
        >>> proj = t.projection([t.a, (t.b + 1).name('b_plus_1')])
        >>> proj  # doctest: +NORMALIZE_WHITESPACE
        ref_0
        UnboundTable[table]
          name: t
          schema:
            a : int64
            b : float64
        <BLANKLINE>
        Selection[table]
          table:
            Table: ref_0
          selections:
            a = Column[int64*] 'a' from table
              ref_0
            b_plus_1 = Add[float64*]
              left:
                b = Column[float64*] 'b' from table
                  ref_0
              right:
                Literal[int8]
                  1
        >>> proj2 = t[t.a, (t.b + 1).name('b_plus_1')]
        >>> proj.equals(proj2)
        True

        Aggregate projection

        >>> agg_proj = t[t.a.sum().name('sum_a'), t.b.mean().name('mean_b')]
        >>> agg_proj  # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
        ref_0
        UnboundTable[table]
          name: t
          schema:
            a : int64
            b : float64
        <BLANKLINE>
        Selection[table]
          table:
            Table: ref_0
          selections:
            sum_a = WindowOp[int64*]
              sum_a = Sum[int64]
                a = Column[int64*] 'a' from table
                  ref_0
                where:
                  None
              <ibis.expr.window.Window object at 0x...>
            mean_b = WindowOp[float64*]
              mean_b = Mean[float64]
                b = Column[float64*] 'b' from table
                  ref_0
                where:
                  None
              <ibis.expr.window.Window object at 0x...>

        Note the ``<ibis.expr.window.Window>`` objects here, their existence
        means that the result of the aggregation will be broadcast across the
        number of rows in the input column. The purpose of this expression
        rewrite is to make it easy to write column/scalar-aggregate operations
        like

        .. code-block:: python

           t[(t.a - t.a.mean()).name('demeaned_a')]

        """
        import ibis.expr.analysis as L

        if isinstance(exprs, (Expr, str)):
            exprs = [exprs]

        projector = L.Projector(self, tuple(exprs))
        op = projector.get_result()
        return op.to_expr()

    def relabel(self, substitutions, replacements=None) -> 'TableExpr':
        """Change table column names, otherwise leaving table unaltered.

        Parameters
        ----------
        substitutions

        Returns
        -------
        TableExpr

        """
        if replacements is not None:
            raise NotImplementedError

        observed = set()

        exprs = []
        for c in self.columns:
            expr = self[c]
            if c in substitutions:
                expr = expr.name(substitutions[c])
                observed.add(c)
            exprs.append(expr)

        for c in substitutions:
            if c not in observed:
                raise KeyError('{!r} is not an existing column'.format(c))
        return self.projection(exprs)

    def view(self):
        """
        Create a new table expression that is semantically equivalent to the
        current one, but is considered a distinct relation for evaluation
        purposes (e.g. in SQL).

        For doing any self-referencing operations, like a self-join, you will
        use this operation to create a reference to the current table
        expression.

        Returns
        -------
        expr : TableExpr
        """
        import ibis.expr.operations as ops
        new_view = ops.SelfReference(self)
        return new_view.to_expr()

    def set_column(self, name: str, expr: 'ValueExpr') -> 'TableExpr':
        """Replace an existing column with a new expression.

        Parameters
        ----------
        name
            Column name to replace
        expr
            New data for column

        Returns
        -------
        TableExpr
            New table expression

        """
        expr = self._ensure_expr(expr)

        if expr._name != name:
            expr = expr.name(name)

        if name not in self:
            raise KeyError('Column {!r} is not in the table'.format(name))

        # TODO: This assumes that projection is required; may be
        # backend-dependent
        proj_exprs = [
            expr if key == name else self[key] for key in self.columns
        ]

        return self.projection(proj_exprs)

    @clean_predicates
    def join(
        self, other: 'TableExpr', predicates=(), how: str = 'inner'
    ) -> 'TableExpr':
        """Perform a relational join between two tables

        The resulting object does not have a resolved schema.

        Parameters
        ----------
        other : TableExpr
            The other table to join
        predicates : join expression(s)
        how : string, default 'inner'
          - 'inner': inner join
          - 'left': left join
          - 'outer': full outer join
          - 'right': right outer join
          - 'semi' or 'left_semi': left semi join
          - 'anti': anti join

        Returns
        -------
        joined : TableExpr
            Note that the schema is not materialized yet
        """

        method = getattr(self, '{}_join'.format(how))

        return method(other, predicates=predicates)

    @clean_predicates
    def inner_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.InnerJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def left_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.LeftJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def any_inner_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.LeftJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def any_left_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.AnyLeftJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def outer_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.OuterJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def semi_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.SemiJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def anti_join(self, other: 'TableExpr', predicates=()) -> 'TableExpr':
        import ibis.expr.operations as ops
        return ops.AntiJoin(self, other, predicates=predicates).to_expr()

    @clean_predicates
    def asof_join(
        self, other: 'TableExpr', predicates=(), by=(), tolerance=None
    ) -> 'TableExpr':
        """Perform an asof join between two tables.

        Similar to a left join except that the match is done on nearest key
        rather than equal keys.

        Optionally, match keys with 'by' before joining with predicates.

        Parameters
        ----------
        other : TableExpr
        predicates : join expression(s)
        by : string
            column to group by before joining
        tolerance : interval
            Amount of time to look behind when joining

        Returns
        -------
        TableExpr
            A TableExpr with an unmaterialized schema

        """
        import ibis.expr.operations as ops
        return ops.AsOfJoin(self, other, predicates, by, tolerance).to_expr()

    def cross_join(
        self, other: 'TableExpr', *tables: 'TableExpr'
    ) -> 'TableExpr':
        """Perform a cross join amongst a list of tables.

        Parameters
        ----------
        tables : ibis.expr.types.TableExpr

        Returns
        -------
        joined : TableExpr

        Examples
        --------
        >>> import ibis
        >>> schemas = [(name, 'int64') for name in 'abcde']
        >>> a, b, c, d, e = [
        ...     ibis.table([(name, type)], name=name) for name, type in schemas
        ... ]
        >>> joined1 = ibis.cross_join(a, b, c, d, e)
        >>> joined1  # doctest: +NORMALIZE_WHITESPACE
        ref_0
        UnboundTable[table]
          name: a
          schema:
            a : int64
        ref_1
        UnboundTable[table]
          name: b
          schema:
            b : int64
        ref_2
        UnboundTable[table]
          name: c
          schema:
            c : int64
        ref_3
        UnboundTable[table]
          name: d
          schema:
            d : int64
        ref_4
        UnboundTable[table]
          name: e
          schema:
            e : int64
        CrossJoin[table]
          left:
            Table: ref_0
          right:
            CrossJoin[table]
              left:
                CrossJoin[table]
                  left:
                    CrossJoin[table]
                      left:
                        Table: ref_1
                      right:
                        Table: ref_2
                  right:
                    Table: ref_3
              right:
                Table: ref_4

        """
        import ibis.expr.operations as ops
        # TODO(phillipc): Implement prefix keyword argument
        op = ops.CrossJoin(
            self,
            functools.reduce(
                type(self).cross_join, itertools.chain([other], tables)
            ),
        )
        return op.to_expr()


# -----------------------------------------------------------------------------
# Declare all typed ValueExprs. This is what the user will actually interact
# with: an instance of each is well-typed and includes all valid methods
# defined for each type.


class AnyValue(ValueExpr): pass  # noqa: E701,E302
class AnyScalar(ScalarExpr, AnyValue): pass  # noqa: E701,E302
class AnyColumn(ColumnExpr, AnyValue): pass  # noqa: E701,E302


class NullValue(AnyValue): pass  # noqa: E701,E302
class NullScalar(AnyScalar, NullValue): pass  # noqa: E701,E302
class NullColumn(AnyColumn, NullValue): pass  # noqa: E701,E302


class NumericValue(AnyValue): pass  # noqa: E701,E302
class NumericScalar(AnyScalar, NumericValue): pass  # noqa: E701,E302
class NumericColumn(AnyColumn, NumericValue): pass  # noqa: E701,E302


class BooleanValue(NumericValue): pass  # noqa: E701,E302
class BooleanScalar(NumericScalar, BooleanValue): pass  # noqa: E701,E302
class BooleanColumn(NumericColumn, BooleanValue): pass  # noqa: E701,E302


class IntegerValue(NumericValue): pass  # noqa: E701,E302
class IntegerScalar(NumericScalar, IntegerValue): pass  # noqa: E701,E302
class IntegerColumn(NumericColumn, IntegerValue): pass  # noqa: E701,E302


class FloatingValue(NumericValue): pass  # noqa: E701,E302
class FloatingScalar(NumericScalar, FloatingValue): pass  # noqa: E701,E302
class FloatingColumn(NumericColumn, FloatingValue): pass  # noqa: E701,E302


class DecimalValue(NumericValue): pass  # noqa: E701,E302
class DecimalScalar(NumericScalar, DecimalValue): pass  # noqa: E701,E302
class DecimalColumn(NumericColumn, DecimalValue): pass  # noqa: E701,E302


class StringValue(AnyValue): pass  # noqa: E701,E302
class StringScalar(AnyScalar, StringValue): pass  # noqa: E701,E302
class StringColumn(AnyColumn, StringValue): pass  # noqa: E701,E302


class BinaryValue(AnyValue): pass  # noqa: E701,E302
class BinaryScalar(AnyScalar, BinaryValue): pass  # noqa: E701,E302
class BinaryColumn(AnyColumn, BinaryValue): pass  # noqa: E701,E302


class TemporalValue(AnyValue): pass  # noqa: E701,E302
class TemporalScalar(AnyScalar, TemporalValue): pass  # noqa: E701,E302
class TemporalColumn(AnyColumn, TemporalValue): pass  # noqa: E701,E302


class TimeValue(TemporalValue): pass  # noqa: E701,E302
class TimeScalar(TemporalScalar, TimeValue): pass  # noqa: E701,E302
class TimeColumn(TemporalColumn, TimeValue): pass  # noqa: E701,E302


class DateValue(TemporalValue): pass  # noqa: E701,E302
class DateScalar(TemporalScalar, DateValue): pass  # noqa: E701,E302
class DateColumn(TemporalColumn, DateValue): pass  # noqa: E701,E302


class TimestampValue(TemporalValue): pass  # noqa: E701,E302
class TimestampScalar(TemporalScalar, TimestampValue): pass  # noqa: E701,E302
class TimestampColumn(TemporalColumn, TimestampValue): pass  # noqa: E701,E302


class CategoryValue(AnyValue): pass  # noqa: E701,E302
class CategoryScalar(AnyScalar, CategoryValue): pass  # noqa: E701,E302
class CategoryColumn(AnyColumn, CategoryValue): pass  # noqa: E701,E302


class EnumValue(AnyValue): pass  # noqa: E701,E302
class EnumScalar(AnyScalar, EnumValue): pass  # noqa: E701,E302
class EnumColumn(AnyColumn, EnumValue): pass  # noqa: E701,E302


class ArrayValue(AnyValue): pass  # noqa: E701,E302
class ArrayScalar(AnyScalar, ArrayValue): pass  # noqa: E701,E302
class ArrayColumn(AnyColumn, ArrayValue): pass  # noqa: E701,E302


class SetValue(AnyValue): pass  # noqa: E701,E302
class SetScalar(AnyScalar, SetValue): pass  # noqa: E701,E302
class SetColumn(AnyColumn, SetValue): pass  # noqa: E701,E302


class MapValue(AnyValue): pass  # noqa: E701,E302
class MapScalar(AnyScalar, MapValue): pass  # noqa: E701,E302
class MapColumn(AnyColumn, MapValue): pass  # noqa: E701,E302


class StructValue(AnyValue):

    def __dir__(self):
        return sorted(frozenset(
            itertools.chain(dir(type(self)), self.type().names)
        ))

class StructScalar(AnyScalar, StructValue): pass  # noqa: E701,E302
class StructColumn(AnyColumn, StructValue): pass  # noqa: E701,E302


class IntervalValue(AnyValue): pass  # noqa: E701,E302
class IntervalScalar(AnyScalar, IntervalValue): pass  # noqa: E701,E302
class IntervalColumn(AnyColumn, IntervalValue): pass  # noqa: E701,E302


class ListExpr(ColumnExpr, AnyValue):

    @property
    def values(self):
        return self.op().values

    def __iter__(self):
        return iter(self.values)

    def __getitem__(self, key):
        return self.values[key]

    def __add__(self, other):
        other_values = tuple(getattr(other, 'values', other))
        return type(self.op())(self.values + other_values).to_expr()

    def __radd__(self, other):
        other_values = tuple(getattr(other, 'values', other))
        return type(self.op())(other_values + self.values).to_expr()

    def __bool__(self):
        return bool(self.values)

    __nonzero__ = __bool__

    def __len__(self):
        return len(self.values)


class TopKExpr(AnalyticExpr):

    def type(self):
        return 'topk'

    def _table_getitem(self):
        return self.to_filter()

    def to_filter(self):
        # TODO: move to api.py
        import ibis.expr.operations as ops
        return ops.SummaryFilter(self).to_expr()

    def to_aggregation(self, metric_name=None, parent_table=None,
                       backup_metric_name=None):
        """
        Convert the TopK operation to a table aggregation
        """
        op = self.op()

        arg_table = find_base_table(op.arg)

        by = op.by
        if not isinstance(by, Expr):
            by = by(arg_table)
            by_table = arg_table
        else:
            by_table = find_base_table(op.by)

        if metric_name is None:
            if by.get_name() == op.arg.get_name():
                by = by.name(backup_metric_name)
        else:
            by = by.name(metric_name)

        if arg_table.equals(by_table):
            agg = arg_table.aggregate(by, by=[op.arg])
        elif parent_table is not None:
            agg = parent_table.aggregate(by, by=[op.arg])
        else:
            raise com.IbisError('Cross-table TopK; must provide a parent '
                                'joined table')

        return agg.sort_by([(by.get_name(), False)]).limit(op.k)


class SortExpr(Expr):

    def _type_display(self):
        return 'array-sort'

    def get_name(self):
        return self.op().resolve_name()


class DayOfWeek(Expr):
    def index(self):
        """Get the index of the day of the week.

        Returns
        -------
        IntegerValue
            The index of the day of the week. Ibis follows pandas conventions,
            where **Monday = 0 and Sunday = 6**.
        """
        import ibis.expr.operations as ops
        return ops.DayOfWeekIndex(self.op().arg).to_expr()

    def full_name(self):
        """Get the name of the day of the week.

        Returns
        -------
        StringValue
            The name of the day of the week
        """
        import ibis.expr.operations as ops
        return ops.DayOfWeekName(self.op().arg).to_expr()


def bind_expr(table, expr):
    if isinstance(expr, (list, tuple)):
        return [bind_expr(table, x) for x in expr]

    return table._ensure_expr(expr)


# TODO: move to analysis
def find_base_table(expr):
    if isinstance(expr, TableExpr):
        return expr

    for arg in expr.op().flat_args():
        if isinstance(arg, Expr):
            r = find_base_table(arg)
            if isinstance(r, TableExpr):
                return r


_NULL = None


def null():
    """Create a NULL/NA scalar"""
    import ibis.expr.operations as ops

    global _NULL
    if _NULL is None:
        _NULL = ops.NullLiteral().to_expr()

    return _NULL


def literal(value, type=None):
    """Create a scalar expression from a Python value.

    Parameters
    ----------
    value : some Python basic type
        A Python value
    type : ibis type or string, optional
        An instance of :class:`ibis.expr.datatypes.DataType` or a string
        indicating the ibis type of `value`. This parameter should only be used
        in cases where ibis's type inference isn't sufficient for discovering
        the type of `value`.

    Returns
    -------
    literal_value : Literal
        An expression representing a literal value

    Examples
    --------
    >>> import ibis
    >>> x = ibis.literal(42)
    >>> x.type()
    int8
    >>> y = ibis.literal(42, type='double')
    >>> y.type()
    float64
    >>> ibis.literal('foobar', type='int64')  # doctest: +ELLIPSIS
    Traceback (most recent call last):
      ...
    TypeError: Value 'foobar' cannot be safely coerced to int64
    """
    import ibis.expr.datatypes as dt
    import ibis.expr.operations as ops

    if hasattr(value, 'op') and isinstance(value.op(), ops.Literal):
        return value

    if value is null:
        dtype = dt.null
    else:
        dtype = dt.infer(value)

    if type is not None:
        try:
            # check that dtype is implicitly castable to explicitly given dtype
            dtype = dtype.cast(type, value=value)
        except com.IbisTypeError:
            raise TypeError('Value {!r} cannot be safely coerced '
                            'to {}'.format(value, type))

    if dtype is dt.null:
        return null().cast(dtype)
    else:
        return ops.Literal(value, dtype=dtype).to_expr()


def sequence(values):
    """
    Wrap a list of Python values as an Ibis sequence type

    Parameters
    ----------
    values : list
      Should all be None or the same type

    Returns
    -------
    seq : Sequence
    """
    import ibis.expr.operations as ops

    return ops.ValueList(values).to_expr()


def as_value_expr(val):
    import pandas as pd
    if not isinstance(val, Expr):
        if isinstance(val, (tuple, list)):
            val = sequence(val)
        elif isinstance(val, pd.Series):
            val = sequence(list(val))
        else:
            val = literal(val)

    return val


def param(type):
    """Create a parameter of a particular type to be defined just before
    execution.

    Parameters
    ----------
    type : dt.DataType
        The type of the unbound parameter, e.g., double, int64, date, etc.

    Returns
    -------
    ScalarExpr

    Examples
    --------
    >>> import ibis
    >>> import ibis.expr.datatypes as dt
    >>> start = ibis.param(dt.date)
    >>> end = ibis.param(dt.date)
    >>> schema = [('timestamp_col', 'timestamp'), ('value', 'double')]
    >>> t = ibis.table(schema)
    >>> predicates = [t.timestamp_col >= start, t.timestamp_col <= end]
    >>> expr = t.filter(predicates).value.sum()
    """
    import ibis.expr.datatypes as dt
    import ibis.expr.operations as ops
    return ops.ScalarParameter(dt.dtype(type)).to_expr()


class UnnamedMarker:
    pass


unnamed = UnnamedMarker()
