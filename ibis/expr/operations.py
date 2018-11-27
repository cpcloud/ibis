import collections
import functools
import itertools
import operator

from contextlib import suppress
from typing import Any, Tuple

import attr
import toolz

from ibis.expr.schema import HasSchema, Schema

import ibis.common as com
import ibis.expr.types as ir
import ibis.expr.rules as rlz
import ibis.expr.schema as sch
import ibis.expr.datatypes as dt

from ibis import util


def _safe_repr(x, memo=None):
    return x._repr(memo=memo) if isinstance(x, (ir.Expr, Node)) else repr(x)


def attrib(
    default=attr.NOTHING,
    validator=None,
    repr=True,
    cmp=True,
    hash=None,
    init=True,
    convert=None,
    metadata=None,
    type=None,
    converter=None,
    factory=None,
    kw_only=False,
    show=None,
):
    return attr.ib(
        default=default,
        validator=validator,
        repr=repr,
        cmp=cmp,
        hash=hash,
        init=init,
        convert=convert,
        metadata=toolz.merge(
            metadata or {},
            dict(show=(lambda arg: True) if show is None else show),
        ),
        type=type,
        converter=converter,
        factory=factory,
        kw_only=kw_only,
    )


# TODO: move to analysis
def distinct_roots(*expressions):
    roots = toolz.concat(
        expression._root_tables() for expression in expressions
    )
    return list(toolz.unique(roots))


# * slots=True to make instances smaller, since large expressions will have a
#   lot of them
# * frozen=True to prevent external mutation
# * cmp=False here to customize hashing
# * repr=False because we have our own repr
node = attr.s(slots=True, frozen=True, cmp=False)


@node
class Node:
    _hash = attrib(
        validator=attr.validators.optional(attr.validators.instance_of(int)),
        init=False,
        default=None,
        repr=False,
    )

    def __attrs_post_init__(self):
        self._validate()

    def _validate(self) -> None:
        ...

    @property
    def argnames(self) -> Tuple[str, ...]:
        return tuple(
            field.name for field in self.signature if field.name != '_hash'
        )

    @property
    def args(self) -> Tuple[Any, ...]:
        return tuple(getattr(self, arg) for arg in self.argnames)

    @property
    def signature(self):
        return attr.fields(type(self))

    def __hash__(self) -> int:
        if self._hash is None:
            value = hash(
                tuple(
                    itertools.chain(
                        [type(self)],
                        (
                            element.op()
                            if isinstance(element, ir.Expr)
                            else element
                            for element in self.flat_args()
                        ),
                    )
                )
            )
            object.__setattr__(self, '_hash', value)
        return self._hash

    def __repr__(self):
        return self._repr()

    def _repr(self, memo=None):
        if memo is None:
            from ibis.expr.format import FormatMemo

            memo = FormatMemo()

        opname = type(self).__name__
        pprint_args = []

        def _pp(x):
            return _safe_repr(x, memo=memo)

        for x in self.args:
            if isinstance(x, (tuple, list)):
                pp = repr(list(map(_pp, x)))
            else:
                pp = _pp(x)
            pprint_args.append(pp)

        return '{}({})'.format(opname, ', '.join(pprint_args))

    @property
    def inputs(self):
        return tuple(self.args)

    def blocks(self) -> bool:
        # The contents of this node at referentially distinct and may not be
        # analyzed deeper
        return False

    def flat_args(self):
        for arg in self.args:
            if not isinstance(arg, str) and isinstance(
                arg, collections.Iterable
            ):
                for x in arg:
                    yield x
            else:
                yield arg

    def __eq__(self, other: 'Node') -> bool:  # type: ignore
        return self.equals(other)

    def __ne__(self, other: 'Node') -> bool:  # type: ignore
        return not self.equals(other)

    def equals(self, other, cache=None):
        if cache is None:
            cache = {}

        key = self, other

        try:
            return cache[key]
        except KeyError:
            cache[key] = result = self is other or (
                type(self) == type(other)
                and all_equal(self.args, other.args, cache=cache)
            )
            return result

    def compatible_with(self, other):
        return self.equals(other)

    def is_ancestor(self, other):
        if isinstance(other, ir.Expr):
            other = other.op()

        return self.equals(other)

    def to_expr(self):
        return self._make_expr()

    def _make_expr(self):
        klass = self.output_type()
        return klass(self)

    def output_type(self):
        """Resolve the output type of the expression.

        Returns the node wrapped in the appropriate
        :class:`~ibis.expr.types.ValueExpr` type.

        """
        raise NotImplementedError(
            'output_type not implemented for type {}'.format(type(self))
        )


@node
class ValueOp(Node):
    def root_tables(self):
        exprs = [arg for arg in self.args if isinstance(arg, ir.Expr)]
        return distinct_roots(*exprs)

    def resolve_name(self):
        raise com.ExpressionError('Expression is not named: %s' % repr(self))

    def has_resolved_name(self):
        return False


def all_equal(left, right, cache=None):
    """Check whether two objects `left` and `right` are equal.

    Parameters
    ----------
    left : Union[object, Expr, Node]
    right : Union[object, Expr, Node]
    cache : Optional[Dict[Tuple[Node, Node], bool]]
        A dictionary indicating whether two Nodes are equal
    """
    if cache is None:
        cache = {}

    if util.is_iterable(left):
        # check that left and right are equal length iterables and that all
        # of their elements are equal
        return (
            util.is_iterable(right)
            and len(left) == len(right)
            and all(
                itertools.starmap(
                    functools.partial(all_equal, cache=cache), zip(left, right)
                )
            )
        )

    if hasattr(left, 'equals'):
        return left.equals(right, cache=cache)
    return left == right


_table_names = ('unbound_table_{:d}'.format(i) for i in itertools.count())


def genname():
    return next(_table_names)


@node
class TableNode(Node):
    def get_type(self, name):
        return self.schema[name]

    def output_type(self):
        return ir.TableExpr

    def aggregate(self, this, metrics, by=None, having=None):
        return Aggregation(this, metrics, by=by, having=having)

    def sort_by(self, expr, sort_exprs):
        return Selection(expr, (), sort_keys=util.to_tuple(sort_exprs))

    def is_ancestor(self, other):
        import ibis.expr.lineage as lin

        if isinstance(other, ir.Expr):
            other = other.op()

        if self.equals(other):
            return True

        fn = lambda e: (lin.proceed, e.op())  # noqa: E731
        expr = self.to_expr()
        for child in lin.traverse(fn, expr):
            if child.equals(other):
                return True
        return False


@node
class TableColumn(ValueOp):
    """Select a column from a :class:`~ibis.expr.types.TableExpr`."""

    table = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    name = attrib(validator=attr.validators.instance_of((str, int)))

    def __attrs_post_init__(self) -> None:
        name = self.name
        table = self.table
        schema = table.schema()
        if isinstance(name, int):
            object.__setattr__(self, 'name', schema.name_at_position(name))
        name = self.name
        if name not in schema:
            raise com.IbisTypeError(
                '{!r} is not a field in {}'.format(name, table.columns)
            )

    def parent(self):
        return self.table

    def resolve_name(self):
        return self.name

    def has_resolved_name(self):
        return True

    def root_tables(self):
        return self.table._root_tables()

    def _make_expr(self):
        dtype = self.table._get_type(self.name)
        klass = dtype.column_type()
        return klass(self, name=self.name)


def check_table_array_view_table_has_one_column(self, attr, value):
    if len(value.schema()) > 1:
        raise ValueError('TableArrayView table must have exactly one column')


def check_table_array_view_name_in_table_schema(self, attr, value):
    schema = self.table.schema()
    if value not in schema:
        raise ValueError(
            'name {!r} is not in table schema:\n{}'.format(schema)
        )


@node
class TableArrayView(ValueOp):
    table = attrib(
        validator=[
            attr.validators.instance_of(ir.TableExpr),
            check_table_array_view_table_has_one_column,
        ]
    )

    @property
    def name(self):
        return self.table.columns[0]

    def _make_expr(self):
        name = self.name
        ctype = self.table._get_type(name)
        klass = ctype.column_type()
        return klass(self, name=name)


def find_all_base_tables(expr, memo=None):
    if memo is None:
        memo = {}

    node = expr.op()

    if isinstance(expr, ir.TableExpr) and node.blocks():
        if expr not in memo:
            memo[node] = expr
        return memo

    for arg in expr.op().flat_args():
        if isinstance(arg, ir.Expr):
            find_all_base_tables(arg, memo)

    return memo


@node
class PhysicalTable(TableNode, HasSchema):
    def blocks(self) -> bool:
        return True


@node
class UnboundTable(PhysicalTable):
    schema = attrib(validator=attr.validators.instance_of(sch.Schema))
    name = attrib(
        converter=attr.converters.default_if_none(factory=genname),
        validator=attr.validators.optional(attr.validators.instance_of(str)),
        factory=genname,
    )


@node
class DatabaseTable(PhysicalTable):
    name = attrib(validator=attr.validators.instance_of(str))
    schema = attrib(validator=attr.validators.instance_of(sch.Schema))
    source = attrib(converter=rlz.client)

    def change_name(self, new_name):
        return type(self)(new_name, self.args[1], self.source)


@node
class SQLQueryResult(TableNode, HasSchema):
    """A table sourced from the result set of a select query"""

    query = attrib(validator=attr.validators.instance_of(str))
    schema = attrib(validator=attr.validators.instance_of(sch.Schema))
    source = attrib(converter=rlz.client)

    def blocks(self) -> bool:
        return True


@node
class UnaryOp(ValueOp):
    arg = attrib(converter=rlz.any)


@node
class BinaryOp(ValueOp):
    """A binary operation"""

    left = attrib(converter=rlz.any)
    right = attrib(converter=rlz.any)


@node
class Cast(ValueOp):
    arg = attrib(converter=rlz.any)
    to = attrib(converter=dt.dtype)

    # see #396 for the issue preventing this
    # def resolve_name(self):
    #     return self.args[0].get_name()

    def output_type(self):
        return rlz.shape_like(self.arg, dtype=self.to)


@node
class TypeOf(UnaryOp):
    output_type = rlz.shape_like('arg', dt.string)


@node
class Negate(UnaryOp):
    arg = attrib(converter=rlz.one_of((rlz.numeric(), rlz.interval())))
    output_type = rlz.typeof('arg')


@node
class IsNull(UnaryOp):
    """Returns true if values are null

    Returns
    -------
    isnull : boolean with dimension of caller
    """

    output_type = rlz.shape_like('arg', dt.boolean)


@node
class NotNull(UnaryOp):
    """Returns true if values are not null

    Returns
    -------
    notnull : boolean with dimension of caller
    """

    output_type = rlz.shape_like('arg', dt.boolean)


@node
class ZeroIfNull(UnaryOp):
    output_type = rlz.typeof('arg')


@node
class IfNull(ValueOp):
    """Equivalent to (but perhaps implemented differently):

    case().when(expr.notnull(), expr)
          .else_(null_substitute_expr)
    """

    arg = attrib(converter=rlz.any)
    ifnull_expr = attrib(converter=rlz.any)
    output_type = rlz.shape_like('args')


@node
class NullIf(ValueOp):
    """Set values to NULL if they equal the null_if_expr"""

    arg = attrib(converter=rlz.any)
    null_if_expr = attrib(converter=rlz.any)
    output_type = rlz.typeof('arg')


@node
class NullIfZero(ValueOp):

    """
    Set values to NULL if they equal to zero. Commonly used in cases where
    divide-by-zero would produce an overflow or infinity.

    Equivalent to (value == 0).ifelse(ibis.NA, value)

    Returns
    -------
    maybe_nulled : type of caller
    """

    arg = attrib(converter=rlz.numeric)
    output_type = rlz.typeof('arg')


@node
class IsNan(ValueOp):
    arg = attrib(converter=rlz.floating)
    output_type = rlz.shape_like('arg', dt.boolean)


@node
class IsInf(ValueOp):
    arg = attrib(converter=rlz.floating)
    output_type = rlz.shape_like('arg', dt.boolean)


@node
class CoalesceLike(ValueOp):

    # According to Impala documentation:
    # Return type: same as the initial argument value, except that integer
    # values are promoted to BIGINT and floating-point values are promoted to
    # DOUBLE; use CAST() when inserting into a smaller numeric column
    arg = attrib(converter=rlz.list_of(rlz.any))

    def output_type(self):
        first = self.arg[0]
        if isinstance(first, (ir.IntegerValue, ir.FloatingValue)):
            dtype = first.type().largest
        else:
            dtype = first.type()

        # self.arg is a list of value expressions
        return rlz.shape_like(self.arg, dtype)


@node
class Coalesce(CoalesceLike):
    pass


@node
class Greatest(CoalesceLike):
    pass


@node
class Least(CoalesceLike):
    pass


@node
class Abs(UnaryOp):
    """Absolute value"""

    output_type = rlz.typeof('arg')


@node
class Ceil(UnaryOp):
    """Round up to the nearest integer value greater than or equal to `arg`.

    Returns
    -------
    ceiled : type depending on input
        Decimal values: yield decimal
        Other numeric values: yield integer (int32)

    """

    arg = attrib(converter=rlz.numeric)

    def output_type(self):
        if isinstance(self.arg.type(), dt.Decimal):
            return self.arg._factory
        return rlz.shape_like(self.arg, dt.int64)


@node
class Floor(UnaryOp):
    """Round down to the nearest integer value less than or equal to `arg`.

    Returns
    -------
    floored : type depending on input
        Decimal values: yield decimal
        Other numeric values: yield integer (int32)

    """

    arg = attrib(converter=rlz.numeric)

    def output_type(self):
        if isinstance(self.arg.type(), dt.Decimal):
            return self.arg._factory
        return rlz.shape_like(self.arg, dt.int64)


@node
class Round(ValueOp):
    arg = attrib(converter=rlz.numeric)
    digits = attrib(
        converter=attr.converters.optional(rlz.numeric), default=None
    )

    def output_type(self):
        if isinstance(self.arg, ir.DecimalValue):
            return self.arg._factory
        elif self.digits is None:
            return rlz.shape_like(self.arg, dt.int64)
        else:
            return rlz.shape_like(self.arg, dt.double)


@node
class Clip(ValueOp):
    arg = attrib(converter=rlz.strict_numeric)
    lower = attrib(
        converter=attr.converters.optional(rlz.strict_numeric), default=None
    )
    upper = attrib(
        converter=attr.converters.optional(rlz.strict_numeric), default=None
    )
    output_type = rlz.typeof('arg')


@node
class BaseConvert(ValueOp):
    arg = attrib(converter=rlz.one_of([rlz.integer, rlz.string]))
    from_base = attrib(converter=rlz.integer)
    to_base = attrib(converter=rlz.integer)

    def output_type(self):
        return rlz.shape_like(tuple(self.flat_args()), dt.string)


@node
class MathUnaryOp(UnaryOp):
    arg = attrib(converter=rlz.numeric)

    def output_type(self):
        arg = self.arg
        if isinstance(self.arg, ir.DecimalValue):
            dtype = arg.type()
        else:
            dtype = dt.double
        return rlz.shape_like(arg, dtype)


@node
class ExpandingTypeMathUnaryOp(MathUnaryOp):
    def output_type(self):
        if not isinstance(self.arg, ir.DecimalValue):
            return super().output_type()
        arg = self.arg
        return rlz.shape_like(arg, arg.type().largest)


@node
class Exp(ExpandingTypeMathUnaryOp):
    pass


@node
class Sign(UnaryOp):
    arg = attrib(converter=rlz.numeric)
    output_type = rlz.typeof('arg')


@node
class Sqrt(MathUnaryOp):
    pass


@node
class Logarithm(MathUnaryOp):
    arg = attrib(converter=rlz.strict_numeric)


@node
class Log(Logarithm):
    arg = attrib(converter=rlz.strict_numeric)
    base = attrib(
        converter=attr.converters.optional(rlz.strict_numeric), default=None
    )


@node
class Ln(Logarithm):
    """Natural logarithm."""


@node
class Log2(Logarithm):
    """Logarithm base 2."""


@node
class Log10(Logarithm):
    """Logarithm base 10."""


@node
class Degrees(ExpandingTypeMathUnaryOp):
    """Converts radians to degrees."""

    arg = attrib(converter=rlz.numeric)


@node
class Radians(MathUnaryOp):
    """Converts degrees to radians."""

    arg = attrib(converter=rlz.numeric)


# TRIGONOMETRIC OPERATIONS


@node
class TrigonometricUnary(MathUnaryOp):
    """One-argument trigonometric operation base class."""

    arg = attrib(converter=rlz.numeric)


@node
class TrigonometricBinary(BinaryOp):
    """Two-argument trigonometric operation base class."""

    left = attrib(converter=rlz.numeric)
    right = attrib(converter=rlz.numeric)
    output_type = rlz.shape_like('args', dt.float64)


@node
class Acos(TrigonometricUnary):
    """Returns the arc cosine of the argument."""


@node
class Asin(TrigonometricUnary):
    """Returns the arc sine of the argument."""


@node
class Atan(TrigonometricUnary):
    """Returns the arc tangent of the argument."""


@node
class Atan2(TrigonometricBinary):
    """Returns the arc tangent of the first and second arguments."""


@node
class Cos(TrigonometricUnary):
    """Returns the cosine of the argument."""


@node
class Cot(TrigonometricUnary):
    """Returns the cotangent of the argument."""


@node
class Sin(TrigonometricUnary):
    """Returns the sine of the argument."""


@node
class Tan(TrigonometricUnary):
    """Returns the tangent of the argument."""


@node
class StringUnaryOp(UnaryOp):
    arg = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.string)


@node
class Uppercase(StringUnaryOp):
    """Convert string to all uppercase."""


@node
class Lowercase(StringUnaryOp):
    """Convert string to all lowercase."""


@node
class Reverse(StringUnaryOp):
    """Reverse string."""


@node
class Strip(StringUnaryOp):
    """Remove whitespace from left and right sides of string."""


@node
class LStrip(StringUnaryOp):
    """Remove whitespace from left side of string."""


@node
class RStrip(StringUnaryOp):
    """Remove whitespace from right side of string."""


@node
class Capitalize(StringUnaryOp):
    """Return a capitalized version of input string."""


@node
class Substring(ValueOp):
    arg = attrib(converter=rlz.string)
    start = attrib(converter=rlz.integer)
    length = attrib(
        converter=attr.converters.optional(rlz.integer), default=None
    )
    output_type = rlz.shape_like('arg', dt.string)


@node
class StrRight(ValueOp):
    arg = attrib(converter=rlz.string)
    nchars = attrib(converter=rlz.integer)
    output_type = rlz.shape_like('arg', dt.string)


@node
class Repeat(ValueOp):
    arg = attrib(converter=rlz.string)
    times = attrib(converter=rlz.integer)
    output_type = rlz.shape_like('arg', dt.string)


@node
class StringFind(ValueOp):
    arg = attrib(converter=rlz.string)
    substr = attrib(converter=rlz.string)
    start = attrib(
        converter=attr.converters.optional(rlz.integer), default=None
    )
    end = attrib(converter=attr.converters.optional(rlz.integer), default=None)
    output_type = rlz.shape_like('arg', dt.int64)


@node
class Translate(ValueOp):
    arg = attrib(converter=rlz.string)
    from_str = attrib(converter=rlz.string)
    to_str = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.string)


@node
class LPad(ValueOp):
    arg = attrib(converter=rlz.string)
    length = attrib(converter=rlz.integer)
    pad = attrib(converter=attr.converters.optional(rlz.string), default=None)
    output_type = rlz.shape_like('arg', dt.string)


@node
class RPad(ValueOp):
    arg = attrib(converter=rlz.string)
    length = attrib(converter=rlz.integer)
    pad = attrib(converter=attr.converters.optional(rlz.string), default=None)
    output_type = rlz.shape_like('arg', dt.string)


@node
class FindInSet(ValueOp):
    needle = attrib(converter=rlz.string)
    values = attrib(converter=rlz.list_of(rlz.string, min_length=1))
    output_type = rlz.shape_like('needle', dt.int64)


@node
class StringJoin(ValueOp):
    sep = attrib(converter=rlz.string)
    arg = attrib(converter=rlz.list_of(rlz.string, min_length=1))

    def output_type(self):
        return rlz.shape_like(tuple(self.flat_args()), dt.string)


@node
class BooleanValueOp:
    pass


@node
class FuzzySearch(ValueOp, BooleanValueOp):
    arg = attrib(converter=rlz.string)
    pattern = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.boolean)


@node
class StringSQLLike(FuzzySearch):
    arg = attrib(converter=rlz.string)
    pattern = attrib(converter=rlz.string)
    escape = attrib(
        validator=attr.validators.optional(attr.validators.instance_of(str)),
        default=None,
    )


@node
class StringSQLILike(StringSQLLike):
    """SQL ILIKE operation."""


@node
class RegexSearch(FuzzySearch):
    pass


@node
class RegexExtract(ValueOp):
    arg = attrib(converter=rlz.string)
    pattern = attrib(converter=rlz.string)
    index = attrib(converter=rlz.integer)
    output_type = rlz.shape_like('arg', dt.string)


@node
class RegexReplace(ValueOp):
    arg = attrib(converter=rlz.string)
    pattern = attrib(converter=rlz.string)
    replacement = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.string)


@node
class StringReplace(ValueOp):
    arg = attrib(converter=rlz.string)
    pattern = attrib(converter=rlz.string)
    replacement = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.string)


@node
class StringSplit(ValueOp):
    arg = attrib(converter=rlz.string)
    delimiter = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.Array(dt.string))


@node
class StringConcat(ValueOp):
    arg = attrib(converter=rlz.list_of(rlz.string))
    output_type = rlz.shape_like('arg', dt.string)


@node
class ParseURL(ValueOp):
    arg = attrib(converter=rlz.string)
    extract = attrib(
        rlz.isin(
            {
                'PROTOCOL',
                'HOST',
                'PATH',
                'REF',
                'AUTHORITY',
                'FILE',
                'USERINFO',
                'QUERY',
            }
        )
    )
    key = attrib(converter=attr.converters.optional(rlz.string), default=None)
    output_type = rlz.shape_like('arg', dt.string)


@node
class StringLength(UnaryOp):
    """Compute the length of strings.

    Returns
    -------
    Int32Value

    """

    output_type = rlz.shape_like('arg', dt.int32)


@node
class StringAscii(UnaryOp):
    output_type = rlz.shape_like('arg', dt.int32)


# ----------------------------------------------------------------------


@node
class Reduction(ValueOp):
    _reduction = True


@node
class Count(Reduction):
    arg = attrib(attr.validators.instance_of((ir.ColumnExpr, ir.TableExpr)))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        return functools.partial(ir.IntegerScalar, dtype=dt.int64)


@node
class Arbitrary(Reduction):
    arg = attrib(converter=rlz.column(rlz.any))
    how = attrib(
        converter=attr.converters.optional(
            rlz.isin({'first', 'last', 'heavy'})
        ),
        default=attr.converters.default_if_none('first'),
    )
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )
    output_type = rlz.scalar_like('arg')


@node
class Sum(Reduction):
    arg = attrib(converter=rlz.column(rlz.numeric))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        if isinstance(self.arg, ir.BooleanValue):
            dtype = dt.int64
        else:
            dtype = self.arg.type().largest
        return dtype.scalar_type()


@node
class Mean(Reduction):
    arg = attrib(converter=rlz.column(rlz.numeric))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        if isinstance(self.arg, ir.DecimalValue):
            dtype = self.arg.type()
        else:
            dtype = dt.float64
        return dtype.scalar_type()


@node
class Quantile(Reduction):
    arg = attrib(converter=rlz.any)
    quantile = attrib(converter=rlz.strict_numeric)
    interpolation = attrib(
        converter=rlz.isin(
            {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
        ),
        default='linear',
    )

    def output_type(self):
        return dt.float64.scalar_type()


@node
class MultiQuantile(Quantile):
    arg = attrib(converter=rlz.any)
    quantile = attrib(converter=rlz.value(dt.Array(dt.float64)))
    interpolation = attrib(
        converter=rlz.isin(
            {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
        ),
        default='linear',
    )

    def output_type(self):
        return dt.Array(dt.float64).scalar_type()


@node
class VarianceBase(Reduction):
    arg = attrib(converter=rlz.column(rlz.numeric))
    how = attrib(
        validator=attr.validators.optional(
            attr.validators.in_({'sample', 'pop'})
        ),
        default=None,
    )
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        if isinstance(self.arg, ir.DecimalValue):
            dtype = self.arg.type().largest
        else:
            dtype = dt.float64
        return dtype.scalar_type()


@node
class StandardDev(VarianceBase):
    pass


@node
class Variance(VarianceBase):
    pass


@node
class Correlation(Reduction):
    """Coefficient of correlation of a set of number pairs."""

    left = attrib(converter=rlz.column(rlz.numeric))
    right = attrib(converter=rlz.column(rlz.numeric))
    how = attrib(
        validator=attr.validators.optional(
            attr.validators.in_({'sample', 'pop'})
        ),
        default=None,
    )
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        return dt.float64.scalar_type()


@node
class Covariance(Reduction):
    """Covariance of a set of number pairs."""

    left = attrib(converter=rlz.column(rlz.numeric))
    right = attrib(converter=rlz.column(rlz.numeric))
    how = attrib(
        validator=attr.validators.optional(
            attr.validators.in_({'sample', 'pop'})
        ),
        default=None,
    )
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        return dt.float64.scalar_type()


@node
class Max(Reduction):
    arg = attrib(converter=rlz.column(rlz.any))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )
    output_type = rlz.scalar_like('arg')


@node
class Min(Reduction):
    arg = attrib(converter=rlz.column(rlz.any))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )
    output_type = rlz.scalar_like('arg')


@node
class HLLCardinality(Reduction):
    """Approximate number of unique values using HyperLogLog algorithm.

    Impala offers the NDV built-in function for this.

    """

    arg = attrib(converter=rlz.column(rlz.any))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        # Impala 2.0 and higher returns a DOUBLE
        # return ir.DoubleScalar
        return functools.partial(ir.IntegerScalar, dtype=dt.int64)


@node
class GroupConcat(Reduction):
    arg = attrib(converter=rlz.column(rlz.any))
    sep = attrib(converter=rlz.string, default=',')
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        return dt.string.scalar_type()


@node
class CMSMedian(Reduction):
    """Compute the approximate median using the Count-Min-Sketch algorithm.

    Column values must be comparable. Exposed in Impala using APPX_MEDIAN.

    """

    arg = attrib(converter=rlz.column(rlz.any))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )
    output_type = rlz.scalar_like('arg')


# ----------------------------------------------------------------------
# Analytic functions


@node
class AnalyticOp(ValueOp):
    pass


@node
class WindowOp(ValueOp):
    expr = attrib(validator=attr.validators.instance_of(ir.ValueExpr))
    window = attrib()
    output_type = rlz.array_like('expr')

    display_argnames = False

    def __attrs_post_init__(self):
        from ibis.expr.window import propagate_down_window
        from ibis.expr.analysis import is_analytic

        if not is_analytic(self.expr):
            raise com.IbisInputError(
                'Expression does not contain a valid window operation'
            )

        table = ir.find_base_table(self.expr)
        if table is not None:
            window = self.window.bind(table)
        else:
            window = self.window

        expr = propagate_down_window(self.expr, window)
        object.__setattr__(self, 'expr', expr)
        object.__setattr__(self, 'window', window)

    def over(self, window):
        new_window = self.window.combine(window)
        return WindowOp(self.expr, new_window)

    @property
    def inputs(self):
        return self.expr.op().inputs[0], self.window

    def root_tables(self):
        result = list(
            toolz.unique(
                toolz.concatv(
                    self.expr._root_tables(),
                    distinct_roots(
                        *toolz.concatv(
                            self.window._order_by, self.window._group_by
                        )
                    ),
                )
            )
        )
        return result


@node
class ShiftBase(AnalyticOp):
    arg = attrib(converter=rlz.column(rlz.any))
    offset = attrib(
        converter=attr.converters.optional(
            rlz.one_of((rlz.integer, rlz.interval))
        ),
        default=None,
    )
    default = attrib(converter=attr.converters.optional(rlz.any), default=None)
    output_type = rlz.typeof('arg')


@node
class Lag(ShiftBase):
    pass


@node
class Lead(ShiftBase):
    pass


@node
class RankBase(AnalyticOp):
    def output_type(self):
        return dt.int64.column_type()


@node
class MinRank(RankBase):
    """Compute position of first element within each group in sorted order.

    Groups are determined by element equality.

    Examples
    --------
    values   ranks
    1        0
    1        0
    2        2
    2        2
    2        2
    3        5

    Returns
    -------
    Int64Column
        Starts from 0.

    """

    # Equivalent to SQL RANK()
    arg = attrib(converter=rlz.column(rlz.any))


@node
class DenseRank(RankBase):
    """Compute position of first element within each group in sorted order.

    Groups are determined by element equality. Ignores duplicate values.

    Examples
    --------
    values   ranks
    1        0
    1        0
    2        1
    2        1
    2        1
    3        2

    Returns
    -------
    Int64Column
        Starts from 0.

    """

    # Equivalent to SQL DENSE_RANK()
    arg = attrib(converter=rlz.column(rlz.any))


@node
class RowNumber(RankBase):
    """Compute row number starting from 0 after sorting by column expression.

    Equivalent to SQL ROW_NUMBER()

    Examples
    --------
    >>> import ibis
    >>> t = ibis.table([('values', dt.int64)])
    >>> w = ibis.window(order_by=t.values)
    >>> row_num = ibis.row_number().over(w)
    >>> result = t[t.values, row_num.name('row_num')]

    Returns
    -------
    Int64Column
        Starting from 0

    """


@node
class CumulativeOp(AnalyticOp):
    pass


@node
class CumulativeSum(CumulativeOp):
    """Cumulative sum. Requires an order window."""

    arg = attrib(converter=rlz.column(rlz.numeric))

    def output_type(self):
        if isinstance(self.arg, ir.BooleanValue):
            dtype = dt.int64
        else:
            dtype = self.arg.type().largest
        return dtype.column_type()


@node
class CumulativeMean(CumulativeOp):
    """Cumulative mean. Requires an order window."""

    arg = attrib(converter=rlz.column(rlz.numeric))

    def output_type(self):
        if isinstance(self.arg, ir.DecimalValue):
            dtype = self.arg.type().largest
        else:
            dtype = dt.float64
        return dtype.column_type()


@node
class CumulativeMax(CumulativeOp):
    """Cumulative max. Requires an order window."""

    arg = attrib(converter=rlz.column(rlz.any))
    output_type = rlz.array_like('arg')


@node
class CumulativeMin(CumulativeOp):
    """Cumulative min. Requires an order window."""

    arg = attrib(converter=rlz.column(rlz.any))
    output_type = rlz.array_like('arg')


@node
class PercentRank(AnalyticOp):
    arg = attrib(converter=rlz.column(rlz.any))
    output_type = rlz.shape_like('arg', dt.double)


@node
class NTile(AnalyticOp):
    arg = attrib(converter=rlz.column(rlz.any))
    buckets = attrib(converter=rlz.integer)
    output_type = rlz.shape_like('arg', dt.int64)


@node
class FirstValue(AnalyticOp):
    arg = attrib(converter=rlz.column(rlz.any))
    output_type = rlz.typeof('arg')


@node
class LastValue(AnalyticOp):
    arg = attrib(converter=rlz.column(rlz.any))
    output_type = rlz.typeof('arg')


@node
class NthValue(AnalyticOp):
    arg = attrib(converter=rlz.column(rlz.any))
    nth = attrib(converter=rlz.integer)
    output_type = rlz.typeof('arg')


# ----------------------------------------------------------------------
# Distinct stuff


@node
class Distinct(TableNode, HasSchema):
    """Table-level unique operation.

    In SQL, you might have:

    .. code-block:: sql

       SELECT DISTINCT foo FROM table
       SELECT DISTINCT foo, bar FROM table

    """

    table = attrib(validator=attr.validators.instance_of(ir.TableExpr))

    def _validate(self):
        # check whether schema has overlapping columns or not
        assert self.schema

    @property
    def schema(self) -> sch.Schema:
        return self.table.schema()

    def blocks(self) -> bool:
        return True


@node
class DistinctColumn(ValueOp):
    """
    COUNT(DISTINCT ...) is really just syntactic sugar, but we provide a
    distinct().count() nicety for users nonetheless.

    For all intents and purposes, like Distinct, but can be distinguished later
    for evaluation if the result should be array-like versus table-like. Also
    for calling count()

    """

    arg = attrib(validator=attr.validators.instance_of(ir.ColumnExpr))
    output_type = rlz.typeof('arg')

    def count(self):
        """Only valid if the distinct contains a single column"""
        return CountDistinct(self.arg)


@node
class CountDistinct(Reduction):
    arg = attrib(converter=rlz.column(rlz.any))
    where = attrib(
        converter=attr.converters.optional(rlz.boolean), default=None
    )

    def output_type(self):
        return dt.int64.scalar_type()


# ---------------------------------------------------------------------
# Boolean reductions and semi/anti join support


@node
class Any(ValueOp):
    # Depending on the kind of input boolean array, the result might either be
    # array-like (an existence-type predicate) or scalar (a reduction)
    arg = attrib(converter=rlz.column(rlz.boolean))

    @property
    def _reduction(self) -> bool:
        roots = self.arg._root_tables()
        return len(roots) < 2

    def output_type(self):
        if self._reduction:
            return dt.boolean.scalar_type()
        return dt.boolean.column_type()

    def negate(self):
        return NotAny(self.arg)


@node
class All(ValueOp):
    arg = attrib(converter=rlz.column(rlz.boolean))
    output_type = rlz.scalar_like('arg')
    _reduction = True

    def negate(self):
        return NotAll(self.arg)


@node
class NotAny(Any):
    def negate(self):
        return Any(self.arg)


@node
class NotAll(All):
    def negate(self):
        return All(self.arg)


@node
class CumulativeAny(CumulativeOp):
    arg = attrib(converter=rlz.column(rlz.boolean))
    output_type = rlz.typeof('arg')


@node
class CumulativeAll(CumulativeOp):
    arg = attrib(converter=rlz.column(rlz.boolean))
    output_type = rlz.typeof('arg')


# ---------------------------------------------------------------------


@node
class TypedCaseBuilder:
    def type(self):
        types = [result.type() for result in self.results]
        return dt.highest_precedence(types)

    def else_(self, result_expr):
        """
        Specify

        Returns
        -------
        builder : CaseBuilder
        """
        kwargs = {
            slot: getattr(self, slot)
            for slot in self.__slots__
            if slot != 'default'
        }

        result_expr = ir.as_value_expr(result_expr)
        kwargs['default'] = result_expr
        # Maintain immutability
        return type(self)(**kwargs)

    def end(self):
        default = self.default
        if default is None:
            default = ir.null().cast(self.type())

        args = [
            getattr(self, slot) for slot in self.__slots__ if slot != 'default'
        ]
        args.append(default)
        op = self.__class__.case_op(*args)
        return op.to_expr()


@node
class SimpleCase(ValueOp):
    base = attrib(converter=rlz.any)
    cases = attrib(converter=rlz.list_of(rlz.any))
    results = attrib(converter=rlz.list_of(rlz.any))
    default = attrib(converter=rlz.any)

    def _validate(self):
        assert len(self.cases) == len(self.results)

    def root_tables(self):
        return distinct_roots(
            *itertools.chain(
                [self.base],
                self.cases,
                self.results,
                [] if self.default is None else [self.default],
            )
        )

    def output_type(self):
        exprs = self.results + [self.default]
        return rlz.shape_like(self.base, dtype=exprs.type())


@node
class SimpleCaseBuilder(TypedCaseBuilder):
    base = attrib()
    cases = attrib(factory=tuple)
    results = attrib(factory=tuple)
    default = attrib(default=None)

    case_op = SimpleCase

    def when(self, case_expr, result_expr):
        """
        Add a new case-result pair.

        Parameters
        ----------
        case : Expr
          Expression to equality-compare with base expression. Must be
          comparable with the base.
        result : Expr
          Value when the case predicate evaluates to true.

        Returns
        -------
        builder : CaseBuilder
        """
        case_expr = ir.as_value_expr(case_expr)
        result_expr = ir.as_value_expr(result_expr)

        if not rlz.comparable(self.base, case_expr):
            raise TypeError(
                'Base expression and passed case are not ' 'comparable'
            )

        cases = list(self.cases)
        cases.append(case_expr)

        results = list(self.results)
        results.append(result_expr)

        # Maintain immutability
        return type(self)(self.base, cases, results, self.default)


@node
class SearchedCase(ValueOp):
    cases = attrib(converter=rlz.list_of(rlz.boolean))
    results = attrib(converter=rlz.list_of(rlz.any))
    default = attrib(converter=rlz.any)

    def _validate(self):
        assert len(self.cases) == len(self.results)

    def root_tables(self):
        cases, results, default = self.args
        return distinct_roots(
            *itertools.chain(
                cases.values,
                results.values,
                [] if default is None else [default],
            )
        )

    def output_type(self):
        exprs = self.results + [self.default]
        dtype = rlz.highest_precedence_dtype(exprs)
        return rlz.shape_like(self.cases, dtype)


@node
class SearchedCaseBuilder(TypedCaseBuilder):
    cases = attrib(factory=tuple)
    results = attrib(factory=tuple)
    default = attrib(default=None)

    case_op = SearchedCase

    def when(self, case_expr, result_expr):
        """Add a new case-result pair.

        Parameters
        ----------
        case : Expr
            Expression to equality-compare with base expression. Must be
            comparable with the base.
        result : Expr
            Value when the case predicate evaluates to true.

        Returns
        -------
        CaseBuilder

        """
        case_expr = ir.as_value_expr(case_expr)
        result_expr = ir.as_value_expr(result_expr)

        if not isinstance(case_expr, ir.BooleanValue):
            raise TypeError(case_expr)

        cases = list(self.cases)
        cases.append(case_expr)

        results = list(self.results)
        results.append(result_expr)

        # Maintain immutability
        return type(self)(cases, results, self.default)


@node
class Where(ValueOp):
    """Ternary case expression.

    Equivalent to::

        bool_expr.case()
                 .when(True, true_expr)
                 .else_(false_or_null_expr)
    """

    bool_expr = attrib(converter=rlz.boolean)
    true_expr = attrib(converter=rlz.any)
    false_null_expr = attrib(converter=rlz.any)

    def output_type(self):
        return rlz.shape_like(self.bool_expr, self.true_expr.type())


def _validate_join_tables(left, right):
    if not isinstance(left, ir.TableExpr):
        raise TypeError(
            'Can only join table expressions, got {} for '
            'left table'.format(type(left).__name__)
        )

    if not isinstance(right, ir.TableExpr):
        raise TypeError(
            'Can only join table expressions, got {} for '
            'right table'.format(type(right).__name__)
        )


def _make_distinct_join_predicates(left, right, predicates):
    # see GH #667

    # If left and right table have a common parent expression (e.g. they
    # have different filters), must add a self-reference and make the
    # appropriate substitution in the join predicates

    if left.equals(right):
        right = right.view()

    predicates = tuple(_clean_join_predicates(left, right, predicates))
    _validate_join_predicates(left, right, predicates)
    return left, right, predicates


def _clean_join_predicates(left, right, predicates):
    import ibis.expr.analysis as L

    if not isinstance(predicates, (list, tuple)):
        predicates = [predicates]

    for pred in predicates:
        if isinstance(pred, tuple):
            if len(pred) != 2:
                raise com.ExpressionError('Join key tuple must be ' 'length 2')
            lk, rk = pred
            lk = left._ensure_expr(lk)
            rk = right._ensure_expr(rk)
            pred = lk == rk
        elif isinstance(pred, str):
            pred = left[pred] == right[pred]
        elif not isinstance(pred, ir.Expr):
            raise NotImplementedError

        if not isinstance(pred, ir.BooleanColumn):
            raise com.ExpressionError('Join predicate must be comparison')

        preds = L.flatten_predicate(pred)
        yield from preds


def _validate_join_predicates(left, right, predicates):
    from ibis.expr.analysis import fully_originate_from

    # Validate join predicates. Each predicate must be valid jointly when
    # considering the roots of each input table
    for predicate in predicates:
        if not fully_originate_from(predicate, [left, right]):
            raise com.RelationError(
                'The expression {!r} does not fully '
                'originate from dependencies of the table '
                'expression.'.format(predicate)
            )


@node
class Join(TableNode):
    left = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    right = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    predicates = attrib(
        validator=attr.validators.instance_of(tuple), factory=tuple
    )

    def __attrs_post_init__(self) -> None:
        left, right = self.left, self.right
        _validate_join_tables(left, right)
        left, right, predicates = _make_distinct_join_predicates(
            left, right, self.predicates
        )
        object.__setattr__(self, 'left', left)
        object.__setattr__(self, 'right', right)
        object.__setattr__(self, 'predicates', predicates)

    def _get_schema(self):
        # For joins retaining both table schemas, merge them together here
        left = self.left
        right = self.right

        if not left._is_materialized():
            left = left.materialize()

        if not right._is_materialized():
            right = right.materialize()

        sleft = left.schema()
        sright = right.schema()

        overlap = set(sleft.names) & set(sright.names)
        if overlap:
            raise com.RelationError(
                'Joined tables have overlapping names: %s' % str(list(overlap))
            )

        return sleft.append(sright)

    def has_schema(self) -> bool:
        return False

    def root_tables(self):
        if util.all_of([self.left.op(), self.right.op()], (Join, Selection)):
            # Unraveling is not possible
            return [self.left.op(), self.right.op()]
        else:
            return distinct_roots(self.left, self.right)


@node
class InnerJoin(Join):
    pass


@node
class LeftJoin(Join):
    pass


@node
class RightJoin(Join):
    pass


@node
class OuterJoin(Join):
    pass


@node
class AnyInnerJoin(Join):
    pass


@node
class AnyLeftJoin(Join):
    pass


@node
class LeftSemiJoin(Join):
    def _get_schema(self) -> sch.Schema:
        return self.left.schema()


@node
class LeftAntiJoin(Join):
    def _get_schema(self) -> sch.Schema:
        return self.left.schema()


@node
class MaterializedJoin(TableNode, HasSchema):
    join = attrib(validator=attr.validators.instance_of(ir.TableExpr))

    @join.validator
    def validate_join(self, attr, value):
        if not isinstance(value.op(), Join):
            raise TypeError()
        # check whether the underlying schema has overlapping columns or not
        if not self.schema:
            raise TypeError()

    @property
    def schema(self) -> sch.Schema:
        return self.join.op()._get_schema()

    def root_tables(self):
        return self.join._root_tables()

    def blocks(self) -> bool:
        return True


@node
class CrossJoin(InnerJoin):
    """Cartesian production join.

    Some databases have a ``CROSS JOIN`` operator, that may be preferential to
    use over an INNER JOIN with no predicates.

    """


@node
class AsOfJoin(Join):
    left = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    right = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    predicates = attrib()
    by = attrib(default=None)
    tolerance = attrib(
        converter=attr.converters.optional(rlz.interval), default=None
    )

    def __attrs_post_init__(self) -> None:
        object.__setattr__(
            self,
            'by',
            tuple(_clean_join_predicates(self.left, self.right, self.by)),
        )
        super().__attrs_post_init__()


@node
class Union(TableNode, HasSchema):
    left = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    right = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    distinct = attrib(
        validator=attr.validators.instance_of(bool), default=False
    )

    def _validate(self):
        if not self.left.schema().equals(self.right.schema()):
            raise com.RelationError(
                'Table schemas must be equal ' 'to form union'
            )

    @property
    def schema(self) -> sch.Schema:
        return self.left.schema()

    def blocks(self) -> bool:
        return True


@node
class Limit(TableNode):
    table = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    n = attrib(validator=attr.validators.instance_of(int))
    offset = attrib(validator=attr.validators.instance_of(int))

    def blocks(self) -> bool:
        return True

    @property
    def schema(self) -> sch.Schema:
        return self.table.schema()

    def has_schema(self) -> bool:
        return self.table.op().has_schema()

    def root_tables(self):
        return [self]


# --------------------------------------------------------------------
# Sorting


def to_sort_key(table, key):
    if isinstance(key, DeferredSortKey):
        key = key.resolve(table)

    if isinstance(key, ir.SortExpr):
        return key

    if isinstance(key, (tuple, list)):
        key, sort_order = key
    else:
        sort_order = True

    if not isinstance(key, ir.Expr):
        key = table._ensure_expr(key)
        if isinstance(key, (ir.SortExpr, DeferredSortKey)):
            return to_sort_key(table, key)

    if isinstance(sort_order, str):
        if sort_order.lower() in ('desc', 'descending'):
            sort_order = False
        elif not isinstance(sort_order, bool):
            sort_order = bool(sort_order)

    return SortKey(key, ascending=sort_order).to_expr()


@node
class SortKey(Node):
    expr = attrib(converter=rlz.column(rlz.any))
    ascending = attrib(
        converter=bool,
        validator=attr.validators.instance_of(bool),
        default=True,
    )

    def __repr__(self):
        # Temporary
        rows = [
            'Sort key:',
            '  ascending: {}'.format(self.ascending),
            util.indent(_safe_repr(self.expr), 2),
        ]
        return '\n'.join(rows)

    def output_type(self):
        return ir.SortExpr

    def root_tables(self):
        return self.expr._root_tables()

    def equals(self, other, cache=None):
        # TODO: might generalize this equals based on fields
        # requires a proxy class with equals for non expr values
        return (
            isinstance(other, SortKey)
            and self.expr.equals(other.expr, cache=cache)
            and self.ascending == other.ascending
        )

    def resolve_name(self):
        return self.expr.get_name()


@node
class DeferredSortKey:
    what = attrib()
    ascending = attrib(
        validator=attr.validators.instance_of(bool), default=True
    )

    def resolve(self, parent):
        what = parent._ensure_expr(self.what)
        return SortKey(what, ascending=self.ascending).to_expr()


@node
class SelfReference(TableNode, HasSchema):
    table = attrib(validator=attr.validators.instance_of(ir.TableExpr))

    @property
    def schema(self) -> sch.Schema:
        return self.table.schema()

    def root_tables(self):
        # The dependencies of this operation are not walked, which makes the
        # table expression holding this relationally distinct from other
        # expressions, so things like self-joins are possible
        return [self]

    def blocks(self) -> bool:
        return True


@node
class Selection(TableNode, HasSchema):
    table = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    selections = attrib(converter=tuple, factory=tuple)
    predicates = attrib(converter=tuple, factory=tuple)
    sort_keys = attrib(converter=tuple, factory=tuple)

    def __attrs_post_init__(self) -> None:
        import ibis.expr.analysis as L

        table = self.table
        projections = tuple(
            table[selection] if isinstance(selection, str) else selection
            for selection in self.selections
        )

        object.__setattr__(self, 'selections', projections)
        object.__setattr__(
            self,
            'sort_keys',
            tuple(to_sort_key(self.table, k) for k in self.sort_keys),
        )
        object.__setattr__(
            self,
            'predicates',
            tuple(toolz.concat(map(L.flatten_predicate, self.predicates))),
        )
        super().__attrs_post_init__()

    def _validate(self):
        from ibis.expr.analysis import FilterValidator

        # Need to validate that the column expressions are compatible with the
        # input table; this means they must either be scalar expressions or
        # array expressions originating from the same root table expression
        dependent_exprs = self.selections + self.sort_keys
        self.table._assert_valid(dependent_exprs)

        # Validate predicates
        validator = FilterValidator([self.table])
        validator.validate_all(self.predicates)

        # Validate no overlapping columns in schema
        assert self.schema

    @property
    def schema(self) -> sch.Schema:
        # Resolve schema and initialize
        if not self.selections:
            return self.table.schema()

        types = []
        names = []

        for projection in self.selections:
            if isinstance(projection, ir.ValueExpr):
                names.append(projection.get_name())
                types.append(projection.type())
            elif isinstance(projection, ir.TableExpr):
                schema = projection.schema()
                names.extend(schema.names)
                types.extend(schema.types)

        return Schema(names, types)

    def blocks(self) -> bool:
        return bool(self.selections)

    def substitute_table(self, table_expr):
        return Selection(table_expr, self.selections)

    def root_tables(self):
        return [self]

    def can_add_filters(self, wrapped_expr, predicates):
        pass

    @staticmethod
    def empty_or_equal(lefts, rights):
        return not lefts or not rights or all_equal(lefts, rights)

    def compatible_with(self, other):
        # self and other are equivalent except for predicates, selections, or
        # sort keys any of which is allowed to be empty. If both are not empty
        # then they must be equal
        if self.equals(other):
            return True

        if not isinstance(other, type(self)):
            return False

        return self.table.equals(other.table) and (
            self.empty_or_equal(self.predicates, other.predicates)
            and self.empty_or_equal(self.selections, other.selections)
            and self.empty_or_equal(self.sort_keys, other.sort_keys)
        )

    # Operator combination / fusion logic

    def aggregate(self, this, metrics, by=None, having=None):
        if self.selections:
            return Aggregation(this, metrics, by=by, having=having)
        else:
            helper = AggregateSelection(this, metrics, by, having)
            return helper.get_result()

    def sort_by(self, expr, sort_exprs):
        sort_exprs = util.promote_tuple(sort_exprs)
        if not self.blocks():
            resolved_keys = _maybe_convert_sort_keys(self.table, sort_exprs)
            if resolved_keys and self.table._is_valid(resolved_keys):
                return Selection(
                    self.table,
                    self.selections,
                    predicates=self.predicates,
                    sort_keys=self.sort_keys + resolved_keys,
                )

        return Selection(expr, (), sort_keys=sort_exprs)


@attr.s(slots=True, frozen=True, repr=False, cache_hash=True)
class AggregateSelection:
    # sort keys cannot be discarded because of order-dependent
    # aggregate functions like GROUP_CONCAT

    parent = attrib()
    metrics = attrib()
    by = attrib()
    having = attrib()

    @property
    def op(self):
        return self.parent.op()

    def get_result(self):
        if self.op.blocks():
            return self._plain_subquery()
        else:
            return self._attempt_pushdown()

    def _plain_subquery(self):
        return Aggregation(
            self.parent, self.metrics, by=self.by, having=self.having
        )

    def _attempt_pushdown(self):
        metrics_valid, lowered_metrics = self._pushdown_exprs(self.metrics)
        by_valid, lowered_by = self._pushdown_exprs(self.by)
        having_valid, lowered_having = self._pushdown_exprs(
            self.having or None
        )

        if metrics_valid and by_valid and having_valid:
            return Aggregation(
                self.op.table,
                lowered_metrics,
                by=lowered_by,
                having=lowered_having,
                predicates=self.op.predicates,
                sort_keys=self.op.sort_keys,
            )
        else:
            return self._plain_subquery()

    def _pushdown_exprs(self, exprs):
        import ibis.expr.analysis as L

        if exprs is None:
            return True, ()

        resolved = self.op.table._resolve(exprs)
        subbed_exprs = ()

        valid = False
        if resolved:
            subbed_exprs = tuple(
                L.sub_for(x, [(self.parent, self.op.table)])
                for x in util.promote_tuple(resolved)
            )
            valid = self.op.table._is_valid(subbed_exprs)
        else:
            valid = False

        return valid, subbed_exprs


def _maybe_convert_sort_keys(table, exprs):
    try:
        return tuple(to_sort_key(table, k) for k in util.promote_tuple(exprs))
    except com.IbisError:
        return None


def tuple_argument_converter(arg):
    return tuple(filter(lambda arg: arg is not None, util.to_tuple(arg)))


@node
class Aggregation(TableNode, HasSchema):
    """
    metrics : per-group scalar aggregates
    by : group expressions
    having : post-aggregation predicate

    TODO: not putting this in the aggregate operation yet
    where : pre-aggregation predicate

    """

    table = attrib(validator=attr.validators.instance_of(ir.TableExpr))
    metrics = attrib(converter=util.to_tuple)
    by = attrib(converter=tuple_argument_converter, factory=tuple)
    having = attrib(converter=tuple_argument_converter, factory=tuple)
    predicates = attrib(converter=tuple_argument_converter, factory=tuple)
    sort_keys = attrib(converter=tuple_argument_converter, factory=tuple)

    def __attrs_post_init__(self) -> None:
        object.__setattr__(
            self, 'metrics', self._rewrite_exprs(self.table, self.metrics)
        )
        object.__setattr__(self, 'by', self.table._resolve(self.by))
        object.__setattr__(
            self, 'having', self._rewrite_exprs(self.table, self.having)
        )
        object.__setattr__(
            self,
            'predicates',
            self._rewrite_exprs(self.table, self.predicates),
        )

        # order by only makes sense with group by in an aggregation
        if not self.by:
            object.__setattr__(self, 'sort_keys', ())
        elif self.sort_keys:
            sort_keys = tuple(
                to_sort_key(self.table, k)
                for k in util.promote_tuple(self.sort_keys)
            )
            object.__setattr__(
                self, 'sort_keys', self._rewrite_exprs(self.table, sort_keys)
            )
        super().__attrs_post_init__()

    def _validate(self):
        from ibis.expr.analysis import is_reduction
        from ibis.expr.analysis import FilterValidator

        # All aggregates are valid
        for expr in self.metrics:
            if not isinstance(expr, ir.ScalarExpr) or not is_reduction(expr):
                raise TypeError(
                    'Passed a non-aggregate expression: {}'.format(
                        _safe_repr(expr)
                    )
                )

        for expr in self.having:
            if not isinstance(expr, ir.BooleanScalar):
                raise com.ExpressionError(
                    'Having clause must be boolean scalar '
                    'expression, was: {}'.format(_safe_repr(expr))
                )

        # All non-scalar refs originate from the input table
        all_exprs = self.metrics + self.by + self.having + self.sort_keys
        self.table._assert_valid(all_exprs)

        # Validate predicates
        validator = FilterValidator((self.table,))
        validator.validate_all(self.predicates)

        # Validate schema has no overlapping columns
        assert self.schema

    def _rewrite_exprs(self, table, what):
        from ibis.expr.analysis import substitute_parents

        all_exprs = tuple(
            toolz.concat(
                tuple(expr.exprs())
                if isinstance(expr, ir.ExprList)
                else (ir.bind_expr(table, expr),)
                for expr in util.promote_tuple(what)
            )
        )

        return tuple(
            substitute_parents(expr, past_projection=False)
            for expr in all_exprs
        )

    def blocks(self) -> bool:
        return True

    def substitute_table(self, table_expr):
        return Aggregation(
            table_expr, self.metrics, by=self.by, having=self.having
        )

    @property
    def schema(self) -> sch.Schema:
        # All exprs must be named
        return Schema.from_tuples(
            (e.get_name(), e.type())
            for e in itertools.chain(self.by, self.metrics)
        )

    def sort_by(self, expr, sort_exprs):
        sort_exprs = util.to_tuple(sort_exprs)

        resolved_keys = _maybe_convert_sort_keys(self.table, sort_exprs)
        if resolved_keys and self.table._is_valid(resolved_keys):
            return Aggregation(
                self.table,
                self.metrics,
                by=self.by,
                having=self.having,
                predicates=self.predicates,
                sort_keys=self.sort_keys + resolved_keys,
            )

        return Selection(expr, (), sort_keys=sort_exprs)


@node
class NumericBinaryOp(BinaryOp):
    left = attrib(converter=rlz.numeric)
    right = attrib(converter=rlz.numeric)


@node
class Add(NumericBinaryOp):
    output_type = rlz.numeric_like('args', operator.add)


@node
class Multiply(NumericBinaryOp):
    output_type = rlz.numeric_like('args', operator.mul)


@node
class Power(NumericBinaryOp):
    def output_type(self):
        if util.all_of(self.args, ir.IntegerValue):
            return rlz.shape_like(self.args, dt.float64)
        else:
            return rlz.shape_like(self.args)


@node
class Subtract(NumericBinaryOp):
    output_type = rlz.numeric_like('args', operator.sub)


@node
class Divide(NumericBinaryOp):
    output_type = rlz.shape_like('args', dt.float64)


@node
class FloorDivide(Divide):
    output_type = rlz.shape_like('args', dt.int64)


class LogicalBinaryOp(BinaryOp):
    left = attrib(converter=rlz.boolean)
    right = attrib(converter=rlz.boolean)
    output_type = rlz.shape_like('args', dt.boolean)


@node
class Not(UnaryOp):
    arg = attrib(converter=rlz.boolean)
    output_type = rlz.shape_like('arg', dt.boolean)


@node
class Modulus(NumericBinaryOp):
    output_type = rlz.numeric_like('args', operator.mod)


@node
class And(LogicalBinaryOp):
    pass


@node
class Or(LogicalBinaryOp):
    pass


@node
class Xor(LogicalBinaryOp):
    pass


@node
class Comparison(BinaryOp, BooleanValueOp):
    left = attrib(converter=rlz.any)
    right = attrib(converter=rlz.any)

    def __attrs_post_init__(self) -> None:
        """
        Casting rules for type promotions (for resolving the output type) may
        depend in some cases on the target backend.

        TODO: how will overflows be handled? Can we provide anything useful in
        Ibis to help the user avoid them?
        """
        left, right = self._maybe_cast_args(self.left, self.right)
        object.__setattr__(self, 'left', left)
        object.__setattr__(self, 'right', right)

    @staticmethod
    def _maybe_cast_args(left, right):
        # it might not be necessary?
        with suppress(com.IbisTypeError):
            return left, rlz.cast(right, left)

        with suppress(com.IbisTypeError):
            return rlz.cast(left, right), right

        return left, right

    def output_type(self):
        if not rlz.comparable(self.left, self.right):
            raise TypeError(
                'Arguments with datatype {} and {} are '
                'not comparable'.format(self.left.type(), self.right.type())
            )
        return rlz.shape_like(self.args, dt.boolean)


@node
class Equals(Comparison):
    pass


@node
class NotEquals(Comparison):
    pass


@node
class GreaterEqual(Comparison):
    pass


@node
class Greater(Comparison):
    pass


@node
class LessEqual(Comparison):
    pass


@node
class Less(Comparison):
    pass


@node
class IdenticalTo(Comparison):
    pass


@node
class Between(ValueOp, BooleanValueOp):
    arg = attrib(converter=rlz.any)
    lower_bound = attrib(converter=rlz.any)
    upper_bound = attrib(converter=rlz.any)

    def output_type(self):
        arg, lower, upper = self.args

        if not (rlz.comparable(arg, lower) and rlz.comparable(arg, upper)):
            raise TypeError('Arguments are not comparable')

        return rlz.shape_like(self.args, dt.boolean)


@node
class BetweenTime(Between):
    arg = attrib(converter=rlz.one_of([rlz.timestamp, rlz.time]))
    lower_bound = attrib(converter=rlz.one_of([rlz.time, rlz.string]))
    upper_bound = attrib(converter=rlz.one_of([rlz.time, rlz.string]))


@node
class Contains(ValueOp, BooleanValueOp):
    value = attrib(converter=rlz.any)
    options = attrib(
        converter=rlz.one_of(
            [
                rlz.list_of(rlz.any),
                rlz.set_,
                rlz.column(rlz.any),
                rlz.array_of(rlz.any),
            ]
        )
    )

    def output_type(self):
        all_args = [self.value]

        if isinstance(self.options, ir.ListExpr):
            all_args += self.options
        else:
            all_args += [self.options]

        return rlz.shape_like(all_args, dt.boolean)


@node
class NotContains(Contains):
    pass


@node
class ReplaceValues(ValueOp):
    """Apply a multi-value replacement on a particular column.

    From SQL: given ``DAYOFWEEK(timestamp_col)``, replace 1 through 5 to
    "WEEKDAY" and 6 and 7 to "WEEKEND".

    """


@node
class SummaryFilter(ValueOp):
    expr = attrib(validator=attr.validators.instance_of(ir.Expr))

    def output_type(self):
        return dt.boolean.column_type()


def check_topk_k(self, attr, value):
    if value < 0:
        raise ValueError(
            "'k' argument to topk must be >= 0, got {!r}".format(value)
        )


@node
class TopK(ValueOp):
    arg = attrib(validator=attr.validators.instance_of(ir.ColumnExpr))
    k = attrib(validator=[attr.validators.instance_of(int), check_topk_k])
    by = attrib(default=None)

    def __attrs_post_init__(self):
        if self.by is None:
            object.__setattr__(self, 'by', self.arg.count())

    def output_type(self):
        return ir.TopKExpr

    def blocks(self) -> bool:
        return True


@node
class Constant(ValueOp):
    pass


@node
class TimestampNow(Constant):
    def output_type(self):
        return dt.timestamp.scalar_type()


@node
class E(Constant):
    r"""The constant $\e$."""

    def output_type(self):
        return functools.partial(ir.FloatingScalar, dtype=dt.float64)


@node
class Pi(Constant):
    r"""The constant $\pi$."""

    def output_type(self):
        return functools.partial(ir.FloatingScalar, dtype=dt.float64)


@node
class TemporalUnaryOp(UnaryOp):
    arg = attrib(converter=rlz.temporal)


@node
class TimestampUnaryOp(UnaryOp):
    arg = attrib(converter=rlz.timestamp)


_date_units = dict(
    Y='Y',
    y='Y',
    year='Y',
    YEAR='Y',
    YYYY='Y',
    SYYYY='Y',
    YYY='Y',
    YY='Y',
    Q='Q',
    q='Q',
    quarter='Q',
    QUARTER='Q',
    M='M',
    month='M',
    MONTH='M',
    w='W',
    W='W',
    week='W',
    WEEK='W',
    d='D',
    D='D',
    J='D',
    day='D',
    DAY='D',
)

_time_units = dict(
    h='h',
    H='h',
    HH24='h',
    hour='h',
    HOUR='h',
    m='m',
    MI='m',
    minute='m',
    MINUTE='m',
    s='s',
    second='s',
    SECOND='s',
    ms='ms',
    millisecond='ms',
    MILLISECOND='ms',
    us='us',
    microsecond='ms',
    MICROSECOND='ms',
    ns='ns',
    nanosecond='ns',
    NANOSECOND='ns',
)

_timestamp_units = toolz.merge(_date_units, _time_units)


@node
class TimestampTruncate(ValueOp):
    arg = attrib(converter=rlz.timestamp)
    unit = attrib(converter=rlz.isin(_timestamp_units))
    output_type = rlz.shape_like('arg', dt.timestamp)


@node
class DateTruncate(ValueOp):
    arg = attrib(converter=rlz.date)
    unit = attrib(converter=rlz.isin(_date_units))
    output_type = rlz.shape_like('arg', dt.date)


@node
class TimeTruncate(ValueOp):
    arg = attrib(converter=rlz.time)
    unit = attrib(converter=rlz.isin(_time_units))
    output_type = rlz.shape_like('arg', dt.time)


@node
class Strftime(ValueOp):
    arg = attrib(converter=rlz.temporal)
    format_str = attrib(converter=rlz.string)
    output_type = rlz.shape_like('arg', dt.string)


@node
class StringToTimestamp(ValueOp):
    arg = attrib(converter=rlz.string)
    format_str = attrib(converter=rlz.string)
    timezone = attrib(
        converter=attr.converters.optional(rlz.string), default=None
    )
    output_type = rlz.shape_like('arg', dt.Timestamp(timezone='UTC'))


@node
class ExtractTemporalField(TemporalUnaryOp):
    output_type = rlz.shape_like('arg', dt.int32)


ExtractTimestampField = ExtractTemporalField


@node
class ExtractDateField(ExtractTemporalField):
    arg = attrib(converter=rlz.one_of([rlz.date, rlz.timestamp]))


@node
class ExtractTimeField(ExtractTemporalField):
    arg = attrib(converter=rlz.one_of([rlz.time, rlz.timestamp]))


@node
class ExtractYear(ExtractDateField):
    pass


@node
class ExtractMonth(ExtractDateField):
    pass


@node
class ExtractDay(ExtractDateField):
    pass


@node
class ExtractHour(ExtractTimeField):
    pass


@node
class ExtractMinute(ExtractTimeField):
    pass


@node
class ExtractSecond(ExtractTimeField):
    pass


@node
class ExtractMillisecond(ExtractTimeField):
    pass


@node
class DayOfWeekIndex(UnaryOp):
    arg = attrib(converter=rlz.one_of([rlz.date, rlz.timestamp]))
    output_type = rlz.shape_like('arg', dt.int16)


@node
class DayOfWeekName(UnaryOp):
    arg = attrib(converter=rlz.one_of([rlz.date, rlz.timestamp]))
    output_type = rlz.shape_like('arg', dt.string)


@node
class DayOfWeekNode(Node):
    arg = attrib(converter=rlz.one_of([rlz.date, rlz.timestamp]))

    def output_type(self):
        return ir.DayOfWeek


@node
class Time(UnaryOp):
    output_type = rlz.shape_like('arg', dt.time)


@node
class Date(UnaryOp):
    output_type = rlz.shape_like('arg', dt.date)


@node
class TimestampFromUNIX(ValueOp):
    arg = attrib(converter=rlz.any)
    unit = attrib(validator=attr.validators.in_({'s', 'ms', 'us'}))
    output_type = rlz.shape_like('arg', dt.timestamp)


@node
class DecimalUnaryOp(UnaryOp):
    arg = attrib(converter=rlz.decimal)


@node
class DecimalPrecision(DecimalUnaryOp):
    output_type = rlz.shape_like('arg', dt.int32)


@node
class DecimalScale(UnaryOp):
    output_type = rlz.shape_like('arg', dt.int32)


@node
class Hash(ValueOp):
    arg = attrib(converter=rlz.any)
    how = attrib(validator=attr.validators.in_({'fnv'}))
    output_type = rlz.shape_like('arg', dt.int64)


@node
class DateAdd(BinaryOp):
    left = attrib(converter=rlz.date)
    right = attrib(converter=rlz.interval(units={'Y', 'Q', 'M', 'W', 'D'}))
    output_type = rlz.shape_like('left')


@node
class DateSub(BinaryOp):
    left = attrib(converter=rlz.date)
    right = attrib(converter=rlz.interval(units={'Y', 'Q', 'M', 'W', 'D'}))
    output_type = rlz.shape_like('left')


@node
class DateDiff(BinaryOp):
    left = attrib(converter=rlz.date)
    right = attrib(converter=rlz.date)
    output_type = rlz.shape_like('left', dt.Interval('D'))


@node
class TimeAdd(BinaryOp):
    left = attrib(converter=rlz.time)
    right = attrib(
        converter=rlz.interval(units={'h', 'm', 's', 'ms', 'us', 'ns'})
    )
    output_type = rlz.shape_like('left')


@node
class TimeSub(BinaryOp):
    left = attrib(converter=rlz.time)
    right = attrib(
        converter=rlz.interval(units={'h', 'm', 's', 'ms', 'us', 'ns'})
    )
    output_type = rlz.shape_like('left')


@node
class TimeDiff(BinaryOp):
    left = attrib(converter=rlz.time)
    right = attrib(converter=rlz.time)
    output_type = rlz.shape_like('left', dt.Interval('s'))


@node
class TimestampAdd(BinaryOp):
    left = attrib(converter=rlz.timestamp)
    right = attrib(
        converter=rlz.interval(
            units={'Y', 'Q', 'M', 'W', 'D', 'h', 'm', 's', 'ms', 'us', 'ns'}
        )
    )
    output_type = rlz.shape_like('left')


@node
class TimestampSub(BinaryOp):
    left = attrib(converter=rlz.timestamp)
    right = attrib(
        converter=rlz.interval(
            units={'Y', 'Q', 'M', 'W', 'D', 'h', 'm', 's', 'ms', 'us', 'ns'}
        )
    )
    output_type = rlz.shape_like('left')


@node
class TimestampDiff(BinaryOp):
    left = attrib(converter=rlz.timestamp)
    right = attrib(converter=rlz.timestamp)
    output_type = rlz.shape_like('left', dt.Interval('s'))


@node
class IntervalBinaryOp(BinaryOp):
    def output_type(self):
        args = [
            arg.cast(arg.type().value_type)
            if isinstance(arg.type(), dt.Interval)
            else arg
            for arg in self.args
        ]
        expr = rlz.numeric_like(args, self.__class__.op)(self)
        dtype = attr.evolve(self.left.type(), value_type=expr.type())
        return rlz.shape_like(self.args, dtype=dtype)


@node
class IntervalAdd(IntervalBinaryOp):
    left = attrib(converter=rlz.interval)
    right = attrib(converter=rlz.interval)
    op = operator.add


@node
class IntervalSubtract(IntervalBinaryOp):
    left = attrib(converter=rlz.interval)
    right = attrib(converter=rlz.interval)
    op = operator.sub


@node
class IntervalMultiply(IntervalBinaryOp):
    left = attrib(converter=rlz.interval)
    right = attrib(converter=rlz.numeric)
    op = operator.mul


@node
class IntervalFloorDivide(IntervalBinaryOp):
    left = attrib(converter=rlz.interval)
    right = attrib(converter=rlz.numeric)
    op = operator.floordiv


@node
class IntervalFromInteger(ValueOp):
    arg = attrib(converter=rlz.integer)
    unit = attrib(
        validator=attr.validators.in_(
            {'Y', 'Q', 'M', 'W', 'D', 'h', 'm', 's', 'ms', 'us', 'ns'}
        )
    )

    @property
    def resolution(self):
        return dt.Interval(self.unit).resolution

    def output_type(self):
        dtype = dt.Interval(self.unit, self.arg.type())
        return rlz.shape_like(self.arg, dtype=dtype)


@node
class ArrayLength(UnaryOp):
    arg = attrib(converter=rlz.array)
    output_type = rlz.shape_like('arg', dt.int64)


@node
class ArraySlice(ValueOp):
    arg = attrib(converter=rlz.array)
    start = attrib(converter=rlz.integer)
    stop = attrib(
        converter=attr.converters.optional(rlz.integer), default=None
    )
    output_type = rlz.typeof('arg')


@node
class ArrayIndex(ValueOp):
    arg = attrib(converter=rlz.array)
    index = attrib(converter=rlz.integer)

    def output_type(self):
        value_dtype = self.arg.type().value_type
        return rlz.shape_like(self.arg, value_dtype)


@node
class ArrayConcat(ValueOp):
    left = attrib(converter=rlz.array)
    right = attrib(converter=rlz.array)
    output_type = rlz.shape_like('left')

    def _validate(self):
        left_dtype, right_dtype = self.left.type(), self.right.type()
        if left_dtype != right_dtype:
            raise com.IbisTypeError(
                'Array types must match exactly in a {} operation. '
                'Left type {} != Right type {}'.format(
                    type(self).__name__, left_dtype, right_dtype
                )
            )


@node
class ArrayRepeat(ValueOp):
    arg = attrib(converter=rlz.array)
    times = attrib(converter=rlz.integer)
    output_type = rlz.typeof('arg')


@node
class ArrayCollect(Reduction):
    arg = attrib(converter=rlz.column(rlz.any))

    def output_type(self):
        dtype = dt.Array(self.arg.type())
        return dtype.scalar_type()


@node
class MapLength(ValueOp):
    arg = attrib(converter=rlz.mapping)
    output_type = rlz.shape_like('arg', dt.int64)


@node
class MapValueForKey(ValueOp):
    arg = attrib(converter=rlz.mapping)
    key = attrib(converter=rlz.one_of([rlz.string, rlz.integer]))

    def output_type(self):
        return rlz.shape_like(tuple(self.args), self.arg.type().value_type)


@node
class MapValueOrDefaultForKey(ValueOp):
    arg = attrib(converter=rlz.mapping)
    key = attrib(converter=rlz.one_of([rlz.string, rlz.integer]))
    default = attrib(converter=rlz.any)

    def output_type(self):
        arg = self.arg
        default = self.default
        map_type = arg.type()
        value_type = map_type.value_type
        default_type = default.type()

        if default is not None and not dt.same_kind(default_type, value_type):
            raise com.IbisTypeError(
                "Default value\n{}\nof type {} cannot be cast to map's value "
                "type {}".format(default, default_type, value_type)
            )

        result_type = dt.highest_precedence((default_type, value_type))
        return rlz.shape_like(tuple(self.args), result_type)


@node
class MapKeys(ValueOp):
    arg = attrib(converter=rlz.mapping)

    def output_type(self):
        arg = self.arg
        return rlz.shape_like(arg, dt.Array(arg.type().key_type))


@node
class MapValues(ValueOp):
    arg = attrib(converter=rlz.mapping)

    def output_type(self):
        arg = self.arg
        return rlz.shape_like(arg, dt.Array(arg.type().value_type))


@node
class MapConcat(ValueOp):
    left = attrib(converter=rlz.mapping)
    right = attrib(converter=rlz.mapping)
    output_type = rlz.typeof('left')


@node
class StructField(ValueOp):
    arg = attrib(converter=rlz.struct)

    # TODO: add validation that the attribute is in the struct's schema?
    field = attrib(validator=attr.validators.instance_of(str))

    def output_type(self):
        struct_dtype = self.arg.type()
        value_dtype = struct_dtype[self.field]
        return rlz.shape_like(self.arg, value_dtype)


@node
class Literal(ValueOp):
    value = attrib()
    dtype = attrib(converter=dt.dtype)

    def equals(self, other, cache=None):
        return (
            isinstance(other, Literal)
            and isinstance(other.value, type(self.value))
            and self.value == other.value
        )

    def output_type(self):
        return self.dtype.scalar_type()

    def root_tables(self):
        return []


@node
class NullLiteral(Literal):
    """Typeless NULL literal."""

    value = attrib(
        validator=attr.validators.instance_of(type(None)), default=None
    )
    dtype = attrib(
        validator=attr.validators.instance_of(dt.Null), default=dt.null
    )


@node
class ScalarParameter(ValueOp):
    _counter = itertools.count()

    dtype = attrib(converter=dt.dtype)
    counter = attrib(
        validator=attr.validators.instance_of(int),
        factory=lambda: next(ScalarParameter._counter),
        show=False,
        repr=False,
    )

    def resolve_name(self):
        return 'param_{:d}'.format(self.counter)

    def output_type(self):
        return self.dtype.scalar_type()

    def equals(self, other, cache=None):
        return (
            isinstance(other, ScalarParameter)
            and self.counter == other.counter
            and self.dtype.equals(other.dtype, cache=cache)
        )

    def root_tables(self):
        return []


@node
class ExpressionList(Node):
    """Data structure for a list of arbitrary expressions"""

    exprs = attrib(converter=lambda values: list(map(rlz.any, values)))

    @property
    def inputs(self):
        return (tuple(self.exprs),)

    def root_tables(self):
        return distinct_roots(self.exprs)

    def output_type(self):
        return ir.ExprList


@node
class ValueList(ValueOp):
    """Data structure for a list of value expressions"""

    values = attrib(converter=lambda values: tuple(map(rlz.any, values)))
    display_argnames = False  # disable showing argnames in repr

    def output_type(self):
        dtype = rlz.highest_precedence_dtype(self.values)
        return functools.partial(ir.ListExpr, dtype=dtype)

    def root_tables(self):
        return distinct_roots(*self.values)
