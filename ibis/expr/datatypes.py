import builtins
import collections
import datetime
import functools
import numbers
import re
import typing

from typing import (
    Any as Any_,
    Callable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set as Set_,
    Tuple,
    TypeVar,
    Union,
)

import attr

import pandas as pd

from multipledispatch import Dispatcher

import ibis.common as com
import ibis.expr.types as ir


datatype = attr.s(slots=True, frozen=True, cache_hash=True)


@datatype
class DataType:
    def equals(
        self, other: 'DataType', cache: Optional[Mapping[Any_, bool]] = None
    ) -> bool:
        return self == other

    def castable(self, target, **kwargs):
        return castable(self, target, **kwargs)

    def cast(self, target, **kwargs):
        return cast(self, target, **kwargs)

    def scalar_type(self):
        return functools.partial(self.scalar, dtype=self)

    def column_type(self):
        return functools.partial(self.column, dtype=self)


@datatype
class Any(DataType):
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )


@datatype
class Primitive(DataType):
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )


@datatype
class Null(DataType):
    nullable = True

    scalar = ir.NullScalar
    column = ir.NullColumn


@datatype
class Variadic(DataType):
    pass


@datatype
class Boolean(Primitive):
    scalar = ir.BooleanScalar
    column = ir.BooleanColumn


Bounds = NamedTuple('Bounds', [('lower', int), ('upper', int)])


@datatype
class Integer(Primitive):
    scalar = ir.IntegerScalar
    column = ir.IntegerColumn

    @property
    def _nbytes(self) -> int:
        raise TypeError(
            "Cannot determine the size in bytes of an abstract integer type."
        )


@datatype
class String(Variadic):
    """A type representing a string.

    Notes
    -----
    Because of differences in the way different backends handle strings, we
    cannot assume that strings are UTF-8 encoded.

    """

    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.StringScalar
    column = ir.StringColumn


@datatype
class Binary(Variadic):
    """A type representing a blob of bytes.

    Notes
    -----
    Some databases treat strings and blobs of equally, and some do not. For
    example, Impala doesn't make a distinction between string and binary types
    but PostgreSQL has a TEXT type and a BYTEA type which are distinct types
    that behave differently.

    """

    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.BinaryScalar
    column = ir.BinaryColumn


@datatype
class Date(Primitive):
    scalar = ir.DateScalar
    column = ir.DateColumn


@datatype
class Time(Primitive):
    scalar = ir.TimeScalar
    column = ir.TimeColumn


@datatype
class Timestamp(DataType):
    timezone = attr.ib(
        validator=attr.validators.optional(attr.validators.instance_of(str)),
        default=None,
    )
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.TimestampScalar
    column = ir.TimestampColumn


@datatype
class SignedInteger(Integer):
    @property
    def largest(self):
        return int64

    @property
    def bounds(self):
        exp = self._nbytes * 8 - 1
        upper = (1 << exp) - 1
        return Bounds(lower=~upper, upper=upper)


@datatype
class UnsignedInteger(Integer):
    @property
    def largest(self):
        return uint64

    @property
    def bounds(self):
        exp = self._nbytes * 8 - 1
        upper = 1 << exp
        return Bounds(lower=0, upper=upper)


@datatype
class Floating(Primitive):
    scalar = ir.FloatingScalar
    column = ir.FloatingColumn

    @property
    def largest(self):
        return float64

    @property
    def _nbytes(self) -> int:
        raise TypeError(
            "Cannot determine the size in bytes of an abstract floating "
            "point type."
        )


@datatype
class Int8(SignedInteger):
    _nbytes = 1


@datatype
class Int16(SignedInteger):
    _nbytes = 2


@datatype
class Int32(SignedInteger):
    _nbytes = 4


@datatype
class Int64(SignedInteger):
    _nbytes = 8


@datatype
class UInt8(UnsignedInteger):
    _nbytes = 1


@datatype
class UInt16(UnsignedInteger):
    _nbytes = 2


@datatype
class UInt32(UnsignedInteger):
    _nbytes = 4


@datatype
class UInt64(UnsignedInteger):
    _nbytes = 8


@datatype
class Float16(Floating):
    _nbytes = 2


@datatype
class Float32(Floating):
    _nbytes = 4


@datatype
class Float64(Floating):
    _nbytes = 8


Halffloat = Float16
Float = Float32
Double = Float64


def check_precision(self, attr, precision):
    if not precision:
        raise ValueError('Decimal type {!r} cannot be 0'.format(attr))
    if precision < 0:
        raise ValueError('Decimal type {!r} cannot be negative'.format(attr))


def check_scale(self, attr, scale):
    if scale < 0:
        raise ValueError('Decimal type scale cannot be negative')

    precision = self.precision
    if precision < scale:
        raise ValueError(
            'Decimal type precision must be greater than or equal to '
            'scale. Got precision={:d} and scale={:d}'.format(
                precision, scale
            )
        )


@datatype
class Decimal(DataType):
    precision = attr.ib(
        validator=[
            attr.validators.instance_of(numbers.Integral),
            check_precision,
        ]
    )
    scale = attr.ib(
        validator=[
            attr.validators.instance_of(numbers.Integral),
            check_scale,
        ]
    )
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.DecimalScalar
    column = ir.DecimalColumn

    @property
    def largest(self) -> 'Decimal':
        return Decimal(38, self.scale)


# based on numpy's units
INTERVAL_UNITS = dict(
    Y='year',
    Q='quarter',
    M='month',
    W='week',
    D='day',
    h='hour',
    m='minute',
    s='second',
    ms='millisecond',
    us='microsecond',
    ns='nanosecond',
)


@datatype
class Interval(DataType):
    unit = attr.ib(
        validator=attr.validators.in_(INTERVAL_UNITS), default='s'
    )
    value_type = attr.ib(
        converter=lambda value: dtype(value if value is not None else int32),
        validator=attr.validators.instance_of(Integer),
        default=None,
    )
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.IntervalScalar
    column = ir.IntervalColumn

    @property
    def bounds(self):
        return self.value_type.bounds

    @property
    def resolution(self):
        """Unit's name"""
        return INTERVAL_UNITS[self.unit]


@datatype
class Category(DataType):
    cardinality = attr.ib(
        validator=attr.validators.optional(attr.validators.instance_of(int)),
        default=None,
    )
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.CategoryScalar
    column = ir.CategoryColumn

    def to_integer_type(self) -> Integer:
        # TODO: this should be removed I guess
        cardinality = self.cardinality
        return int64 if cardinality else infer(self.cardinality)


def check_names(self, attr: str, names) -> None:
    invalid_types = {
        name: type(name) for name in names if not isinstance(name, str)
    }
    if invalid_types:
        raise ValueError(
            'Invalid struct field names {!r}'.format(invalid_types)
        )


def check_non_empty_sequence(self, attr: str, value) -> None:
    if not value:
        raise ValueError('Argument {!r} must be a non-empty sequence')


def check_names_and_types(self, attr: str, types) -> None:
    if len(self.names) != len(types):
        raise ValueError('names and types must have the same length')


@datatype
class Struct(DataType):
    names = attr.ib(
        converter=list,
        validator=[
            attr.validators.instance_of(collections.Sequence),
            check_non_empty_sequence,
            check_names,
        ],
    )
    types = attr.ib(
        converter=list,
        validator=[
            attr.validators.instance_of(collections.Sequence),
            check_non_empty_sequence,
            check_names_and_types,
        ],
    )
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.StructScalar
    column = ir.StructColumn

    @classmethod
    def from_tuples(
        self,
        pairs: Sequence[Tuple[str, Union[str, DataType]]],
        nullable: bool = True,
    ) -> 'Struct':
        names, types = zip(*pairs)
        return Struct(list(names), list(map(dtype, types)), nullable=nullable)

    @property
    def pairs(self) -> Mapping:
        return collections.OrderedDict(zip(self.names, self.types))

    def __getitem__(self, key: str) -> DataType:
        return self.pairs[key]


@datatype
class Array(Variadic):
    value_type = attr.ib(converter=lambda value: dtype(value))
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.ArrayScalar
    column = ir.ArrayColumn


@datatype
class Set(Variadic):
    value_type = attr.ib(converter=lambda value: dtype(value))
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.SetScalar
    column = ir.SetColumn


@datatype
class Enum(DataType):
    rep_type = attr.ib(converter=lambda value: dtype(value))
    value_type = attr.ib(converter=lambda value: dtype(value))
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.EnumScalar
    column = ir.EnumColumn


@datatype
class Map(Variadic):
    key_type = attr.ib(converter=lambda value: dtype(value))
    value_type = attr.ib(converter=lambda value: dtype(value))
    nullable = attr.ib(
        validator=attr.validators.instance_of(bool), default=True
    )

    scalar = ir.MapScalar
    column = ir.MapColumn


dtype = Dispatcher('dtype')

validate_type = dtype


@dtype.register(object)
def default(value, **kwargs) -> DataType:
    raise com.IbisTypeError('Value {!r} is not a valid datatype'.format(value))


@dtype.register(DataType)
def from_ibis_dtype(value: DataType) -> DataType:
    return value


@dtype.register(str)
def from_string(value: str) -> DataType:
    try:
        return TypeParser(value).parse()
    except SyntaxError:
        raise com.IbisTypeError(
            '{!r} cannot be parsed as a datatype'.format(value)
        )


@dtype.register(list)
def from_list(values: List[Any_]) -> Array:
    if not values:
        return Array(null)
    return Array(highest_precedence(map(dtype, values)))


@dtype.register(collections.Set)
def from_set(values: Set_) -> Set:
    if not values:
        return Set(null)
    return Set(highest_precedence(map(dtype, values)))


# ---------------------------------------------------------------------
any = Any()
null = Null()
boolean = Boolean()
int_ = Integer()
int8 = Int8()
int16 = Int16()
int32 = Int32()
int64 = Int64()
uint_ = UnsignedInteger()
uint8 = UInt8()
uint16 = UInt16()
uint32 = UInt32()
uint64 = UInt64()
float = Float()
halffloat = Halffloat()
float16 = Halffloat()
float32 = Float32()
float64 = Float64()
double = Double()
string = String()
binary = Binary()
date = Date()
time = Time()
timestamp = Timestamp()
interval = Interval()
category = Category()


_primitive_types = [
    ('any', any),
    ('null', null),
    ('boolean', boolean),
    ('bool', boolean),
    ('int8', int8),
    ('int16', int16),
    ('int32', int32),
    ('int64', int64),
    ('uint8', uint8),
    ('uint16', uint16),
    ('uint32', uint32),
    ('uint64', uint64),
    ('float16', float16),
    ('float32', float32),
    ('float64', float64),
    ('float', float),
    ('halffloat', float16),
    ('double', double),
    ('string', string),
    ('binary', binary),
    ('date', date),
    ('time', time),
    ('timestamp', timestamp),
    ('interval', interval),
]  # type: List[Tuple[str, DataType]]


class Tokens:
    """Class to hold tokens for lexing."""

    __slots__ = ()

    ANY = 0
    NULL = 1
    PRIMITIVE = 2
    DECIMAL = 3
    VARCHAR = 4
    CHAR = 5
    ARRAY = 6
    MAP = 7
    STRUCT = 8
    INTEGER = 9
    FIELD = 10
    COMMA = 11
    COLON = 12
    LPAREN = 13
    RPAREN = 14
    LBRACKET = 15
    RBRACKET = 16
    STRARG = 17
    TIMESTAMP = 18
    TIME = 19
    INTERVAL = 20
    SET = 21

    @staticmethod
    def name(value):
        return _token_names[value]


_token_names = dict(
    (getattr(Tokens, n), n) for n in dir(Tokens) if n.isalpha() and n.isupper()
)


Token = collections.namedtuple('Token', ('type', 'value'))


# Adapted from tokenize.String
_STRING_REGEX = """('[^\n'\\\\]*(?:\\\\.[^\n'\\\\]*)*'|"[^\n"\\\\"]*(?:\\\\.[^\n"\\\\]*)*")"""  # noqa: E501


Action = Optional[Callable[[str], Token]]


_TYPE_RULES = collections.OrderedDict(
    [
        # any, null, bool|boolean
        ('(?P<ANY>any)', lambda token: Token(Tokens.ANY, any)),
        ('(?P<NULL>null)', lambda token: Token(Tokens.NULL, null)),
        (
            '(?P<BOOLEAN>bool(?:ean)?)',
            typing.cast(
                Action, lambda token: Token(Tokens.PRIMITIVE, boolean)
            ),
        ),
    ]
    + [
        # primitive types
        (
            '(?P<{}>{})'.format(token.upper(), token),
            typing.cast(
                Action,
                lambda token, value=value: Token(Tokens.PRIMITIVE, value),
            ),
        )
        for token, value in _primitive_types
        if token
        not in {'any', 'null', 'timestamp', 'time', 'interval', 'boolean'}
    ]
    + [
        # timestamp
        (
            r'(?P<TIMESTAMP>timestamp)',
            lambda token: Token(Tokens.TIMESTAMP, token),
        )
    ]
    + [
        # interval - should remove?
        (
            r'(?P<INTERVAL>interval)',
            lambda token: Token(Tokens.INTERVAL, token),
        )
    ]
    + [
        # time
        (r'(?P<TIME>time)', lambda token: Token(Tokens.TIME, token))
    ]
    + [
        # decimal + complex types
        (
            '(?P<{}>{})'.format(token.upper(), token),
            typing.cast(
                Action, lambda token, toktype=toktype: Token(toktype, token)
            ),
        )
        for token, toktype in zip(
            (
                'decimal',
                'varchar',
                'char',
                'array',
                'set',
                'map',
                'struct',
                'interval',
            ),
            (
                Tokens.DECIMAL,
                Tokens.VARCHAR,
                Tokens.CHAR,
                Tokens.ARRAY,
                Tokens.SET,
                Tokens.MAP,
                Tokens.STRUCT,
                Tokens.INTERVAL,
            ),
        )
    ]
    + [
        # integers, for decimal spec
        (r'(?P<INTEGER>\d+)', lambda token: Token(Tokens.INTEGER, int(token))),
        # struct fields
        (
            r'(?P<FIELD>[a-zA-Z_][a-zA-Z_0-9]*)',
            lambda token: Token(Tokens.FIELD, token),
        ),
        # timezones
        ('(?P<COMMA>,)', lambda token: Token(Tokens.COMMA, token)),
        ('(?P<COLON>:)', lambda token: Token(Tokens.COLON, token)),
        (r'(?P<LPAREN>\()', lambda token: Token(Tokens.LPAREN, token)),
        (r'(?P<RPAREN>\))', lambda token: Token(Tokens.RPAREN, token)),
        ('(?P<LBRACKET><)', lambda token: Token(Tokens.LBRACKET, token)),
        ('(?P<RBRACKET>>)', lambda token: Token(Tokens.RBRACKET, token)),
        (r'(?P<WHITESPACE>\s+)', None),
        (
            '(?P<STRARG>{})'.format(_STRING_REGEX),
            lambda token: Token(Tokens.STRARG, token),
        ),
    ]
)


_TYPE_KEYS = tuple(_TYPE_RULES.keys())
_TYPE_PATTERN = re.compile('|'.join(_TYPE_KEYS), flags=re.IGNORECASE)


def _generate_tokens(pat: Any_, text: str) -> Iterator[Token]:
    """Generate a sequence of tokens from `text` that match `pat`

    Parameters
    ----------
    pat : compiled regex
        The pattern to use for tokenization
    text : str
        The text to tokenize

    """
    rules = _TYPE_RULES
    keys = _TYPE_KEYS
    groupindex = pat.groupindex
    scanner = pat.scanner(text)
    for m in iter(scanner.match, None):
        lastgroup = m.lastgroup
        func = rules[keys[groupindex[lastgroup] - 1]]
        if func is not None:
            yield func(m.group(lastgroup))


class TypeParser:
    """A type parser for complex types.

    Parameters
    ----------
    text : str
        The text to parse

    Notes
    -----
    Adapted from David Beazley's and Brian Jones's Python Cookbook

    """

    __slots__ = 'text', 'tokens', 'tok', 'nexttok'

    def __init__(self, text: str) -> None:
        self.text = text  # type: str
        self.tokens = _generate_tokens(_TYPE_PATTERN, text)
        self.tok = None  # type: Optional[Token]
        self.nexttok = None  # type: Optional[Token]

    def _advance(self) -> None:
        self.tok, self.nexttok = self.nexttok, next(self.tokens, None)

    def _accept(self, toktype: int) -> bool:
        if self.nexttok is not None and self.nexttok.type == toktype:
            self._advance()
            assert (
                self.tok is not None
            ), 'self.tok should not be None when _accept succeeds'
            return True
        return False

    def _expect(self, toktype: int) -> None:
        if not self._accept(toktype):
            raise SyntaxError(
                'Expected {} after {!r} in {!r}'.format(
                    Tokens.name(toktype),
                    getattr(self.tok, 'value', self.tok),
                    self.text,
                )
            )

    def parse(self) -> DataType:
        self._advance()

        # any and null types cannot be nested
        if self._accept(Tokens.ANY) or self._accept(Tokens.NULL):
            assert (
                self.tok is not None
            ), 'self.tok was None when parsing ANY or NULL type'
            return self.tok.value

        t = self.type()
        if self.nexttok is None:
            return t
        else:
            # additional junk was passed at the end, throw an error
            additional_tokens = []
            while self.nexttok is not None:
                additional_tokens.append(self.nexttok.value)
                self._advance()
            raise SyntaxError(
                'Found additional tokens {}'.format(additional_tokens)
            )

    def type(self) -> DataType:
        """
        type : primitive
             | decimal
             | array
             | set
             | map
             | struct

        primitive : "any"
                  | "null"
                  | "bool"
                  | "boolean"
                  | "int8"
                  | "int16"
                  | "int32"
                  | "int64"
                  | "uint8"
                  | "uint16"
                  | "uint32"
                  | "uint64"
                  | "halffloat"
                  | "float"
                  | "double"
                  | "float16"
                  | "float32"
                  | "float64"
                  | "string"
                  | "time"

        timestamp : "timestamp"
                  | "timestamp" "(" timezone ")"

        interval : "interval"
                 | "interval" "(" unit ")"
                 | "interval" "<" type ">" "(" unit ")"

        decimal : "decimal"
                | "decimal" "(" integer "," integer ")"

        integer : [0-9]+

        array : "array" "<" type ">"

        set : "set" "<" type ">"

        map : "map" "<" type "," type ">"

        struct : "struct" "<" field ":" type ("," field ":" type)* ">"

        field : [a-zA-Z_][a-zA-Z_0-9]*
        """
        if self._accept(Tokens.PRIMITIVE):
            assert self.tok is not None
            return self.tok.value

        elif self._accept(Tokens.TIMESTAMP):
            if self._accept(Tokens.LPAREN):
                self._expect(Tokens.STRARG)
                assert self.tok is not None
                timezone = self.tok.value[1:-1]  # remove surrounding quotes
                self._expect(Tokens.RPAREN)
                return Timestamp(timezone=timezone)
            return timestamp

        elif self._accept(Tokens.TIME):
            return Time()

        elif self._accept(Tokens.INTERVAL):
            if self._accept(Tokens.LBRACKET):
                self._expect(Tokens.PRIMITIVE)
                assert self.tok is not None
                value_type = self.tok.value
                self._expect(Tokens.RBRACKET)
            else:
                value_type = int32

            if self._accept(Tokens.LPAREN):
                self._expect(Tokens.STRARG)
                assert self.tok is not None
                unit = self.tok.value[1:-1]  # remove surrounding quotes
                self._expect(Tokens.RPAREN)
            else:
                unit = 's'

            return Interval(unit, value_type)

        elif self._accept(Tokens.DECIMAL):
            if self._accept(Tokens.LPAREN):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                precision = self.tok.value

                self._expect(Tokens.COMMA)

                self._expect(Tokens.INTEGER)
                scale = self.tok.value

                self._expect(Tokens.RPAREN)
            else:
                precision = 9
                scale = 0
            return Decimal(precision, scale)

        elif self._accept(Tokens.VARCHAR) or self._accept(Tokens.CHAR):
            # VARCHAR, VARCHAR(n), CHAR, and CHAR(n) all parse as STRING
            if self._accept(Tokens.LPAREN):
                self._expect(Tokens.INTEGER)
                self._expect(Tokens.RPAREN)
                return string
            return string

        elif self._accept(Tokens.ARRAY):
            self._expect(Tokens.LBRACKET)

            value_type = self.type()

            self._expect(Tokens.RBRACKET)
            return Array(value_type)

        elif self._accept(Tokens.SET):
            self._expect(Tokens.LBRACKET)

            value_type = self.type()

            self._expect(Tokens.RBRACKET)
            return Set(value_type)

        elif self._accept(Tokens.MAP):
            self._expect(Tokens.LBRACKET)

            self._expect(Tokens.PRIMITIVE)
            assert self.tok is not None
            key_type = self.tok.value

            self._expect(Tokens.COMMA)

            value_type = self.type()

            self._expect(Tokens.RBRACKET)

            return Map(key_type, value_type)

        elif self._accept(Tokens.STRUCT):
            self._expect(Tokens.LBRACKET)

            self._expect(Tokens.FIELD)
            assert self.tok is not None
            names = [self.tok.value]

            self._expect(Tokens.COLON)

            types = [self.type()]

            while self._accept(Tokens.COMMA):
                self._expect(Tokens.FIELD)
                names.append(self.tok.value)

                self._expect(Tokens.COLON)
                types.append(self.type())

            self._expect(Tokens.RBRACKET)
            return Struct(names, types)
        else:
            raise SyntaxError('Type cannot be parsed: {}'.format(self.text))


infer = Dispatcher('infer')


def higher_precedence(left: DataType, right: DataType) -> DataType:
    if castable(left, right, upcast=True):
        return right
    elif castable(right, left, upcast=True):
        return left

    raise com.IbisTypeError(
        'Cannot compute precedence for {} and {} types'.format(left, right)
    )


def highest_precedence(dtypes: Iterator[DataType]) -> DataType:
    """Compute the highest precedence of `dtypes`."""
    return functools.reduce(higher_precedence, dtypes)


@infer.register(object)
def infer_dtype_default(value: Any_) -> DataType:
    """Default implementation of :func:`~ibis.expr.datatypes.infer`."""
    raise com.InputTypeError(value)


@infer.register(collections.OrderedDict)
def infer_struct(value: Mapping[str, Any_]) -> Struct:
    """Infer the :class:`~ibis.expr.datatypes.Struct` type of `value`."""
    if not value:
        raise TypeError('Empty struct type not supported')
    return Struct(list(value.keys()), list(map(infer, value.values())))


@infer.register(collections.abc.Mapping)
def infer_map(value: Mapping[Any_, Any_]) -> Map:
    """Infer the :class:`~ibis.expr.datatypes.Map` type of `value`."""
    if not value:
        return Map(null, null)
    return Map(
        highest_precedence(map(infer, value.keys())),
        highest_precedence(map(infer, value.values())),
    )


@infer.register(list)
def infer_list(values: List[Any_]) -> Array:
    """Infer the :class:`~ibis.expr.datatypes.Array` type of `values`."""
    if not values:
        return Array(null)
    return Array(highest_precedence(map(infer, values)))


@infer.register((set, frozenset))
def infer_set(values: Set_) -> Set:
    """Infer the :class:`~ibis.expr.datatypes.Set` type of `values`."""
    if not values:
        return Set(null)
    return Set(highest_precedence(map(infer, values)))


@infer.register(datetime.time)
def infer_time(value: datetime.time) -> Time:
    return time


@infer.register(datetime.date)
def infer_date(value: datetime.date) -> Date:
    return date


@infer.register(datetime.datetime)
def infer_timestamp(value: datetime.datetime) -> Timestamp:
    if value.tzinfo:
        return Timestamp(timezone=str(value.tzinfo))
    else:
        return timestamp


@infer.register(datetime.timedelta)
def infer_interval(value: datetime.timedelta) -> Interval:
    return interval


@infer.register(str)
def infer_string(value: str) -> String:
    return string


@infer.register(builtins.float)
def infer_floating(value: builtins.float) -> Double:
    return double


@infer.register(int)
def infer_integer(value: int, allow_overflow: bool = False) -> Integer:
    for dtype in (int8, int16, int32, int64):
        if dtype.bounds.lower <= value <= dtype.bounds.upper:
            return dtype

    if not allow_overflow:
        raise OverflowError(value)

    return int64


@infer.register(bool)
def infer_boolean(value: bool) -> Boolean:
    return boolean


@infer.register((type(None), Null))
def infer_null(value: Optional[Null]) -> Null:
    return null


castable = Dispatcher('castable')


@castable.register(DataType, DataType)
def can_cast_subtype(source: DataType, target: DataType, **kwargs) -> bool:
    return isinstance(target, type(source))


@castable.register(Any, DataType)
@castable.register(DataType, Any)
@castable.register(Any, Any)
@castable.register(Null, Any)
@castable.register(Integer, Category)
@castable.register(Integer, (Floating, Decimal))
@castable.register(Floating, Decimal)
@castable.register((Date, Timestamp), (Date, Timestamp))
def can_cast_any(source: DataType, target: DataType, **kwargs) -> bool:
    return True


@castable.register(Null, DataType)
def can_cast_null(source: DataType, target: DataType, **kwargs) -> bool:
    return target.nullable


Integral = TypeVar('Integral', SignedInteger, UnsignedInteger)


@castable.register(SignedInteger, UnsignedInteger)
@castable.register(UnsignedInteger, SignedInteger)
def can_cast_to_differently_signed_integer_type(
    source: Integral, target: Integral, value: Optional[int] = None, **kwargs
) -> bool:
    if value is None:
        return False
    bounds = target.bounds
    return bounds.lower <= value <= bounds.upper


@castable.register(SignedInteger, SignedInteger)
@castable.register(UnsignedInteger, UnsignedInteger)
def can_cast_integers(source: Integral, target: Integral, **kwargs) -> bool:
    return target._nbytes >= source._nbytes


@castable.register(Floating, Floating)
def can_cast_floats(
    source: Floating, target: Floating, upcast: bool = False, **kwargs
) -> bool:
    if upcast:
        return target._nbytes >= source._nbytes

    # double -> float must be allowed because
    # float literals are inferred as doubles
    return True


@castable.register(Decimal, Decimal)
def can_cast_decimals(source: Decimal, target: Decimal, **kwargs) -> bool:
    return (
        target.precision >= source.precision and target.scale >= source.scale
    )


@castable.register(Interval, Interval)
def can_cast_intervals(source: Interval, target: Interval, **kwargs) -> bool:
    return source.unit == target.unit and castable(
        source.value_type, target.value_type
    )


@castable.register(Integer, Boolean)
def can_cast_integer_to_boolean(
    source: Integer, target: Boolean, value: Optional[int] = None, **kwargs
) -> bool:
    return value is not None and (value == 0 or value == 1)


@castable.register(Integer, Interval)
def can_cast_integer_to_interval(
    source: Interval, target: Interval, **kwargs
) -> bool:
    return castable(source, target.value_type)


@castable.register(String, (Date, Time, Timestamp))
def can_cast_string_to_temporal(
    source: String,
    target: Union[Date, Time, Timestamp],
    value: Optional[str] = None,
    **kwargs
) -> bool:
    if value is None:
        return False
    try:
        pd.Timestamp(value)
    except ValueError:
        return False
    else:
        return True


Collection = TypeVar('Collection', Array, Set)


@castable.register(Array, Array)
@castable.register(Set, Set)
def can_cast_variadic(
    source: Collection, target: Collection, **kwargs
) -> bool:
    return castable(source.value_type, target.value_type)


# @castable.register(Map, Map)
# def can_cast_maps(source, target):
#     return (source.equals(target) or
#             source.equals(Map(null, null)) or
#             source.equals(Map(any, any)))
# TODO cast category


def cast(
    source: Union[DataType, str], target: Union[DataType, str], **kwargs
) -> DataType:
    """Attempts to implicitly cast from source dtype to target dtype"""
    source, result_target = dtype(source), dtype(target)

    if not castable(source, result_target, **kwargs):
        raise com.IbisTypeError(
            'Datatype {} cannot be implicitly '
            'casted to {}'.format(source, result_target)
        )
    return result_target


same_kind = Dispatcher(
    'same_kind',
    doc="""\
Compute whether two :class:`~ibis.expr.datatypes.DataType` instances are the
same kind.

Parameters
----------
a : DataType
b : DataType

Returns
-------
bool
    Whether two :class:`~ibis.expr.datatypes.DataType` instances are the same
    kind.
""",
)


@same_kind.register(DataType, DataType)
def same_kind_default(a: DataType, b: DataType) -> bool:
    """Return whether `a` is exactly equiavlent to `b`"""
    return a.equals(b)


Numeric = TypeVar('Numeric', Integer, Floating)


@same_kind.register(Integer, Integer)
@same_kind.register(Floating, Floating)
def same_kind_numeric(a: Numeric, b: Numeric) -> bool:
    """Return ``True``."""
    return True


@same_kind.register(DataType, Null)
def same_kind_right_null(a: DataType, _: Null) -> bool:
    """Return whether `a` is nullable."""
    return a.nullable


@same_kind.register(Null, DataType)
def same_kind_left_null(_: Null, b: DataType) -> bool:
    """Return whether `b` is nullable."""
    return b.nullable


@same_kind.register(Null, Null)
def same_kind_both_null(a: Null, b: Null) -> bool:
    """Return ``True``."""
    return True
