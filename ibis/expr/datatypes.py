# Copyright 2014 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import six
import toolz
import datetime
import itertools
import functools
import numpy as np
import pandas as pd

from collections import namedtuple, OrderedDict
from multipledispatch import Dispatcher

import ibis.common as com
from ibis.compat import builtins, PY2, DatetimeTZDtype, CategoricalDtype


class DataType(object):

    __slots__ = 'nullable',

    def __init__(self, nullable=True):
        self.nullable = nullable

    def __call__(self, nullable=True):
        return self._factory(nullable=nullable)

    def _factory(self, nullable=True):
        return type(self)(nullable=nullable)

    def __eq__(self, other):
        return self.equals(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        custom_parts = tuple(
            getattr(self, slot)
            for slot in toolz.unique(self.__slots__ + ('nullable',))
        )
        return hash((type(self),) + custom_parts)

    def __repr__(self):
        return '{}({})'.format(
            self.name,
            ', '.join(
                '{}={!r}'.format(slot, getattr(self, slot))
                for slot in toolz.unique(self.__slots__ + ('nullable',))
            )
        )

    if PY2:
        def __getstate__(self):
            return {
                slot: getattr(self, slot)
                for slot in toolz.unique(self.__slots__ + ('nullable',))
            }

        def __setstate__(self, instance_dict):
            for key, value in instance_dict.items():
                setattr(self, key, value)

    def __str__(self):
        return self.name.lower()

    @property
    def name(self):
        return type(self).__name__

    def equals(self, other, cache=None):
        if isinstance(other, six.string_types):
            other = dtype(other)

        return (
            isinstance(other, type(self)) and
            self.nullable == other.nullable and
            self._equal_part(other, cache=cache)
        )

    def _equal_part(self, other, cache=None):
        return True

    def issubtype(self, parent):
        return issubtype(self, parent)

    def castable(self, target):
        return castable(self, target)

    def cast(self, target):
        return cast(self, target)

    def scalar_type(self):
        import ibis.expr.types as ir
        return getattr(ir, '{}Scalar'.format(self.name))

    def array_type(self):
        import ibis.expr.types as ir
        return getattr(ir, '{}Column'.format(self.name))


class Any(DataType):

    __slots__ = ()


class Primitive(DataType):

    __slots__ = ()

    def __repr__(self):
        name = self.name.lower()
        if not self.nullable:
            return '{}[non-nullable]'.format(name)
        return name


class Null(DataType):

    __slots__ = ()


class Variadic(DataType):

    __slots__ = ()


class Boolean(Primitive):

    __slots__ = ()


Bounds = namedtuple('Bounds', ('lower', 'upper'))


class Integer(Primitive):

    __slots__ = ()

    @property
    def bounds(self):
        exp = self._nbytes * 8 - 1
        lower = -1 << exp
        return Bounds(lower=lower, upper=~lower)


class String(Variadic):
    """A type representing a string.

    Notes
    -----
    Because of differences in the way different backends handle strings, we
    cannot assume that strings are UTF-8 encoded.
    """

    __slots__ = ()


class Binary(Variadic):
    """A type representing a blob of bytes.

    Notes
    -----
    Some databases treat strings and blobs of equally, and some do not. For
    example, Impala doesn't make a distinction between string and binary types
    but PostgreSQL has a TEXT type and a BYTEA type which are distinct types
    that behave differently.
    """


class Date(Primitive):

    __slots__ = ()


class Time(Primitive):

    __slots__ = ()


def parametric(cls):
    type_name = cls.__name__
    array_type_name = '{}Column'.format(type_name)
    scalar_type_name = '{}Scalar'.format(type_name)

    def array_type(self):
        def constructor(op, name=None):
            import ibis.expr.types as ir
            return getattr(ir, array_type_name)(op, self, name=name)
        return constructor

    def scalar_type(self):
        def constructor(op, name=None):
            import ibis.expr.types as ir
            return getattr(ir, scalar_type_name)(op, self, name=name)
        return constructor

    cls.array_type = array_type
    cls.scalar_type = scalar_type
    return cls


@parametric
class Timestamp(Primitive):

    __slots__ = 'timezone',

    def __init__(self, timezone=None, nullable=True):
        super(Timestamp, self).__init__(nullable=nullable)
        self.timezone = timezone

    def _equal_part(self, other, cache=None):
        return self.timezone == other.timezone

    def __call__(self, timezone=None, nullable=True):
        return type(self)(timezone=timezone, nullable=nullable)

    def __str__(self):
        timezone = self.timezone
        typename = self.name.lower()
        if timezone is None:
            return typename
        return '{}({!r})'.format(typename, timezone)

    def __repr__(self):
        return DataType.__repr__(self)


class SignedInteger(Integer):
    pass


class UnsignedInteger(Integer):

    @property
    def bounds(self):
        exp = self._nbytes * 8 - 1
        upper = 1 << exp
        return Bounds(lower=0, upper=upper)


class Floating(Primitive):

    __slots__ = ()


class Int8(SignedInteger):

    __slots__ = ()

    _nbytes = 1


class Int16(SignedInteger):

    __slots__ = ()

    _nbytes = 2


class Int32(SignedInteger):

    __slots__ = ()

    _nbytes = 4


class Int64(SignedInteger):

    __slots__ = ()

    _nbytes = 8


class UInt8(UnsignedInteger):

    _nbytes = 1


class UInt16(UnsignedInteger):

    _nbytes = 2


class UInt32(UnsignedInteger):

    _nbytes = 4


class UInt64(UnsignedInteger):

    _nbytes = 8


class Halffloat(Floating):

    _nbytes = 2


class Float(Floating):

    __slots__ = ()

    _nbytes = 4


class Double(Floating):

    __slots__ = ()

    _nbytes = 8


@parametric
class Decimal(DataType):

    __slots__ = 'precision', 'scale'

    def __init__(self, precision, scale, nullable=True):
        super(Decimal, self).__init__(nullable=nullable)
        self.precision = precision
        self.scale = scale

    def __str__(self):
        return '{}({:d}, {:d})'.format(
            self.name.lower(),
            self.precision,
            self.scale,
        )

    def _equal_part(self, other, cache=None):
        return self.precision == other.precision and self.scale == other.scale


assert hasattr(Decimal, '__hash__')


@parametric
class Interval(DataType):

    __slots__ = 'value_type', 'unit'

    _units = dict(
        Y='year',
        Q='quarter',
        M='month',
        w='week',
        d='day',
        h='hour',
        m='minute',
        s='second',
        ms='millisecond',
        us='microsecond',
        ns='nanosecond'
    )

    def __init__(self, unit='s', value_type=None, nullable=True):
        super(Interval, self).__init__(nullable=nullable)
        if unit not in self._units:
            raise ValueError('Unsupported interval unit `{}`'.format(unit))

        if value_type is None:
            value_type = int32
        else:
            value_type = dtype(value_type)

        if not isinstance(value_type, Integer):
            raise TypeError("Interval's inner type must be an Integer subtype")

        self.unit = unit
        self.value_type = value_type

    @property
    def resolution(self):
        """Unit's name"""
        return self._units[self.unit]

    def __str__(self):
        unit = self.unit
        typename = self.name.lower()
        value_type_name = self.value_type.name.lower()
        return '{}<{}>(unit={!r})'.format(typename, value_type_name, unit)

    def _equal_part(self, other, cache=None):
        return (self.unit == other.unit and
                self.value_type.equals(other.value_type, cache=cache))


@parametric
class Category(DataType):

    __slots__ = 'cardinality',

    def __init__(self, cardinality=None, nullable=True):
        super(Category, self).__init__(nullable=nullable)
        self.cardinality = cardinality

    def __repr__(self):
        if self.cardinality is not None:
            cardinality = self.cardinality
        else:
            cardinality = 'unknown'
        return '{}(cardinality={!r})'.format(self.name, cardinality)

    def _equal_part(self, other, cache=None):
        return (
            self.cardinality == other.cardinality and
            self.nullable == other.nullable
        )

    def to_integer_type(self):
        # TODO: this should be removed I guess
        if self.cardinality is None:
            return int64
        else:
            return infer(self.cardinality)


@parametric
class Struct(DataType):

    __slots__ = 'pairs',

    def __init__(self, names, types, nullable=True):
        """Construct a ``Struct`` type from a `names` and `types`.

        Parameters
        ----------
        names : Sequence[str]
            Sequence of strings indicating the name of each field in the
            struct.
        types : Sequence[Union[str, DataType]]
            Sequence of strings or :class:`~ibis.expr.datatypes.DataType`
            instances, one for each field
        nullable : bool, optional
            Whether the struct can be null
        """
        if len(names) != len(types):
            raise ValueError('names and types must have the same length')

        super(Struct, self).__init__(nullable=nullable)
        self.pairs = OrderedDict(zip(names, types))

    @classmethod
    def from_tuples(self, pairs):
        return Struct(*map(list, zip(*pairs)))

    @property
    def names(self):
        return self.pairs.keys()

    @property
    def types(self):
        return self.pairs.values()

    def __getitem__(self, key):
        return self.pairs[key]

    def __hash__(self):
        return hash((
            type(self), tuple(self.names), tuple(self.types), self.nullable
        ))

    def __repr__(self):
        return '{}({}, nullable={})'.format(
            self.name, list(self.pairs.items()), self.nullable
        )

    def __str__(self):
        return '{}<{}>'.format(
            self.name.lower(),
            ', '.join(itertools.starmap('{}: {}'.format, self.pairs.items()))
        )

    def _equal_part(self, other, cache=None):
        return self.names == other.names and (
            left.equals(right, cache=cache)
            for left, right in zip(self.types, other.types)
        )


@parametric
class Array(Variadic):

    __slots__ = 'value_type',

    def __init__(self, value_type, nullable=True):
        super(Array, self).__init__(nullable=nullable)
        self.value_type = dtype(value_type)

    def __str__(self):
        return '{}<{}>'.format(self.name.lower(), self.value_type)

    def _equal_part(self, other, cache=None):
        return self.value_type.equals(other.value_type, cache=cache)


@parametric
class Enum(DataType):

    __slots__ = 'rep_type', 'value_type'

    def __init__(self, rep_type, value_type, nullable=True):
        super(Enum, self).__init__(nullable=nullable)
        self.rep_type = dtype(rep_type)
        self.value_type = dtype(value_type)

    def _equal_part(self, other, cache=None):
        return (
            self.rep_type.equals(other.rep_type, cache=cache) and
            self.value_type.equals(other.value_type, cache=cache)
        )


@parametric
class Map(Variadic):

    __slots__ = 'key_type', 'value_type'

    def __init__(self, key_type, value_type, nullable=True):
        super(Map, self).__init__(nullable=nullable)
        self.key_type = dtype(key_type)
        self.value_type = dtype(value_type)

    def __str__(self):
        return '{}<{}, {}>'.format(
            self.name.lower(),
            self.key_type,
            self.value_type,
        )

    def _equal_part(self, other, cache=None):
        return (
            self.key_type.equals(other.key_type, cache=cache) and
            self.value_type.equals(other.value_type, cache=cache)
        )


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
float32 = Float()
float64 = Double()
double = Double()
string = String()
binary = Binary()
date = Date()
time = Time()
timestamp = Timestamp()
interval = Interval()


_primitive_types = (
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
    ('interval', interval)
)


def array_type(t):
    # compatibility
    return dtype(t).array_type()


def scalar_type(t):
    # compatibility
    return dtype(t).scalar_type()


_numpy_to_ibis = toolz.keymap(np.dtype, {
    'bool': boolean,
    'int8': int8,
    'int16': int16,
    'int32': int32,
    'int64': int64,
    'uint8': uint8,
    'uint16': uint16,
    'uint32': uint32,
    'uint64': uint64,
    'float16': float16,
    'float32': float32,
    'float64': float64,
    'double': double,
    'str': string,
    'datetime64': timestamp,
    'datetime64[ns]': timestamp,
    'timedelta64': interval,
    'timedelta64[ns]': Interval('ns')
})


dtype = Dispatcher('dtype')

validate_type = dtype


@dtype.register(object)
def default(value):
    raise TypeError('Value {!r} is not a valid type or string'.format(value))


@dtype.register(DataType)
def from_ibis_dtype(value):
    return value


@dtype.register(np.dtype)
def from_numpy_dtype(value):
    return _numpy_to_ibis[value]


@dtype.register(DatetimeTZDtype)
def from_pandas_tzdtype(value):
    return Timestamp(timezone=str(value.tz))


@dtype.register(CategoricalDtype)
def from_pandas_categorical(value):
    return Category()


@dtype.register(six.string_types)
def from_string(value):
    from ibis.expr.parser import parser
    from ibis.expr.lexer import lexer
    return parser.parse(lexer.tokenize(value))


infer = Dispatcher('infer')


def higher_precedence(left, right):
    if castable(left, right, upcast=True):
        return right
    elif castable(right, left, upcast=True):
        return left

    raise com.IbisTypeError('Cannot compute precedence for {} '
                            'and {} types'.format(left, right))


def highest_precedence(dtypes):
    return functools.reduce(higher_precedence, dtypes)


@infer.register(object)
def infer_dtype_default(value):
    raise com.InputTypeError(value)


@infer.register(OrderedDict)
def infer_struct(value):
    if not value:
        raise TypeError('Empty struct type not supported')
    return Struct(
        list(value.keys()),
        list(map(infer, value.values()))
    )


@infer.register(dict)
def infer_map(value):
    if not value:
        return Map(null, null)
    return Map(
        highest_precedence(map(infer, value.keys())),
        highest_precedence(map(infer, value.values())),
    )


@infer.register(list)
def infer_list(value):
    if not value:
        return Array(null)
    return Array(highest_precedence(map(infer, value)))


@infer.register(np.ndarray)
def infer_array(value):
    # TODO: infer series
    return Array(dtype(value.dtype.name))


@infer.register(datetime.time)
def infer_time(value):
    return time


@infer.register(datetime.date)
def infer_date(value):
    return date


@infer.register(datetime.datetime)
def infer_timestamp(value):
    return timestamp


@infer.register(datetime.timedelta)
def infer_interval(value):
    return interval


@infer.register(six.string_types)
def infer_string(value):
    return string


@infer.register(builtins.float)
def infer_floating(value):
    return double


@infer.register(six.integer_types)
def infer_integer(value, allow_overflow=False):
    for dtype in (int8, int16, int32, int64):
        if dtype.bounds.lower <= value <= dtype.bounds.upper:
            return dtype

    if not allow_overflow:
        raise OverflowError(value)

    return int64


@infer.register(np.generic)
def infer_numpy_scalar(value):
    return dtype(value.dtype)


@infer.register(pd.Timestamp)
def infer_pandas_timestamp(value):
    return Timestamp(timezone=str(value.tz))


@infer.register(bool)
def infer_boolean(value):
    return boolean


@infer.register((type(None), Null))
def infer_null(value):
    return null


castable = Dispatcher('castable')


@castable.register(DataType, DataType)
def can_cast_subtype(source, target, **kwargs):
    return isinstance(target, type(source))


@castable.register(Any, DataType)
@castable.register(Integer, Category)
@castable.register(Integer, (Floating, Decimal))
@castable.register(Floating, Decimal)
@castable.register(Decimal, Floating)
@castable.register((Date, Timestamp), (Date, Timestamp))
def can_cast_any(source, target, **kwargs):
    return True


@castable.register(Null, DataType)
def can_cast_null(source, target, **kwargs):
    return target.nullable


@castable.register(Integer, Integer)
def can_cast_integers(source, target, **kwargs):
    return target._nbytes >= source._nbytes


@castable.register(Floating, Floating)
def can_cast_floats(source, target, upcast=False, **kwargs):
    if upcast:
        return target._nbytes >= source._nbytes

    # double -> float must be allowed because
    # float literals are inferred as doubles
    return True


@castable.register(Interval, Interval)
def can_cast_intervals(source, target, **kwargs):
    return (
        source.unit == target.unit and
        castable(source.value_type, target.value_type)
    )


@castable.register(Integer, Boolean)
def can_cast_integer_to_boolean(source, target, value=None, **kwargs):
    return value == 0 or value == 1


@castable.register(Integer, Interval)
def can_cast_integer_to_interval(source, target, **kwargs):
    return castable(source, target.value_type)


@castable.register(String, (Date, Time, Timestamp))
def can_cast_string_to_temporal(source, target, value=None, **kwargs):
    if value is None:
        return False
    try:
        pd.Timestamp(value)
        return True
    except ValueError:
        return False


@castable.register(Array, Array)
def can_cast_arrays(source, target, **kwargs):
    return castable(source.value_type, target.value_type)


# @castable.register(Map, Map)
# def can_cast_maps(source, target):
#     return (source.equals(target) or
#             source.equals(Map(null, null)) or
#             source.equals(Map(any, any)))
# TODO cast category


def cast(source, target, **kwargs):
    """Attempts to implicitly cast from source dtype to target dtype"""
    source, target = dtype(source), dtype(target)

    if not castable(source, target, **kwargs):
        raise com.IbisTypeError('Datatype {} cannot be implicitly '
                                'casted to {}'.format(source, target))
    return target


def issubtype(dtype, dtype_or_tuple):
    if not isinstance(dtype_or_tuple, tuple):
        parents = (dtype_or_tuple,)
    for parent in parents:
        if isinstance(dtype, type(parent)):
            return True
        elif isinstance(dtype, Any):
            return True

    return False
