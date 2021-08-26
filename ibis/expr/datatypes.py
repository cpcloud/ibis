import abc
import builtins
import collections
import datetime
import enum
import functools
import itertools
import numbers
import re
import typing
from typing import (
    Callable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import pandas as pd
import toolz
from multipledispatch import Dispatcher
from org.apache.arrow.computeir.flatbuf import (
    ArrayLiteral,
    BinaryLiteral,
    BooleanLiteral,
    DateLiteral,
    DecimalLiteral,
    Float16Literal,
    Float32Literal,
    Float64Literal,
    Int8Literal,
    Int16Literal,
    Int32Literal,
    Int64Literal,
    IntervalLiteral,
    IntervalLiteralDaysMilliseconds,
    IntervalLiteralImpl,
    IntervalLiteralMonths,
    LiteralImpl,
    MapLiteral,
    NullLiteral,
    StringLiteral,
    StructLiteral,
    TimeLiteral,
    TimestampLiteral,
    UInt8Literal,
    UInt16Literal,
    UInt32Literal,
    UInt64Literal,
)
from org.apache.arrow.flatbuf import Array as ArrayType
from org.apache.arrow.flatbuf import Binary as BinaryType
from org.apache.arrow.flatbuf import Bool as BooleanType
from org.apache.arrow.flatbuf import Date as DateType
from org.apache.arrow.flatbuf import DateUnit
from org.apache.arrow.flatbuf import Decimal as DecimalType
from org.apache.arrow.flatbuf import Field
from org.apache.arrow.flatbuf import FloatingPoint as FloatingPointType
from org.apache.arrow.flatbuf import Int as IntType
from org.apache.arrow.flatbuf import Interval as IntervalType
from org.apache.arrow.flatbuf import List as ListType
from org.apache.arrow.flatbuf import Map as MapType
from org.apache.arrow.flatbuf import Null as NullType
from org.apache.arrow.flatbuf import Precision
from org.apache.arrow.flatbuf import Struct_ as StructType
from org.apache.arrow.flatbuf import Time as TimeType
from org.apache.arrow.flatbuf import Timestamp as TimestampType
from org.apache.arrow.flatbuf import TimeUnit, Type
from org.apache.arrow.flatbuf import Utf8 as Utf8Type
from org.apache.arrow.flatbuf.IntervalUnit import IntervalUnit

import ibis.common.exceptions as com
import ibis.expr.types as ir
from ibis import util

IS_SHAPELY_AVAILABLE = False
try:
    import shapely.geometry

    IS_SHAPELY_AVAILABLE = True
except ImportError:
    ...


class DataType(abc.ABC):

    __slots__ = ('nullable',)

    def __init__(self, nullable: bool = True, **kwargs) -> None:
        self.nullable = nullable

    def __call__(self, nullable: bool = True) -> 'DataType':
        if nullable is not True and nullable is not False:
            raise TypeError(
                "__call__ only accepts the 'nullable' argument. "
                "Please construct a new instance of the type to change the "
                "values of the attributes."
            )
        return self._factory(nullable=nullable)

    def _factory(self, nullable: bool = True) -> 'DataType':
        slots = {
            slot: getattr(self, slot)
            for slot in self.__slots__
            if slot != 'nullable'
        }
        return type(self)(nullable=nullable, **slots)

    def __eq__(self, other) -> bool:
        return self.equals(other)

    def __ne__(self, other) -> bool:
        return not (self == other)

    def __hash__(self) -> int:
        custom_parts = tuple(
            getattr(self, slot)
            for slot in toolz.unique(self.__slots__ + ('nullable',))
        )
        return hash((type(self),) + custom_parts)

    def __repr__(self) -> str:
        return '{}({})'.format(
            self.name,
            ', '.join(
                '{}={!r}'.format(slot, getattr(self, slot))
                for slot in toolz.unique(self.__slots__ + ('nullable',))
            ),
        )

    def __str__(self) -> str:
        return '{}{}'.format(
            self.name.lower(), '[non-nullable]' if not self.nullable else ''
        )

    @property
    def name(self) -> str:
        return type(self).__name__

    def equals(
        self,
        other: 'DataType',
        cache: Optional[Mapping[typing.Any, bool]] = None,
    ) -> bool:
        if isinstance(other, str):
            raise TypeError(
                'Comparing datatypes to strings is not allowed. Convert '
                '{!r} to the equivalent DataType instance.'.format(other)
            )
        return (
            isinstance(other, type(self))
            and self.nullable == other.nullable
            and self.__slots__ == other.__slots__
            and all(
                getattr(self, slot) == getattr(other, slot)
                for slot in self.__slots__
            )
        )

    def castable(self, target, **kwargs):
        return castable(self, target, **kwargs)

    def cast(self, target, **kwargs):
        return cast(self, target, **kwargs)

    def scalar_type(self):
        return functools.partial(self.scalar, dtype=self)

    def column_type(self):
        return functools.partial(self.column, dtype=self)

    def _literal_value_hash_key(self, value):
        """Return a hash for `value`."""
        return self, value

    def ir_literal_value(self, builder, value):
        return value

    def make_ir_literal(self, builder, value):
        ir_literal_type = self.ir_literal_type
        typename = flatbuffers_type_name_from_module(ir_literal_type)
        ir_value = self.ir_literal_value(builder, value)
        getattr(ir_literal_type, f"{typename}Start")(builder)
        getattr(ir_literal_type, f"{typename}AddValue")(builder, ir_value)
        return getattr(ir_literal_type, f"{typename}End")(builder), getattr(
            LiteralImpl.LiteralImpl, f"{typename}"
        )

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        ir_type = self.ir_type
        typename = flatbuffers_type_name_from_module(ir_type)
        getattr(ir_type, f"{typename}Start")(builder)
        ty = getattr(ir_type, f"{typename}End")(builder)
        ty_ty = getattr(Type.Type, typename)
        name_string = make_string(builder, name)
        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddType(builder, ty)
        Field.FieldAddTypeType(builder, ty_ty)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)


def flatbuffers_type_name_from_module(module) -> str:
    last_dot = module.__name__.rfind(".")
    return module.__name__[last_dot + 1 :]


class Any(DataType):
    __slots__ = ()


class Primitive(DataType):
    __slots__ = ()

    def __repr__(self) -> str:
        name = self.name.lower()
        if not self.nullable:
            return '{}[non-nullable]'.format(name)
        return name


class Null(DataType):
    scalar = ir.NullScalar
    column = ir.NullColumn

    ir_type = NullType
    ir_literal_type = NullLiteral

    __slots__ = ()


class Variadic(DataType):
    __slots__ = ()


class Boolean(Primitive):
    scalar = ir.BooleanScalar
    column = ir.BooleanColumn

    ir_type = BooleanType
    ir_literal_type = BooleanLiteral

    __slots__ = ()


Bounds = NamedTuple('Bounds', [('lower', int), ('upper', int)])


class Integer(Primitive):
    scalar = ir.IntegerScalar
    column = ir.IntegerColumn

    ir_type = IntType

    __slots__ = ()

    @property
    @abc.abstractmethod
    def _nbytes(self) -> int:
        ...

    @property
    def _bit_width(self) -> int:
        return self._nbytes * 8

    @property
    @abc.abstractmethod
    def is_signed(self) -> bool:
        pass

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        self.ir_type.IntStart(builder)
        self.ir_type.IntAddBitWidth(builder, self._bit_width)
        self.ir_type.IntAddIsSigned(builder, self.is_signed)
        int_ty = self.ir_type.IntEnd(builder)

        name_string = make_string(builder, name)
        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddNullable(builder, self.nullable)
        Field.FieldAddType(builder, int_ty)
        Field.FieldAddTypeType(builder, Type.Type.Int)
        return Field.FieldEnd(builder)


def make_string(builder, name: Optional[str]) -> Optional[GenericAny]:
    return builder.CreateString(name) if name is not None else None


class String(Variadic):
    """A type representing a string.

    Notes
    -----
    Because of differences in the way different backends handle strings, we
    cannot assume that strings are UTF-8 encoded.
    """

    scalar = ir.StringScalar
    column = ir.StringColumn

    ir_type = Utf8Type
    ir_literal_type = StringLiteral

    __slots__ = ()

    def ir_literal_value(self, builder, value):
        return builder.CreateString(value)


class Binary(Variadic):
    """A type representing a blob of bytes.

    Notes
    -----
    Some databases treat strings and blobs of equally, and some do not. For
    example, Impala doesn't make a distinction between string and binary types
    but PostgreSQL has a TEXT type and a BYTEA type which are distinct types
    that behave differently.
    """

    scalar = ir.BinaryScalar
    column = ir.BinaryColumn

    ir_type = BinaryType
    ir_literal_type = BinaryLiteral

    __slots__ = ()

    def ir_literal_value(self, builder, value):
        return builder.CreateByteVector(value)


class Date(Primitive):
    scalar = ir.DateScalar
    column = ir.DateColumn

    ir_type = DateType
    ir_literal_type = DateLiteral

    __slots__ = ()

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        name_string = make_string(builder, name)

        self.ir_type.DateStart(builder)
        self.ir_type.DateAddUnit(builder, DateUnit.DateUnit.MILLISECOND)
        date_ty = self.ir_type.DateEnd(builder)

        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddType(builder, date_ty)
        Field.FieldAddTypeType(builder, Type.Type.Date)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)

    def make_ir_literal(self, builder, value) -> Tuple[int, int]:
        self.ir_literal_type.DateLiteralStart(builder)
        self.ir_literal_type.DateLiteralAddValue(
            # milliseconds since epoch
            builder,
            pd.Timestamp(value, unit="D").value // 1_000_000,
        )
        return (
            self.ir_literal_type.DateLiteralEnd(builder),
            LiteralImpl.LiteralImpl.DateLiteral,
        )


class Time(Primitive):
    scalar = ir.TimeScalar
    column = ir.TimeColumn

    ir_type = TimeType
    ir_literal_type = TimeLiteral

    __slots__ = ()


class Timestamp(DataType):
    scalar = ir.TimestampScalar
    column = ir.TimestampColumn

    ir_type = TimestampType
    ir_literal_type = TimestampLiteral

    __slots__ = ('timezone',)

    def __init__(
        self, timezone: Optional[str] = None, nullable: bool = True
    ) -> None:
        super().__init__(nullable=nullable)
        self.timezone = timezone

    def __str__(self) -> str:
        timezone = self.timezone
        typename = self.name.lower()
        if timezone is None:
            return typename
        return '{}({!r})'.format(typename, timezone)

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        timezone = make_string(builder, self.timezone)
        name_string = make_string(builder, name)

        self.ir_type.TimestampStart(builder)
        self.ir_type.TimestampAddUnit(builder, TimeUnit.TimeUnit.MICROSECOND)
        self.ir_type.TimestampAddTimezone(builder, timezone)
        timestamp_ty = self.ir_type.TimestampEnd(builder)

        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddType(builder, timestamp_ty)
        Field.FieldAddTypeType(builder, Type.Type.Timestamp)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)

    def make_ir_literal(self, builder, value):
        timezone = builder.CreateString(self.timezone)
        self.ir_literal_type.TimestampLiteralStart(builder)
        self.ir_literal_type.TimestampLiteralAddValue(builder, int(value))
        self.ir_literal_type.TimestampLiteralAddTimezone(builder, timezone)
        return (
            self.ir_literal_type.TimestampLiteralEnd(builder),
            LiteralImpl.LiteralImpl.TimestampLiteral,
        )


class SignedInteger(Integer):
    @property
    def largest(self):
        return int64

    @property
    def bounds(self):
        exp = self._bit_width - 1
        upper = (1 << exp) - 1
        return Bounds(lower=~upper, upper=upper)

    @property
    def is_signed(self) -> bool:
        return True


class UnsignedInteger(Integer):
    @property
    def largest(self):
        return uint64

    @property
    def bounds(self):
        exp = self._bit_width - 1
        upper = 1 << exp
        return Bounds(lower=0, upper=upper)

    @property
    def is_signed(self) -> bool:
        return False


class Floating(Primitive):
    scalar = ir.FloatingScalar
    column = ir.FloatingColumn

    __slots__ = ()

    @property
    def largest(self):
        return float64

    @property
    @abc.abstractmethod
    def _nbytes(self) -> int:
        ...

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        name_string = make_string(builder, name)
        nbytes_to_precision = {
            2: Precision.Precision.HALF,
            4: Precision.Precision.SINGLE,
            8: Precision.Precision.DOUBLE,
        }

        FloatingPointType.FloatingPointStart(builder)
        FloatingPointType.FloatingPointAddPrecision(
            builder, nbytes_to_precision[self._nbytes]
        )
        float_ty = FloatingPointType.FloatingPointEnd(builder)

        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddType(builder, float_ty)
        Field.FieldAddTypeType(builder, Type.Type.FloatingPoint)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)


class Int8(SignedInteger):
    __slots__ = ()
    _nbytes = 1

    ir_literal_type = Int8Literal


class Int16(SignedInteger):
    __slots__ = ()
    _nbytes = 2

    ir_literal_type = Int16Literal


class Int32(SignedInteger):
    __slots__ = ()
    _nbytes = 4

    ir_literal_type = Int32Literal


class Int64(SignedInteger):
    __slots__ = ()
    _nbytes = 8

    ir_literal_type = Int64Literal


class UInt8(UnsignedInteger):
    __slots__ = ()
    _nbytes = 1

    ir_literal_type = UInt8Literal


class UInt16(UnsignedInteger):
    __slots__ = ()
    _nbytes = 2

    ir_literal_type = UInt16Literal


class UInt32(UnsignedInteger):
    __slots__ = ()
    _nbytes = 4

    ir_literal_type = UInt32Literal


class UInt64(UnsignedInteger):
    __slots__ = ()
    _nbytes = 8

    ir_literal_type = UInt64Literal


class Float16(Floating):
    __slots__ = ()
    _nbytes = 2

    ir_literal_type = Float16Literal


class Float32(Floating):
    __slots__ = ()
    _nbytes = 4

    ir_literal_type = Float32Literal


class Float64(Floating):
    __slots__ = ()
    _nbytes = 8

    ir_literal_type = Float64Literal


Halffloat = Float16
Float = Float32
Double = Float64


class Decimal(DataType):
    scalar = ir.DecimalScalar
    column = ir.DecimalColumn

    ir_type = DecimalType
    ir_literal_type = DecimalLiteral

    __slots__ = 'precision', 'scale'

    ir_type = DecimalType

    def __init__(
        self, precision: int, scale: int, nullable: bool = True
    ) -> None:
        if not isinstance(precision, numbers.Integral):
            raise TypeError('Decimal type precision must be an integer')
        if not isinstance(scale, numbers.Integral):
            raise TypeError('Decimal type scale must be an integer')
        if precision < 0:
            raise ValueError('Decimal type precision cannot be negative')
        if not precision:
            raise ValueError('Decimal type precision cannot be zero')
        if scale < 0:
            raise ValueError('Decimal type scale cannot be negative')
        if precision < scale:
            raise ValueError(
                'Decimal type precision must be greater than or equal to '
                'scale. Got precision={:d} and scale={:d}'.format(
                    precision, scale
                )
            )

        super().__init__(nullable=nullable)
        self.precision = precision  # type: int
        self.scale = scale  # type: int

    def __str__(self) -> str:
        return '{}({:d}, {:d})'.format(
            self.name.lower(), self.precision, self.scale
        )

    @property
    def largest(self) -> 'Decimal':
        return Decimal(38, self.scale)

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        self.ir_type.DecimalStart(builder)
        self.ir_type.DecimalAddPrecision(builder, self.precision)
        self.ir_type.DecimalAddScale(builder, self.scale)
        dec_ty = self.ir_type.DecimalEnd(builder)
        name_string = make_string(builder, name)
        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddType(builder, dec_ty)
        Field.FieldAddTypeType(builder, Type.Type.Decimal)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)


class Interval(DataType):
    scalar = ir.IntervalScalar
    column = ir.IntervalColumn

    ir_literal_type = IntervalLiteral
    ir_type = IntervalType

    __slots__ = 'value_type', 'unit'

    # based on numpy's units
    _units = {
        'Y': 'year',
        'Q': 'quarter',
        'M': 'month',
        'W': 'week',
        'D': 'day',
        'h': 'hour',
        'm': 'minute',
        's': 'second',
        'ms': 'millisecond',
        'us': 'microsecond',
        'ns': 'nanosecond',
    }

    _timedelta_to_interval_units = {
        'days': 'D',
        'hours': 'h',
        'minutes': 'm',
        'seconds': 's',
        'milliseconds': 'ms',
        'microseconds': 'us',
        'nanoseconds': 'ns',
    }

    _ir_units = {
        "Y": IntervalUnit.YEAR_MONTH,
        "Q": IntervalUnit.YEAR_MONTH,
        "M": IntervalUnit.YEAR_MONTH,
        "D": IntervalUnit.DAY_TIME,
        "h": IntervalUnit.DAY_TIME,
        "m": IntervalUnit.DAY_TIME,
        "s": IntervalUnit.DAY_TIME,
        "ms": IntervalUnit.DAY_TIME,
    }

    def _convert_timedelta_unit_to_interval_unit(self, unit: str):
        if unit not in self._timedelta_to_interval_units:
            raise ValueError
        return self._timedelta_to_interval_units[unit]

    def __init__(
        self,
        unit: str = 's',
        value_type: Integer = None,
        nullable: bool = True,
    ) -> None:
        super().__init__(nullable=nullable)
        if unit not in self._units:
            try:
                unit = self._convert_timedelta_unit_to_interval_unit(unit)
            except ValueError:
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
    def bounds(self):
        return self.value_type.bounds

    @property
    def resolution(self):
        """Unit's name"""
        return self._units[self.unit]

    def __str__(self):
        unit = self.unit
        typename = self.name.lower()
        value_type_name = self.value_type.name.lower()
        return '{}<{}>(unit={!r})'.format(typename, value_type_name, unit)

    @property
    def ir_unit(self):
        try:
            return self._ir_units[self.unit]
        except KeyError as e:
            raise KeyError(
                "Arrow compute IR does not support interval unit: "
                f"`{self.unit!r}`"
            ) from e

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        unit = self.ir_unit
        self.ir_type.IntervalStart(builder)
        self.ir_type.IntervalAddUnit(builder, unit)
        interval_ty = self.ir_type.IntervalEnd(builder)
        name_string = make_string(builder, name)
        Field.FieldStart(builder)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddType(builder, interval_ty)
        Field.FieldAddTypeType(builder, Type.Type.Interval)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)

    def make_ir_literal(self, builder, value) -> Tuple[int, int]:
        unit = self.ir_unit
        if unit == IntervalUnit.YEAR_MONTH:
            IntervalLiteralMonths.IntervalLiteralMonthsStart(builder)
            IntervalLiteralMonths.IntervalLiteralMonthsAddMonths(
                builder, value
            )
            value = IntervalLiteralMonths.IntervalLiteralMonthsEnd(builder)
            ty = IntervalLiteralImpl.IntervalLiteralImpl.IntervalLiteralMonths
        else:
            assert (
                unit == IntervalUnit.DAY_TIME
            ), f"got unexpected unit: `{unit}`"
            IntervalLiteralDaysMilliseconds.IntervalLiteralDaysMillisecondsStart(  # noqa: E501
                builder
            )
            IntervalLiteralDaysMilliseconds.IntervalLiteralDaysMillisecondsAddDays(  # noqa: E501
                builder, value
            )
            IntervalLiteralDaysMilliseconds.IntervalLiteralDaysMillisecondsAddMilliseconds(  # noqa: E501
                builder, value
            )
            value = IntervalLiteralDaysMilliseconds.IntervalLiteralDaysMillisecondsEnd(  # noqa: E501
                builder
            )
            ty = (
                IntervalLiteralImpl.IntervalLiteralImpl.IntervalLiteralDaysMilliseconds  # noqa: E501
            )
        self.ir_literal_type.IntervalLiteralStart(builder)
        self.ir_literal_type.IntervalLiteralAddValue(builder, value)
        self.ir_literal_type.IntervalLiteralAddValueType(builder, ty)

        return (
            self.ir_literal_type.IntervalLiteralEnd(builder),
            LiteralImpl.LiteralImpl.DateLiteral,
        )


class Category(DataType):
    scalar = ir.CategoryScalar
    column = ir.CategoryColumn

    __slots__ = ('cardinality',)

    def __init__(self, cardinality=None, nullable=True):
        super().__init__(nullable=nullable)
        self.cardinality = cardinality

    def __repr__(self):
        if self.cardinality is not None:
            cardinality = self.cardinality
        else:
            cardinality = 'unknown'
        return '{}(cardinality={!r})'.format(self.name, cardinality)

    def to_integer_type(self):
        # TODO: this should be removed I guess
        if self.cardinality is None:
            return int64
        else:
            return infer(self.cardinality)


class Struct(DataType):
    scalar = ir.StructScalar
    column = ir.StructColumn

    ir_type = StructType
    ir_literal_type = StructLiteral

    __slots__ = 'names', 'types'

    def __init__(
        self, names: List[str], types: List[DataType], nullable: bool = True
    ) -> None:
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
        if not (names and types):
            raise ValueError('names and types must not be empty')
        if len(names) != len(types):
            raise ValueError('names and types must have the same length')

        super().__init__(nullable=nullable)
        self.names = names
        self.types = types

    @classmethod
    def from_tuples(
        cls,
        pairs: Sequence[Tuple[str, Union[str, DataType]]],
        nullable: bool = True,
    ) -> 'Struct':
        names, types = zip(*pairs)
        return cls(list(names), list(map(dtype, types)), nullable=nullable)

    @classmethod
    def from_dict(
        cls, pairs: Mapping[str, Union[str, DataType]], nullable: bool = True,
    ) -> 'Struct':
        names, types = pairs.keys(), pairs.values()
        return cls(list(names), list(map(dtype, types)), nullable=nullable)

    @property
    def pairs(self) -> Mapping:
        return collections.OrderedDict(zip(self.names, self.types))

    def __getitem__(self, key: str) -> DataType:
        return self.pairs[key]

    def __hash__(self) -> int:
        return hash(
            (type(self), tuple(self.names), tuple(self.types), self.nullable)
        )

    def __repr__(self) -> str:
        return '{}({}, nullable={})'.format(
            self.name, list(self.pairs.items()), self.nullable
        )

    def __str__(self) -> str:
        return '{}<{}>'.format(
            self.name.lower(),
            ', '.join(itertools.starmap('{}: {}'.format, self.pairs.items())),
        )

    def _literal_value_hash_key(self, value):
        return self, _tuplize(value.items())

    def compile_ir(self, builder, name: Optional[str] = None):
        children = [
            typ.compile_ir(builder, name=name) for name, typ in self.pairs
        ]
        Field.FieldStartChildrenVector(builder, len(children))
        for child in reversed(children):
            builder.PrependUOffsetTRelative(child)
        children_offsets = builder.EndVector()

        StructType.StructStart(builder)
        struct_ty = StructType.StructEnd(builder)

        if name is not None:
            name_string = builder.CreateString(name)
        else:
            name_string = None

        Field.FieldStart(builder)
        Field.FieldAddType(builder, struct_ty)

        if name_string is not None:
            Field.FieldAddName(builder, name_string)

        Field.FieldAddTypeType(builder, Type.Type.Struct)
        Field.FieldAddChildren(builder, children_offsets)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)


def _tuplize(values):
    """Recursively convert `values` to a tuple of tuples."""

    def tuplize_iter(values):
        yield from (
            tuple(tuplize_iter(value)) if util.is_iterable(value) else value
            for value in values
        )

    return tuple(tuplize_iter(values))


class Array(Variadic):
    scalar = ir.ArrayScalar
    column = ir.ArrayColumn

    ir_type = ArrayType
    ir_literal_type = ArrayLiteral

    __slots__ = ('value_type',)

    def __init__(
        self, value_type: Union[str, DataType], nullable: bool = True
    ) -> None:
        super().__init__(nullable=nullable)
        self.value_type = dtype(value_type)

    def __str__(self) -> str:
        return '{}<{}>'.format(self.name.lower(), self.value_type)

    def _literal_value_hash_key(self, value):
        return self, _tuplize(value)

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        field_ty = self.value_type.compile_ir(builder)

        Field.FieldStartChildrenVector(builder, 1)
        builder.PrependUOffsetTRelative(field_ty)
        field_children_offsets = builder.EndVector()

        ListType.ListStart(builder)
        list_ty = ListType.ListEnd(builder)

        name_string = make_string(builder, name)

        Field.FieldStart(builder)
        Field.FieldAddType(builder, list_ty)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddTypeType(builder, Type.Type.List)
        Field.FieldAddChildren(builder, field_children_offsets)
        Field.FieldAddNullable(builder, self.nullable)
        return Field.FieldEnd(builder)


class Set(Variadic):
    scalar = ir.SetScalar
    column = ir.SetColumn

    __slots__ = ('value_type',)

    def __init__(
        self, value_type: Union[str, DataType], nullable: bool = True
    ) -> None:
        super().__init__(nullable=nullable)
        self.value_type = dtype(value_type)

    def __str__(self) -> str:
        return '{}<{}>'.format(self.name.lower(), self.value_type)


class Enum(DataType):
    scalar = ir.EnumScalar
    column = ir.EnumColumn

    __slots__ = 'rep_type', 'value_type'

    def __init__(
        self, rep_type: DataType, value_type: DataType, nullable: bool = True
    ) -> None:
        super().__init__(nullable=nullable)
        self.rep_type = dtype(rep_type)
        self.value_type = dtype(value_type)


class Map(Variadic):
    scalar = ir.MapScalar
    column = ir.MapColumn

    ir_type = MapType
    ir_literal_type = MapLiteral

    __slots__ = 'key_type', 'value_type'

    def __init__(
        self, key_type: DataType, value_type: DataType, nullable: bool = True
    ) -> None:
        super().__init__(nullable=nullable)
        self.key_type = dtype(key_type)
        self.value_type = dtype(value_type)

    def __str__(self) -> str:
        return '{}<{}, {}>'.format(
            self.name.lower(), self.key_type, self.value_type
        )

    def _literal_value_hash_key(self, value):
        return self, _tuplize(value.items())

    def compile_ir(self, builder, name: Optional[str] = None) -> int:
        ty = Array(
            Struct.from_tuples(
                [("key", self.key_type), ("value", self.value_type)]
            )
        )
        field_ty = ty.compile_ir(builder)

        Field.FieldStartChildrenVector(builder, 1)
        builder.PrependUOffsetTRelative(field_ty)
        field_children_offsets = builder.EndVector()

        MapType.MapStart(builder)
        map_ty = MapType.MapEnd(builder)

        name_string = make_string(builder, name)

        Field.FieldStart(builder)
        Field.FieldAddType(builder, map_ty)
        Field.FieldAddTypeType(builder, Type.Type.Map)
        if name_string is not None:
            Field.FieldAddName(builder, name_string)
        Field.FieldAddChildren(builder, field_children_offsets)
        Field.FieldAddNullable(builder, False)  # entries cannot be nullable

        return Field.FieldEnd(builder)


class JSON(String):
    """JSON (JavaScript Object Notation) text format."""

    scalar = ir.JSONScalar
    column = ir.JSONColumn


class JSONB(Binary):
    """JSON (JavaScript Object Notation) data stored as a binary
    representation, which eliminates whitespace, duplicate keys,
    and key ordering.
    """

    scalar = ir.JSONBScalar
    column = ir.JSONBColumn


class GeoSpatial(DataType):
    __slots__ = 'geotype', 'srid'

    column = ir.GeoSpatialColumn
    scalar = ir.GeoSpatialScalar

    def __init__(
        self, geotype: str = None, srid: int = None, nullable: bool = True
    ):
        """Geospatial data type base class

        Parameters
        ----------
        geotype : str
            Specification of geospatial type which could be `geography` or
            `geometry`.
        srid : int
            Spatial Reference System Identifier
        nullable : bool, optional
            Whether the struct can be null
        """
        super().__init__(nullable=nullable)

        if geotype not in (None, 'geometry', 'geography'):
            raise ValueError(
                'The `geotype` parameter should be `geometry` or `geography`'
            )

        self.geotype = geotype
        self.srid = srid

    def __str__(self) -> str:
        geo_op = self.name.lower()
        if self.geotype is not None:
            geo_op += ':' + self.geotype
        if self.srid is not None:
            geo_op += ';' + str(self.srid)
        return geo_op

    def _literal_value_hash_key(self, value):
        if IS_SHAPELY_AVAILABLE:
            geo_shapes = (
                shapely.geometry.Point,
                shapely.geometry.LineString,
                shapely.geometry.Polygon,
                shapely.geometry.MultiLineString,
                shapely.geometry.MultiPoint,
                shapely.geometry.MultiPolygon,
            )
            if isinstance(value, geo_shapes):
                return self, value.wkt
        return self, value


class Geometry(GeoSpatial):
    """Geometry is used to cast from geography types."""

    column = ir.GeoSpatialColumn
    scalar = ir.GeoSpatialScalar

    __slots__ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.geotype = 'geometry'

    def __str__(self) -> str:
        return self.name.lower()


class Geography(GeoSpatial):
    """Geography is used to cast from geometry types."""

    column = ir.GeoSpatialColumn
    scalar = ir.GeoSpatialScalar

    __slots__ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.geotype = 'geography'

    def __str__(self) -> str:
        return self.name.lower()


class Point(GeoSpatial):
    """A point described by two coordinates."""

    scalar = ir.PointScalar
    column = ir.PointColumn

    __slots__ = ()


class LineString(GeoSpatial):
    """A sequence of 2 or more points."""

    scalar = ir.LineStringScalar
    column = ir.LineStringColumn

    __slots__ = ()


class Polygon(GeoSpatial):
    """A set of one or more rings (closed line strings), with the first
    representing the shape (external ring) and the rest representing holes in
    that shape (internal rings).
    """

    scalar = ir.PolygonScalar
    column = ir.PolygonColumn

    __slots__ = ()


class MultiLineString(GeoSpatial):
    """A set of one or more line strings."""

    scalar = ir.MultiLineStringScalar
    column = ir.MultiLineStringColumn

    __slots__ = ()


class MultiPoint(GeoSpatial):
    """A set of one or more points."""

    scalar = ir.MultiPointScalar
    column = ir.MultiPointColumn

    __slots__ = ()


class MultiPolygon(GeoSpatial):
    """A set of one or more polygons."""

    scalar = ir.MultiPolygonScalar
    column = ir.MultiPolygonColumn

    __slots__ = ()


class UUID(String):
    """A universally unique identifier (UUID) is a 128-bit number used to
    identify information in computer systems.
    """

    scalar = ir.UUIDScalar
    column = ir.UUIDColumn

    __slots__ = ()


class MACADDR(String):
    """Media Access Control (MAC) Address of a network interface."""

    scalar = ir.MACADDRScalar
    column = ir.MACADDRColumn

    __slots__ = ()


class INET(String):
    """IP address type."""

    scalar = ir.INETScalar
    column = ir.INETColumn

    __slots__ = ()


# ---------------------------------------------------------------------
any = Any()
null = Null()
boolean = Boolean()
int8 = Int8()
int16 = Int16()
int32 = Int32()
int64 = Int64()
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
# geo spatial data type
geometry = GeoSpatial()
geography = GeoSpatial()
point = Point()
linestring = LineString()
polygon = Polygon()
multilinestring = MultiLineString()
multipoint = MultiPoint()
multipolygon = MultiPolygon()
# json
json = JSON()
jsonb = JSONB()
# special string based data type
uuid = UUID()
macaddr = MACADDR()
inet = INET()

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
    ('category', category),
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
    GEOGRAPHY = 22
    GEOMETRY = 23
    POINT = 24
    LINESTRING = 25
    POLYGON = 26
    MULTILINESTRING = 27
    MULTIPOINT = 28
    MULTIPOLYGON = 29
    SEMICOLON = 30
    JSON = 31
    JSONB = 32
    UUID = 33
    MACADDR = 34
    INET = 35

    @staticmethod
    def name(value):
        return _token_names[value]


_token_names = {
    getattr(Tokens, n): n for n in dir(Tokens) if n.isalpha() and n.isupper()
}

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
        # geo spatial data type
        (
            '(?P<{}>{})'.format(token.upper(), token),
            lambda token, toktype=toktype: Token(toktype, token),
        )
        for token, toktype in zip(
            (
                'geometry',
                'geography',
                'point',
                'linestring',
                'polygon',
                'multilinestring',
                'multipoint',
                'multipolygon',
            ),
            (
                Tokens.GEOMETRY,
                Tokens.GEOGRAPHY,
                Tokens.POINT,
                Tokens.LINESTRING,
                Tokens.POLYGON,
                Tokens.MULTILINESTRING,
                Tokens.MULTIPOINT,
                Tokens.MULTIPOLYGON,
            ),
        )
    ]
    + [
        # json data type
        (
            '(?P<{}>{})'.format(token.upper(), token),
            lambda token, toktype=toktype: Token(toktype, token),
        )
        for token, toktype in zip(
            # note: `jsonb` should be first to avoid conflict with `json`
            ('jsonb', 'json'),
            (Tokens.JSONB, Tokens.JSON),
        )
    ]
    + [
        # special string based data types
        ('(?P<UUID>uuid)', lambda token: Token(Tokens.UUID, token)),
        ('(?P<MACADDR>macaddr)', lambda token: Token(Tokens.MACADDR, token)),
        ('(?P<INET>inet)', lambda token: Token(Tokens.INET, token)),
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
        ('(?P<SEMICOLON>;)', lambda token: Token(Tokens.SEMICOLON, token)),
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


def _generate_tokens(pat: re.Pattern, text: str) -> Iterator[Token]:
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
    m: re.Match
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

        geography: "geography"

        geometry: "geometry"

        point : "point"
              | "point" ";" srid
              | "point" ":" geotype
              | "point" ";" srid ":" geotype

        linestring : "linestring"
                   | "linestring" ";" srid
                   | "linestring" ":" geotype
                   | "linestring" ";" srid ":" geotype

        polygon : "polygon"
                | "polygon" ";" srid
                | "polygon" ":" geotype
                | "polygon" ";" srid ":" geotype

        multilinestring : "multilinestring"
                   | "multilinestring" ";" srid
                   | "multilinestring" ":" geotype
                   | "multilinestring" ";" srid ":" geotype

        multipoint : "multipoint"
                   | "multipoint" ";" srid
                   | "multipoint" ":" geotype
                   | "multipoint" ";" srid ":" geotype

        multipolygon : "multipolygon"
                     | "multipolygon" ";" srid
                     | "multipolygon" ":" geotype
                     | "multipolygon" ";" srid ":" geotype

        json : "json"

        jsonb : "jsonb"

        uuid : "uuid"

        macaddr : "macaddr"

        inet : "inet"

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

        # json data types
        elif self._accept(Tokens.JSON):
            return JSON()

        elif self._accept(Tokens.JSONB):
            return JSONB()

        # geo spatial data type
        elif self._accept(Tokens.GEOMETRY):
            return Geometry()

        elif self._accept(Tokens.GEOGRAPHY):
            return Geography()

        elif self._accept(Tokens.POINT):
            geotype = None
            srid = None

            if self._accept(Tokens.SEMICOLON):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                srid = self.tok.value

            if self._accept(Tokens.COLON):
                if self._accept(Tokens.GEOGRAPHY):
                    geotype = 'geography'
                elif self._accept(Tokens.GEOMETRY):
                    geotype = 'geometry'

            return Point(geotype=geotype, srid=srid)

        elif self._accept(Tokens.LINESTRING):
            geotype = None
            srid = None

            if self._accept(Tokens.SEMICOLON):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                srid = self.tok.value

            if self._accept(Tokens.COLON):
                if self._accept(Tokens.GEOGRAPHY):
                    geotype = 'geography'
                elif self._accept(Tokens.GEOMETRY):
                    geotype = 'geometry'

            return LineString(geotype=geotype, srid=srid)

        elif self._accept(Tokens.POLYGON):
            geotype = None
            srid = None

            if self._accept(Tokens.SEMICOLON):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                srid = self.tok.value

            if self._accept(Tokens.COLON):
                if self._accept(Tokens.GEOGRAPHY):
                    geotype = 'geography'
                elif self._accept(Tokens.GEOMETRY):
                    geotype = 'geometry'

            return Polygon(geotype=geotype, srid=srid)

        elif self._accept(Tokens.MULTILINESTRING):
            geotype = None
            srid = None

            if self._accept(Tokens.SEMICOLON):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                srid = self.tok.value

            if self._accept(Tokens.COLON):
                if self._accept(Tokens.GEOGRAPHY):
                    geotype = 'geography'
                elif self._accept(Tokens.GEOMETRY):
                    geotype = 'geometry'

            return MultiLineString(geotype=geotype, srid=srid)

        elif self._accept(Tokens.MULTIPOINT):
            geotype = None
            srid = None

            if self._accept(Tokens.SEMICOLON):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                srid = self.tok.value

            if self._accept(Tokens.COLON):
                if self._accept(Tokens.GEOGRAPHY):
                    geotype = 'geography'
                elif self._accept(Tokens.GEOMETRY):
                    geotype = 'geometry'

            return MultiPoint(geotype=geotype, srid=srid)

        elif self._accept(Tokens.MULTIPOLYGON):
            geotype = None
            srid = None

            if self._accept(Tokens.SEMICOLON):
                self._expect(Tokens.INTEGER)
                assert self.tok is not None
                srid = self.tok.value

            if self._accept(Tokens.COLON):
                if self._accept(Tokens.GEOGRAPHY):
                    geotype = 'geography'
                elif self._accept(Tokens.GEOMETRY):
                    geotype = 'geometry'

            return MultiPolygon(geotype=geotype, srid=srid)

        # special string based data types
        elif self._accept(Tokens.UUID):
            return UUID()

        elif self._accept(Tokens.MACADDR):
            return MACADDR()

        elif self._accept(Tokens.INET):
            return INET()

        else:
            raise SyntaxError('Type cannot be parsed: {}'.format(self.text))


dtype = Dispatcher('dtype')

validate_type = dtype


def _get_timedelta_units(
    timedelta: datetime.timedelta | pd.Timedelta,
) -> List[str]:
    # pandas Timedelta has more granularity
    if isinstance(timedelta, pd.Timedelta):
        unit_fields = timedelta.components._fields
        base_object = timedelta.components
    # datetime.timedelta only stores days, seconds, and microseconds internally
    else:
        unit_fields = ['days', 'seconds', 'microseconds']
        base_object = timedelta

    time_units = [
        field for field in unit_fields if getattr(base_object, field) > 0
    ]
    return time_units


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
def from_list(values: List[typing.Any]) -> Array:
    if not values:
        return Array(null)
    return Array(highest_precedence(map(dtype, values)))


@dtype.register(collections.abc.Set)
def from_set(values: typing.Set) -> Set:
    if not values:
        return Set(null)
    return Set(highest_precedence(map(dtype, values)))


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
def infer_dtype_default(value: typing.Any) -> DataType:
    """Default implementation of :func:`~ibis.expr.datatypes.infer`."""
    raise com.InputTypeError(value)


@infer.register(collections.OrderedDict)
def infer_struct(value: Mapping[str, typing.Any]) -> Struct:
    """Infer the :class:`~ibis.expr.datatypes.Struct` type of `value`."""
    if not value:
        raise TypeError('Empty struct type not supported')
    return Struct(list(value.keys()), list(map(infer, value.values())))


@infer.register(collections.abc.Mapping)
def infer_map(value: Mapping[typing.Any, typing.Any]) -> Map:
    """Infer the :class:`~ibis.expr.datatypes.Map` type of `value`."""
    if not value:
        return Map(null, null)
    return Map(
        highest_precedence(map(infer, value.keys())),
        highest_precedence(map(infer, value.values())),
    )


@infer.register(list)
def infer_list(values: List[typing.Any]) -> Array:
    """Infer the :class:`~ibis.expr.datatypes.Array` type of `values`."""
    if not values:
        return Array(null)
    return Array(highest_precedence(map(infer, values)))


@infer.register((set, frozenset))
def infer_set(values: typing.Set) -> Set:
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
    time_units = _get_timedelta_units(value)
    # we can attempt a conversion in the simplest case, i.e. there is exactly
    # one unit (e.g. pd.Timedelta('2 days') vs. pd.Timedelta('2 days 3 hours')
    if len(time_units) == 1:
        unit = time_units[0]
        return Interval(unit)
    else:
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


@infer.register(enum.Enum)
def infer_enum(value: enum.Enum) -> Enum:
    return Enum(infer(value.name), infer(value.value),)


@infer.register(bool)
def infer_boolean(value: bool) -> Boolean:
    return boolean


@infer.register((type(None), Null))
def infer_null(value: Optional[Null]) -> Null:
    return null


if IS_SHAPELY_AVAILABLE:

    @infer.register(shapely.geometry.Point)
    def infer_shapely_point(value: shapely.geometry.Point) -> Point:
        return point

    @infer.register(shapely.geometry.LineString)
    def infer_shapely_linestring(
        value: shapely.geometry.LineString,
    ) -> LineString:
        return linestring

    @infer.register(shapely.geometry.Polygon)
    def infer_shapely_polygon(value: shapely.geometry.Polygon) -> Polygon:
        return polygon

    @infer.register(shapely.geometry.MultiLineString)
    def infer_shapely_multilinestring(
        value: shapely.geometry.MultiLineString,
    ) -> MultiLineString:
        return multilinestring

    @infer.register(shapely.geometry.MultiPoint)
    def infer_shapely_multipoint(
        value: shapely.geometry.MultiPoint,
    ) -> MultiPoint:
        return multipoint

    @infer.register(shapely.geometry.MultiPolygon)
    def infer_shapely_multipolygon(
        value: shapely.geometry.MultiPolygon,
    ) -> MultiPolygon:
        return multipolygon


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
    **kwargs,
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


@castable.register(JSON, JSON)
def can_cast_json(source, target, **kwargs):
    return True


@castable.register(JSONB, JSONB)
def can_cast_jsonb(source, target, **kwargs):
    return True


# geo spatial data type
# cast between same type, used to cast from/to geometry and geography
GEO_TYPES = (
    Point,
    LineString,
    Polygon,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
)


@castable.register(Array, GEO_TYPES)
@castable.register(GEO_TYPES, Geometry)
@castable.register(GEO_TYPES, Geography)
def can_cast_geospatial(source, target, **kwargs):
    return True


@castable.register(UUID, UUID)
@castable.register(MACADDR, MACADDR)
@castable.register(INET, INET)
def can_cast_special_string(source, target, **kwargs):
    return True


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
