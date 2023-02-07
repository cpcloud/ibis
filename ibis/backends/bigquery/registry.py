"""Module to convert from Ibis expression to SQL string."""

from __future__ import annotations

import base64
import operator
from functools import partial
from typing import TYPE_CHECKING

import numpy as np
import sqlalchemy as sa
import sqlalchemy_bigquery as sab
from multipledispatch import Dispatcher
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import GenericFunction

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.backends.base.sql.alchemy.registry import _literal as literal
from ibis.backends.base.sql.alchemy.registry import (
    fixed_arity,
    reduction,
    sqlalchemy_operation_registry,
    sqlalchemy_window_functions_registry,
    unary,
)
from ibis.backends.bigquery.datatypes import ibis_type_to_bigquery_type

if TYPE_CHECKING:
    from ibis.backends.bigquery.compiler import BigQueryExprTranslator


def _extract_field(sql_attr):
    def extract_field_formatter(translator, op):
        arg = translator.translate(op.arg)
        if sql_attr == "epochseconds":
            return sa.func.unix_seconds(arg)
        else:
            return sa.extract(sql_attr, arg)

    return extract_field_formatter


bigquery_cast = Dispatcher("bigquery_cast")


@bigquery_cast.register(dt.Timestamp, dt.Integer)
def bigquery_cast_timestamp_to_integer(from_, to):
    """Convert TIMESTAMP to INT64 (seconds since Unix epoch)."""
    return sa.func.unix_micros


@bigquery_cast.register(dt.Integer, dt.Timestamp)
def bigquery_cast_integer_to_timestamp(from_, to):
    """Convert INT64 (seconds since Unix epoch) to Timestamp."""
    return sa.func.timestamp_seconds


@bigquery_cast.register(str, dt.Interval, dt.Integer)
def bigquery_cast_interval_to_integer(from_, to):
    return partial(sa.extract, from_.resolution.upper())


@bigquery_cast.register(dt.DataType, dt.DataType)
def bigquery_cast_generate(from_, to):
    """Cast to desired type."""
    return lambda arg: sa.cast(arg, ibis_type_to_bigquery_type(to))


@bigquery_cast.register(dt.DataType)
def bigquery_cast_generate_simple(to):
    return bigquery_cast(to, to)


def _cast(translator, op):
    arg, target_type = op.args
    arg_formatted = translator.translate(arg)
    input_dtype = arg.output_dtype
    return bigquery_cast(input_dtype, target_type)(arg_formatted)


def integer_to_timestamp(translator: BigQueryExprTranslator, op) -> str:
    """Interprets an integer as a timestamp."""
    arg = translator.translate(op.arg)
    unit = op.unit

    if unit == "s":
        return sa.func.timestamp_seconds(arg)
    elif unit == "ms":
        return sa.func.timestamp_millis(arg)
    elif unit == "us":
        return sa.func.timestamp_micros(arg)
    elif unit == "ns":
        # Timestamps are represented internally as elapsed microseconds, so some
        # rounding is required if an integer represents nanoseconds.
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp_type
        return sa.func.timestamp_micros(sa.cast(sa.func.round(arg / 1000), sa.BIGINT))

    raise NotImplementedError(f"cannot cast unit {unit}")


class _struct_column(GenericFunction):
    def __init__(self, fields, *, type: sab.STRUCT) -> None:
        super().__init__()
        self.fields = fields
        self.type = type


@compiles(_struct_column, "bigquery")
def compiles_struct_column(element, compiler, **kw):
    args = ", ".join(
        f"{compiler.process(value, **kw)} AS {name}"
        for name, value in element.fields.items()
    )
    return f"STRUCT({args})"


class _array_column(GenericFunction):
    def __init__(self, elements, *, type) -> None:
        self.elements = elements
        self.type = type


@compiles(_array_column, "bigquery")
def compiles_array_column(element, compiler, **kw):
    args = ", ".join(compiler.process(value, **kw) for value in element.elements)
    return f"[{args}]"


def _hash(translator, op):
    arg, how = op.args

    arg_formatted = translator.translate(arg)

    if how == "farm_fingerprint":
        return sa.func.farm_fingerprint(arg_formatted)
    else:
        raise NotImplementedError(how)


def _string_find(translator, op):
    haystack, needle, start, end = op.args

    if start is not None:
        raise NotImplementedError("start not implemented for string find")
    if end is not None:
        raise NotImplementedError("end not implemented for string find")

    return (
        sa.func.strpos(translator.translate(haystack), translator.translate(needle)) - 1
    )


def _translate_pattern(translator, op):
    # add 'r' to string literals to indicate to BigQuery this is a raw string
    return "r" * isinstance(op, ops.Literal) + translator.translate(op)


def _regex_search(translator, op):
    arg = translator.translate(op.arg)
    regex = _translate_pattern(translator, op.pattern)
    return sa.func.regexp_contains(arg, regex)


def _regex_extract(translator, op):
    arg = translator.translate(op.arg)
    regex = _translate_pattern(translator, op.pattern)
    index = translator.translate(op.index)
    matches = sa.func.regexp_contains(arg, regex)
    extract = sa.func.regexp_extract_all(arg, regex)[sa.func.safe_ordinal(index)]
    if_ = getattr(sa.func, "if")
    return if_(matches, if_(sa.func.coalesce(index, 0) == 0, arg, extract), sa.null())


def _regex_replace(translator, op):
    arg = translator.translate(op.arg)
    regex = _translate_pattern(translator, op.pattern)
    replacement = translator.translate(op.replacement)
    return sa.func.regexp_replace(arg, regex, replacement)


def _string_concat(translator, op):
    return sa.func.concat(*map(translator.translate, op.arg))


def _string_join(translator, op):
    sep, args = op.args
    return "ARRAY_TO_STRING([{}], {})".format(
        ", ".join(map(translator.translate, args)), translator.translate(sep)
    )


def _string_ascii(translator, op):
    arg = translator.translate(op.arg)
    return sa.func.to_code_points(arg)[1]


def _string_right(translator, op):
    arg, nchars = map(translator.translate, op.args)
    return sa.func.substr(arg, -sa.func.least(sa.func.length(arg), nchars))


def _string_substring(translator, op):
    _, _, length = op.args
    if (length := getattr(length, "value", None)) is not None and length < 0:
        raise ValueError("Length parameter must be a non-negative value.")

    base_substring = sqlalchemy_operation_registry[ops.Substring]
    return base_substring(translator, op)


def _array_literal_format(op):
    return sa.literal_column(str(list(op.value)))


def _log(translator, op):
    arg, base = op.args
    arg_formatted = translator.translate(arg)

    if base is None:
        return sa.func.ln(arg_formatted)

    base_formatted = translator.translate(base)
    return sa.func.log(arg_formatted, base_formatted)


def _literal(translator, op):
    value = op.value
    dtype = op.output_dtype
    if dtype.is_decimal():
        if value.is_nan():
            return sa.cast(sa.literal("NaN"), ibis_type_to_bigquery_type(dtype))
        if value.is_infinite():
            prefix = "-" * value.is_signed()
            return sa.cast(
                sa.literal(f"{prefix}inf"), ibis_type_to_bigquery_type(dtype)
            )
        else:
            return sa.cast(sa.literal(op.value), ibis_type_to_bigquery_type(dtype))
    elif dtype.is_uuid():
        return translator.translate(ops.Literal(str(op.value), dtype=dt.str))

    if isinstance(dtype, dt.Numeric):
        if not np.isfinite(value):
            return sa.cast(sa.literal(value), ibis_type_to_bigquery_type(dtype))

    # special case literal timestamp, date, and time scalars
    if isinstance(op, ops.Literal):
        if dtype.is_date():
            return sa.literal(value, type_=sa.DATE())
        elif dtype.is_timestamp():
            return sa.literal(
                value, type_=sa.TIMESTAMP(timezone=dtype.timezone is not None)
            )
        elif dtype.is_time():
            # TODO: define extractors on TimeValue expressions
            return sa.literal(value, type_=sa.TIME())
        elif dtype.is_binary():
            return sa.func.from_base64(base64.b64encode(value).decode(encoding="utf-8"))
        elif dtype.is_struct():
            return _struct_column(
                {
                    key: translator.translate(ops.Literal(val, dtype=dtype[key]))
                    for key, val in value.items()
                },
                type=translator.get_sqla_type(dtype),
            )

    try:
        return literal(translator, op)
    except NotImplementedError:
        if isinstance(dtype, dt.Array):
            return _array_literal_format(op)
        raise NotImplementedError(f'Unsupported type: {dtype!r}')


def _arbitrary(translator, op):
    arg, how, where = op.args

    if where is not None:
        arg = ops.Where(where, arg, ibis.NA)

    if how != "first":
        raise com.UnsupportedOperationError(
            f"{how!r} value not supported for arbitrary in BigQuery"
        )

    return sa.func.any_value(translator.translate(arg))


_date_units = {
    "Y": "YEAR",
    "Q": "QUARTER",
    "W": "WEEK(MONDAY)",
    "M": "MONTH",
    "D": "DAY",
}


_timestamp_units = {
    "us": "MICROSECOND",
    "ms": "MILLISECOND",
    "s": "SECOND",
    "m": "MINUTE",
    "h": "HOUR",
}
_timestamp_units.update(_date_units)


def _truncate(kind, units):
    def truncator(translator, op):
        arg, unit = op.args
        trans_arg = translator.translate(arg)
        valid_unit = units.get(unit)
        if valid_unit is None:
            raise com.UnsupportedOperationError(
                "BigQuery does not support truncating {} values to unit "
                "{!r}".format(arg.output_dtype, unit)
            )
        func = getattr(sa.func, f"{kind}_trunc")
        return func(trans_arg, valid_unit)

    return truncator


def _timestamp_op(func, units):
    def _formatter(translator, op):
        arg, offset = op.args

        unit = offset.output_dtype.unit
        if unit not in units:
            raise com.UnsupportedOperationError(
                "BigQuery does not allow binary operation "
                "{} with INTERVAL offset {}".format(func, unit)
            )
        return func(translator.translate(arg), translator.translate(offset))

    return _formatter


def _geo_boundingbox(dimension_name):
    def _formatter(translator, op):
        geog = op.arg
        geog_formatted = translator.translate(geog)
        return sa.func.st_boundingbox(geog_formatted).op(".")(
            sa.literal_column(dimension_name)
        )

    return _formatter


def _geo_simplify(translator, op):
    geog, tolerance, preserve_collapsed = op.args
    if preserve_collapsed.value:
        raise com.UnsupportedOperationError(
            "BigQuery simplify does not support preserving collapsed geometries, "
            "must pass preserve_collapsed=False"
        )
    geog, tolerance = map(translator.translate, (geog, tolerance))
    return sa.func.st_simplify(geog, tolerance)


def bigquery_day_of_week_index(t, op):
    """Convert timestamp to day-of-week integer."""
    arg = t.translate(op.arg)
    return sa.func.mod(sa.extract('DAYOFWEEK', arg) + 5, 7)


class _format_cast(GenericFunction):
    def __init__(self, arg, fmt: str) -> None:
        super().__init__()
        self.arg = arg
        self.fmt = fmt


@compiles(_format_cast, "bigquery")
def compiles_format_cast(element, compiler, **kw):
    arg = compiler.process(element.arg, **kw)
    return f"cast({arg} AS {element.fmt})"


def bigquery_day_of_week_name(t, op):
    """Convert timestamp to day-of-week name."""
    return sa.func.initcap(_format_cast(t.translate(op.arg), "STRING FORMAT 'DAY'"))


def bigquery_compiles_divide(t, op):
    """Floating point division."""
    return sa.func.ieee_divide(t.translate(op.left), t.translate(op.right))


def compiles_strftime(translator, op):
    """Timestamp formatting."""
    dtype = op.arg.output_dtype
    fmt_string = translator.translate(op.format_str)
    arg_formatted = translator.translate(op.arg)
    func = getattr(sa.func, f"format_{dtype.__class__.__name__.lower()}")
    args = [fmt_string, arg_formatted]
    if dtype.is_timestamp():
        args.append(tz if (tz := dtype.timezone) is not None else "UTC")
    return func(*args)


def compiles_string_to_timestamp(translator, op):
    """Timestamp parsing."""
    fmt_string = translator.translate(op.format_str)
    arg_formatted = translator.translate(op.arg)
    return sa.func.parse_timestamp(fmt_string, arg_formatted)


def compiles_floor(t, op):
    bigquery_type = ibis_type_to_bigquery_type(op.output_dtype)
    arg = op.arg
    return sa.cast(sa.func.floor(t.translate(arg)), bigquery_type)


def compiles_approx(translator, op):
    arg = op.arg
    where = op.where

    if where is not None:
        arg = ops.Where(where, arg, ibis.NA)

    return sa.func.approx_quantiles(
        translator.translate(arg), 2, type_=sa.ARRAY(sab.FLOAT64())
    )[1]


def compiles_covar_corr(func):
    def translate(translator, op):
        left = op.left
        right = op.right

        if (where := op.where) is not None:
            left = ops.Where(where, left, None)
            right = ops.Where(where, right, None)

        left = translator.translate(
            ops.Cast(left, dt.int64) if left.output_dtype.is_boolean() else left
        )
        right = translator.translate(
            ops.Cast(right, dt.int64) if right.output_dtype.is_boolean() else right
        )
        return func(left, right)

    return translate


def _covar(translator, op):
    how = op.how[:4].lower()
    assert how in ("pop", "samp"), 'how not in ("POP", "SAMP")'
    return compiles_covar_corr(getattr(sa.func, f"covar_{how}"))(translator, op)


def _corr(translator, op):
    if (how := op.how) == "sample":
        raise ValueError(f"Correlation with how={how!r} is not supported.")
    return compiles_covar_corr(sa.func.corr)(translator, op)


def _identical_to(t, op):
    left = t.translate(op.left)
    right = t.translate(op.right)
    return left.op("IS NOT DISTINCT FROM")(right)


def _floor_divide(t, op):
    left = t.translate(op.left)
    right = t.translate(op.right)
    return bigquery_cast(op.output_dtype)(
        sa.func.floor(sa.func.ieee_divide(left, right))
    )


def _log2(t, op):
    return sa.func.log(t.translate(op.arg), 2)


def _is_nan(t, op):
    return sa.func.is_nan(t.translate(op.arg))


def _is_inf(t, op):
    return sa.func.is_inf(t.translate(op.arg))


def _nullifzero(t, op):
    return sa.func.nullif(t.translate(op.arg), 0)


def _zeroifnull(t, op):
    return sa.func.coalesce(t.translate(op.arg), 0)


class _array_agg_func(GenericFunction):
    def __init__(
        self, arg, *, ignore_nulls=True, order_by=None, limit: int | None = None
    ):
        super().__init__(arg)
        self.ignore_nulls = ignore_nulls
        self.order_by = order_by
        self.limit = limit


@compiles(_array_agg_func, "bigquery")
def compiles_array_agg(element, compiler, **kw):
    arg = compiler.function_argspec(element)
    args = [arg]

    if element.ignore_nulls:
        args.append("IGNORE NULLS")

    if (order_by := element.order_by) is not None:
        args.append(f"ORDER BY {compiler.process(order_by, **kw)}")

    if (limit := element.limit) is not None:
        args.append(f"LIMIT {limit}")

    return f"ARRAY_AGG({' '.join(args)})"


def _array_agg(t, op):
    arg = op.arg
    if (where := op.where) is not None:
        arg = ops.Where(where, arg, ibis.NA)
    return _array_agg_func(t.translate(arg), ignore_nulls=True)


def _arg_min_max(sort_func):
    def translate(t, op: ops.ArgMin | ops.ArgMax) -> str:
        arg = op.arg
        if (where := op.where) is not None:
            arg = ops.Where(where, arg, None)
        arg = t.translate(arg)
        key = t.translate(op.key)
        return _array_agg_func(
            arg, ignore_nulls=True, order_by=sort_func(key), limit=1
        )[1]

    return translate


def _array_repeat(t, op):
    start = step = 1
    times = t.translate(op.times)
    arg = t.translate(op.arg)
    array_length = sa.func.array_length(arg)
    stop = sa.func.greatest(times, 0) * array_length
    series = sa.func.unnest(sa.func.generate_array(start, stop, step)).column_valued(
        "i"
    )
    idx = sa.func.coalesce(
        sa.func.nullif(sa.func.mod(series.c.i, array_length), 0), array_length
    )
    return sa.func.array(sa.select(arg[idx - 1]).scalar_subquery())


def _neg_idx_to_pos(array, idx):
    if_ = getattr(sa.func, "if")
    return if_(idx < 0, sa.func.array_length(array) + idx, idx)


class _unnest(GenericFunction):
    def __init__(self, arg, *, with_offset: str | None = None) -> None:
        super().__init__(arg)
        self.with_offset = with_offset

        columns = [sa.Column("el", arg.type)]
        if with_offset is not None:
            columns.append(sa.Column(with_offset, sa.BIGINT))

        self.type = sa.sql.sqltypes.TableValueType(*columns)


@compiles(_unnest, "bigquery")
def compiles_bigquery_unnest(element, compiler, **kw):
    arg = compiler.visit_table_valued_alias(element, **kw)

    args = [arg]
    if (with_offset := element.with_offset) is not None:
        args.append(f"WITH OFFSET {with_offset}")
    return " ".join(args)


def _array_slice(t, op):
    arg = t.translate(op.arg)
    elements = _unnest(arg, with_offset="index")
    index = elements.c.index
    cond = index >= _neg_idx_to_pos(arg, t.translate(op.start))
    if op.stop:
        cond &= index < _neg_idx_to_pos(arg, t.translate(op.stop))
    return sa.func.array(sa.select(elements.c.el).where(cond).scalar_subquery())


def _capitalize(t, op):
    arg = t.translate(op.arg)
    return sa.func.concat(
        sa.func.upper(sa.func.substr(arg, 1, 1), sa.func.substr(arg, 2))
    )


def _nth_value(t, op):
    arg = t.translate(op.arg)

    if not isinstance(nth_op := op.nth, ops.Literal):
        raise TypeError(f"Bigquery nth must be a literal; got {type(op.nth)}")

    return sa.func.nth_value(arg, nth_op.value + 1)


@compiles(sa.sql.selectable.TableValuedAlias, "bigquery")
def compiles_tv_alias(element, compiler, **kw):
    # HACK: workaround bigquery's workaround ¯\_(ツ)_/¯
    return sa.sql.compiler.SQLCompiler.visit_table_valued_alias(compiler, element, **kw)


def _hash_bytes(translator, op):
    if (how := op.how) not in ("md5", "sha1", "sha256", "sha512"):
        raise NotImplementedError(how)
    return getattr(sa.func, how)(translator.translate(op.arg))


def _interval_multiply(t, op):
    if isinstance(op.left, ops.Literal) and isinstance(op.right, ops.Literal):
        value = op.left.value * op.right.value
        literal = ops.Literal(value, op.left.output_dtype)
        return t.translate(literal)

    left, right = t.translate(op.left), t.translate(op.right)
    unit = op.left.output_dtype.resolution.upper()
    return sa.text(f"INTERVAL EXTRACT({unit} from {left}) * {right} {unit}")


OPERATION_REGISTRY = {
    **sqlalchemy_operation_registry,
    **sqlalchemy_window_functions_registry,
    # Literal
    ops.Literal: _literal,
    # Logical
    ops.Any: reduction(sa.func.logical_or),
    ops.All: reduction(sa.func.logical_and),
    ops.NotAny: reduction(lambda arg: sa.not_(sa.func.logical_or(arg))),
    ops.NotAll: reduction(lambda arg: sa.not_(sa.func.logical_and(arg))),
    ops.IfNull: fixed_arity(sa.func.ifnull, 2),
    ops.NullIf: fixed_arity(sa.func.nullif, 2),
    ops.NullIfZero: _nullifzero,
    ops.ZeroIfNull: _zeroifnull,
    # Reductions
    ops.ApproxMedian: compiles_approx,
    ops.Covariance: _covar,
    ops.Correlation: _corr,
    # Math
    ops.Divide: bigquery_compiles_divide,
    ops.Floor: compiles_floor,
    ops.Modulus: fixed_arity(sa.func.mod, 2),
    ops.Sign: unary(sa.func.sign),
    ops.Degrees: unary(lambda arg: 180 * arg / sa.func.acos(-1)),
    ops.Radians: unary(lambda arg: sa.func.acos(-1) * arg / 180),
    ops.BitwiseNot: unary(lambda arg: ~arg),
    ops.BitwiseXor: fixed_arity(operator.xor, 2),
    ops.BitwiseOr: fixed_arity(operator.or_, 2),
    ops.BitwiseAnd: fixed_arity(operator.and_, 2),
    ops.BitwiseLeftShift: fixed_arity(operator.lshift, 2),
    ops.BitwiseRightShift: fixed_arity(operator.rshift, 2),
    # Temporal functions
    ops.Date: unary(sa.func.date),
    ops.DateFromYMD: fixed_arity(sa.func.date, 3),
    ops.DateAdd: _timestamp_op(sa.func.date_add, {"D", "W", "M", "Q", "Y"}),
    ops.DateSub: _timestamp_op(sa.func.date_sub, {"D", "W", "M", "Q", "Y"}),
    ops.DateTruncate: _truncate("date", _date_units),
    ops.DayOfWeekIndex: bigquery_day_of_week_index,
    ops.DayOfWeekName: bigquery_day_of_week_name,
    ops.ExtractEpochSeconds: _extract_field("epochseconds"),
    ops.ExtractYear: _extract_field("year"),
    ops.ExtractQuarter: _extract_field("quarter"),
    ops.ExtractMonth: _extract_field("month"),
    ops.ExtractWeekOfYear: _extract_field("isoweek"),
    ops.ExtractDay: _extract_field("day"),
    ops.ExtractDayOfYear: _extract_field("dayofyear"),
    ops.ExtractHour: _extract_field("hour"),
    ops.ExtractMinute: _extract_field("minute"),
    ops.ExtractSecond: _extract_field("second"),
    ops.ExtractMillisecond: _extract_field("millisecond"),
    ops.Strftime: compiles_strftime,
    ops.StringToTimestamp: compiles_string_to_timestamp,
    ops.Time: unary(sa.func.time),
    ops.TimeFromHMS: fixed_arity(sa.func.time, 3),
    ops.TimeTruncate: _truncate("date", _timestamp_units),
    ops.TimestampAdd: _timestamp_op(sa.func.timestamp_add, {"h", "m", "s", "ms", "us"}),
    ops.TimestampFromUNIX: integer_to_timestamp,
    ops.TimestampFromYMDHMS: fixed_arity(sa.func.datetime, 6),
    ops.TimestampNow: fixed_arity(sa.func.current_timestamp, 0),
    ops.TimestampSub: _timestamp_op(sa.func.timestamp_sub, {"h", "m", "s", "ms", "us"}),
    ops.TimestampTruncate: _truncate("timestamp", _timestamp_units),
    ops.IntervalMultiply: _interval_multiply,
    ops.Hash: _hash,
    ops.HashBytes: _hash_bytes,
    ops.StringReplace: fixed_arity(sa.func.replace, 3),
    ops.StringSplit: fixed_arity(sa.func.split, 2),
    ops.StringConcat: _string_concat,
    ops.StringJoin: _string_join,
    ops.StringAscii: _string_ascii,
    ops.StringFind: _string_find,
    ops.Substring: _string_substring,
    ops.StrRight: _string_right,
    ops.Capitalize: _capitalize,
    ops.Translate: fixed_arity(sa.func.translate, 3),
    ops.Repeat: fixed_arity(sa.func.repeat, 2),
    ops.RegexSearch: _regex_search,
    ops.RegexExtract: _regex_extract,
    ops.RegexReplace: _regex_replace,
    ops.GroupConcat: reduction(sa.func.string_agg),
    ops.Cast: _cast,
    ops.StructField: lambda t, op: t.translate(op.arg).op(".")(op.field),
    ops.StructColumn: lambda t, op: _struct_column(
        dict(zip(op.names, op.values)), type=t.get_sqla_type(op.output_dtype)
    ),
    ops.ArrayCollect: _array_agg,
    ops.ArrayConcat: lambda t, op: sa.func.array_concat(*map(t.translate, op.args)),
    ops.ArrayColumn: lambda t, op: _array_column(
        list(map(t.translate, op.cols)), type=t.get_sqla_type(op.output_dtype)
    ),
    ops.ArrayIndex: fixed_arity(lambda arr, idx: arr[idx], 2),
    ops.ArrayLength: unary(sa.func.array_length),
    ops.ArrayRepeat: _array_repeat,
    ops.ArraySlice: _array_slice,
    ops.Log: _log,
    ops.Log2: _log2,
    ops.Arbitrary: _arbitrary,
    # Geospatial Columnar
    ops.GeoUnaryUnion: unary(sa.func.st_union_agg),
    # Geospatial
    ops.GeoArea: unary(sa.func.st_area),
    ops.GeoAsBinary: unary(sa.func.st_asbinary),
    ops.GeoAsText: unary(sa.func.st_astext),
    ops.GeoAzimuth: fixed_arity(sa.func.st_azimuth, 2),
    ops.GeoBuffer: fixed_arity(sa.func.st_buffer, 2),
    ops.GeoCentroid: unary(sa.func.st_centroid),
    ops.GeoContains: fixed_arity(sa.func.st_contains, 2),
    ops.GeoCovers: fixed_arity(sa.func.st_covers, 2),
    ops.GeoCoveredBy: fixed_arity(sa.func.st_coveredby, 2),
    ops.GeoDWithin: fixed_arity(sa.func.st_dwithin, 3),
    ops.GeoDifference: fixed_arity(sa.func.st_difference, 2),
    ops.GeoDisjoint: fixed_arity(sa.func.st_disjoint, 2),
    ops.GeoDistance: fixed_arity(sa.func.st_distance, 2),
    ops.GeoEndPoint: unary(sa.func.st_endpoint),
    ops.GeoEquals: fixed_arity(sa.func.st_equals, 2),
    ops.GeoGeometryType: unary(sa.func.st_geometrytype),
    ops.GeoIntersection: fixed_arity(sa.func.st_intersection, 2),
    ops.GeoIntersects: fixed_arity(sa.func.st_intersects, 2),
    ops.GeoLength: unary(sa.func.st_length),
    ops.GeoMaxDistance: fixed_arity(sa.func.st_maxdistance, 2),
    ops.GeoNPoints: unary(sa.func.st_numpoints),
    ops.GeoPerimeter: unary(sa.func.st_perimeter),
    ops.GeoPoint: fixed_arity(sa.func.st_geogpoint, 2),
    ops.GeoPointN: fixed_arity(sa.func.st_pointn, 2),
    ops.GeoSimplify: _geo_simplify,
    ops.GeoStartPoint: unary(sa.func.st_startpoint),
    ops.GeoTouches: fixed_arity(sa.func.st_touches, 2),
    ops.GeoUnion: fixed_arity(sa.func.st_union, 2),
    ops.GeoWithin: fixed_arity(sa.func.st_within, 2),
    ops.GeoX: unary(sa.func.st_x),
    ops.GeoXMax: _geo_boundingbox("xmax"),
    ops.GeoXMin: _geo_boundingbox("xmin"),
    ops.GeoY: unary(sa.func.ST_Y),
    ops.GeoYMax: _geo_boundingbox("ymax"),
    ops.GeoYMin: _geo_boundingbox("ymin"),
    ops.BitAnd: reduction(sa.func.bit_and),
    ops.BitOr: reduction(sa.func.bit_or),
    ops.BitXor: reduction(sa.func.bit_xor),
    ops.ApproxCountDistinct: reduction(sa.func.approx_count_distinct),
    ops.ApproxMedian: compiles_approx,
    ops.IdenticalTo: _identical_to,
    ops.FloorDivide: _floor_divide,
    ops.IsNan: _is_nan,
    ops.IsInf: _is_inf,
    ops.ArgMin: _arg_min_max(sa.asc),
    ops.ArgMax: _arg_min_max(sa.desc),
    ops.Pi: lambda *_: sa.func.acos(-1),
    ops.E: lambda *_: sa.func.exp(1),
    ops.RandomScalar: fixed_arity(sa.func.rand, 0),
    ops.NthValue: _nth_value,
    ops.JSONGetItem: fixed_arity(lambda arg, idx: arg[idx], 2),
    ops.Unnest: lambda t, op: sa.func.unnest(t.translate(op.arg)).column_valued(
        op.arg.name
    ),
}

_invalid_operations = {
    ops.FindInSet,
    ops.DateDiff,
    ops.TimestampDiff,
    ops.ExtractAuthority,
    ops.ExtractFile,
    ops.ExtractFragment,
    ops.ExtractHost,
    ops.ExtractPath,
    ops.ExtractProtocol,
    ops.ExtractQuery,
    ops.ExtractUserInfo,
}

OPERATION_REGISTRY = {
    k: v for k, v in OPERATION_REGISTRY.items() if k not in _invalid_operations
}
