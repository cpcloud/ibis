from __future__ import annotations

import functools
import math
import operator

import sqlglot as sg

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.backends.base.sqlglot import (
    NULL,
    STAR,
    AggGen,
    F,
    interval,
    lit,
    make_cast,
    paren,
    parenthesize,
)
from ibis.backends.base.sqlglot.datatypes import PostgresType
from ibis.expr.operations.udf import InputType


def _aggregate(funcname, *args, where):
    expr = F[funcname](*args)
    if where is not None:
        return sg.exp.Filter(this=expr, expression=sg.exp.Where(this=where))
    return expr


@functools.singledispatch
def translate_val(op, **_):
    """Translate a value expression into sqlglot."""
    raise com.OperationNotDefinedError(f"No translation rule for {type(op)}")


agg = AggGen(aggfunc=_aggregate)
cast = make_cast(PostgresType)

_simple_ops = {
    ops.Abs: "abs",
    ops.Ln: "ln",
    ops.Log2: "log2",
    ops.Log10: "log10",
    ops.Sqrt: "sqrt",
    ops.Reverse: "reverse",
    ops.Strip: "trim",
    ops.LStrip: "ltrim",
    ops.RStrip: "rtrim",
    ops.Lowercase: "lower",
    ops.Uppercase: "upper",
    ops.StringLength: "character_length",
    ops.Capitalize: "initcap",
    ops.Repeat: "repeat",
    ops.LPad: "lpad",
    ops.RPad: "rpad",
    ops.Count: "count",
    ops.Min: "min",
    ops.Max: "max",
    ops.Mean: "avg",
    ops.Median: "median",
    ops.ApproxMedian: "approx_median",
    ops.Acos: "acos",
    ops.Asin: "asin",
    ops.Atan: "atan",
    ops.Cos: "cos",
    ops.Sin: "sin",
    ops.Tan: "tan",
    ops.Exp: "exp",
    ops.Power: "power",
    ops.RandomScalar: "random",
    ops.Translate: "translate",
    ops.StringAscii: "ascii",
    ops.StartsWith: "starts_with",
    ops.StrRight: "right",
    ops.StringReplace: "replace",
    ops.Sign: "sign",
    ops.ExtractEpochSeconds: "extract_epoch_seconds",
    ops.ExtractMicrosecond: "extract_microsecond",
    ops.ExtractMillisecond: "extract_millisecond",
    ops.DayOfWeekName: "extract_dow_name",
    ops.ExtractSecond: "extract_second",
    ops.ExtractUserInfo: "extract_user_info",
}


for _op, _name in _simple_ops.items():
    assert isinstance(type(_op), type), type(_op)
    if issubclass(_op, ops.Reduction):

        @translate_val.register(_op)
        def _fmt(_, _name: str = _name, *, where, **kw):
            return agg[_name](*kw.values(), where=where)

    else:

        @translate_val.register(_op)
        def _fmt(_, _name: str = _name, **kw):
            return F[_name](*kw.values())


del _fmt, _name, _op

_binary_infix_ops = {
    # Binary operations
    ops.Add: operator.add,
    ops.Subtract: operator.sub,
    ops.Multiply: operator.mul,
    ops.Modulus: operator.mod,
    # Comparisons
    ops.Equals: sg.exp.Condition.eq,
    ops.NotEquals: sg.exp.Condition.neq,
    ops.GreaterEqual: operator.ge,
    ops.Greater: operator.gt,
    ops.LessEqual: operator.le,
    ops.Less: operator.lt,
    # Boolean comparisons
    ops.And: operator.and_,
    ops.Or: operator.or_,
    ops.Xor: F.xor,
    ops.DateAdd: operator.add,
    ops.DateSub: operator.sub,
    ops.DateDiff: operator.sub,
    ops.TimestampAdd: operator.add,
    ops.TimestampSub: operator.sub,
    ops.TimestampDiff: operator.sub,
}


def _binary_infix(func):
    def formatter(op, *, left, right, **_):
        return func(parenthesize(op.left, left), parenthesize(op.right, right))

    return formatter


for _op, _func in _binary_infix_ops.items():
    translate_val.register(_op)(_binary_infix(_func))

del _op, _func


@translate_val.register(ops.Alias)
def alias(op, *, arg, name, **_):
    return arg.as_(name)


@translate_val.register(ops.Literal)
def _literal(op, *, value, dtype, **kw):
    if value is None and dtype.nullable:
        if dtype.is_null():
            return NULL
        return cast(NULL, dtype)
    elif dtype.is_boolean():
        return lit(bool(value))
    elif dtype.is_inet() or dtype.is_decimal():
        return cast(lit(str(value)), dtype)
    elif dtype.is_string() or dtype.is_macaddr():
        return lit(str(value))
    elif dtype.is_numeric():
        return lit(value)
    elif dtype.is_interval():
        if dtype.unit.short in {"ms", "us", "ns"}:
            raise com.UnsupportedOperationError(
                "PostgreSQL doesn't support subsecond interval resolutions"
            )

        return interval(value, unit=dtype.resolution.upper())
    elif dtype.is_timestamp():
        args = (
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second + value.microsecond / 1e6,
        )
        if (tz := dtype.timezone) is not None:
            return F.make_timestamptz(*args, tz)
        else:
            return F.make_timestamp(*args)
    elif dtype.is_date():
        return F.make_date(value.year, value.month, value.day)
    elif dtype.is_time():
        return F.make_time(
            value.hour, value.minute, value.second + value.microsecond / 1e6
        )
    elif dtype.is_array():
        vtype = dtype.value_type
        values = [
            _literal(ops.Literal(v, dtype=vtype), value=v, dtype=vtype, **kw)
            for v in value
        ]
        return F.array(*values)
    elif dtype.is_map():
        vtype = dtype.value_type
        keys = []
        values = []

        for k, v in value.items():
            keys.append(lit(k))
            values.append(
                _literal(ops.Literal(v, dtype=vtype), value=v, dtype=vtype, **kw)
            )

        return F.map(F.array(*keys), F.array(*values))
    elif dtype.is_struct():
        fields = [
            _literal(ops.Literal(v, dtype=ftype), value=v, dtype=ftype, **kw)
            for ftype, v in zip(dtype.types, value.values())
        ]
        return cast(sg.exp.Struct.from_arg_list(fields), dtype)
    else:
        raise NotImplementedError(f"Unsupported type: {dtype!r}")


@translate_val.register(ops.Cast)
def cast(op, *, arg, to, **_):
    return cast(arg, to)


@translate_val.register(ops.TableColumn)
def column(op, *, table, name, **_):
    return sg.column(name, table=table.alias_or_name, quoted=True)


@translate_val.register
def sort_key(op: ops.SortKey, *, expr, ascending: bool, **_):
    return sg.exp.Ordered(this=expr, desc=not ascending)


@translate_val.register(ops.Not)
def invert(op, *, arg, **_):
    return sg.not_(paren(arg))


@translate_val.register(ops.And)
def and_(op, *, left, right, **_):
    return sg.and_(left, right)


@translate_val.register(ops.Or)
def or_(op, *, left, right, **_):
    return sg.or_(left, right)


@translate_val.register(ops.Ceil)
@translate_val.register(ops.Floor)
def ceil_floor(op, *, arg, **_):
    return cast(F[type(op).__name__.lower()].ceil(arg), dt.int64)


@translate_val.register(ops.Round)
def round(op, *, arg, digits, **_):
    if digits is not None:
        return F.round(arg, digits)
    return F.round(arg)


@translate_val.register(ops.Substring)
def substring(op, *, arg, start, length, **_):
    start += 1
    if length is not None:
        return F.substr(arg, start, length)
    return F.substr(arg, start)


@translate_val.register(ops.Divide)
def div(op, *, left, right, **_):
    return cast(left, dt.float64) / cast(right, dt.float64)


@translate_val.register(ops.FloorDivide)
def floordiv(op, *, left, right, **_):
    return F.floor(left / right)


@translate_val.register(ops.CountDistinct)
def count_distinct(op, *, arg, where, **_):
    return agg.count(sg.exp.Distinct(expressions=[arg]), where=where)


@translate_val.register(ops.CountStar)
def count_star(op, *, where, **_):
    return agg.count(STAR, where=where)


@translate_val.register(ops.Sum)
def sum(op, *, arg, where, **_):
    if op.arg.dtype.is_boolean():
        arg = cast(arg, dt.int64)
    return agg.sum(arg, where=where)


@translate_val.register(ops.Variance)
def variance(op, *, arg, how, where, **_):
    if how == "sample":
        return agg.var_samp(arg, where=where)
    elif how == "pop":
        return agg.var_pop(arg, where=where)
    else:
        raise ValueError(f"Unrecognized how value: {how}")


@translate_val.register(ops.StandardDev)
def stddev(op, *, arg, how, where, **_):
    if how == "sample":
        return agg.stdev_samp(arg, where=where)
    elif how == "pop":
        return agg.stdev_pop(arg, where=where)
    else:
        raise ValueError(f"Unrecognized how value: {how}")


@translate_val.register(ops.InValues)
def in_values(op, *, value, options, **_):
    return parenthesize(op.value, value).isin(*options)


@translate_val.register(ops.Negate)
def negate(op, *, arg, **_):
    return -paren(arg)


@translate_val.register(ops.Atan2)
def atan2(op, *, left, right, **_):
    return F.atan(left / right)


@translate_val.register(ops.Cot)
def cot(op, *, arg, **_):
    return 1.0 / F.tan(arg)


@translate_val.register(ops.Radians)
def radians(op, *, arg, **_):
    return arg * lit(math.pi) / lit(180)


@translate_val.register(ops.Degrees)
def degrees(op, *, arg, **_):
    return arg * lit(180) / lit(math.pi)


@translate_val.register(ops.Coalesce)
def coalesce(op, *, arg, **_):
    return F.coalesce(*arg)


@translate_val.register(ops.NullIf)
def nullif(op, *, arg, null_if_expr, **_):
    return F.nullif(arg, null_if_expr)


@translate_val.register(ops.Log)
def log(op, *, arg, base, **_):
    return F.log(base, arg)


@translate_val.register(ops.Pi)
def pi(op, **_):
    return lit(math.pi)


@translate_val.register(ops.E)
def e(op, **_):
    return lit(math.e)


@translate_val.register(ops.ScalarUDF)
def scalar_udf(op, **kw):
    input_type = op.input_type
    if input_type in (InputType.PYARROW, InputType.BUILTIN):
        return F[op.__full_name__](*kw.values())
    else:
        raise NotImplementedError(
            f"DataFusion only supports PyArrow UDFs: got a {input_type.name.lower()} UDF"
        )


@translate_val.register(ops.AggUDF)
def agg_udf(op, **kw):
    return agg[op.__full_name__](*kw.values())


@translate_val.register(ops.StringConcat)
def string_concat(op, *, arg, **_):
    return F.concat(*arg)


@translate_val.register(ops.RegexExtract)
def regex_extract(op, *, arg, pattern, index, **_):
    if not isinstance(op.index, ops.Literal):
        raise ValueError(
            "re_extract `index` expressions must be literals. "
            "Arbitrary expressions are not supported in the DataFusion backend"
        )
    return F.regexp_match(arg, F.concat("(", pattern, ")"))[index]


@translate_val.register(ops.RegexReplace)
def regex_replace(op, *, arg, pattern, replacement, **_):
    return F.regexp_replace(arg, pattern, replacement, lit("g"))


@translate_val.register(ops.StringFind)
def string_find(op, *, arg, substr, start, end, **_):
    if end is not None:
        raise NotImplementedError("`end` not yet implemented")

    if start is not None:
        pos = F.strpos(F.substr(arg, start + 1), substr)
        return F.coalesce(F.nullif(pos + start, start), 0) - 1

    return F.strpos(arg, substr) - 1


@translate_val.register(ops.RegexSearch)
def regex_search(op, *, arg, pattern, **_):
    return F.array_length(F.regexp_match(arg, pattern)) > 0


@translate_val.register(ops.StringContains)
def string_contains(op, *, haystack, needle, **_):
    return F.strpos(haystack, needle) > lit(0)


@translate_val.register(ops.StringJoin)
def string_join(op, *, sep, arg, **_):
    if not isinstance(op.sep, ops.Literal):
        raise ValueError(
            "join `sep` expressions must be literals. "
            "Arbitrary expressions are not supported in the DataFusion backend"
        )

    return F.concat_ws(sep, *arg)


@translate_val.register(ops.ExtractFragment)
def _(op, *, arg, **_):
    return F.extract_url_field(arg, "fragment")


@translate_val.register(ops.ExtractProtocol)
def extract_protocol(op, *, arg, **_):
    return F.extract_url_field(arg, "scheme")


@translate_val.register(ops.ExtractAuthority)
def extract_authority(op, *, arg, **_):
    return F.extract_url_field(arg, "netloc")


@translate_val.register(ops.ExtractPath)
def extract_path(op, *, arg, **_):
    return F.extract_url_field(arg, "path")


@translate_val.register(ops.ExtractHost)
def extract_host(op, *, arg, **_):
    return F.extract_url_field(arg, "hostname")


@translate_val.register(ops.ExtractQuery)
def extract_query(op, *, arg, key, **_):
    if key is not None:
        return F.extract_query_param(arg, key)
    return F.extract_query(arg)


@translate_val.register(ops.ExtractYear)
@translate_val.register(ops.ExtractMonth)
@translate_val.register(ops.ExtractQuarter)
@translate_val.register(ops.ExtractDay)
@translate_val.register(ops.ExtractHour)
@translate_val.register(ops.ExtractMinute)
def extract(op, *, arg, **_):
    skip = len("Extract")
    part = type(op).__name__[skip:].lower()
    return F.date_part(part, arg)


@translate_val.register(ops.ExtractDayOfYear)
def extract_day_of_the_year(op, *, arg, **_):
    return F.date_part("doy", arg)


@translate_val.register(ops.DayOfWeekIndex)
def extract_day_of_the_week_index(op, *, arg, **_):
    return (F.date_part("dow", arg) + 6) % 7


@translate_val.register(ops.Date)
def date(op, *, arg, **_):
    return F.date_trunc("day", arg)


@translate_val.register(ops.ExtractWeekOfYear)
def extract_week_of_year(op, *, arg, **_):
    return F.date_part("week", arg)
