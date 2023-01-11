from __future__ import annotations

import functools

import parsy
from public import public

import ibis.expr.datatypes.core as dt
from ibis.common.exceptions import IbisTypeError
from ibis.common.parsing import (
    COLON,
    COMMA,
    FIELD,
    LANGLE,
    LPAREN,
    NUMBER,
    PRECISION,
    RANGLE,
    RAW_NUMBER,
    RAW_STRING,
    RPAREN,
    SCALE,
    SEMICOLON,
    SINGLE_DIGIT,
    spaceless,
    spaceless_string,
)


@functools.lru_cache(maxsize=None)
def _make_parser():
    srid = NUMBER
    geotype = spaceless_string("geography", "geometry")

    srid_geotype = SEMICOLON.then(parsy.seq(srid=srid.skip(COLON), geotype=geotype))
    geotype_part = COLON.then(parsy.seq(geotype=geotype))
    srid_part = SEMICOLON.then(parsy.seq(srid=srid))

    geotype_parser = lambda name: spaceless_string(name).then(
        parsy.alt(srid_geotype, geotype_part, srid_part).optional(default={})
    )

    primitive = parsy.alt(
        spaceless_string("boolean").result(dt.boolean),  # docprecated
        spaceless_string("bool").result(dt.boolean),
        spaceless_string("int8").result(dt.int8),
        spaceless_string("int16").result(dt.int16),
        spaceless_string("int32").result(dt.int32),
        spaceless_string("int64").result(dt.int64),
        spaceless_string("uint8").result(dt.uint8),
        spaceless_string("uint16").result(dt.uint16),
        spaceless_string("uint32").result(dt.uint32),
        spaceless_string("uint64").result(dt.uint64),
        spaceless_string("halffloat").result(dt.float16),  # docprecated
        spaceless_string("double").result(dt.float64),  # docprecated
        spaceless_string("float16").result(dt.float16),
        spaceless_string("float32").result(dt.float32),
        spaceless_string("float64").result(dt.float64),
        spaceless_string("float").result(dt.float64),
        spaceless_string("string").result(dt.string),
        spaceless_string("binary").result(dt.binary),  # docprecated
        spaceless_string("bytes").result(dt.binary),
        spaceless_string("timestamp").result(dt.Timestamp()),
        spaceless_string("time").result(dt.time),
        spaceless_string("date").result(dt.date),
        spaceless_string("category").result(dt.category),
        spaceless_string("geometry").result(dt.geometry),
        spaceless_string("geography").result(dt.geography),
        spaceless_string("null").result(dt.null),
        spaceless_string("json").result(dt.json),
        spaceless_string("uuid").result(dt.uuid),
        spaceless_string("macaddr").result(dt.macaddr),
        spaceless_string("inet").result(dt.inet),
        spaceless_string("geography").result(dt.geography),
        spaceless_string("geometry").result(dt.geometry),
        geotype_parser("linestring").combine_dict(dt.LineString),
        geotype_parser("polygon").combine_dict(dt.Polygon),
        geotype_parser("point").combine_dict(dt.Point),
        geotype_parser("multilinestring").combine_dict(dt.MultiLineString),
        geotype_parser("multipolygon").combine_dict(dt.MultiPolygon),
        geotype_parser("multipoint").combine_dict(dt.MultiPoint),
    )

    varchar_or_char = (
        spaceless_string("varchar", "char")
        .then(LPAREN.then(RAW_NUMBER).skip(RPAREN).optional())
        .result(dt.string)
    )

    decimal = spaceless_string("decimal").then(
        LPAREN.then(parsy.seq(spaceless(PRECISION).skip(COMMA), spaceless(SCALE)))
        .skip(RPAREN)
        .optional(default=(None, None))
        .combine(dt.Decimal)
    )

    parened_string = LPAREN.then(RAW_STRING).skip(RPAREN)
    timestamp_scale = SINGLE_DIGIT.map(int)

    timestamp_tz_args = LPAREN.then(
        parsy.seq(timezone=RAW_STRING, scale=COMMA.then(timestamp_scale).optional())
    ).skip(RPAREN)

    timestamp_no_tz_args = LPAREN.then(parsy.seq(scale=timestamp_scale)).skip(RPAREN)

    timestamp = spaceless_string("timestamp").then(
        parsy.alt(timestamp_tz_args, timestamp_no_tz_args)
        .optional(default={})
        .combine_dict(dt.Timestamp)
    )

    ty = parsy.forward_declaration()

    angle_type = LANGLE.then(ty).skip(RANGLE)

    interval = spaceless_string("interval").then(
        parsy.seq(
            value_type=angle_type.optional(), unit=parened_string.optional()
        ).combine_dict(dt.Interval)
    )

    array = spaceless_string("array").then(angle_type.map(dt.Array))
    set = spaceless_string("set").then(angle_type.map(dt.Set))

    map = (
        spaceless_string("map")
        .then(LANGLE)
        .then(parsy.seq(primitive.skip(COMMA), ty))
        .skip(RANGLE)
        .combine(dt.Map)
    )

    struct = (
        spaceless_string("struct")
        .then(LANGLE)
        .then(
            parsy.seq(spaceless(FIELD).skip(COLON), ty)
            .map(tuple)
            .sep_by(COMMA)
            .map(dt.Struct.from_tuples)
        )
        .skip(RANGLE)
    )

    nullable = spaceless_string("!").then(ty).map(lambda typ: typ(nullable=False))

    ty.become(
        parsy.alt(
            nullable,
            timestamp,
            primitive,
            decimal,
            varchar_or_char,
            interval,
            array,
            set,
            map,
            struct,
            # must come after struct because `str` is strict subset of `struct`
            spaceless_string("str").result(dt.string),
            # must come after struct because `int` is strict subset of `interval`
            spaceless_string("int").result(dt.int64),
        )
    )
    return ty


@public
@functools.lru_cache(maxsize=100)
def parse(text: str) -> dt.DataType:
    """Parse a type from a [`str`][str] `text`.

    The default `maxsize` parameter for caching is chosen to cache the most
    commonly used types--there are about 30--along with some capacity for less
    common but repeatedly-used complex types.

    Parameters
    ----------
    text
        The type string to parse

    Examples
    --------
    Parse an array type from a string

    >>> import ibis
    >>> import ibis.expr.datatypes as dt
    >>> dt.parse("array<int64>")
    Array(value_type=Int64(nullable=True), nullable=True)

    You can avoid parsing altogether by constructing objects directly

    >>> import ibis
    >>> import ibis.expr.datatypes as dt
    >>> ty = dt.parse("array<int64>")
    >>> ty == dt.Array(dt.int64)
    True
    """
    ty = _make_parser()
    return ty.parse(text)


@dt.dtype.register(str)
def from_string(value: str) -> dt.DataType:
    try:
        return parse(value)
    except SyntaxError:
        raise IbisTypeError(f'{value!r} cannot be parsed as an ibis datatype')
