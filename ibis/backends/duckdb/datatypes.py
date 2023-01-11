from __future__ import annotations

import functools

import parsy
import toolz

import ibis.expr.datatypes as dt
from ibis.common.parsing import (
    COMMA,
    FIELD,
    LBRACKET,
    LPAREN,
    PRECISION,
    RBRACKET,
    RPAREN,
    SCALE,
    spaceless,
    spaceless_string,
)
from ibis.util import deprecated


@functools.lru_cache(maxsize=None)
def _make_parser(*, default_precision: int, default_scale: int):
    primitive = parsy.alt(
        spaceless_string("interval").result(dt.Interval()),
        spaceless_string("bigint", "int8", "long").result(dt.int64),
        spaceless_string("boolean", "bool", "logical").result(dt.boolean),
        spaceless_string("blob", "bytea", "binary", "varbinary").result(dt.binary),
        spaceless_string("double", "float8").result(dt.float64),
        spaceless_string("real", "float4", "float").result(dt.float32),
        spaceless_string("smallint", "int2", "short").result(dt.int16),
        spaceless_string(
            "timestamp with time zone",
            "timestamp_tz",
            "timestamp_sec",
            "timestamp_ms",
            "timestamp_ns",
            "timestamp",
            "datetime",
        ).result(dt.Timestamp(timezone="UTC")),
        spaceless_string("date").result(dt.date),
        spaceless_string("time").result(dt.time),
        spaceless_string("tinyint", "int1").result(dt.int8),
        spaceless_string("integer", "int4", "int", "signed").result(dt.int32),
        spaceless_string("ubigint").result(dt.uint64),
        spaceless_string("usmallint").result(dt.uint16),
        spaceless_string("uinteger").result(dt.uint32),
        spaceless_string("utinyint").result(dt.uint8),
        spaceless_string("uuid").result(dt.uuid),
        spaceless_string("varchar", "string", "bpchar", "char", "text").result(
            dt.string
        ),
        spaceless_string("json").result(dt.json),
    )

    decimal = spaceless_string("decimal", "numeric").then(
        LPAREN.then(
            parsy.seq(precision=PRECISION.skip(COMMA), scale=SCALE).combine_dict(
                dt.Decimal
            )
        )
        .skip(RPAREN)
        .optional(dt.Decimal(precision=default_precision, scale=default_scale))
    )

    ty = parsy.forward_declaration()
    non_pg_array_type = parsy.forward_declaration()

    parened_type = LPAREN.then(ty).skip(RPAREN)
    list_array = spaceless_string("list").then(parened_type).map(dt.Array)
    brackets = LBRACKET.then(RBRACKET)

    pg_array = parsy.seq(non_pg_array_type, brackets.at_least(1).map(len)).map(
        lambda value_type, ndims: toolz.nth(ndims, toolz.iterate(dt.Array, value_type))
    )

    map = (
        spaceless_string("map")
        .then(LPAREN)
        .then(parsy.seq(primitive.skip(COMMA), ty).combine(dt.Map))
        .skip(RPAREN)
    )

    field = spaceless(FIELD)

    struct = (
        spaceless_string("struct")
        .then(LPAREN)
        .then(parsy.seq(field, ty).map(tuple).sep_by(COMMA).map(dt.Struct.from_tuples))
        .skip(RPAREN)
    )

    non_pg_array_type.become(parsy.alt(primitive, decimal, list_array, map, struct))
    ty.become(parsy.alt(pg_array, non_pg_array_type))

    return ty


@functools.lru_cache(maxsize=100)
def parse(
    text: str, default_precision: int = 18, default_scale: int = 3
) -> dt.DataType:
    """Parse a DuckDB type into an ibis data type."""
    ty = _make_parser(default_precision=default_precision, default_scale=default_scale)
    return ty.parse(text)


@deprecated(
    instead=f"use {parse.__module__}.{parse.__name__}", as_of="4.0", removed_in="5.0"
)
def parse_type(*args, **kwargs):
    return parse(*args, **kwargs)
