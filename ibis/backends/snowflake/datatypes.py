from __future__ import annotations

import functools
from functools import partial
from typing import TYPE_CHECKING

import parsy
import sqlalchemy as sa
from snowflake.sqlalchemy import (
    ARRAY,
    OBJECT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
)
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy import to_sqla_type
from ibis.common.parsing import (
    COMMA,
    LPAREN,
    NUMBER,
    PRECISION,
    RPAREN,
    SCALE,
    spaceless_string,
)
from ibis.expr.datatypes import (
    Array,
    Decimal,
    Map,
    Timestamp,
    binary,
    boolean,
    date,
    float64,
    int64,
    json,
    string,
    time,
)

if TYPE_CHECKING:
    from ibis.expr.datatypes import DataType


@functools.lru_cache(maxsize=None)
def _make_parser(*, default_precision: int, default_scale: int):
    optional_parened_number = LPAREN.then(NUMBER).then(RPAREN).optional()

    varchar = (
        spaceless_string(
            "varchar",
            "char varying",
            "character",
            "char",
            "nchar varying",
            "nchar",
            "string",
            "text",
            "nvarchar2",
            "nvarchar",
        )
        .then(optional_parened_number)
        .result(string)
    )

    decimal = (
        spaceless_string("number", "decimal", "numeric")
        .then(LPAREN)
        .then(parsy.seq(PRECISION.skip(COMMA), SCALE).map(tuple))
        .skip(RPAREN)
        .optional(default=(default_precision, default_scale))
        .combine(
            lambda precision, scale: int64 if not scale else Decimal(precision, scale)
        )
    )

    timestamp_ntz = spaceless_string("timestamp_ntz").then(
        parsy.seq(scale=optional_parened_number).combine_dict(Timestamp)
    )

    timestamp_ltz_tz = spaceless_string("timestamp_ltz", "timestamp_tz").then(
        parsy.seq(scale=optional_parened_number).combine_dict(
            partial(Timestamp, timezone="UTC")
        )
    )

    timestamp = spaceless_string("timestamp").then(
        parsy.seq(scale=optional_parened_number).combine_dict(Timestamp)
    )

    ty = parsy.alt(
        spaceless_string("boolean").result(boolean),
        spaceless_string("binary", "varbinary").result(binary),
        spaceless_string(
            "double precision",
            "double",
            "float8",
            "float4",
            "float",
            "real",
        ).result(float64),
        spaceless_string(
            "integer",
            "int",
            "bigint",
            "smallint",
            "tinyint",
            "byteint",
        ).result(int64),
        spaceless_string("datetime").result(Timestamp()),
        spaceless_string("date").result(date),
        timestamp_ntz,
        timestamp_ltz_tz,
        timestamp,
        spaceless_string("time").result(time),
        spaceless_string("object").result(Map(string, json)),
        spaceless_string("array").result(Array(json)),
        spaceless_string("variant").result(json),
        varchar,
        decimal,
    )
    return ty


def parse(text: str, default_precision: int = 38, default_scale: int = 0) -> DataType:
    """Parse a Snowflake type into an ibis data type."""
    ty = _make_parser(default_precision=default_precision, default_scale=default_scale)
    return ty.parse(text)


@dt.dtype.register(SnowflakeDialect, (TIMESTAMP_LTZ, TIMESTAMP_TZ))
def sa_sf_timestamp_ltz(_, satype, nullable=True):
    return dt.Timestamp(timezone="UTC", nullable=nullable)


@dt.dtype.register(SnowflakeDialect, TIMESTAMP_NTZ)
def sa_sf_timestamp_ntz(_, satype, nullable=True):
    return dt.Timestamp(timezone=None, nullable=nullable)


@dt.dtype.register(SnowflakeDialect, ARRAY)
def sa_sf_array(_, satype, nullable=True):
    return dt.Array(dt.json, nullable=nullable)


@dt.dtype.register(SnowflakeDialect, VARIANT)
def sa_sf_variant(_, satype, nullable=True):
    return dt.JSON(nullable=nullable)


@dt.dtype.register(SnowflakeDialect, OBJECT)
def sa_sf_object(_, satype, nullable=True):
    return dt.Map(dt.string, dt.json, nullable=nullable)


@dt.dtype.register(SnowflakeDialect, sa.Numeric)
def sa_sf_numeric(_, satype, nullable=True):
    if (scale := satype.scale) == 0:
        # kind of a lie, should be int128 because 38 digits
        return dt.Int64(nullable=nullable)
    return dt.Decimal(
        precision=satype.precision or 38,
        scale=scale or 0,
        nullable=nullable,
    )


@dt.dtype.register(SnowflakeDialect, (sa.REAL, sa.FLOAT, sa.Float))
def sa_sf_real_float(_, satype, nullable=True):
    return dt.Float64(nullable=nullable)


@to_sqla_type.register(SnowflakeDialect, dt.Array)
def _sf_array(_, itype):
    return ARRAY


@to_sqla_type.register(SnowflakeDialect, (dt.Map, dt.Struct))
def _sf_map_struct(_, itype):
    return OBJECT


@to_sqla_type.register(SnowflakeDialect, dt.JSON)
def _sf_json(_, itype):
    return VARIANT
