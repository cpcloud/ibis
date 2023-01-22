from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import PGDialect

import ibis.backends.duckdb.datatypes as ddb
import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy import to_sqla_type

_BRACKETS = "[]"


def _get_type(typestr: str) -> dt.DataType:
    is_array = typestr.endswith(_BRACKETS)
    if (typ := _type_mapping.get(typestr.replace(_BRACKETS, ""))) is None:
        return dt.Array(typ) if is_array else typ
    return ddb.parse(typestr)


_type_mapping = {
    "boolean": dt.bool,
    "bytea": dt.binary,
    "character(1)": dt.string,
    "bigint": dt.int64,
    "smallint": dt.int16,
    "integer": dt.int32,
    "text": dt.string,
    "json": dt.json,
    "point": dt.point,
    "polygon": dt.polygon,
    "line": dt.linestring,
    "real": dt.float32,
    "double precision": dt.float64,
    "macaddr8": dt.macaddr,
    "macaddr": dt.macaddr,
    "inet": dt.inet,
    "character": dt.string,
    "character varying": dt.string,
    "date": dt.date,
    "time without time zone": dt.time,
    "timestamp without time zone": dt.timestamp,
    "timestamp with time zone": dt.Timestamp("UTC"),
    "interval": dt.interval,
    # NB: this isn't correct because we're losing the "with time zone"
    # information (ibis doesn't have time type that is time-zone aware), but we
    # try to do _something_ here instead of failing
    "time with time zone": dt.time,
    "numeric": dt.decimal,
    "uuid": dt.uuid,
    "jsonb": dt.json,
    "geometry": dt.geometry,
    "geography": dt.geography,
}


@to_sqla_type.register(PGDialect, dt.Array)
def _pg_array(dialect, itype):
    # Unwrap the array element type because sqlalchemy doesn't allow arrays of
    # arrays. This doesn't affect the underlying data.
    while itype.is_array():
        itype = itype.value_type
    return sa.ARRAY(to_sqla_type(dialect, itype))


@to_sqla_type.register(PGDialect, dt.Map)
def _pg_map(dialect, itype):
    if not (itype.key_type.is_string() and itype.value_type.is_string()):
        raise TypeError(f"PostgreSQL only supports map<string, string>, got: {itype}")
    return postgresql.HSTORE


@dt.dtype.register(PGDialect, postgresql.DOUBLE_PRECISION)
def sa_double(_, satype, nullable=True):
    return dt.Float64(nullable=nullable)


@dt.dtype.register(PGDialect, postgresql.UUID)
def sa_uuid(_, satype, nullable=True):
    return dt.UUID(nullable=nullable)


@dt.dtype.register(PGDialect, postgresql.MACADDR)
def sa_macaddr(_, satype, nullable=True):
    return dt.MACADDR(nullable=nullable)


@dt.dtype.register(PGDialect, postgresql.HSTORE)
def sa_hstore(_, satype, nullable=True):
    return dt.Map(dt.string, dt.string, nullable=nullable)


@dt.dtype.register(PGDialect, postgresql.INET)
def sa_inet(_, satype, nullable=True):
    return dt.INET(nullable=nullable)


@dt.dtype.register(PGDialect, postgresql.JSONB)
def sa_json(_, satype, nullable=True):
    return dt.JSON(nullable=nullable)


POSTGRES_FIELD_TO_IBIS_UNIT = {
    "YEAR": "Y",
    "MONTH": "M",
    "DAY": "D",
    "HOUR": "h",
    "MINUTE": "m",
    "SECOND": "s",
    "YEAR TO MONTH": "M",
    "DAY TO HOUR": "h",
    "DAY TO MINUTE": "m",
    "DAY TO SECOND": "s",
    "HOUR TO MINUTE": "m",
    "HOUR TO SECOND": "s",
    "MINUTE TO SECOND": "s",
}


@dt.dtype.register(PGDialect, postgresql.INTERVAL)
def sa_postgres_interval(_, satype, nullable=True):
    field = satype.fields.upper()
    unit = POSTGRES_FIELD_TO_IBIS_UNIT.get(field, None)
    if unit is None:
        raise ValueError(f"Unknown PostgreSQL interval field {field!r}")
    elif unit in {"Y", "M"}:
        raise ValueError(
            "Variable length timedeltas are not yet supported with PostgreSQL"
        )
    return dt.Interval(unit=unit, nullable=nullable)


@dt.dtype.register(PGDialect, sa.ARRAY)
def sa_pg_array(dialect, satype, nullable=True):
    dimensions = satype.dimensions
    if dimensions is not None and dimensions != 1:
        raise NotImplementedError(
            f"Nested array types not yet supported for {dialect.name} dialect"
        )

    value_dtype = dt.dtype(dialect, satype.item_type)
    return dt.Array(value_dtype, nullable=nullable)
