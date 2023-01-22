from __future__ import annotations

import ibis.backends.duckdb.datatypes as ddb
import ibis.expr.datatypes as dt

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
