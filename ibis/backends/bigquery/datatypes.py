from __future__ import annotations
import functools

import sqlalchemy as sa
import sqlalchemy_bigquery as sab
from sqlalchemy_bigquery import (
    ARRAY,
    BIGNUMERIC,
    BYTES,
    DATE,
    FLOAT64,
    INT64,
    NUMERIC,
    STRUCT,
    TIMESTAMP,
    BigQueryDialect,
)

import ibis.expr.datatypes as dt


def ibis_type_to_bigquery_type(t):
    return sa.types.to_instance(_ibis_type_to_bigquery_type(t))


@functools.singledispatch
def _ibis_type_to_bigquery_type(_):
    ...


@_ibis_type_to_bigquery_type.register(str)
def trans_string_default(datatype):
    return ibis_type_to_bigquery_type(dt.dtype(datatype))


@_ibis_type_to_bigquery_type.register(dt.Floating)
def trans_float64(_):
    return FLOAT64


@_ibis_type_to_bigquery_type.register(dt.Integer)
def trans_integer(_):
    return INT64


@_ibis_type_to_bigquery_type.register(dt.Binary)
def trans_binary(_):
    return BYTES


@_ibis_type_to_bigquery_type.register(dt.UInt64)
def trans_lossy_integer(_):
    raise TypeError("Conversion from uint64 to BigQuery integer type (int64) is lossy")


@_ibis_type_to_bigquery_type.register(dt.Array)
def trans_array(t):
    return ARRAY(ibis_type_to_bigquery_type(t.value_type))


@_ibis_type_to_bigquery_type.register(dt.Struct)
def trans_struct(t):
    return STRUCT(
        *(
            (name, ibis_type_to_bigquery_type(dt.dtype(type_)))
            for name, type_ in t.fields.items()
        )
    )


@_ibis_type_to_bigquery_type.register(dt.Date)
def trans_date(_):
    return DATE


@_ibis_type_to_bigquery_type.register(dt.Timestamp)
def trans_timestamp(t):
    if t.timezone is not None:
        raise TypeError("BigQuery does not support timestamps with timezones")
    return TIMESTAMP


@_ibis_type_to_bigquery_type.register(dt.DataType)
def trans_type(t):
    return getattr(sab, str(t).upper())


@_ibis_type_to_bigquery_type.register(dt.Decimal)
def trans_numeric(t):
    if (t.precision, t.scale) == (76, 38):
        return BIGNUMERIC
    if (t.precision, t.scale) in [(38, 9), (None, None)]:
        return NUMERIC
    raise TypeError(
        "BigQuery only supports decimal types with precision of 38 and "
        f"scale of 9 (NUMERIC) or precision of 76 and scale of 38 (BIGNUMERIC). "
        f"Current precision: {t.precision}. Current scale: {t.scale}"
    )


@_ibis_type_to_bigquery_type.register(dt.JSON)
def trans_json(_):
    return sa.JSON


def spread_type(dt: dt.DataType):
    """Returns a generator that contains all the types in the given type.

    For complex types like set and array, it returns the types of the elements.
    """
    if dt.is_array():
        yield from spread_type(dt.value_type)
    elif dt.is_struct():
        for type_ in dt.types:
            yield from spread_type(type_)
    elif dt.is_map():
        yield from spread_type(dt.key_type)
        yield from spread_type(dt.value_type)
    yield dt


@dt.dtype.register(BigQueryDialect, STRUCT)
def _(dialect, sqla_type, nullable: bool = True):
    return dt.Struct(
        {name: dt.dtype(dialect, typ) for name, typ in sqla_type._STRUCT_fields}
    )


@dt.dtype.register(BigQueryDialect, ARRAY)
def _(dialect, sqla_type, nullable: bool = True):
    return dt.Array(dt.dtype(dialect, sqla_type.item_type))
