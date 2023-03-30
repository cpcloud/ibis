"""The pandas client implementation."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import toolz
from pandas.api.types import CategoricalDtype, DatetimeTZDtype

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
from ibis.backends.base import Database
from ibis.expr.operations.relations import TableProxy

_ibis_dtypes = toolz.valmap(
    np.dtype,
    {
        dt.Boolean: np.bool_,
        dt.Null: np.object_,
        dt.Array: np.object_,
        dt.String: np.object_,
        dt.Binary: np.object_,
        dt.Date: 'datetime64[ns]',
        dt.Time: 'timedelta64[ns]',
        dt.Timestamp: 'datetime64[ns]',
        dt.Int8: np.int8,
        dt.Int16: np.int16,
        dt.Int32: np.int32,
        dt.Int64: np.int64,
        dt.UInt8: np.uint8,
        dt.UInt16: np.uint16,
        dt.UInt32: np.uint32,
        dt.UInt64: np.uint64,
        dt.Float32: np.float32,
        dt.Float64: np.float64,
        dt.Decimal: np.object_,
        dt.Struct: np.object_,
    },
)

if TYPE_CHECKING:
    import pyarrow as pa


@dt.dtype.register(DatetimeTZDtype)
def from_pandas_tzdtype(value):
    return dt.Timestamp(timezone=str(value.tz))


@dt.dtype.register(CategoricalDtype)
def from_pandas_categorical(_):
    return dt.String()


@dt.dtype.register(pd.core.dtypes.base.ExtensionDtype)
def from_pandas_extension_dtype(t):
    return getattr(dt, t.__class__.__name__.replace("Dtype", "").lower())


try:
    _arrow_dtype_class = pd.ArrowDtype
except AttributeError:
    warnings.warn(
        f"The `ArrowDtype` class is not available in pandas {pd.__version__}. "
        "Install pandas >= 1.5.0 for interop with pandas and arrow dtype support"
    )
else:

    @dt.dtype.register(_arrow_dtype_class)
    def from_pandas_arrow_extension_dtype(t):
        import ibis.backends.pyarrow.datatypes as _  # noqa: F401

        return dt.dtype(t.pyarrow_dtype)


@sch.schema.register(pd.Series)
def schema_from_series(s):
    return sch.schema(tuple(s.items()))


@sch.infer.register(pd.DataFrame)
def infer_pandas_schema(df: pd.DataFrame, schema=None):
    schema = schema if schema is not None else {}

    pairs = []
    for column_name in df.dtypes.keys():
        if not isinstance(column_name, str):
            raise TypeError('Column names must be strings to use the pandas backend')

        if column_name in schema:
            ibis_dtype = dt.dtype(schema[column_name])
        else:
            ibis_dtype = dt.infer(df[column_name]).value_type

        pairs.append((column_name, ibis_dtype))

    return sch.schema(pairs)


def ibis_dtype_to_pandas(ibis_dtype: dt.DataType):
    """Convert ibis dtype to the pandas / numpy alternative."""
    assert isinstance(ibis_dtype, dt.DataType)

    if ibis_dtype.is_timestamp() and ibis_dtype.timezone:
        return DatetimeTZDtype('ns', ibis_dtype.timezone)
    elif ibis_dtype.is_interval():
        return np.dtype(f'timedelta64[{ibis_dtype.unit.short}]')
    else:
        return _ibis_dtypes.get(type(ibis_dtype), np.dtype(np.object_))


def ibis_schema_to_pandas(schema):
    return list(zip(schema.names, map(ibis_dtype_to_pandas, schema.types)))


class DataFrameProxy(TableProxy):
    __slots__ = ()

    def to_frame(self) -> pd.DataFrame:
        return self._data

    def to_pyarrow(self, schema: sch.Schema) -> pa.Table:
        import pyarrow as pa

        from ibis.backends.pyarrow.datatypes import ibis_to_pyarrow_schema

        return pa.Table.from_pandas(self._data, schema=ibis_to_pyarrow_schema(schema))


class PandasTable(ops.DatabaseTable):
    pass


class PandasDatabase(Database):
    pass
