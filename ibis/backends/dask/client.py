"""The dask client implementation."""

from __future__ import annotations

import dask.dataframe as dd
import numpy as np

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
from ibis.backends.base import Database
from ibis.backends.pandas.client import ibis_dtype_to_pandas, ibis_schema_to_pandas


@sch.schema.register(dd.Series)
def schema_from_series(s):
    return sch.schema(tuple(s.items()))


@sch.infer.register(dd.DataFrame)
def infer_dask_schema(df, schema=None):
    schema = schema if schema is not None else {}

    pairs = []
    for column_name, dask_dtype in df.dtypes.items():
        if not isinstance(column_name, str):
            raise TypeError('Column names must be strings to use the dask backend')

        if column_name in schema:
            ibis_dtype = dt.dtype(schema[column_name])
        elif dask_dtype == np.object_:
            # TODO: don't call compute here. ibis should just assume that
            # object dtypes are strings, which is what dask does. The user
            # can always explicitly pass in `schema=...` when creating a
            # table if they want to use a different dtype.
            ibis_dtype = dt.infer(df[column_name].compute()).value_type
        else:
            ibis_dtype = dt.dtype(dask_dtype)

        pairs.append((column_name, ibis_dtype))

    return sch.schema(pairs)


ibis_dtype_to_dask = ibis_dtype_to_pandas

ibis_schema_to_dask = ibis_schema_to_pandas


dt.DataType.to_dask = ibis_dtype_to_dask
sch.Schema.to_dask = ibis_schema_to_dask


class DaskTable(ops.DatabaseTable):
    pass


class DaskDatabase(Database):
    pass
