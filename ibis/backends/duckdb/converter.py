from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from ibis.formats.pandas import PandasData
from ibis.formats.pyarrow import PyArrowData

if TYPE_CHECKING:
    import ibis.expr.datatypes as dt


class DuckDBPandasData(PandasData):
    @staticmethod
    def convert_Array(s, dtype, pandas_type):
        return s.replace(float("nan"), None)


class DuckDBPyArrowData(PyArrowData):
    @classmethod
    def convert_scalar(cls, scalar: pa.Scalar, dtype: dt.DataType) -> pa.Scalar:
        if dtype.is_null():
            return pa.scalar(None)
        return super().convert_scalar(scalar, dtype)

    @classmethod
    def convert_column(cls, column: pa.Array, dtype: dt.DataType) -> pa.Array:
        if dtype.is_null():
            return pa.nulls(len(column))
        return super().convert_column(column, dtype)

    @classmethod
    def convert_UUID(cls, scalar: pa.Scalar, dtype: dt.DataType) -> pa.Scalar:
        if scalar.is_valid:
            return pa.scalar(str(scalar.as_py()), type=pa.string())
        return pa.scalar(None, type=pa.string())
