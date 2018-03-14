from __future__ import absolute_import

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (BooleanType, NullType, ArrayType,
                               StringType, BinaryType, DateType,
                               TimestampType, ShortType, IntegerType,
                               LongType, FloatType, DoubleType,
                               DecimalType, StructType, DataType)

import ibis
import ibis.client as client
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops

from ibis.compat import parse_version


_ibis_dtypes = {
    dt.Boolean: BooleanType,
    dt.Null: NullType,
    dt.Array: ArrayType,
    dt.String: StringType,
    dt.Binary: BinaryType,
    dt.Date: DateType,
    dt.Time: TimestampType,
    dt.Timestamp: TimestampType,
    dt.Int8: ShortType,
    dt.Int16: ShortType,
    dt.Int32: IntegerType,
    dt.Int64: LongType,
    dt.UInt8: IntegerType,
    dt.UInt16: IntegerType,
    dt.UInt32: LongType,
    dt.UInt64: LongType,
    dt.Float32: FloatType,
    dt.Float64: DoubleType,
    dt.Decimal: DecimalType,
    dt.Struct: StructType
}

_spark_types_to_ibis_types = {
    spark_type: ibis_type for ibis_type, spark_type in _ibis_dtypes.items()
}


def ibis_dtype_to_spark(ibis_dtype):
    """Convert ibis dtype to the pandas / numpy alternative"""
    return _ibis_dtypes[type(ibis_dtype)]


def ibis_schema_to_spark(schema):
    return list(zip(schema.names, map(ibis_dtype_to_spark,
                                      schema.types)))


@dt.dtype.register(DataType)
def spark_data_type(datatype):
    return _spark_types_to_ibis_types[type(datatype)]()


@dt.dtype.register(DecimalType)
def spark_decimal_type(decimal):
    return dt.Decimal(decimal.precision, decimal.scale)


@dt.dtype.register(ArrayType)
def spark_array_type(array):
    return dt.Array(dt.dtype(array.elementType))


@dt.dtype.register(StructType)
def spark_struct_type(struct):
    return dt.Struct.from_tuples([
        (field.name, dt.dtype(field.dataType)) for field in struct.fields
    ])


class SparkTable(ops.DatabaseTable):
    pass


class SparkClient(client.Client):

    def __init__(self, dictionary, sc=None):
        if sc is None:
            conf = SparkConf().setAppName('ibis').setMaster('local[*]')
            sc = SparkContext.getOrCreate(conf=conf)
        self.sc = sc
        self.spark = SparkSession(self.sc)

        # Instantiate the lambda functions in dictionary with the SparkSession
        self.dictionary = {
            k: func(self.spark) for k, func in dictionary.items()
        }

    def table(self, name, schema=None):
        spark_schema = self.dictionary[name].schema
        pairs = dt.dtype(spark_schema).pairs.items()
        ibis_schema = ibis.schema(pairs)
        return SparkTable(name, ibis_schema, self)

    def execute(self, expr, *args, **kwargs):
        raise NotImplementedError(
            '{} not implemented'.format(type(self).__name__))

    def database(self, name=None):
        return SparkDatabase(name, self)

    @property
    def version(self):
        return parse_version(pyspark.__version__)


class SparkDatabase(client.Database):
    pass
