import operator

from ibis.compat import reduce

import ibis.sql.compiler as comp
import ibis.expr.operations as ops

from pyspark.sql import DataFrame, Column as _Column, functions as F

from ibis.pandas.execution import constants
from ibis.pandas.dispatch import execute_node, execute
from ibis.pandas.core import integer_types


class Column(_Column):
    def __init__(self, jc, child):
        super(Column, self).__init__(jc)
        self.child = child


@execute_node.register(ops.Limit, DataFrame, integer_types, integer_types)
def spark_compile_limit(op, df, limit, offset, **kwargs):
    return df.limit(limit)


@execute_node.register(ops.TableColumn, DataFrame)
def spark_compile_column(op, df, **kwargs):
    return Column(df[op.name]._jc, df)


@execute_node.register(ops.Selection, DataFrame)
def spark_compile_selection(op, df, scope=None, **kwargs):
    result = df

    if op.selections:
        selections = [
            execute(selection, scope, **kwargs) for selection in op.selections
        ]
        result = result.select(selections)

    if op.predicates:
        predicates = reduce(
            operator.and_,
            [
                execute(predicate, scope, **kwargs)
                for predicate in op.predicates
            ]
        )
        result = result.where(predicates)

    if op.sort_keys:
        sort_keys = [
            execute(sort_key, scope, **kwargs) for sort_key in op.sort_keys
        ]
        result = result.orderBy(sort_keys)
    return result


@execute_node.register(ops.BinaryOp, Column, integer_types)
def spark_compile_comparison(op, left, right, **kwargs):
    op_type = type(op)
    try:
        operation = constants.BINARY_OPERATIONS[op_type]
    except KeyError:
        raise NotImplementedError(
            'Binary operation {} not implemented'.format(op_type.__name__)
        )
    else:
        return Column(operation(left, right), left.parent)


@execute_node.register(ops.Reduction, Column, type(None))
def spark_compile_reduction(op, data, mask, **kwargs):
    func = getattr(F, type(op).__name__.lower())
    return func(data)


@execute_node.register(ops.Reduction, Column, Column)
def spark_compile_reduction_masked(op, data, mask, **kwargs):
    func = getattr(F, type(op).__name__.lower())
    return func(F.when(mask, data).otherwise(None))


class SparkDialect(comp.Dialect):
    pass


dialect = SparkDialect
