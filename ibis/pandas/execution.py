import ibis.expr.types as ir
import ibis.expr.operations as ops
import ibis.expr.datatypes as dt

from ibis.pandas.dispatch import execute, execute_node


@execute_node.register(ops.PhysicalTable, pd.DataFrame)
def execute_physical_table_dataframe(op, data, scope=None):
    return data


@execute_node.register(ops.TableColumn, pd.DataFrame)
def execute_table_column_dataframe(op, data, scope=None):
    return data[op.name]


@execute.register(ir.Expr, dict)
def execute_with_scope(expr, scope):
    op = expr.op()
    evaluated_arguments = [
        execute(arg, scope) for arg in op.args if isinstance(arg, ir.Expr)
    ] or [
        scope.get(arg, arg) for arg in op.args
    ]
    return execute_node(op, *evaluated_arguments, scope=scope)
