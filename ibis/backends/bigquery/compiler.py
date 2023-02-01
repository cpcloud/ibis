"""Module to convert from Ibis expression to SQL string."""

from __future__ import annotations

from functools import partial

import sqlalchemy as sa
import sqlalchemy_bigquery as sab
import toolz
from google.cloud import bigquery as bq
from sqlalchemy.ext.compiler import compiles
from sqlalchemy_bigquery._types import STRUCT_FIELD_TYPES, _get_sqla_column_type

import ibis.common.graph as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis.backends.base.sql import compiler as sql_compiler
from ibis.backends.base.sql.alchemy.query_builder import AlchemyCompiler
from ibis.backends.base.sql.alchemy.translator import AlchemyExprTranslator
from ibis.backends.bigquery import operations, registry, rewrites


class BigQueryUDFDefinition(sql_compiler.DDL):
    """Represents definition of a temporary UDF."""

    def __init__(self, expr, context):
        self.expr = expr
        self.context = context

    def compile(self):
        """Generate UDF string from definition."""
        op = expr.op() if isinstance(expr := self.expr, ir.Expr) else expr
        return op.sql


def find_bigquery_udf(op):
    """Filter which includes only UDFs from expression tree."""
    if type(op) in BigQueryExprTranslator._rewrites:
        op = BigQueryExprTranslator._rewrites[type(op)](op)
    if isinstance(op, operations.BigQueryUDFNode):
        result = op
    else:
        result = None
    return lin.proceed, result


class BigQueryDialect(sab.BigQueryDialect):
    supports_statement_cache = sab.BigQueryDialect.supports_statement_cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        # Override this method to prevent field flattening
        table = self._get_table(connection, table_name, schema)

        return [
            {
                "name": field.name,
                "type": _get_sqla_column_type(field),
                "nullable": field.mode == "NULLABLE" or field.mode == "REPEATED",
                "comment": field.description,
                "default": None,
                "precision": field.precision,
                "scale": field.scale,
                "max_length": field.max_length,
            }
            for field in table.schema
        ]


class BigQueryExprTranslator(AlchemyExprTranslator):
    """Translate expressions to strings."""

    _registry = registry.OPERATION_REGISTRY
    _rewrites = rewrites.REWRITES

    _forbids_frame_clause = (
        *sql_compiler.ExprTranslator._forbids_frame_clause,
        ops.Lag,
        ops.Lead,
    )
    supports_unnest_in_select = False
    _dialect_name = "bigquery"


class BigQueryCompiler(AlchemyCompiler):
    translator_class = BigQueryExprTranslator

    support_values_syntax_in_select = False

    @staticmethod
    def _generate_setup_queries(expr, context):
        """Generate DDL for temporary resources."""
        nodes = lin.traverse(find_bigquery_udf, expr)
        queries = map(partial(BigQueryUDFDefinition, context=context), nodes)

        # UDFs are uniquely identified by the name of the Node subclass we
        # generate.
        def key(x):
            expr = x.expr
            op = expr.op() if isinstance(expr, ir.Expr) else expr
            return op.__class__.__name__

        return list(toolz.unique(queries, key=key))


# Register custom UDFs
import ibis.backends.bigquery.custom_udfs  # noqa:  F401, E402
