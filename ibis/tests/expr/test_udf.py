import pytest

import ibis
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops


@pytest.fixture
def table():
    return ibis.table(
        [
            ("a", "int8"),
            ("b", "string"),
            ("c", "bool"),
        ],
        name="test",
    )


@pytest.mark.parametrize(
    "klass",
    [
        ops.ElementWiseVectorizedUDF,
        ops.ReductionVectorizedUDF,
        ops.AnalyticVectorizedUDF,
    ],
)
def test_vectorized_udf_operations(table, klass):
    udf = klass(
        func=lambda a, *_: a,  # noqa: U101
        func_args=[table.a, table.b, table.c],
        input_type=[dt.int8(), dt.string(), dt.boolean()],
        return_type=dt.int8(),
    )
    assert udf.func_args[0].equals(table.a)
    assert udf.func_args[1].equals(table.b)
    assert udf.func_args[2].equals(table.c)
    assert udf.input_type == tuple([dt.int8(), dt.string(), dt.boolean()])
    assert udf.return_type == dt.int8()

    expr = udf.to_expr()
    assert isinstance(expr, udf.output_type)

    with pytest.raises(com.IbisTypeError):
        # wrong function type
        klass(
            func=1,
            func_args=[ibis.literal(1), table.b, table.c],
            input_type=[dt.int8(), dt.string(), dt.boolean()],
            return_type=dt.int8(),
        )

    with pytest.raises(com.IbisTypeError):
        # scalar type instead of column type
        klass(
            func=lambda a, *_: a,  # noqa: U101
            func_args=[ibis.literal(1), table.b, table.c],
            input_type=[dt.int8(), dt.string(), dt.boolean()],
            return_type=dt.int8(),
        )

    with pytest.raises(com.IbisTypeError):
        # wrong input type
        klass(
            func=lambda a, *_: a,  # noqa: U101
            func_args=[ibis.literal(1), table.b, table.c],
            input_type="int8",
            return_type=dt.int8(),
        )

    with pytest.raises(com.IbisTypeError):
        # wrong return type
        klass(
            func=lambda a, *_: a,  # noqa: U101
            func_args=[ibis.literal(1), table.b, table.c],
            input_type=[dt.int8(), dt.string(), dt.boolean()],
            return_type=table,
        )
