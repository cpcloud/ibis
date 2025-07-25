from __future__ import annotations

import contextlib
import copy
import datetime
import functools
import inspect
import itertools
import math
import operator
import os
import random
import string

import pytest
from pytest import param

import ibis
import ibis.common.exceptions as exc
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import ibis.selectors as s
from ibis.backends import _get_backend_names

pytestmark = [pytest.mark.benchmark]


def make_t():
    return ibis.table(
        [
            ("_timestamp", "int32"),
            ("dim1", "int32"),
            ("dim2", "int32"),
            ("valid_seconds", "int32"),
            ("meas1", "int32"),
            ("meas2", "int32"),
            ("year", "int32"),
            ("month", "int32"),
            ("day", "int32"),
            ("hour", "int32"),
            ("minute", "int32"),
        ],
        name="t",
    )


@pytest.fixture(scope="module")
def t():
    return make_t()


def make_base(t):
    return t.filter(
        (
            (t.year > 2016)
            | ((t.year == 2016) & (t.month > 6))
            | ((t.year == 2016) & (t.month == 6) & (t.day > 6))
            | ((t.year == 2016) & (t.month == 6) & (t.day == 6) & (t.hour > 6))
            | (
                (t.year == 2016)
                & (t.month == 6)
                & (t.day == 6)
                & (t.hour == 6)
                & (t.minute >= 5)
            )
        )
        & (
            (t.year < 2016)
            | ((t.year == 2016) & (t.month < 6))
            | ((t.year == 2016) & (t.month == 6) & (t.day < 6))
            | ((t.year == 2016) & (t.month == 6) & (t.day == 6) & (t.hour < 6))
            | (
                (t.year == 2016)
                & (t.month == 6)
                & (t.day == 6)
                & (t.hour == 6)
                & (t.minute <= 5)
            )
        )
    )


@pytest.fixture(scope="module")
def base(t):
    return make_base(t)


def make_large_expr(base):
    src_table = base
    src_table = src_table.mutate(
        _timestamp=(src_table["_timestamp"] - src_table["_timestamp"] % 3600)
        .cast("int32")
        .name("_timestamp"),
        valid_seconds=ibis.literal(300),
    )

    aggs = [src_table[f"meas{i}"].sum().cast("float").name(f"meas{i}") for i in (1, 2)]
    src_table = src_table.aggregate(
        aggs, by=["_timestamp", "dim1", "dim2", "valid_seconds"]
    )

    part_keys = ["year", "month", "day", "hour", "minute"]
    ts_col = src_table["_timestamp"].cast("timestamp")
    new_cols = {}
    for part_key in part_keys:
        part_col = getattr(ts_col, part_key)()
        new_cols[part_key] = part_col
    src_table = src_table.mutate(**new_cols)
    return src_table[
        [
            "_timestamp",
            "dim1",
            "dim2",
            "meas1",
            "meas2",
            "year",
            "month",
            "day",
            "hour",
            "minute",
        ]
    ]


@pytest.fixture(scope="module")
def large_expr(base):
    return make_large_expr(base)


@pytest.mark.benchmark(group="construction")
@pytest.mark.parametrize(
    "construction_fn",
    [
        pytest.param(lambda *_: make_t(), id="small"),
        pytest.param(lambda t, *_: make_base(t), id="medium"),
        pytest.param(lambda _, base: make_large_expr(base), id="large"),
    ],
)
def test_construction(benchmark, construction_fn, t, base):
    benchmark(construction_fn, t, base)


@pytest.mark.benchmark(group="builtins")
@pytest.mark.parametrize(
    "expr_fn",
    [
        pytest.param(lambda t, _base, _large_expr: t, id="small"),
        pytest.param(lambda _t, base, _large_expr: base, id="medium"),
        pytest.param(lambda _t, _base, large_expr: large_expr, id="large"),
    ],
)
@pytest.mark.parametrize("builtin", [hash, str])
def test_builtins(benchmark, expr_fn, builtin, t, base, large_expr):
    expr = expr_fn(t, base, large_expr)
    benchmark(builtin, expr)


_backends = _get_backend_names()

_XFAIL_COMPILE_BACKENDS = ("polars",)


@pytest.mark.benchmark(group="compilation")
@pytest.mark.parametrize(
    "module",
    [
        pytest.param(
            mod,
            marks=pytest.mark.xfail(
                condition=mod in _XFAIL_COMPILE_BACKENDS,
                reason=f"{mod} backend doesn't support compiling UnboundTable",
            ),
        )
        for mod in _backends
    ],
)
@pytest.mark.parametrize(
    "expr_fn",
    [
        pytest.param(lambda t, _base, _large_expr: t, id="small"),
        pytest.param(lambda _t, base, _large_expr: base, id="medium"),
        pytest.param(lambda _t, _base, large_expr: large_expr, id="large"),
    ],
)
def test_compile(benchmark, module, expr_fn, t, base, large_expr):
    try:
        mod = getattr(ibis, module)
    except (AttributeError, ImportError) as e:
        pytest.skip(str(e))
    else:
        expr = expr_fn(t, base, large_expr)
        try:
            benchmark(mod.compile, expr)
        except ImportError as e:  # delayed imports
            pytest.skip(str(e))


@pytest.fixture
def con():
    pytest.importorskip("duckdb")
    return ibis.duckdb.connect()


@pytest.fixture
def pt(con):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")

    n = 60_000
    data = pd.DataFrame(
        {
            "key": np.random.choice(16000, size=n),
            "low_card_key": np.random.choice(30, size=n),
            "value": np.random.rand(n),
            "timestamps": pd.date_range(
                start="2023-05-05 16:37:57", periods=n, freq="s"
            ).values,
            "timestamp_strings": pd.date_range(
                start="2023-05-05 16:37:39", periods=n, freq="s"
            ).values.astype(str),
            "repeated_timestamps": pd.date_range(start="2018-09-01", periods=30).repeat(
                int(n / 30)
            ),
        }
    )

    return con.create_table("df", data)


def high_card_group_by(t):
    return t.group_by(t.key).aggregate(avg_value=t.value.mean())


def cast_to_dates(t):
    return t.timestamps.cast(dt.date)


def cast_to_dates_from_strings(t):
    return t.timestamp_strings.cast(dt.date)


def multikey_group_by_with_mutate(t):
    return (
        t.mutate(dates=t.timestamps.cast("date"))
        .group_by(["low_card_key", "dates"])
        .aggregate(avg_value=lambda t: t.value.mean())
    )


def simple_sort(t):
    return t.order_by([t.key])


def simple_sort_projection(t):
    return t[["key", "value"]].order_by(["key"])


def multikey_sort(t):
    return t.order_by(["low_card_key", "key"])


def multikey_sort_projection(t):
    return t[["low_card_key", "key", "value"]].order_by(["low_card_key", "key"])


def low_card_rolling_window(t):
    return ibis.trailing_range_window(
        ibis.interval(days=2),
        order_by=t.repeated_timestamps,
        group_by=t.low_card_key,
    )


def low_card_grouped_rolling(t):
    return t.value.mean().over(low_card_rolling_window(t))


def high_card_rolling_window(t):
    return ibis.trailing_range_window(
        ibis.interval(days=2),
        order_by=t.repeated_timestamps,
        group_by=t.key,
    )


def high_card_grouped_rolling(t):
    return t.value.mean().over(high_card_rolling_window(t))


def low_card_window(t):
    return ibis.window(group_by=t.low_card_key)


def high_card_window(t):
    return ibis.window(group_by=t.key)


@pytest.mark.benchmark(group="execution")
@pytest.mark.parametrize(
    "expression_fn",
    [
        pytest.param(high_card_group_by, id="high_card_group_by"),
        pytest.param(cast_to_dates, id="cast_to_dates"),
        pytest.param(cast_to_dates_from_strings, id="cast_to_dates_from_strings"),
        pytest.param(multikey_group_by_with_mutate, id="multikey_group_by_with_mutate"),
        pytest.param(simple_sort, id="simple_sort"),
        pytest.param(simple_sort_projection, id="simple_sort_projection"),
        pytest.param(multikey_sort, id="multikey_sort"),
        pytest.param(multikey_sort_projection, id="multikey_sort_projection"),
        pytest.param(low_card_grouped_rolling, id="low_card_grouped_rolling"),
        pytest.param(high_card_grouped_rolling, id="high_card_grouped_rolling"),
    ],
)
def test_execute(benchmark, expression_fn, pt):
    expr = expression_fn(pt)
    benchmark(expr.execute)


@pytest.fixture(scope="module")
def part():
    return ibis.table(
        dict(
            p_partkey="int64",
            p_size="int64",
            p_type="string",
            p_mfgr="string",
        ),
        name="part",
    )


@pytest.fixture(scope="module")
def supplier():
    return ibis.table(
        dict(
            s_suppkey="int64",
            s_nationkey="int64",
            s_name="string",
            s_acctbal="decimal(15, 3)",
            s_address="string",
            s_phone="string",
            s_comment="string",
        ),
        name="supplier",
    )


@pytest.fixture(scope="module")
def partsupp():
    return ibis.table(
        dict(
            ps_partkey="int64",
            ps_suppkey="int64",
            ps_supplycost="decimal(15, 3)",
        ),
        name="partsupp",
    )


@pytest.fixture(scope="module")
def nation():
    return ibis.table(
        dict(n_nationkey="int64", n_regionkey="int64", n_name="string"),
        name="nation",
    )


@pytest.fixture(scope="module")
def region():
    return ibis.table(dict(r_regionkey="int64", r_name="string"), name="region")


@pytest.fixture(scope="module")
def tpc_h02(part, supplier, partsupp, nation, region):
    REGION = "EUROPE"
    SIZE = 25
    TYPE = "BRASS"

    expr = (
        part.join(partsupp, part.p_partkey == partsupp.ps_partkey)
        .join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = subexpr.filter(
        (subexpr.r_name == REGION) & (expr.p_partkey == subexpr.ps_partkey)
    )

    filters = [
        expr.p_size == SIZE,
        expr.p_type.like(f"%{TYPE}"),
        expr.r_name == REGION,
        expr.ps_supplycost == subexpr.ps_supplycost.min(),
    ]
    q = expr.filter(filters)

    q = q.select(
        [
            q.s_acctbal,
            q.s_name,
            q.n_name,
            q.p_partkey,
            q.p_mfgr,
            q.s_address,
            q.s_phone,
            q.s_comment,
        ]
    )

    return q.order_by(
        [
            ibis.desc(q.s_acctbal),
            q.n_name,
            q.s_name,
            q.p_partkey,
        ]
    ).limit(100)


@pytest.mark.benchmark(group="repr")
def test_repr_tpc_h02(benchmark, tpc_h02):
    benchmark(repr, tpc_h02)


@pytest.mark.benchmark(group="repr")
def test_repr_huge_union(benchmark):
    n = 10
    raw_types = [
        "int64",
        "float64",
        "string",
        "array<struct<a: array<string>, b: map<string, array<int64>>>>",
    ]
    tables = [
        ibis.table(
            list(zip(string.ascii_letters, itertools.cycle(raw_types))),
            name=f"t{i:d}",
        )
        for i in range(n)
    ]
    expr = functools.reduce(ir.Table.union, tables)
    benchmark(repr, expr)


@pytest.mark.benchmark(group="node_args")
def test_op_argnames(benchmark):
    t = ibis.table([("a", "int64")])
    expr = t[["a"]]
    benchmark(lambda op: op.argnames, expr.op())


@pytest.mark.benchmark(group="node_args")
def test_op_args(benchmark):
    t = ibis.table([("a", "int64")])
    expr = t[["a"]]
    benchmark(lambda op: op.args, expr.op())


@pytest.mark.benchmark(group="datatype")
def test_complex_datatype_parse(benchmark):
    type_str = "array<struct<a: array<string>, b: map<string, array<int64>>>>"
    expected = dt.Array(
        dt.Struct(dict(a=dt.Array(dt.string), b=dt.Map(dt.string, dt.Array(dt.int64))))
    )
    assert dt.parse(type_str) == expected
    benchmark(dt.parse, type_str)


@pytest.mark.benchmark(group="datatype")
@pytest.mark.parametrize("func", [str, hash])
def test_complex_datatype_builtins(benchmark, func):
    datatype = dt.Array(
        dt.Struct(dict(a=dt.Array(dt.string), b=dt.Map(dt.string, dt.Array(dt.int64))))
    )
    benchmark(func, datatype)


@pytest.mark.benchmark(group="equality")
def test_large_expr_equals(benchmark, tpc_h02):
    benchmark(ir.Expr.equals, tpc_h02, copy.deepcopy(tpc_h02))


@pytest.mark.benchmark(group="datatype")
@pytest.mark.parametrize(
    "dtypes",
    [
        pytest.param(
            [
                obj
                for _, obj in inspect.getmembers(
                    dt,
                    lambda obj: isinstance(obj, dt.DataType),
                )
            ],
            id="singletons",
        ),
        pytest.param(
            dt.Array(
                dt.Struct(
                    dict(
                        a=dt.Array(dt.string),
                        b=dt.Map(dt.string, dt.Array(dt.int64)),
                    )
                )
            ),
            id="complex",
        ),
    ],
)
def test_eq_datatypes(benchmark, dtypes):
    def eq(a, b):
        assert a == b

    benchmark(eq, dtypes, copy.deepcopy(dtypes))


def multiple_joins(table, num_joins):
    for _ in range(num_joins):
        table = table.mutate(dummy=ibis.literal(""))
        table = table.left_join(table.view(), ["dummy"]).select(table)


@pytest.mark.parametrize("num_joins", [1, 10])
@pytest.mark.parametrize("num_columns", [1, 10, 100])
def test_multiple_joins(benchmark, num_joins, num_columns):
    table = ibis.table(
        {f"col_{i:d}": "string" for i in range(num_columns)},
        name="t",
    )
    benchmark(multiple_joins, table, num_joins)


@pytest.fixture
def customers():
    return ibis.table(
        dict(
            customerid="int32",
            name="string",
            address="string",
            citystatezip="string",
            birthdate="date",
            phone="string",
            timezone="string",
            lat="float64",
            long="float64",
        ),
        name="customers",
    )


@pytest.fixture
def orders():
    return ibis.table(
        dict(
            orderid="int32",
            customerid="int32",
            ordered="timestamp",
            shipped="timestamp",
            items="string",
            total="float64",
        ),
        name="orders",
    )


@pytest.fixture
def orders_items():
    return ibis.table(
        dict(orderid="int32", sku="string", qty="int32", unit_price="float64"),
        name="orders_items",
    )


@pytest.fixture
def products():
    return ibis.table(
        dict(
            sku="string",
            desc="string",
            weight_kg="float64",
            cost="float64",
            dims_cm="string",
        ),
        name="products",
    )


@pytest.mark.benchmark(group="compilation")
@pytest.mark.parametrize(
    "module",
    [
        pytest.param(
            mod,
            marks=pytest.mark.xfail(
                condition=mod in _XFAIL_COMPILE_BACKENDS,
                reason=f"{mod} backend doesn't support compiling UnboundTable",
            ),
        )
        for mod in _backends
    ],
)
def test_compile_with_drops(
    benchmark, module, customers, orders, orders_items, products
):
    expr = (
        customers.join(orders, "customerid")
        .join(orders_items, "orderid")
        .join(products, "sku")
        .drop("customerid", "qty", "total", "items")
        .drop("dims_cm", "cost")
        .mutate(o_date=lambda t: t.shipped)
        .filter(lambda t: t.ordered == t.shipped)
    )

    try:
        mod = getattr(ibis, module)
    except (AttributeError, ImportError) as e:
        pytest.skip(str(e))
    else:
        benchmark(mod.compile, expr)


def test_repr_join(benchmark, customers, orders, orders_items, products):
    expr = (
        customers.join(orders, "customerid")
        .join(orders_items, "orderid")
        .join(products, "sku")
        .drop("customerid", "qty", "total", "items")
    )
    op = expr.op()
    benchmark(repr, op)


@pytest.mark.parametrize("overwrite", [True, False], ids=["overwrite", "no_overwrite"])
def test_insert_duckdb(benchmark, overwrite, tmp_path):
    pytest.importorskip("duckdb")

    n_rows = int(1e4)
    table_name = "t"
    schema = ibis.schema(dict(a="int64", b="int64", c="int64"))
    t = ibis.memtable(dict.fromkeys(list("abc"), range(n_rows)), schema=schema)

    con = ibis.duckdb.connect(tmp_path / "test_insert.ddb")
    con.create_table(table_name, schema=schema)
    benchmark(con.insert, table_name, t, overwrite=overwrite)


def test_snowflake_medium_sized_to_pandas(benchmark):
    pytest.importorskip("snowflake.connector")

    if (url := os.environ.get("SNOWFLAKE_URL")) is None:
        pytest.skip("SNOWFLAKE_URL environment variable not set")

    con = ibis.connect(url)

    # LINEITEM at scale factor 1 is around 6MM rows, but we limit to 1,000,000
    # to make the benchmark fast enough for development, yet large enough to show a
    # difference if there's a performance hit
    lineitem = con.table(
        "LINEITEM", database=("SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1")
    ).limit(1_000_000)

    benchmark.pedantic(lineitem.to_pandas, rounds=5, iterations=1, warmup_rounds=1)


def test_parse_many_duckdb_types(benchmark):
    from ibis.backends.sql.datatypes import DuckDBType

    def parse_many(types):
        list(map(DuckDBType.from_string, types))

    types = ["VARCHAR", "INTEGER", "DOUBLE", "BIGINT"] * 1000
    benchmark(parse_many, types)


@pytest.fixture(scope="session")
def sql() -> str:
    return """
    SELECT t1.id as t1_id, x, t2.id as t2_id, y
    FROM t1 INNER JOIN t2
      ON t1.id = t2.id
    """


@pytest.fixture(scope="session")
def ddb(tmp_path_factory):
    duckdb = pytest.importorskip("duckdb")

    N = 20_000_000

    path = str(tmp_path_factory.mktemp("duckdb") / "data.ddb")
    sql = (
        lambda var, table, n=N: f"""
        CREATE TABLE {table} AS
        SELECT ROW_NUMBER() OVER () AS id, {var}
        FROM (
            SELECT {var}
            FROM RANGE({n}) _ ({var})
            ORDER BY RANDOM()
        )
        """
    )

    with duckdb.connect(path) as cur:
        cur.execute(sql("x", table="t1"))
        cur.execute(sql("y", table="t2"))
    return path


def test_duckdb_to_pyarrow(benchmark, sql, ddb) -> None:
    # yes, we're benchmarking duckdb here, not ibis
    #
    # we do this to get a baseline for comparison
    pytest.importorskip("pyarrow")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(ddb, read_only=True)

    benchmark(lambda sql: con.sql(sql).to_arrow_table(), sql)


def test_ibis_duckdb_to_pyarrow(benchmark, sql, ddb) -> None:
    pytest.importorskip("pyarrow")
    pytest.importorskip("duckdb")

    con = ibis.duckdb.connect(ddb, read_only=True)

    expr = con.sql(sql)
    benchmark(expr.to_pyarrow)


@pytest.fixture
def diffs():
    return ibis.table(
        {
            "id": "int64",
            "validation_name": "string",
            "difference": "float64",
            "pct_difference": "float64",
            "pct_threshold": "float64",
            "validation_status": "string",
        },
        name="diffs",
    )


@pytest.fixture
def srcs():
    return ibis.table(
        {
            "id": "int64",
            "validation_name": "string",
            "validation_type": "string",
            "aggregation_type": "string",
            "table_name": "string",
            "column_name": "string",
            "primary_keys": "string",
            "num_random_rows": "string",
            "agg_value": "float64",
        },
        name="srcs",
    )


@pytest.fixture
def nrels():
    return 50


def make_big_union(t, nrels):
    return ibis.union(*[t] * nrels)


@pytest.fixture
def src(srcs, nrels):
    return make_big_union(srcs, nrels)


@pytest.fixture
def diff(diffs, nrels):
    return make_big_union(diffs, nrels)


def test_big_eq_expr(benchmark, src, diff):
    benchmark(ops.core.Node.equals, src.op(), diff.op())


def test_big_join_expr(benchmark, src, diff):
    benchmark(ir.Table.join, src, diff, ["validation_name"], how="outer")


def test_big_join_compile(benchmark, src, diff):
    pytest.importorskip("duckdb")

    expr = src.join(diff, ["validation_name"], how="outer")
    t = benchmark.pedantic(
        lambda expr=expr: ibis.to_sql(expr, dialect="duckdb"),
        rounds=1,
        iterations=1,
        warmup_rounds=1,
    )
    assert len(t)


@pytest.mark.timeout(5)
def test_big_expression_compile(benchmark):
    pytest.importorskip("duckdb")

    from ibis.tests.benchmarks.benchfuncs import clean_names

    t = ibis.table(
        schema={
            "id": "int64",
            "prefix": "string",
            "first_name": "string",
            "middle_name": "string",
            "last_name": "string",
            "suffix": "string",
            "nickname": "string",
        },
        name="names",
    )
    t2 = clean_names(t)

    assert benchmark(ibis.to_sql, t2, dialect="duckdb")


@pytest.fixture(scope="module")
def many_cols():
    return ibis.table({f"x{i:d}": "int" for i in range(10000)}, name="t")


@pytest.mark.parametrize(
    "getter",
    [lambda t: t["x0"], lambda t: t[0], lambda t: t.x0],
    ids=["str", "int", "attr"],
)
def test_column_access(benchmark, many_cols, getter):
    benchmark(getter, many_cols)


@pytest.fixture(scope="module", params=[1000, 10000])
def many_tables(request):
    num_cols = 10
    return [
        ibis.table({f"c{i}": "int" for i in range(num_cols)})
        for _ in range(request.param)
    ]


def test_large_union_construct(benchmark, many_tables):
    assert benchmark(lambda args: ibis.union(*args), many_tables) is not None


@pytest.mark.timeout(180)
def test_large_union_compile(benchmark, many_tables):
    pytest.importorskip("duckdb")

    expr = ibis.union(*many_tables)
    assert benchmark(ibis.to_sql, expr, dialect="duckdb") is not None


@pytest.mark.parametrize("cols", [128, 256])
@pytest.mark.parametrize("op", ["construct", "compile"])
def test_large_add(benchmark, cols, op):
    t = ibis.table(name="t", schema={f"x{i}": "int" for i in range(cols)})

    def construct():
        return functools.reduce(operator.add, (t[c] for c in t.columns))

    def compile(expr):
        return ibis.to_sql(expr, dialect="duckdb")

    if op == "construct":
        benchmark(construct)
    else:
        benchmark(compile, construct())


@pytest.fixture(scope="session")
def lots_of_tables(tmp_path_factory):
    duckdb = pytest.importorskip("duckdb")
    db = str(tmp_path_factory.mktemp("data") / "lots_of_tables.ddb")
    n = 100_000
    d = int(math.log10(n))
    sql = ";".join(f"CREATE TABLE t{i:0>{d}} (x TINYINT)" for i in range(n))
    with duckdb.connect(db) as con:
        con.execute(sql)
    return ibis.duckdb.connect(db)


@pytest.mark.timeout(120)
def test_memtable_register(lots_of_tables, benchmark):
    t = ibis.memtable({"x": [1, 2, 3]})
    result = benchmark(lots_of_tables.execute, t)
    assert len(result) == 3


@pytest.fixture(params=[10, 100, 1_000, 10_000], scope="module")
def wide_table(request):
    num_cols = request.param
    return ibis.table(name="t", schema={f"a{i}": "int" for i in range(num_cols)})


@pytest.fixture(
    params=[param(0.01, id="1"), param(0.5, id="50"), param(0.99, id="99")],
    scope="module",
)
def cols_to_drop(wide_table, request):
    perc_cols_to_drop = request.param
    total_cols = len(wide_table.columns)
    ncols = math.floor(perc_cols_to_drop * total_cols)
    cols_to_drop = random.sample(range(total_cols), ncols)
    return [f"a{i}" for i in cols_to_drop]


def test_wide_drop_construct(benchmark, wide_table, cols_to_drop):
    benchmark(wide_table.drop, *cols_to_drop)


def test_wide_drop_compile(benchmark, wide_table, cols_to_drop):
    pytest.importorskip("duckdb")

    benchmark(
        lambda expr: ibis.to_sql(expr, dialect="duckdb"), wide_table.drop(*cols_to_drop)
    )


@pytest.mark.parametrize(
    "method",
    [
        "snake_case",
        "ALL_CAPS",
        lambda x: f"t_{x}",
        "t_{name}",
        lambda x: x,
        "{name}",
        {"b0": "a0"},
    ],
    ids=[
        "snake_case",
        "ALL_CAPS",
        "function",
        "format_string",
        "no_op_function",
        "no_op_string",
        "mapping",
    ],
)
@pytest.mark.parametrize("cols", [1_000, 10_000])
def test_wide_rename(benchmark, method, cols):
    t = ibis.table(name="t", schema={f"a{i}": "int" for i in range(cols)})
    benchmark(t.rename, method)


@pytest.mark.parametrize(
    ("input", "column", "relative"),
    [("before", "a{}", "a0"), ("after", "a0", "a{}")],
    ids=["before", "after"],
)
@pytest.mark.parametrize("cols", [10, 100, 1_000, 10_000])
def test_wide_relocate(benchmark, input, column, relative, cols):
    last = cols - 1
    t = ibis.table(name="t", schema={f"a{i}": "int" for i in range(cols)})
    benchmark(t.relocate, column.format(last), **{input: relative.format(last)})


def test_duckdb_timestamp_conversion(benchmark, con):
    start = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    stop = datetime.datetime(2000, 2, 1, tzinfo=datetime.timezone.utc)
    expr = ibis.range(start, stop, ibis.interval(seconds=1)).unnest()

    series = benchmark(con.execute, expr)
    assert series.size == (stop - start).total_seconds()


@pytest.mark.parametrize("cols", [1_000, 10_000])
def test_selectors(benchmark, cols):
    t = ibis.table(name="t", schema={f"col{i}": "int" for i in range(cols)})
    n = cols - cols // 10
    sel = s.across(s.cols(*[f"col{i}" for i in range(n)]), lambda c: c.cast("str"))
    benchmark(sel.expand, t)


@pytest.mark.parametrize("ncols", [10_000, 100_000, 1_000_000])
def test_dot_columns(benchmark, ncols):
    t = ibis.table(name="t", schema={f"col{i}": "int" for i in range(ncols)})
    result = benchmark(lambda t: t.columns, t)
    assert len(result) == ncols


def test_dedup_schema_failure_mode(benchmark):
    def dedup_schema(pairs):
        with contextlib.suppress(exc.IntegrityError):
            sch.Schema.from_tuples(pairs)

    benchmark(
        dedup_schema,
        [("a", "int"), ("b", "string"), ("c", "array<int>"), ("d", "float")] * 2_500,
    )


def test_dedup_schema(benchmark):
    benchmark(
        sch.Schema.from_tuples,
        zip(
            map("col{}".format, range(10_000)),
            itertools.cycle(("int", "string", "array<int>", "float")),
        ),
    )


@pytest.fixture(scope="session")
def pgtable(data_dir):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("psycopg")

    from ibis.backends.postgres.tests.conftest import (
        IBIS_TEST_POSTGRES_DB,
        PG_HOST,
        PG_PASS,
        PG_PORT,
        PG_USER,
    )

    con = ibis.postgres.connect(
        user=PG_USER,
        password=PG_PASS,
        host=PG_HOST,
        port=PG_PORT,
        database=IBIS_TEST_POSTGRES_DB,
    )
    name = ibis.util.gen_name("functional_alltypes_bench")
    yield con.create_table(
        name, obj=pd.read_csv(data_dir / "csv" / "functional_alltypes.csv"), temp=True
    )
    con.disconnect()


def test_postgres_record_batches(pgtable, benchmark):
    benchmark(pgtable.to_pyarrow)
