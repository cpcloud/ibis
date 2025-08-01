from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import pytest
import sqlglot as sg
import sqlglot.expressions as sge

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
from ibis.common.exceptions import IntegrityError
from ibis.common.grounds import Annotable
from ibis.common.patterns import CoercedTo


def test_whole_schema():
    schema = {
        "cid": "int64",
        "mktsegment": "string",
        "address": """struct<city: string,
                             street: string,
                             street_number: int32,
                             zip: int16>""",
        "phone_numbers": "array<string>",
        "orders": """array<struct<oid: int64,
                                  status: string,
                                  totalprice: decimal(12, 2),
                                  order_date: string,
                                  items: array<struct<iid: int64,
                                                      name: string,
                                                      price: decimal(12, 2),
                                                      discount_perc: decimal(12, 2),
                                                      shipdate: string>>>>
                                                      """,
        "web_visits": """map<string, struct<user_agent: string,
                                            client_ip: string,
                                            visit_date: string,
                                            duration_ms: int32>>""",
        "support_calls": """array<struct<agent_id: int64,
                                         call_date: string,
                                         duration_ms: int64,
                                         issue_resolved: boolean,
                                         agent_comment: string>>""",
    }
    expected = {
        "cid": dt.int64,
        "mktsegment": dt.string,
        "address": dt.Struct(
            {
                "city": dt.string,
                "street": dt.string,
                "street_number": dt.int32,
                "zip": dt.int16,
            }
        ),
        "phone_numbers": dt.Array(dt.string),
        "orders": dt.Array(
            dt.Struct(
                {
                    "oid": dt.int64,
                    "status": dt.string,
                    "totalprice": dt.Decimal(12, 2),
                    "order_date": dt.string,
                    "items": dt.Array(
                        dt.Struct(
                            {
                                "iid": dt.int64,
                                "name": dt.string,
                                "price": dt.Decimal(12, 2),
                                "discount_perc": dt.Decimal(12, 2),
                                "shipdate": dt.string,
                            }
                        )
                    ),
                }
            )
        ),
        "web_visits": dt.Map(
            dt.string,
            dt.Struct(
                {
                    "user_agent": dt.string,
                    "client_ip": dt.string,
                    "visit_date": dt.string,
                    "duration_ms": dt.int32,
                }
            ),
        ),
        "support_calls": dt.Array(
            dt.Struct(
                {
                    "agent_id": dt.int64,
                    "call_date": dt.string,
                    "duration_ms": dt.int64,
                    "issue_resolved": dt.boolean,
                    "agent_comment": dt.string,
                }
            )
        ),
    }
    assert sch.schema(schema) == sch.schema(expected)


def test_schema_from_tuples():
    schema = sch.Schema.from_tuples(
        [
            ("a", "int64"),
            ("b", "string"),
            ("c", "double"),
            ("d", "boolean"),
        ]
    )
    expected = sch.Schema(
        {"a": dt.int64, "b": dt.string, "c": dt.double, "d": dt.boolean}
    )

    assert schema == expected

    # test that duplicate field names are prohibited
    with pytest.raises(IntegrityError):
        sch.Schema.from_tuples([("a", "int64"), ("a", "string")])


def test_schema_subset():
    s1 = sch.schema([("a", dt.int64), ("b", dt.int32), ("c", dt.string)])
    s2 = sch.schema([("a", dt.int64), ("c", dt.string)])

    assert s1 > s2
    assert s2 < s1

    assert s1 >= s2
    assert s2 <= s1


def test_empty_schema():
    s1 = sch.Schema({})
    s2 = sch.schema([])

    assert s1 == s2

    for s in [s1, s2]:
        assert len(s.items()) == 0
        assert repr(s) == "ibis.Schema {\n}"


def test_nullable_output():
    s = sch.schema(
        [
            ("foo", "int64"),
            ("bar", dt.int64(nullable=False)),
            ("baz", "boolean"),
        ]
    )

    sch_str = str(s)
    assert "foo  int64" in sch_str
    assert "foo  !int64" not in sch_str
    assert "bar  !int64" in sch_str
    assert "baz  boolean" in sch_str
    assert "baz  !boolean" not in sch_str


def test_api_accepts_schema_objects():
    s1 = sch.schema(dict(a="int", b="str"))
    s2 = sch.schema(s1)
    assert s1 == s2


def test_schema_mapping_api():
    s = sch.Schema(
        {
            "a": "map<double, string>",
            "b": "array<map<string, array<int32>>>",
            "c": "array<string>",
            "d": "int8",
        }
    )

    assert s["a"] == dt.Map(dt.double, dt.string)
    assert s["b"] == dt.Array(dt.Map(dt.string, dt.Array(dt.int32)))
    assert s["c"] == dt.Array(dt.string)
    assert s["d"] == dt.int8

    assert "a" in s
    assert "e" not in s
    assert len(s) == 4
    assert tuple(s) == s.names
    assert tuple(s.keys()) == s.names
    assert tuple(s.values()) == s.types
    assert tuple(s.items()) == tuple(zip(s.names, s.types))


def test_schema_equality():
    s1 = sch.schema({"a": "int64", "b": "string"})
    s2 = sch.schema({"a": "int64", "b": "string"})
    s3 = sch.schema({"b": "string", "a": "int64"})
    s4 = sch.schema({"a": "int64", "b": "int64", "c": "string"})

    assert s1 == s2
    assert s1 != s3
    assert s1 != s4
    assert s3 != s2


class BarSchema:
    a: int
    b: str


class FooSchema:
    a: int
    b: str
    c: float
    d: tuple[str]
    e: list[int]
    f: dict[str, int]
    g: BarSchema
    h: list[BarSchema]
    j: dict[str, BarSchema]


foo_schema = sch.Schema(
    {
        "a": "int64",
        "b": "string",
        "c": "float64",
        "d": "array<string>",
        "e": "array<int64>",
        "f": "map<string, int64>",
        "g": "struct<a: int64, b: string>",
        "h": "array<struct<a: int64, b: string>>",
        "j": "map<string, struct<a: int64, b: string>>",
    }
)


def test_schema_from_annotated_class():
    assert sch.schema(FooSchema) == foo_schema


class NamedBar(NamedTuple):
    a: int
    b: str


class NamedFoo(NamedTuple):
    a: int
    b: str
    c: float
    d: tuple[str]
    e: list[int]
    f: dict[str, int]
    g: NamedBar
    h: list[NamedBar]
    j: dict[str, NamedBar]


def test_schema_from_namedtuple():
    assert sch.schema(NamedFoo) == foo_schema


@dataclass
class DataBar:
    a: int
    b: str


@dataclass
class DataFooBase:
    a: int
    b: str
    c: float
    d: tuple[str]


@dataclass
class DataFoo(DataFooBase):
    e: list[int]
    f: dict[str, int]
    g: DataBar
    h: list[DataBar]
    j: dict[str, DataBar]


def test_schema_from_dataclass():
    assert sch.schema(DataFoo) == foo_schema


class PreferenceA:
    a: dt.int64
    b: dt.Array(dt.int64)


class PreferenceB:
    a: dt.int64
    b: dt.Array[dt.int64]


class PreferenceC:
    a: dt.Int64
    b: dt.Array[dt.Int64]


def test_preferences():
    a = sch.schema(PreferenceA)
    b = sch.schema(PreferenceB)
    c = sch.schema(PreferenceC)
    assert a == b == c


class ObjectWithSchema(Annotable):
    schema: sch.Schema


def test_schema_is_coercible():
    s = sch.Schema({"a": dt.int64, "b": dt.Array(dt.int64)})
    assert CoercedTo(sch.Schema).match(PreferenceA, {}) == s

    o = ObjectWithSchema(schema=PreferenceA)
    assert o.schema == s


def test_schema_set_operations():
    a = sch.Schema({"a": dt.string, "b": dt.int64, "c": dt.float64})
    b = sch.Schema({"a": dt.string, "c": dt.float64, "d": dt.boolean, "e": dt.date})
    c = sch.Schema({"i": dt.int64, "j": dt.float64, "k": dt.string})
    d = sch.Schema({"i": dt.int64, "j": dt.float64, "k": dt.string, "l": dt.boolean})

    assert a & b == sch.Schema({"a": dt.string, "c": dt.float64})
    assert a | b == sch.Schema(
        {"a": dt.string, "b": dt.int64, "c": dt.float64, "d": dt.boolean, "e": dt.date}
    )
    assert a - b == sch.Schema({"b": dt.int64})
    assert b - a == sch.Schema({"d": dt.boolean, "e": dt.date})
    assert a ^ b == sch.Schema({"b": dt.int64, "d": dt.boolean, "e": dt.date})

    assert not a.isdisjoint(b)
    assert a.isdisjoint(c)

    assert a <= a
    assert a >= a
    assert not a < a
    assert not a > a
    assert not a <= b
    assert not a >= b
    assert not a >= c
    assert not a <= c
    assert c <= d
    assert c < d
    assert d >= c
    assert d > c


@pytest.mark.parametrize("lazy", [False, True])
def test_schema_infer_polars_dataframe(lazy):
    pl = pytest.importorskip("polars")
    df = pl.DataFrame(
        {"a": [1, 2, 3], "b": ["a", "b", "c"], "c": [True, False, True]},
        schema={"a": pl.Int64, "b": pl.Utf8, "c": pl.Boolean},
    )
    if lazy:
        df = df.lazy()
    s = sch.infer(df)
    assert s == sch.Schema({"a": dt.int64, "b": dt.string, "c": dt.boolean})


def test_schema_from_to_polars_schema():
    pl = pytest.importorskip("polars")

    polars_schema = {
        "a": pl.Int64,
        "b": pl.Utf8,
        "c": pl.Boolean,
    }
    ibis_schema = sch.Schema({"a": dt.int64, "b": dt.string, "c": dt.boolean})

    res = sch.Schema.from_polars(polars_schema)
    assert res == ibis_schema

    res = ibis_schema.to_polars()
    assert res == polars_schema


def test_schema_from_to_numpy_dtypes():
    np = pytest.importorskip("np")
    numpy_dtypes = [
        ("a", np.dtype("int64")),
        ("b", np.dtype("str")),
        ("c", np.dtype("bool")),
    ]
    ibis_schema = sch.Schema.from_numpy(numpy_dtypes)
    assert ibis_schema == sch.Schema({"a": dt.int64, "b": dt.string, "c": dt.boolean})

    restored_dtypes = ibis_schema.to_numpy()
    expected_dtypes = [
        ("a", np.dtype("int64")),
        ("b", np.dtype("object")),
        ("c", np.dtype("bool")),
    ]
    assert restored_dtypes == expected_dtypes


def test_schema_from_to_pandas_dtypes():
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    pandas_schema = pd.Series(
        [
            ("a", np.dtype("int64")),
            ("b", np.dtype("str")),
            ("c", pd.CategoricalDtype(["a", "b", "c"])),
            ("d", pd.DatetimeTZDtype(tz="US/Eastern", unit="ns")),
        ]
    )
    ibis_schema = sch.Schema.from_pandas(pandas_schema)
    assert ibis_schema == sch.schema(pandas_schema)

    expected = sch.Schema(
        {
            "a": dt.int64,
            "b": dt.string,
            "c": dt.string,
            "d": dt.Timestamp(timezone="US/Eastern"),
        }
    )
    assert ibis_schema == expected

    restored_dtypes = ibis_schema.to_pandas()
    expected_dtypes = [
        ("a", np.dtype("int64")),
        ("b", np.dtype("object")),
        ("c", np.dtype("object")),
        ("d", pd.DatetimeTZDtype(tz="US/Eastern", unit="ns")),
    ]
    assert restored_dtypes == expected_dtypes


def test_null_fields():
    assert sch.schema({"a": "int64", "b": "string"}).null_fields == ()
    assert sch.schema({"a": "null", "b": "string"}).null_fields == ("a",)
    assert sch.schema({"a": "null", "b": "null"}).null_fields == ("a", "b")


def test_to_sqlglot_column_defs():
    schema = sch.schema({"a": "int64", "b": "string", "c": "!string"})
    columns = schema.to_sqlglot_column_defs("duckdb")

    assert len(columns) == 3
    assert all(isinstance(col, sge.ColumnDef) for col in columns)
    assert all(col.this.quoted is True for col in columns)
    assert [col.this.this for col in columns] == ["a", "b", "c"]

    assert not columns[0].constraints
    assert not columns[1].constraints
    assert len(columns[2].constraints) == 1
    assert isinstance(columns[2].constraints[0].kind, sge.NotNullColumnConstraint)


def test_to_sqlglot_column_defs_empty_schema():
    schema = sch.schema({})
    columns = schema.to_sqlglot_column_defs("duckdb")
    assert columns == []


def test_to_sqlglot_column_defs_create_table_integration():
    schema = sch.schema({"id": "!int64", "name": "string"})
    columns = schema.to_sqlglot_column_defs("duckdb")

    table = sg.table("test_table", quoted=True)
    create_stmt = sge.Create(
        kind="TABLE",
        this=sge.Schema(this=table, expressions=columns),
    )

    sql = create_stmt.sql(dialect="duckdb")
    expected = 'CREATE TABLE "test_table" ("id" BIGINT NOT NULL, "name" TEXT)'
    assert sql == expected


def test_schema_from_sqlglot():
    columns = [
        sge.ColumnDef(
            this=sg.to_identifier("bigint_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.BIGINT),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("int_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.INT),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("smallint_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.SMALLINT),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("tinyint_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.TINYINT),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("double_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.DOUBLE),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("float_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.FLOAT),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("varchar_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.VARCHAR),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("text_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.TEXT),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("boolean_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.BOOLEAN),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("date_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.DATE),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("timestamp_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.DATETIME),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("time_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.TIME),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("binary_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.BINARY),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("uuid_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.UUID),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("json_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.JSON),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("decimal_col", quoted=True),
            kind=sge.DataType(
                this=sge.DataType.Type.DECIMAL,
                expressions=[
                    sge.DataTypeParam(this=sge.Literal.number(10)),
                    sge.DataTypeParam(this=sge.Literal.number(2)),
                ],
            ),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("not_null_col", quoted=True),
            kind=sge.DataType(this=sge.DataType.Type.VARCHAR),
            constraints=[sge.ColumnConstraint(kind=sge.NotNullColumnConstraint())],
        ),
        sge.ColumnDef(
            this=sg.to_identifier("array_col", quoted=True),
            kind=sge.DataType(
                this=sge.DataType.Type.ARRAY,
                expressions=[sge.DataType(this=sge.DataType.Type.VARCHAR)],
                nested=True,
            ),
        ),
        sge.ColumnDef(
            this=sg.to_identifier("map_col", quoted=True),
            kind=sge.DataType(
                this=sge.DataType.Type.MAP,
                expressions=[
                    sge.DataType(this=sge.DataType.Type.VARCHAR),
                    sge.DataType(this=sge.DataType.Type.INT),
                ],
                nested=True,
            ),
        ),
    ]

    sqlglot_schema = sge.Schema(expressions=columns)
    ibis_schema = sch.Schema.from_sqlglot(sqlglot_schema)
    expected = sch.Schema(
        {
            "bigint_col": dt.int64,
            "int_col": dt.int32,
            "smallint_col": dt.int16,
            "tinyint_col": dt.int8,
            "double_col": dt.float64,
            "float_col": dt.float32,
            "varchar_col": dt.string,
            "text_col": dt.string,
            "boolean_col": dt.boolean,
            "date_col": dt.date,
            "timestamp_col": dt.timestamp,
            "time_col": dt.time,
            "binary_col": dt.binary,
            "uuid_col": dt.uuid,
            "json_col": dt.json,
            "decimal_col": dt.Decimal(10, 2),
            "not_null_col": dt.String(nullable=False),
            "array_col": dt.Array(dt.string),
            "map_col": dt.Map(dt.string, dt.int32),
        }
    )

    assert ibis_schema == expected
