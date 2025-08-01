from __future__ import annotations

import re
import string

import pytest

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.legacy.udf.vectorized as udf
from ibis import util
from ibis.common.graph import Node as Traversable
from ibis.expr.format import fmt, pretty


@pytest.mark.parametrize("cls", [ops.PhysicalTable, ops.Relation])
def test_tables_have_format_value_rules(cls):
    assert cls in fmt.registry


def test_format_table_column(alltypes, snapshot):
    # GH #507
    result = repr(alltypes.f)
    assert "float64" in result
    snapshot.assert_match(result, "repr.txt")


def test_format_projection(alltypes, snapshot):
    # This should produce a ref to the projection
    proj = alltypes[["c", "a", "f"]]
    expr = proj["a"]
    result = repr(expr)
    snapshot.assert_match(result, "repr.txt")


def test_format_table_with_empty_schema(snapshot):
    # GH #6837
    schema = ibis.table({}, name="t")
    result = repr(schema)
    snapshot.assert_match(result, "repr.txt")


def test_table_type_output(snapshot):
    foo = ibis.table(
        [
            ("job", "string"),
            ("dept_id", "string"),
            ("year", "int32"),
            ("y", "double"),
        ],
        name="foo",
    )

    expr = foo.dept_id == foo.view().dept_id
    result = repr(expr)
    assert "UnboundTable: foo" in result
    snapshot.assert_match(result, "repr.txt")


def test_aggregate_arg_names(alltypes, snapshot):
    # Not sure how to test this *well*
    t = alltypes

    by_exprs = [t.g.name("key1"), t.f.round().name("key2")]
    metrics = [t.c.sum().name("c"), t.d.mean().name("d")]

    expr = t.group_by(by_exprs).aggregate(metrics)
    result = repr(expr)
    assert "metrics" in result
    assert "groups" in result

    snapshot.assert_match(result, "repr.txt")


def test_format_multiple_join_with_projection(snapshot):
    # Star schema with fact table
    table = ibis.table(
        [
            ("c", "int32"),
            ("f", "double"),
            ("foo_id", "string"),
            ("bar_id", "string"),
        ],
        "one",
    )

    table2 = ibis.table([("foo_id", "string"), ("value1", "double")], "two")

    table3 = ibis.table([("bar_id", "string"), ("value2", "double")], "three")

    filtered = table.filter(table["f"] > 0)

    pred1 = filtered["foo_id"] == table2["foo_id"]
    pred2 = filtered["bar_id"] == table3["bar_id"]

    j1 = filtered.left_join(table2, [pred1])
    j2 = j1.inner_join(table3, [pred2])

    # Project out the desired fields
    view = j2.select(filtered, table2["value1"], table3["value2"])

    # it works!
    result = repr(view)
    snapshot.assert_match(result, "repr.txt")


def test_memoize_filtered_table(snapshot):
    airlines = ibis.table(
        [("dest", "string"), ("origin", "string"), ("arrdelay", "int32")],
        "airlines",
    )

    dests = ["ORD", "JFK", "SFO"]
    t = airlines.filter(airlines.dest.isin(dests))
    delay_filter = t.dest.topk(10, by=t.arrdelay.mean())

    result = repr(delay_filter)
    snapshot.assert_match(result, "repr.txt")


def test_named_value_expr_show_name(alltypes, snapshot):
    expr = alltypes.f * 2
    expr2 = expr.name("baz")

    # it works!
    result = repr(expr)
    result2 = repr(expr2)

    assert "baz" not in result
    assert "baz" in result2

    snapshot.assert_match(result, "repr.txt")
    snapshot.assert_match(result2, "repr2.txt")


def test_memoize_filtered_tables_in_join(snapshot):
    # related: GH #667
    purchases = ibis.table(
        [
            ("region", "string"),
            ("kind", "string"),
            ("user", "int64"),
            ("amount", "double"),
        ],
        "purchases",
    )

    metric = purchases.amount.sum().name("total")
    agged = purchases.group_by(["region", "kind"]).aggregate(metric)

    left = agged.filter(agged.kind == "foo")
    right = agged.filter(agged.kind == "bar")

    cond = left.region == right.region
    joined = left.join(right, cond).select(left, right.total.name("right_total"))

    result = repr(joined)
    snapshot.assert_match(result, "repr.txt")


def test_argument_repr_shows_name(snapshot):
    t = ibis.table([("fakecolname1", "int64")], name="fakename2")
    expr = t.fakecolname1.nullif(2)
    result = repr(expr)

    assert "fakecolname1" in result
    assert "fakename2" in result
    snapshot.assert_match(result, "repr.txt")


def test_scalar_parameter_formatting():
    value = ibis.param("array<date>")
    assert re.match(r"^param_\d+: \$\(array<date>\)$", str(value)) is not None

    value = ibis.param("int64").name("my_param")
    assert str(value) == "my_param: $(int64)"


def test_same_column_multiple_aliases(snapshot):
    table = ibis.table([("col", "int64")], name="t")
    expr = table.select(table.col.name("fakealias1"), table.col.name("fakealias2"))
    result = repr(expr)

    assert "UnboundTable: t" in result
    assert "col int64" in result
    assert "fakealias1: r0.col" in result
    assert "fakealias2: r0.col" in result
    snapshot.assert_match(result, "repr.txt")


def test_scalar_parameter_repr():
    value = ibis.param(dt.timestamp).name("value")
    assert repr(value) == "value: $(timestamp)"


def test_repr_exact(snapshot):
    # NB: This is the only exact repr test. Do
    # not add new exact repr tests. New repr tests
    # should only check for the presence of substrings.
    table = ibis.table(
        [("col", "int64"), ("col2", "string"), ("col3", "double")],
        name="t",
    ).mutate(col4=lambda t: t.col2.length())

    result = repr(table)
    snapshot.assert_match(result, "repr.txt")


def test_complex_repr(snapshot):
    t = (
        ibis.table(dict(a="int64"), name="t")
        .filter([lambda t: t.a < 42, lambda t: t.a >= 42])
        .mutate(x=lambda t: t.a + 42)
        .group_by("x")
        .aggregate(y=lambda t: t.a.sum())
        .limit(10)
    )
    result = repr(t)

    snapshot.assert_match(result, "repr.txt")


def test_value_exprs_repr():
    t = ibis.table(dict(a="int64", b="string"), name="t")
    assert "r0.a" in repr(t.a)
    assert "Sum(r0.a)" in repr(t.a.sum())


def test_show_types(monkeypatch):
    monkeypatch.setattr(ibis.options.repr, "show_types", True)

    t = ibis.table(dict(a="int64", b="string"), name="t")
    expr = t.a / 1.0
    assert "# int64" in repr(t.a)
    assert "# float64" in repr(expr)
    assert "# float64" in repr(expr.sum())


def test_schema_truncation(monkeypatch, snapshot):
    schema = dict(zip(string.ascii_lowercase[:20], ["string"] * 20))
    t = ibis.table(schema, name="t")

    monkeypatch.setattr(ibis.options.repr, "table_columns", 0)
    with pytest.raises(ValueError):
        repr(t)

    monkeypatch.setattr(ibis.options.repr, "table_columns", 1)
    result = repr(t)
    assert util.VERTICAL_ELLIPSIS not in result
    snapshot.assert_match(result, "repr1.txt")

    monkeypatch.setattr(ibis.options.repr, "table_columns", 8)
    result = repr(t)
    assert util.VERTICAL_ELLIPSIS in result
    snapshot.assert_match(result, "repr8.txt")

    monkeypatch.setattr(ibis.options.repr, "table_columns", 1000)
    result = repr(t)
    assert util.VERTICAL_ELLIPSIS not in result
    snapshot.assert_match(result, "repr_all.txt")


def test_table_count_expr(snapshot):
    t1 = ibis.table([("a", "int"), ("b", "float")], name="t1")
    t2 = ibis.table([("a", "int"), ("b", "float")], name="t2")

    cnt = t1.count()
    join_cnt = t1.join(t2, t1.a == t2.a).count()
    union_cnt = ibis.union(t1, t2).count()

    snapshot.assert_match(repr(cnt), "cnt_repr.txt")
    snapshot.assert_match(repr(join_cnt), "join_repr.txt")
    snapshot.assert_match(repr(union_cnt), "union_repr.txt")


def test_window_no_group_by(snapshot):
    t = ibis.table(dict(a="int64", b="string"), name="t")
    expr = t.a.mean().over(ibis.window(preceding=0))
    result = repr(expr)

    assert "group_by=[]" not in result
    snapshot.assert_match(result, "repr.txt")


def test_window_group_by(snapshot):
    t = ibis.table(dict(a="int64", b="string"), name="t")
    expr = t.a.mean().over(ibis.window(group_by=t.b))

    result = repr(expr)
    assert "start=0" not in result
    assert "group_by=[r0.b]" in result
    snapshot.assert_match(result, "repr.txt")


def test_fill_null(snapshot):
    t = ibis.table(dict(a="int64", b="string"), name="t")

    expr = t.fill_null({"a": 3})
    snapshot.assert_match(repr(expr), "fill_null_dict_repr.txt")

    expr = t[["a"]].fill_null(3)
    snapshot.assert_match(repr(expr), "fill_null_int_repr.txt")

    expr = t[["b"]].fill_null("foo")
    snapshot.assert_match(repr(expr), "fill_null_str_repr.txt")


def test_asof_join(snapshot):
    left = ibis.table([("time1", "int32"), ("value", "double")], name="left")
    right = ibis.table([("time2", "int32"), ("value2", "double")], name="right")
    right_ = right.view()
    joined = left.asof_join(right, ("time1", "time2")).inner_join(
        right_, left.value == right_.value2
    )

    result = repr(joined)
    snapshot.assert_match(result, "repr.txt")


def test_two_inner_joins(snapshot):
    left = ibis.table(
        [("time1", "int32"), ("value", "double"), ("a", "string")], name="left"
    )
    right = ibis.table(
        [("time2", "int32"), ("value2", "double"), ("b", "string")], name="right"
    )
    right_ = right.view()
    joined = left.inner_join(right, left.a == right.b).inner_join(
        right_, left.value == right_.value2
    )

    result = repr(joined)
    snapshot.assert_match(result, "repr.txt")


def test_destruct_selection(snapshot):
    table = ibis.table([("col", "int64")], name="t")

    with pytest.warns(FutureWarning, match="v9\\.0"):

        @udf.reduction(
            input_type=["int64"],
            output_type=dt.Struct({"sum": "int64", "mean": "float64"}),
        )
        def multi_output_udf(v):
            return v.sum(), v.mean()

    expr = multi_output_udf(table["col"])
    expr = table.aggregate(agged_struct=expr).unpack("agged_struct")
    result = repr(expr)

    snapshot.assert_match(result, "repr.txt")


@pytest.mark.parametrize(
    "literal, typ, output",
    [(42, None, "42"), ("42", None, "'42'"), (42, "double", "42.0")],
)
def test_format_literal(literal, typ, output):
    expr = ibis.literal(literal, type=typ)
    assert repr(expr) == output


def test_format_dummy_table(snapshot):
    t = ops.DummyTable({"foo": ibis.array([1]).cast("array<int8>")}).to_expr()

    result = repr(t)
    snapshot.assert_match(result, "repr.txt")


def test_format_in_memory_table(snapshot):
    pytest.importorskip("pandas")

    t = ibis.memtable([(1, 2), (3, 4), (5, 6)], columns=["x", "y"])
    expr = t.x.sum() + t.y.sum()

    result = repr(expr)
    assert "InMemoryTable" in result
    snapshot.assert_match(result, "repr.txt")


def test_format_unbound_table_namespace(snapshot):
    t = ibis.table(name="bork", schema=(("a", "int"), ("b", "int")))

    result = repr(t)
    snapshot.assert_match(result, "repr.txt")

    t = ibis.table(name="bork", schema=(("a", "int"), ("b", "int")), database="bork")

    result = repr(t)
    snapshot.assert_match(result, "reprdb.txt")

    t = ibis.table(
        name="bork", schema=(("a", "int"), ("b", "int")), catalog="ork", database="bork"
    )

    result = repr(t)
    snapshot.assert_match(result, "reprcatdb.txt")


def test_format_new_relational_operation(alltypes, snapshot):
    class MyRelation(ops.Relation):
        parent: ops.Relation
        kind: str

        @property
        def schema(self):
            return self.parent.schema

        @property
        def values(self):
            return {}

    table = MyRelation(alltypes, kind="foo").to_expr()
    expr = table.select(table, table.a.name("a2"))
    result = repr(expr)

    snapshot.assert_match(result, "repr.txt")


def test_format_new_value_operation(alltypes):
    class Inc(ops.Value):
        arg: ops.Value

        @property
        def dtype(self):
            return self.arg.dtype

        @property
        def shape(self):
            return self.arg.shape

    expr = Inc(alltypes.a).to_expr().name("incremented")
    result = repr(expr)
    last_line = result.splitlines()[-1]

    assert "Inc" in result
    assert last_line == "incremented: Inc(r0.a)"


def test_format_show_variables(monkeypatch, alltypes, snapshot):
    monkeypatch.setattr(ibis.options.repr, "show_variables", True)

    filtered = alltypes.filter(alltypes.f > 0)
    ordered = filtered.order_by("f")
    projected = ordered[["a", "b", "f"]]

    add = projected.a + projected.b
    sub = projected.a - projected.b
    expr = add * sub

    result = repr(expr)

    assert "projected.a" in result
    assert "projected.b" in result
    assert "filtered" in result
    assert "ordered" in result

    snapshot.assert_match(result, "repr.txt")


def test_default_format_implementation(snapshot):
    class ValueList(ops.Node):
        values: tuple[ops.Value, ...]

    t = ibis.table([("a", "int64")], name="t")
    vl = ValueList((1, 2.0, "three", t.a))
    result = pretty(vl)

    snapshot.assert_match(result, "repr.txt")


def test_arbitrary_traversables_are_supported(snapshot):
    class MyNode(Traversable):
        __slots__ = ("children", "obj")
        __argnames__ = ("obj", "children")

        def __init__(self, obj, children):
            self.obj = obj.op()
            self.children = tuple(child.op() for child in children)

        @property
        def __args__(self):
            return self.obj, self.children

        def __hash__(self):
            return hash((self.__class__, self.obj, self.children))

    t = ibis.table([("a", "int64")], name="t")
    node = MyNode(t.a, [t.a, t.a + 1])
    result = pretty(node)

    snapshot.assert_match(result, "repr.txt")
