import pytest
from pytest import param
from toolz import identity

import ibis
import ibis.expr.operations as ops
from ibis import _
from ibis.expr.optimize import optimize as opt


@pytest.fixture(scope="session")
def t():
    return ibis.table(dict(a="string", b="float64"), name="t")


@pytest.fixture(scope="session")
def s():
    return ibis.table(dict(c="int32", d="array<string>"), name="s")


@pytest.mark.parametrize(
    ("expr_fn", "expected_fn"),
    [
        param(lambda t: t.select([]), identity, id="empty_project"),
        param(
            lambda t: t[[t[col] for col in t.columns]],
            identity,
            id="all_columns",
        ),
        param(lambda t: t[list(t.columns)], identity, id="all_columns_str"),
        param(lambda t: t.filter([]), identity, id="empty_filter"),
        param(lambda t: t.order_by([]), identity, id="empty_order_by"),
        param(lambda t: t.filter(_.a == _.a), identity, id="useless_pred_eq"),
        param(
            lambda t: t.filter([ibis.literal(True)]),
            identity,
            id="useless_pred_true",
        ),
        param(
            lambda t: t.filter([t.a == t.a, ibis.literal(True)]),
            identity,
            id="useless_pred_eq_true",
        ),
        param(
            lambda t: t.filter((_.a == _.a) & True),
            identity,
            id="useless_pred_eq_and",
        ),
        param(
            lambda t: t.filter((_.b == _.b) & True),
            lambda t: t.filter(_.b == _.b),
            id="useless_pred_partial",
        ),
        param(
            lambda t: (t.filter(_.a == "1").filter(_.b == 2.0).filter(_.a < "b")),
            lambda t: t.filter([_.a == "1", _.b == 2.0, _.a < "b"]),
            id="compose_filters",
        ),
        param(
            lambda t: t[["a", "b"]]["a"],
            lambda t: t["a"],
            id="single_column",
        ),
        param(
            lambda t: t[["a", "b"]][["a"]],
            lambda t: t[["a"]],
            id="single_column_project",
        ),
        # sweet, thank you matchpy
        param(
            lambda t: t[["a", "b"]][["a", "b"]],
            identity,
            id="redundant_project",
        ),
        param(
            lambda t: t[["a", "b"]].select([_.a.length().name("c")]),
            lambda t: t.select([_.a.length().name("c")]),
            id="simple_project",
        ),
        param(
            lambda t: t.mutate(c=_.b + 1.0).select(["a"]),
            lambda t: t[["a"]],
            id="useless_mutate",
        ),
        param(
            lambda t: t.mutate(c=_.b + 1.0).select(["c"]),
            lambda t: t.select([(_.b + 1.0).name("c")]),
            id="useful_mutate",
        ),
        param(
            lambda t: t.mutate(c=_.b + 1.0, d=_.a.length() - 2).select(["c", "d"]),
            lambda t: t.select([(_.b + 1.0).name("c"), (_.a.length() - 2).name("d")]),
            id="useful_multi_mutate",
        ),
        param(
            lambda t: t[["a", "b"]].select(["a", _.a.length().name("c")]),
            lambda t: t[["a", _.a.length().name("c")]],
            id="useless_column",
        ),
        param(
            lambda t: t[["a", "b"]].select(["a", "b", _.a.length().name("c")]),
            lambda t: t[["a", "b", _.a.length().name("c")]],
            id="useless_project",
        ),
        param(
            lambda t: (t.a == "1") & True,
            lambda t: t.a == "1",
            id="and_useless_true_right",
        ),
        param(
            lambda t: True & (t.a == "1"),
            lambda t: t.a == "1",
            id="and_useless_true_left",
        ),
        param(
            lambda t: (t.a == "1") & False,
            lambda _: ibis.literal(False),
            id="and_useless_false_right",
        ),
        param(
            lambda t: False & (t.a == "1"),
            lambda _: ibis.literal(False),
            id="and_useless_false_left",
        ),
        param(
            lambda t: (t.a == "1") & (t.a == "1"),
            lambda t: t.a == "1",
            id="and_redundant",
        ),
        param(
            lambda t: (t.a == "1") | True,
            lambda _: ibis.literal(True),
            id="or_useless_true_right",
        ),
        param(
            lambda t: True | (t.a == "1"),
            lambda _: ibis.literal(True),
            id="or_useless_true_left",
        ),
        param(
            lambda t: (t.a == "1") | False,
            lambda t: t.a == "1",
            id="or_useless_false_right",
        ),
        param(
            lambda t: False | (t.a == "1"),
            lambda t: t.a == "1",
            id="or_useless_false_left",
        ),
        param(
            lambda t: (t.a == "1") | (t.a == "1"),
            lambda t: t.a == "1",
            id="or_redundant",
        ),
        param(lambda t: t[t.b > 1], lambda t: t[1 < t.b], id="greater"),
        param(
            lambda t: t[t.b >= 1],
            lambda t: t[1 <= t.b],
            id="greater_equal",
        ),
        param(
            lambda t: t[~(t.a != "1")],
            lambda t: t[t.a == "1"],
            id="not_not_eq",
        ),
        param(
            lambda t: t[~(t.a == "1")],
            lambda t: t[t.a != "1"],
            id="not_eq",
        ),
        param(
            lambda _: ~ibis.literal(True),
            lambda _: ibis.literal(False),
            id="not_true",
        ),
        param(
            lambda _: ~ibis.literal(False),
            lambda _: ibis.literal(True),
            id="not_false",
        ),
        param(
            lambda t: t[t],
            identity,
            id="reproject_table",
        ),
    ],
)
def test_optimize(t, expr_fn, expected_fn):
    expr = expr_fn(t)
    expected = expected_fn(t)
    result = opt(expr)
    assert result.equals(expected)


def test_no_opt_filter(t):
    expr = t.filter([t.a == "1"])
    result = opt(expr)
    assert isinstance(result.op(), ops.Filter)


@pytest.mark.parametrize(
    ("expr_fn", "expected_fn"),
    [
        param(
            lambda t, _: t.union(t),
            lambda t, _: t.union(t),
            id="basic_union_all",
        ),
        param(
            lambda t, _: t.union(t, distinct=True),
            lambda t, _: t,
            id="no_op_union",
        ),
        param(
            lambda t, _: t[t.a == "1"].union(t[t.a == "2"]),
            lambda t, _: t[(t.a == "1") | (t.a == "2")],
            id="union_all_to_or",
        ),
        param(
            lambda t, _: t[t.a == "foo"].intersect(t[t.b == 42.42]),
            lambda t, _: t.filter([t.b == 42.42, t.a == "foo"]),
            id="intersect_filter",
        ),
        param(
            lambda t, _: t[t.a == "foo"].difference(t[t.b == 42.42]),
            lambda t, _: t.filter([ibis.literal(False)]),
            id="difference_filter",
        ),
    ],
)
def test_set_ops(t, s, expr_fn, expected_fn):
    expr = expr_fn(t, s)
    result = opt(expr)
    expected = expected_fn(t, s)
    assert result.equals(expected)


def test_simple_join(t, s):
    expr = t.join(s[["c"]], [t.b == s.c])
    result = opt(expr)
    assert result.columns == list("abc")
    assert result.equals(expr)


def test_select_join(t, s):
    rel1 = t.select([(t.a + "1").name("x"), "b"])
    rel2 = s.select(["c", "d"])
    expr = rel1.inner_join(
        rel2,
        [(rel1.x == "1") & (rel1.b == 2), rel2.c == 1, rel2.d.length() == 2],
    )

    result = opt(expr)

    expected = (
        ops.Selection(
            t.op(),
            selections=(
                (t.a + "1").name("x").op(),
                t.b.op(),
            ),
            predicates=(
                # TODO(cpcloud): can we remove the alias here? does it matter?
                ((t.a + "1").name("x") == "1").op(),
                (t.b == 2).op(),
            ),
        )
        .to_expr()
        .cross_join(s.filter([_.c == 1, _.d.length() == 2]))
    )
    assert result.equals(expected)


def test_select_join_remaining(t, s):
    rel1 = t.select([(t.a + "1").name("x"), "b"])
    rel2 = s.select(["c", "d"])
    expr = rel1.inner_join(
        rel2,
        [
            (rel1.x == "1") & (rel1.b == 2),
            rel2.c == 1,
            rel2.d.length() == 2,
            rel1.x == rel2.d[0],
        ],
    )

    result = opt(expr)

    lhs = ops.Selection(
        t.op(),
        selections=(
            (t.a + "1").name("x").op(),
            t.b.op(),
        ),
        predicates=(
            ((t.a + "1").name("x") == "1").op(),
            (t.b == 2).op(),
        ),
    ).to_expr()
    rhs = s.filter([_.c == 1, _.d.length() == 2])
    expected = lhs.inner_join(rhs, [lhs.x == rhs.d[0]])

    assert result.equals(expected)


def test_fuse_mutate():
    t = expr = ibis.table(ibis.schema([('col', 'int32')]), 't')

    expr = expr.mutate(col1=_.col + 1)
    expr = expr.mutate(col2=_.col + 2)
    expr = expr.mutate(col3=_.col + 3)
    expr = expr.mutate(col=_.col - 1)
    expr = expr.mutate(col4=_.col + 4, col5=_.col + _.col)

    result = opt(expr)
    expected = t.mutate(
        col=t.col - 1,
        col1=t.col + 1,
        col2=t.col + 2,
        col3=t.col + 3,
        col4=((t.col - 1) + 4).name("col4"),
        col5=((t.col - 1) + (t.col - 1)).name("col5"),
    )
    x = ibis.to_sql(result, dialect="duckdb")
    y = ibis.to_sql(expected, dialect="duckdb")
    assert x == y


def test_collapse(t):
    expr = t.filter(_.a > "1").mutate(c=_.a + "2").filter(_.c == "foo")
    result = opt(expr)
    expected = ops.Selection(
        t.op(),
        selections=(
            t.a.op(),
            t.b.op(),
            (t.a + "2").name("c").op(),
        ),
        predicates=(
            (t.a > "1").op(),
            ((t.a + "2").name("c") == "foo").op(),
        ),
    ).to_expr()
    assert result.equals(expected)


def test_collapse_limit(t):
    expr = t.filter(_.a > "1").mutate(c=_.a + "2").filter(_.c == "foo").limit(10)
    result = opt(expr)
    expected = (
        ops.Selection(
            t.op(),
            selections=(
                t.a.op(),
                t.b.op(),
                (t.a + "2").name("c").op(),
            ),
            predicates=(
                (t.a > "1").op(),
                ((t.a + "2").name("c") == "foo").op(),
            ),
        )
        .to_expr()
        .limit(10)
    )
    assert result.equals(expected)
