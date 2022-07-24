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
            lambda t: (
                t.filter(_.a == "1").filter(_.b == 2.0).filter(_.a < "b")
            ),
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
            lambda t: t[["a", "b"]].select([_.a.length().name("c")]),
            id="simple_project",
        ),
        param(
            lambda t: t.mutate(c=_.b + 1.0).select(["a"]),
            lambda t: t[["a"]],
            id="useless_mutate",
        ),
        param(
            lambda t: t.mutate(c=_.b + 1.0).select(["c"]),
            lambda t: t.mutate(c=_.b + 1.0).select(["c"]),
            id="useful_mutate",
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
    ],
)
def test_optimize(t, expr_fn, expected_fn):
    expr = expr_fn(t)
    expected = expected_fn(t)
    result = opt(expr)
    assert result.equals(expected)


def test_no_opt_filter(t):
    expr = t.filter([t.a == "1"])
    assert isinstance(expr.op(), ops.Filter)
