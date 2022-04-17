import pytest
from matchpy import Symbol
from pytest import param

from ibis.expr.optimize import (
    Add,
    And,
    Eq,
    Exprs,
    FloatLiteral,
    Gt,
    IntLiteral,
    Join,
    Lt,
    Ne,
    Not,
    OptimizedRead,
    Or,
    Project,
    ProjectedRead,
    Read,
    Ref,
    Refs,
    Select,
    SelectedRead,
    false,
    null,
    optimize,
    true,
)


@pytest.fixture
def t():
    return Symbol("t")


@pytest.fixture
def s():
    return Symbol("s")


@pytest.fixture
def a(t):
    return Ref(t, Symbol("a"))


@pytest.fixture
def t_a(a):
    return a


@pytest.fixture
def s_c(s):
    return Ref(s, Symbol("c"))


@pytest.fixture
def s_d(s):
    return Ref(s, Symbol("d"))


@pytest.fixture
def b(t):
    return Ref(t, Symbol("b"))


@pytest.fixture
def t_b(b):
    return b


@pytest.fixture
def c(t):
    return Ref(t, Symbol("c"))


@pytest.fixture
def d(t):
    return Ref(t, Symbol("d"))


def test_project_read(t, a, b, c, benchmark):
    expr = Project(Read(t), Exprs(a, b, c))
    result = optimize(expr)
    assert result == ProjectedRead(t, Refs(a, b, c))
    benchmark(optimize, expr)


def test_reproject_refs(t, a, b, c, benchmark):
    expr = Project(Project(Read(t), Refs(a, b, c)), Refs(a, b))
    result = optimize(expr)
    assert result == ProjectedRead(t, Refs(a, b))
    benchmark(optimize, expr)


def test_reproject_exprs(t, a, b, c, benchmark):
    expr = Project(Project(Read(t), Exprs(a, b, c)), Exprs(a, b))
    result = optimize(expr)
    assert result == ProjectedRead(t, Refs(a, b))
    benchmark(optimize, expr)


def test_reproject_identical(t, a, b, benchmark):
    expr = Project(Project(Read(t), Exprs(a, b)), Exprs(a, b))
    result = optimize(expr)
    expected = ProjectedRead(t, Refs(a, b))
    assert result == expected
    benchmark(optimize, expr)


def test_project_with_add(t, a, b, c, benchmark):
    expr = Project(Read(t), Exprs(a, b, Add(a, b, c)))
    result = optimize(expr)
    assert result == Project(
        ProjectedRead(t, Refs(a, b, c)), Exprs(a, b, Add(a, b, c))
    )
    benchmark(optimize, expr)


def test_select_project(t, a, b, c, d, benchmark):
    expr = Project(Select(Read(t), Gt(c, d)), Exprs(a, b))
    result = optimize(expr)
    assert result == OptimizedRead(t, Refs(a, b), Lt(d, c))
    benchmark(optimize, expr)


def test_select_select(t, b, c, benchmark):
    expr = Select(Select(Read(t), Gt(b, c)), Lt(c, b))
    result = optimize(expr)
    assert result == SelectedRead(t, Lt(c, b))
    benchmark(optimize, expr)


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        (And(And(true, true), true, Or(true, true)), true),
        (And(And(false, true), true, Or(true, true)), false),
        (
            And(Gt(Symbol("a"), Symbol("b")), Lt(Symbol("b"), Symbol("a"))),
            And(Lt(Symbol("b"), Symbol("a"))),
        ),
        (
            And(
                Gt(Symbol("a"), Symbol("b")),
                Lt(Symbol("b"), Symbol("a")),
                Gt(Symbol("a"), Symbol("b")),
            ),
            And(Lt(Symbol("b"), Symbol("a"))),
        ),
        (Or(false, Or(true, false)), true),
        (Not(true), false),
        (Not(false), true),
        (Not(Eq(Symbol("a"), Symbol("b"))), Ne(Symbol("a"), Symbol("b"))),
        (Not(Ne(Symbol("a"), Symbol("b"))), Eq(Symbol("a"), Symbol("b"))),
    ],
)
def test_logical(expr, expected, benchmark):
    result = optimize(expr)
    assert result == expected
    benchmark(optimize, expr)


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        (Add(IntLiteral(1), IntLiteral(2)), IntLiteral(3)),
        (Add(IntLiteral(1), IntLiteral(2), IntLiteral(3)), IntLiteral(6)),
        (Add(IntLiteral(1), Add(IntLiteral(2), IntLiteral(3))), IntLiteral(6)),
        (Add(FloatLiteral(1.0), FloatLiteral(2.0)), FloatLiteral(3.0)),
        (
            Add(FloatLiteral(1.0), FloatLiteral(2.0), FloatLiteral(3.0)),
            FloatLiteral(6.0),
        ),
        (
            Add(FloatLiteral(1.0), Add(FloatLiteral(2.0), FloatLiteral(3))),
            FloatLiteral(6.0),
        ),
        (Add(IntLiteral(1), FloatLiteral(2.0)), FloatLiteral(3.0)),
        (
            Add(FloatLiteral(1.0), IntLiteral(2), IntLiteral(3)),
            FloatLiteral(6.0),
        ),
        (
            Add(IntLiteral(1), Add(FloatLiteral(2.0), IntLiteral(3))),
            FloatLiteral(6.0),
        ),
        (
            Add(
                IntLiteral(1),
                Add(
                    Symbol("a"), IntLiteral(2), Add(IntLiteral(4), Symbol("b"))
                ),
            ),
            Add(IntLiteral(7), Symbol("a"), Symbol("b")),
        ),
    ],
)
def test_constant_fold(expr, expected, benchmark):
    result = optimize(expr)
    assert result == expected
    benchmark(optimize, expr)


def test_select_join(t, s, t_a, t_b, s_c, s_d, benchmark):
    rel1 = Project(Read(t), Exprs(t_a, t_b))
    rel2 = Project(Read(s), Exprs(s_c, s_d))
    expr = Join(
        rel1,
        rel2,
        And(
            Eq(t_a, IntLiteral(1)),
            Eq(t_b, IntLiteral(2)),
            Eq(s_c, IntLiteral(1)),
            Eq(s_d, IntLiteral(2)),
        ),
    )
    result = optimize(expr)
    expected = Join(
        Select(
            rel1,
            And(
                Eq(t_a, IntLiteral(1)),
                Eq(t_b, IntLiteral(2)),
            ),
        ),
        Select(
            rel2,
            And(
                Eq(s_c, IntLiteral(1)),
                Eq(s_d, IntLiteral(2)),
            ),
        ),
        true,
    )
    assert result == expected
    benchmark(optimize, expr)


def test_select_join_cross_predicate(t, s, t_a, t_b, s_c, s_d, benchmark):
    rel1 = Project(Read(t), Exprs(t_a, t_b))
    rel2 = Project(Read(s), Exprs(s_c, s_d))
    expr = Join(
        rel1,
        rel2,
        And(Eq(t_a, s_c), Eq(t_b, s_d)),
    )
    result = optimize(expr)
    expected = Join(
        ProjectedRead(t, Refs(t_a, t_b)),
        ProjectedRead(s, Refs(s_c, s_d)),
        And(Eq(s_c, t_a), Eq(s_d, t_b)),
    )
    assert result == expected
    benchmark(optimize, expr)


ands = [
    param(And(), true, id="and_empty"),
    param(And(true), true, id="and_true"),
    param(And(false), false, id="and_false"),
    param(And(null), null, id="and_null"),
    param(And(false, false), false, id="and_false_false"),
    param(And(false, null), false, id="and_false_null"),
    param(And(false, true), false, id="and_false_true"),
    param(And(true, true), true, id="and_true_true"),
    param(And(true, null), null, id="and_true_null"),
    param(And(true, false), false, id="and_true_false"),
    param(And(null, true), null, id="and_null_true"),
    param(And(null, null), null, id="and_null_null"),
    param(And(null, false), false, id="and_null_false"),
    param(And(Symbol("a"), Symbol("a")), Symbol("a"), id="and_a"),
    param(
        And(Symbol("a"), true, Symbol("b")),
        And(Symbol("a"), Symbol("b")),
        id="and_a_b",
    ),
    param(And(Symbol("a"), false, Symbol("b")), false, id="and_a_false_b"),
    param(And(Symbol("a"), null, Symbol("b")), null, id="and_a_null_b"),
    param(
        And(Symbol("a"), null, Symbol("b"), false, Symbol("c")),
        false,
        id="and_a_null_b_false_c",
    ),
    param(
        And(Symbol("a"), false, Symbol("b"), false, Symbol("c")),
        false,
        id="and_a_false_b_false_c",
    ),
    param(And(Symbol("a"), false), false, id="and_a_false"),
    param(And(false, Symbol("a")), false, id="and_false_a"),
    param(And(Symbol("a"), true), Symbol("a"), id="and_a_true"),
    param(And(true, Symbol("a")), Symbol("a"), id="and_true_a"),
]


ors = [
    param(Or(), false, id="or_empty"),
    param(Or(true), true, id="or_true"),
    param(Or(false), false, id="or_false"),
    param(Or(null), null, id="or_null"),
    param(Or(false, false), false, id="or_false_false"),
    param(Or(false, null), null, id="or_false_null"),
    param(Or(false, true), true, id="or_false_true"),
    param(Or(true, true), true, id="or_true_true"),
    param(Or(true, null), true, id="or_true_null"),
    param(Or(true, false), true, id="or_true_false"),
    param(Or(null, true), true, id="or_null_true"),
    param(Or(null, null), null, id="or_null_null"),
    param(Or(null, false), null, id="or_null_false"),
    param(Or(Symbol("a"), Symbol("a")), Symbol("a"), id="or_a_a"),
    param(
        Or(Symbol("a"), Symbol("b")),
        Or(Symbol("a"), Symbol("b")),
        id="or_a_b",
    ),
    param(
        Or(Symbol("a"), false, Symbol("b")),
        Or(Symbol("a"), Symbol("b")),
        id="or_a_false_b",
    ),
    param(Or(Symbol("a"), true, Symbol("b")), true, id="or_a_true_b"),
    param(
        Or(Symbol("a"), null, Symbol("b")),
        Or(Symbol("a"), Symbol("b")),
        id="or_a_null_b",
    ),
    param(
        Or(Symbol("a"), null, Symbol("b"), true, Symbol("c")),
        true,
        id="or_a_null_b_true_c",
    ),
    param(
        Or(Symbol("a"), false, Symbol("b"), true, Symbol("c")),
        true,
        id="or_a_false_b_true_c",
    ),
    param(Or(Symbol("a"), true), true, id="or_a_true"),
    param(Or(true, Symbol("a")), true, id="or_true_a"),
    param(Or(Symbol("a"), false), Symbol("a"), id="or_a_false"),
    param(Or(false, Symbol("a")), Symbol("a"), id="or_false_a"),
]


@pytest.mark.parametrize(("expr", "expected"), ands + ors)
def test_boolean_ops(expr, expected, benchmark):
    result = optimize(expr)
    assert result == expected
    benchmark(optimize, expr)
