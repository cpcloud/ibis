import pytest
from matchpy import Symbol

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


def test_project_read(t, a, b, c):
    expr = Project(Read(t), Exprs(a, b, c))
    result = optimize(expr)
    assert result == ProjectedRead(t, Refs(a, b, c))


def test_reproject_refs(t, a, b, c):
    expr = Project(Project(Read(t), Refs(a, b, c)), Refs(a, b))
    result = optimize(expr)
    assert result == ProjectedRead(t, Refs(a, b))


def test_reproject_exprs(t, a, b, c):
    expr = Project(Project(Read(t), Exprs(a, b, c)), Exprs(a, b))
    result = optimize(expr)
    assert result == ProjectedRead(t, Refs(a, b))


def test_reproject_identical(t, a, b):
    expr = Project(Project(Read(t), Exprs(a, b)), Exprs(a, b))
    result = optimize(expr)
    expected = ProjectedRead(t, Refs(a, b))
    assert result == expected


def test_project_with_add(t, a, b, c):
    expr = Project(Read(t), Exprs(a, b, Add(a, b, c)))
    result = optimize(expr)
    assert result == Project(
        ProjectedRead(t, Refs(a, b, c)), Exprs(a, b, Add(a, b, c))
    )


def test_select_project(t, a, b, c, d):
    expr = Project(Select(Read(t), Gt(c, d)), Exprs(a, b))
    result = optimize(expr)
    assert result == Project(
        OptimizedRead(t, Refs(a, b), Gt(c, d)),
        Exprs(a, b),
    )


def test_select_select(t, a, b, c):
    expr = Select(Select(Read(t), Gt(b, c)), Lt(c, b))
    result = optimize(expr)
    assert result == SelectedRead(t, Gt(b, c))


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
def test_logical(expr, expected):
    result = optimize(expr)
    assert result == expected


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
def test_constant_fold(expr, expected):
    result = optimize(expr)
    assert result == expected


def test_select_join(t, s, t_a, t_b, s_c, s_d):
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


def test_select_join_cross_predicate(t, s, t_a, t_b, s_c, s_d):
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


ands = pytest.mark.parametrize(
    ("expr", "expected"),
    [
        (And(), true),
        (And(true), true),
        (And(false), false),
        (And(null), null),
        (And(false, false), false),
        (And(false, null), false),
        (And(false, true), false),
        (And(true, true), true),
        (And(true, null), null),
        (And(true, false), false),
        (And(null, true), null),
        (And(null, null), null),
        (And(null, false), false),
        (And(Symbol("a"), Symbol("a")), Symbol("a")),
        (And(Symbol("a"), true, Symbol("b")), And(Symbol("a"), Symbol("b"))),
        (And(Symbol("a"), false, Symbol("b")), false),
        (
            And(Symbol("a"), null, Symbol("b")),
            And(null, Symbol("a"), Symbol("b")),
        ),
        (And(Symbol("a"), null, Symbol("b"), false, Symbol("c")), false),
        (And(Symbol("a"), false, Symbol("b"), false, Symbol("c")), false),
        (And(Symbol("a"), false), false),
        (And(false, Symbol("a")), false),
        (And(Symbol("a"), true), Symbol("a")),
        (And(true, Symbol("a")), Symbol("a")),
    ],
)


ors = pytest.mark.parametrize(
    ("expr", "expected"),
    [
        (Or(), false),
        (Or(true), true),
        (Or(false), false),
        (Or(null), null),
        (Or(false, false), false),
        (Or(false, null), null),
        (Or(false, true), true),
        (Or(true, true), true),
        (Or(true, null), true),
        (Or(true, false), true),
        (Or(null, true), true),
        (Or(null, null), null),
        (Or(null, false), null),
        (Or(Symbol("a"), Symbol("a")), Symbol("a")),
        (Or(Symbol("a"), Symbol("b")), Or(Symbol("a"), Symbol("b"))),
        (Or(Symbol("a"), false, Symbol("b")), Or(Symbol("a"), Symbol("b"))),
        (Or(Symbol("a"), true, Symbol("b")), true),
        (
            Or(Symbol("a"), null, Symbol("b")),
            Or(null, Symbol("a"), Symbol("b")),
        ),
        (Or(Symbol("a"), null, Symbol("b"), true, Symbol("c")), true),
        (Or(Symbol("a"), false, Symbol("b"), true, Symbol("c")), true),
        (Or(Symbol("a"), true), true),
        (Or(true, Symbol("a")), true),
        (Or(Symbol("a"), false), Symbol("a")),
        (Or(false, Symbol("a")), Symbol("a")),
    ],
)


@ands
@ors
def test_boolean_ops(expr, expected):
    result = optimize(expr)
    assert result == expected
