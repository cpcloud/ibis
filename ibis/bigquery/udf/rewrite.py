import ast
import itertools


def matches(value, pattern):
    """Check whether `value` matches `pattern`.

    Parameters
    ----------
    value : ast.AST
    pattern : ast.AST

    Returns
    -------
    matched : bool
    """
    # types must match exactly
    if type(value) != type(pattern):
        return False

    # primitive value, such as None, True, False etc
    if not isinstance(value, ast.AST) and not isinstance(pattern, ast.AST):
        return value == pattern

    fields = [
        (field, getattr(pattern, field))
        for field in pattern._fields if hasattr(pattern, field)
    ]
    for field_name, field_value in fields:
        if not matches(getattr(value, field_name), field_value):
            return False
    return True


class Rewriter:
    """AST pattern matching to enable rewrite rules.

    Attributes
    ----------
    funcs : List[Tuple[ast.AST, Callable[ast.expr, [ast.expr]]]]
    """
    def __init__(self):
        self.funcs = []

    def register(self, pattern):
        def wrapper(f):
            self.funcs.append((pattern, f))
            return f
        return wrapper

    def __call__(self, node):
        # TODO: more efficient way of doing this?
        for pattern, func in self.funcs:
            if matches(node, pattern):
                return func(node)
        return node


rewrite = Rewriter()


@rewrite.register(ast.Call(func=ast.Name(id='print')))
def rewrite_print(node):
    return ast.Call(
        func=ast.Attribute(
            value=ast.Name(id='console', ctx=ast.Load()),
            attr='log',
            ctx=ast.Load()
        ),
        args=node.args,
        keywords=node.keywords,
    )


@rewrite.register(ast.Call(func=ast.Attribute(attr='append')))
def rewrite_append(node):
    return ast.Call(
        func=ast.Attribute(
            value=node.func.value,
            attr='push',
            ctx=ast.Load(),
        ),
        args=node.args,
        keywords=node.keywords,
    )


@rewrite.register(
    ast.Call(func=ast.Attribute(value=ast.Name(id='Array'), attr='from_'))
)
def rewrite_array_from(node):
    return ast.Call(
        func=ast.Attribute(value=node.func.value, attr='from'),
        args=node.args,
        keywords=node.keywords,
    )


@rewrite.register(ast.Call(func=ast.Name(id='len')))
def rewrite_len(node):
    assert len(node.args) == 1
    return ast.Attribute(value=node.args[0], attr='length', ctx=ast.Load())


_names = itertools.count()


def genname():
    return 'var{:d}'.format(next(_names))


def rewrite_reduction(binop):
    def rewrite(node):
        x, y = genname(), genname()
        return ast.Call(
            func=ast.Attribute(
                value=node.args[0],
                attr='reduce',
                ctx=ast.Load(),
            ),
            args=[binop(x, y)],
            keywords=[],
        )
    return rewrite


rewrite_sum = rewrite.register(ast.Call(func=ast.Name(id='sum')))(
    rewrite_reduction(
        lambda x, y: ast.Lambda(
            args=ast.arguments(
                args=[ast.arg(arg=x), ast.arg(arg=y)], vararg=None),
            keywords=[],
            body=ast.BinOp(
                left=ast.Name(id=x, ctx=ast.Load()),
                op=ast.Add(),
                right=ast.Name(id=y, ctx=ast.Load())
            )
        )
    )
)


rewrite_all = rewrite.register(ast.Call(func=ast.Name(id='all')))(
    rewrite_reduction(
        lambda x, y: ast.Lambda(
            args=ast.arguments(
                args=[ast.arg(arg=x), ast.arg(arg=y)], vararg=None),
            keywords=[],
            body=ast.BoolOp(
                op=ast.And(),
                values=[
                    ast.Name(id=x, ctx=ast.Load()),
                    ast.Name(id=y, ctx=ast.Load())
                ]
            )
        )
    )
)


rewrite_any = rewrite.register(ast.Call(func=ast.Name(id='any')))(
    rewrite_reduction(
        lambda x, y: ast.Lambda(
            args=ast.arguments(
                args=[ast.arg(arg=x), ast.arg(arg=y)], vararg=None),
            keywords=[],
            body=ast.BoolOp(
                op=ast.Or(),
                values=[
                    ast.Name(id=x, ctx=ast.Load()),
                    ast.Name(id=y, ctx=ast.Load())
                ]
            )
        )
    )
)


def rewrite_minmax(name):
    def rewrite(node):
        nargs = len(node.args)
        x, y = genname(), genname()
        if nargs == 1:  # min(x): assume a sequence
            return ast.Call(
                func=ast.Attribute(
                    value=node.args[0],
                    attr='reduce',
                    ctx=ast.Load(),
                ),
                args=[
                    ast.Lambda(
                        args=ast.arguments(
                            args=[ast.arg(arg=x), ast.arg(arg=y)], vararg=None
                        ),
                        keywords=[],
                        body=ast.Call(
                            func=ast.Attribute(
                                value=ast.Name(id='Math', ctx=ast.Load()),
                                attr=name,
                                ctx=ast.Load()
                            ),
                            args=[
                                ast.Name(id=x, ctx=ast.Load()),
                                ast.Name(id=y, ctx=ast.Load())
                            ],
                            keywords=node.keywords,
                        )
                    )
                ],
                keywords=node.keywords,
            )
        elif nargs == 2:  # min(x, y): min of two arguments
            return ast.Call(
                func=ast.Attribute(
                    value=ast.Name(id='Math', ctx=ast.Load()),
                    attr=name,
                    ctx=ast.Load()
                ),
                args=node.args,
                keywords=node.keywords,
            )
        else:
            raise ValueError('Invalid number of arguments for {}')
    return rewrite


rewrite_min = rewrite.register(ast.Call(func=ast.Name(id='min')))(
    rewrite_minmax('min'))
rewrite_max = rewrite.register(ast.Call(func=ast.Name(id='max')))(
    rewrite_minmax('max'))
