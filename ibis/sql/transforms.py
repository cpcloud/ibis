from ..expr import analysis as L
from ..expr import datatypes as dt
from ..expr import operations as ops
from ..expr import rules as rlz
from ..expr import types as ir
from ..expr.signature import Argument as Arg


class ExistsExpr(ir.AnalyticExpr):
    def type(self):
        return 'exists'


class ExistsSubquery(ops.Node):
    """Helper class"""

    foreign_table = Arg(rlz.noop)
    predicates = Arg(rlz.noop)

    def output_type(self):
        return ExistsExpr


class NotExistsSubquery(ops.Node):
    foreign_table = Arg(rlz.noop)
    predicates = Arg(rlz.noop)

    def output_type(self):
        return ExistsExpr


class AnyToExistsTransform:

    """
    Some code duplication with the correlated ref check; should investigate
    better code reuse.
    """

    def __init__(self, context, expr, parent_table):
        self.context = context
        self.expr = expr
        self.parent_table = parent_table
        self.query_roots = frozenset(self.parent_table._root_tables())

    def get_result(self):
        self.foreign_table = None
        self.predicates = []

        self._visit(self.expr)

        if type(self.expr.op()) == ops.Any:
            op = ExistsSubquery(self.foreign_table, self.predicates)
        else:
            op = NotExistsSubquery(self.foreign_table, self.predicates)

        expr_type = dt.boolean.column_type()
        return expr_type(op)

    def _visit(self, expr):
        node = expr.op()

        for arg in node.flat_args():
            if isinstance(arg, ir.TableExpr):
                self._visit_table(arg)
            elif isinstance(arg, ir.BooleanColumn):
                for sub_expr in L.flatten_predicate(arg):
                    self.predicates.append(sub_expr)
                    self._visit(sub_expr)
            elif isinstance(arg, ir.Expr):
                self._visit(arg)
            else:
                continue

    def _visit_table(self, expr):
        node = expr.op()

        if isinstance(expr, ir.TableExpr):
            base_table = _find_blocking_table(expr)
            if base_table is not None:
                base_node = base_table.op()
                if self._is_root(base_node):
                    pass
                else:
                    # Foreign ref
                    self.foreign_table = expr
        else:
            if not node.blocks():
                for arg in node.flat_args():
                    if isinstance(arg, ir.Expr):
                        self._visit(arg)

    def _is_root(self, what):
        if isinstance(what, ir.Expr):
            what = what.op()
        return what in self.query_roots


def _find_blocking_table(expr):
    node = expr.op()

    if node.blocks():
        return expr

    for arg in node.flat_args():
        if isinstance(arg, ir.Expr):
            result = _find_blocking_table(arg)
            if result is not None:
                return result
