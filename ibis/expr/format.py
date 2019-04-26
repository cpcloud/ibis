from collections import Counter
from typing import Any, Callable, MutableMapping, Optional, Set

import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.util as util


class FormatMemo:
    # A little sanity hack to simplify the below

    def __init__(self) -> None:
        self.formatted: MutableMapping[ops.Node, str] = {}
        self.aliases: MutableMapping[ops.Node, str] = {}
        self.counts: MutableMapping[ops.Node, int] = Counter()
        self.subexprs: MutableMapping[ops.Node, str] = {}
        self.visit_memo: Set[ops.Node] = set()

    def __contains__(self, obj: ir.Expr) -> bool:
        return obj.op() in self.formatted

    def _format(self, expr: ir.Expr) -> str:
        return expr.op()._repr(memo=self)

    def observe(
        self,
        expr: ir.Expr,
        formatter: Optional[Callable[[ir.Expr], str]] = None,
    ) -> None:
        if formatter is None:
            formatter = self._format
        key = expr.op()
        if key not in self.formatted:
            self.aliases[key] = 'ref_{:d}'.format(len(self.formatted))
            self.formatted[key] = formatter(expr)

        self.counts[key] += 1

    def count(self, expr: ir.Expr) -> int:
        return self.counts[expr.op()]

    def get_alias(self, expr: ir.Expr) -> str:
        return self.aliases[expr.op()]

    def get_formatted(self, expr: ir.Expr) -> str:
        return self.formatted[expr.op()]


class ExprFormatter:
    """For creating a nice tree-like representation of an expression graph.

    Notes
    -----
    TODO: detect reused DAG nodes and do not display redundant information

    """

    def __init__(
        self,
        expr: ir.Expr,
        indent_size: int = 2,
        base_level: int = 0,
        memo: Optional[FormatMemo] = None,
        memoize: bool = True,
    ) -> None:
        self.expr = expr
        self.indent_size = indent_size
        self.base_level = base_level
        self.memoize = memoize

        # For tracking "extracted" objects, like tables, that we don't want to
        # print out more than once, and simply alias in the expression tree
        if memo is None:
            memo = FormatMemo()
        self.memo = memo

    def get_result(self) -> str:
        what = self.expr.op()

        if self.memoize:
            self._memoize_tables()

        if isinstance(what, ops.TableNode) and what.has_schema():
            # This should also catch aggregations
            if not self.memoize and self.expr in self.memo:
                text = 'Table: %s' % self.memo.get_alias(self.expr)
            elif isinstance(what, ops.PhysicalTable):
                text = self._format_table(self.expr)
            else:
                # Any other node type
                text = self._format_node(self.expr)
        elif isinstance(what, ops.TableColumn):
            text = self._format_column(self.expr)
        elif isinstance(what, ops.Literal):
            text = 'Literal[{}]\n  {}'.format(
                self._get_type_display(), str(what.value)
            )
        elif isinstance(what, ops.ScalarParameter):
            text = 'ScalarParameter[{}]'.format(self._get_type_display())
        elif isinstance(what, ops.Node):
            text = self._format_node(self.expr)

        if isinstance(self.expr, ir.ValueExpr) and self.expr._name is not None:
            text = '{} = {}'.format(self.expr.get_name(), text)

        if self.memoize:
            alias_to_text = sorted(
                (
                    self.memo.aliases[x],
                    self.memo.formatted[x],
                    self.memo.ops[x],
                )
                for x in self.memo.formatted
            )

            # A hack to suppress printing out of a ref that is the result of
            # the top level expression
            refs = [
                "{x}\n{y}".format(x=x, y=y)
                for x, y, op in alias_to_text
                if not op.equals(what)
            ]

            text = '\n\n'.join(refs + [text])
        return self._indent(text, self.base_level)

    def _memoize_tables(self) -> None:
        table_memo_ops = (ops.Aggregation, ops.Selection, ops.SelfReference)
        expr = self.expr
        if expr.op() in self.memo.visit_memo:
            return

        stack = [expr]
        seen = set()
        memo = self.memo

        while stack:
            e = stack.pop()
            op = e.op()

            if op not in seen:
                seen.add(op)

                if isinstance(op, ops.PhysicalTable):
                    memo.observe(e, self._format_table)
                elif isinstance(op, ops.Node):
                    stack.extend(
                        arg
                        for arg in reversed(op.args)
                        if isinstance(arg, ir.Expr)
                    )
                    if isinstance(op, table_memo_ops):
                        memo.observe(e, self._format_node)
                elif isinstance(op, ops.TableNode) and op.has_schema():
                    memo.observe(e, self._format_table)
                memo.visit_memo.add(op)

    def _indent(self, text: str, indents: int = 1) -> str:
        return util.indent(text, self.indent_size * indents)

    def _format_table(self, expr: ir.Expr):
        table = expr.op()
        # format the schema
        rows = ['name: {0!s}\nschema:'.format(table.name)]
        rows.extend(
            [
                '  %s : %s' % tup
                for tup in zip(table.schema.names, table.schema.types)
            ]
        )
        opname = type(table).__name__
        type_display = self._get_type_display(expr)
        opline = '%s[%s]' % (opname, type_display)
        return '{0}\n{1}'.format(opline, self._indent('\n'.join(rows)))

    def _format_column(self, expr: ir.Expr) -> str:
        # HACK: if column is pulled from a Filter of another table, this parent
        # will not be found in the memo
        col = expr.op()
        parent = col.parent()

        if parent not in self.memo:
            self.memo.observe(parent, formatter=self._format_node)

        table_formatted = self.memo.get_alias(parent)
        table_formatted = self._indent(table_formatted)

        type_display = self._get_type_display(self.expr)
        return "Column[{0}] '{1}' from table\n{2}".format(
            type_display, col.name, table_formatted
        )

    def _format_node(self, expr: Any) -> str:
        op = expr.op()
        formatted_args = []

        def visit(what, extra_indents=0):
            if isinstance(what, ir.Expr):
                result = self._format_subexpr(what)
            else:
                result = self._indent(str(what))

            if extra_indents > 0:
                result = util.indent(result, self.indent_size)

            formatted_args.append(result)

        arg_names = getattr(op, 'display_argnames', op.argnames)

        if not arg_names:
            util.consume(map(visit, op.flat_args()))
        else:
            signature = op.signature
            arg_name_pairs = (
                (arg, name)
                for arg, name in zip(op.args, arg_names)
                if signature[name].show
            )
            for arg, name in arg_name_pairs:
                if name == 'arg' and isinstance(op, ops.ValueOp):
                    # don't display first argument's name in repr
                    name = None
                if name is not None:
                    name = self._indent('{}:'.format(name))
                if util.is_iterable(arg):
                    if name is not None and len(arg) > 0:
                        formatted_args.append(name)
                        indents = 1
                    else:
                        indents = 0
                    for x in arg:
                        visit(x, extra_indents=indents)
                else:
                    if name is not None:
                        formatted_args.append(name)
                        indents = 1
                    else:
                        indents = 0
                    visit(arg, extra_indents=indents)

        opname = type(op).__name__
        type_display = self._get_type_display(expr)
        opline = "{}[{}]".format(opname, type_display)
        return "\n".join([opline] + formatted_args)

    def _format_subexpr(self, expr: ir.Expr) -> str:
        subexprs = self.memo.subexprs
        key = expr.op()
        try:
            result = subexprs[key]
        except KeyError:
            formatter = ExprFormatter(expr, memo=self.memo, memoize=False)
            result = subexprs[key] = self._indent(formatter.get_result(), 1)
        return result

    def _get_type_display(self, expr: Optional[ir.Expr] = None) -> str:
        if expr is None:
            expr = self.expr
        return expr._type_display()
