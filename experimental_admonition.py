from __future__ import annotations

import ast
import inspect

from griffe.dataclasses import Function
from griffe.extensions import VisitorExtension

from ibis import util

DOCSTRING_MODIFYING_DECORATORS = {"backend_sensitive", "experimental"}


class ExperimentalAdmonitionExtension(VisitorExtension):
    def visit_functiondef(self, node: ast.FunctionDef) -> None:
        function: Function = self.visitor.current.members[node.name]  # type: ignore[assignment]
        for decorator in getattr(function, "decorators", ()):
            value = decorator.value
            if value in DOCSTRING_MODIFYING_DECORATORS or value.endswith(
                tuple(map(".{dec}".format, DOCSTRING_MODIFYING_DECORATORS))
            ):
                name = value.rsplit(".", 1)[0]
                func = getattr(util, name)
                sig = inspect.signature(func)
                function.docstring.value = util.append_admonition(
                    function.docstring.value,
                    msg=sig.parameters["msg"].default,
                    body=sig.parameters["why"].default,
                )
            elif value == "deprecated" or value.endswith(".deprecated"):
                function.docstring.value = util.append_admonition(
                    function.docstring.value,
                    msg=sig.parameters["msg"].default,
                    body=sig.parameters["why"].default,
                )


Extension = ExperimentalAdmonitionExtension
