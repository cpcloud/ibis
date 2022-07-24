"""Sort key operations."""

from public import public

import ibis.expr.rules as rlz
from ibis.expr.operations.core import Value


@public
class SortKey(Value):
    """A sort operation."""

    expr = rlz.any

    output_dtype = rlz.dtype_like("expr")
    output_shape = rlz.Shape.COLUMNAR

    @property
    def name(self) -> str:
        return self.expr.name

    @property
    def output_dtype(self):
        return self.expr.output_dtype


@public
class SortAsc(SortKey):
    pass


@public
class SortDesc(SortKey):
    pass
