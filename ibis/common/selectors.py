from __future__ import annotations

import abc
from functools import reduce
from operator import and_, or_
from typing import TYPE_CHECKING

from pyroaring import BitMap

from ibis.common.bases import Abstract
from ibis.common.grounds import Concrete
from ibis.common.typing import VarTuple  # noqa: TC001

if TYPE_CHECKING:
    from collections.abc import Sequence

    import ibis.expr.types as ir


class Expandable(Abstract):
    __slots__ = ()

    @abc.abstractmethod
    def expand(self, table: ir.Table) -> Sequence[ir.Value]:
        """Expand `table` into value expressions that match the expandable object.

        Parameters
        ----------
        table
            An ibis table expression

        Returns
        -------
        Sequence[Value]
            A sequence of value expressions that match the expandable object.
        """


class Selector(Concrete, Expandable):
    """A column selector."""

    def expand(self, table: ir.Table) -> Sequence[ir.Value]:
        """Expand `table` into value expressions that match the selector.

        Parameters
        ----------
        table
            An ibis table expression

        Returns
        -------
        Sequence[Value]
            A sequence of value expressions that match the selector
        """
        return list(map(table.__getitem__, self.expand_offsets(table)))

    @abc.abstractmethod
    def expand_offsets(self, table: ir.Table) -> BitMap:
        """Compute the set of column names that match the selector."""

    def __and__(self, other: Selector) -> Selector:
        """Compute the logical conjunction of two `Selector`s.

        Parameters
        ----------
        other
            Another selector
        """
        if not isinstance(other, Selector):
            return NotImplemented
        return And(self, other)

    def __or__(self, other: Selector) -> Selector:
        """Compute the logical disjunction of two `Selector`s.

        Parameters
        ----------
        other
            Another selector
        """
        if not isinstance(other, Selector):
            return NotImplemented
        return Or(self, other)

    def __invert__(self) -> Selector:
        """Compute the logical negation of a `Selector`."""
        return Not(self)


class Or(Selector):
    left: Selector
    right: Selector

    def expand_offsets(self, table: ir.Table) -> BitMap:
        return self.left.expand_offsets(table) | self.right.expand_offsets(table)


class And(Selector):
    left: Selector
    right: Selector

    def expand_offsets(self, table: ir.Table) -> BitMap:
        return self.left.expand_offsets(table) & self.right.expand_offsets(table)


class Any(Selector):
    selectors: VarTuple[Selector]

    def expand_offsets(self, table: ir.Table) -> BitMap:
        names = (selector.expand_offsets(table) for selector in self.selectors)
        return reduce(or_, names)


class All(Selector):
    selectors: VarTuple[Selector]

    def expand_offsets(self, table: ir.Table) -> BitMap:
        names = (selector.expand_offsets(table) for selector in self.selectors)
        return reduce(and_, names)


class Not(Selector):
    selector: Selector

    def expand_offsets(self, table: ir.Table) -> BitMap:
        offsets = self.selector.expand_offsets(table)
        return BitMap(range(len(table.columns))) - offsets
