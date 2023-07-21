from __future__ import annotations

from abc import abstractmethod
from typing import Generic, Mapping, Sequence, TypeVar

from bidict import FrozenOrderedBidict, frozenbidict

from ibis.common.typing import VarTuple
from ibis.expr.datatypes.core import DataType
from ibis.expr.operations.core import Named, Node, Value
from ibis.expr.operations.sortkeys import SortKey
from ibis.expr.schema import Schema

T_co = TypeVar("T_co", covariant=True, bound="Relation")


class Root(Generic[T_co]):
    rel: T_co
    names: FrozenOrderedBidict[str, int]

    def __init__(self, rel: T_co, names: FrozenOrderedBidict[str, int]) -> None:
        self.rel = rel
        self.names = names

    @property
    def schema(self) -> Schema:
        """Return the schema for the underlying relation, including column names."""
        names = self.names.inverse
        return Schema(
            {names[i]: dtype for i, dtype in self.rel.effective_schema.items()}
        )

    def to_expr(self):
        import ibis.expr.types as ir

        return ir.Table(self)


class Relation(Node):
    @property
    @abstractmethod
    def schema(self) -> Sequence[DataType]:
        """Return a list of types, one for each column."""

    @property
    def emit(self) -> Sequence[int]:
        return range(len(self.schema))

    @property
    def effective_schema(self) -> Mapping[int, DataType]:
        return {i: self.schema[i] for i in self.emit}


class PhysicalTable(Relation, Named):
    name: str


class UnboundTable(PhysicalTable):
    schema: tuple[DataType, ...]


class Project(Relation):
    parent: Relation
    fields: tuple[Value, ...]
    emit: tuple[int, ...]

    @property
    def schema(self) -> Sequence[DataType]:
        return self.parent.schema + tuple(field.output_dtype for field in self.fields)

    @classmethod
    def as_root(cls, root: Root[Project], exprs: dict[str, Value]) -> Root:
        import ibis.expr.operations as ops

        parent = root.rel
        emit = []
        additional_fields = []
        names = []
        root_names = root.names
        for i, (name, op) in enumerate(exprs.items()):
            names.append(name)

            if isinstance(op, ops.TableColumn):
                idx = root_names[name]
            else:
                additional_fields.append(op)
                idx = len(parent.schema) + i

            emit.append(idx)

        return Root(
            rel=cls(parent=parent, fields=additional_fields, emit=emit),
            names=frozenbidict(zip(names, emit)),
        )


class Filter(Relation):
    parent: Relation
    predicate: Value

    @property
    def schema(self):
        return self.parent.schema


class Sort(Relation):
    parent: Relation
    order_by: VarTuple[SortKey]

    @property
    def schema(self):
        return self.parent.schema


class Join(Relation):
    left: Relation
    right: Relation
    predicate: Value

    @property
    def schema(self) -> Sequence[DataType]:
        return list(self.left.schema) + list(self.right.schema)


class Union(Relation):
    left: Relation
    right: Relation
    distinct: bool

    @property
    def schema(self):
        return self.left.schema


if __name__ == "__main__":
    import ibis

    def table(schema: Schema, name: str) -> PhysicalTable:
        return Root(
            UnboundTable(schema=list(schema.values()), name=name),
            names=frozenbidict(dict(zip(schema.keys(), range(len(schema))))),
        )

    def select(root, *names: str):
        import ibis.expr.operations as ops

        return Project.as_root(
            root,
            {
                name: ops.Add(ops.TableColumn(name, output_dtype=root.schema[name]), 1)
                for name in names
            },
        )

    t = table(ibis.schema(dict(a="int", b="int")), name="t")
    rel = t.rel
    print("                          table:    [emit]:", rel.emit)
    print()

    p1 = select(t, "a", "b")

    rel = p1.rel
    print("           select a, b [fields]:", rel.fields, "[emit]:", rel.emit)
    print()

    p2 = select(p1, "b")

    rel = p2.rel
    print("select b (select a, b) [fields]:", rel.fields, "[emit]:", rel.emit)

    p3 = select(p2, "b")
    rel = p3.rel
    print("select b (select a, b) [fields]:", rel.fields, "[emit]:", rel.emit)
