from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping

from multipledispatch import Dispatcher

import ibis.expr.datatypes as dt
from ibis.common.annotations import attribute
from ibis.common.collections import FrozenDict, MapSet
from ibis.common.exceptions import IntegrityError
from ibis.common.grounds import Concrete
from ibis.common.validators import Coercible
from ibis.util import indent


class Schema(Concrete, Coercible, MapSet):
    """An object for holding table schema information."""

    fields: FrozenDict[str, dt.DataType]
    """A mapping of [`str`][str] to [`DataType`][ibis.expr.datatypes.DataType] objects
    representing the type of each column."""

    def __repr__(self) -> str:
        space = 2 + max(map(len, self.names), default=0)
        return "ibis.Schema {{{}\n}}".format(
            indent(
                ''.join(
                    f'\n{name.ljust(space)}{str(type)}' for name, type in self.items()
                ),
                2,
            )
        )

    def __rich_repr__(self):
        for name, dtype in self.items():
            yield name, str(dtype)

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self) -> Iterator[str]:
        return iter(self.fields)

    def __getitem__(self, name: str) -> dt.DataType:
        return self.fields[name]

    @classmethod
    def __coerce__(cls, value) -> Schema:
        return schema(value)

    @attribute.default
    def names(self) -> tuple[str, ...]:
        return tuple(self.keys())

    @attribute.default
    def types(self) -> tuple[dt.DataType, ...]:
        return tuple(self.values())

    @attribute.default
    def _name_locs(self) -> dict[str, int]:
        return {v: i for i, v in enumerate(self.names)}

    def equals(self, other: Schema) -> bool:
        """Return whether `other` is equal to `self`.

        Parameters
        ----------
        other
            Schema to compare `self` to.

        Examples
        --------
        >>> import ibis
        >>> first = ibis.schema({"a": "int"})
        >>> second = ibis.schema({"a": "int"})
        >>> assert first.equals(second)
        >>> third = ibis.schema({"a": "array<int>"})
        >>> assert not first.equals(third)
        """
        if not isinstance(other, Schema):
            raise TypeError(
                f"invalid equality comparison between Schema and {type(other)}"
            )
        return self.__cached_equals__(other)

    @classmethod
    def from_tuples(
        cls,
        values: Iterable[tuple[str, str | dt.DataType]],
    ) -> Schema:
        """Construct a `Schema` from an iterable of pairs.

        Parameters
        ----------
        values
            An iterable of pairs of name and type.

        Returns
        -------
        Schema
            A new schema

        Examples
        --------
        >>> import ibis
        >>> ibis.Schema.from_tuples([("a", "int"), ("b", "string")])
        ibis.Schema {
          a  int64
          b  string
        }
        """
        return cls(dict(values))

    def to_pandas(self):
        """Return the equivalent pandas datatypes."""
        from ibis.backends.pandas.client import ibis_schema_to_pandas

        return ibis_schema_to_pandas(self)

    def to_pyarrow(self):
        """Return the equivalent pyarrow schema."""
        from ibis.backends.pyarrow.datatypes import ibis_to_pyarrow_schema

        return ibis_to_pyarrow_schema(self)

    def as_struct(self) -> dt.Struct:
        return dt.Struct(self)

    def name_at_position(self, i: int) -> str:
        """Return the name of a schema column at position `i`.

        Parameters
        ----------
        i
            The position of the column

        Returns
        -------
        str
            The name of the column in the schema at position `i`.

        Examples
        --------
        >>> import ibis
        >>> sch = ibis.Schema({"a": "int", "b": "string"})
        >>> sch.name_at_position(0)
        'a'
        >>> sch.name_at_position(1)
        'b'
        """
        return self.names[i]


schema = Dispatcher('schema')
infer = Dispatcher('infer')


@schema.register()
def schema_from_kwargs(**kwargs):
    return Schema(kwargs)


@schema.register(Schema)
def schema_from_schema(s):
    return s


@schema.register(Mapping)
def schema_from_mapping(d):
    return Schema(d)


@schema.register(Iterable)
def schema_from_pairs(lst):
    return Schema.from_tuples(lst)


@schema.register(type)
def schema_from_class(cls):
    return Schema(dt.dtype(cls))


@schema.register(Iterable, Iterable)
def schema_from_names_types(names, types):
    # validate lengths of names and types are the same
    if len(names) != len(types):
        raise IntegrityError('Schema names and types must have the same length')

    # validate unique field names
    name_locs = {v: i for i, v in enumerate(names)}
    if len(name_locs) < len(names):
        duplicate_names = list(names)
        for v in name_locs:
            duplicate_names.remove(v)
        raise IntegrityError(f'Duplicate column name(s): {duplicate_names}')

    # construct the schema
    fields = dict(zip(names, types))
    return Schema(fields)
