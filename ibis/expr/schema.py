import collections

from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from multipledispatch import Dispatcher

import ibis.common as com
import ibis.util as util
import ibis.expr.datatypes as dt


class Schema:
    """An object for holding table schema information.

    Parameters
    ----------
    names
        A sequence of ``str`` indicating the name of each column.
    types
        A sequence of :class:`~ibis.expr.datatypes.DataType` objects
        representing type of each column.

    """

    __slots__ = '_names', '_types', '_name_locs'

    def __init__(
        self, names: Iterable[str], types: Iterable[Union[dt.DataType, str]]
    ) -> None:
        self._names = tuple(names)
        self._types = tuple(map(dt.dtype, types))
        self._name_locs = {v: i for i, v in enumerate(self.names)}

        if len(self._name_locs) < len(self._names):
            raise com.IntegrityError('Duplicate column names')

    @property
    def names(self) -> List[str]:
        return list(self._names)

    @property
    def types(self) -> List[dt.DataType]:
        return list(self._types)

    def __repr__(self) -> str:
        space = 2 + max(map(len, self._names))
        return "ibis.Schema {{{}\n}}".format(
            util.indent(
                ''.join(
                    '\n{}{}'.format(name.ljust(space), str(type))
                    for name, type in zip(self._names, self._types)
                ),
                2,
            )
        )

    def __hash__(self) -> int:
        return hash((type(self), self._names, self._types))

    def __len__(self) -> int:
        return len(self._names)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __contains__(self, name: str) -> bool:
        return name in self._name_locs

    def __getitem__(self, name: str) -> dt.DataType:
        return self._types[self._name_locs[name]]

    def __getstate__(self) -> Dict[str, Any]:
        return {slot: getattr(self, slot) for slot in self.__class__.__slots__}

    def __setstate__(self, instance_dict: Mapping[str, Any]) -> None:
        for key, value in instance_dict.items():
            setattr(self, key, value)

    def delete(self, names_to_delete: Sequence[str]) -> 'Schema':
        for name in names_to_delete:
            if name not in self:
                raise KeyError(
                    '{!r} is not a column in:\n{}'.format(name, self)
                )

        names, types = zip(
            *(
                (name, type)
                for name, type in zip(self._names, self._types)
                if name not in names_to_delete
            )
        )
        return Schema(names, types)

    @classmethod
    def from_tuples(
        cls, values: Iterable[Tuple[str, Union[str, dt.DataType]]]
    ) -> 'Schema':
        pairs = tuple(values)
        if pairs:
            names, types = zip(*pairs)
        else:
            names, types = (), ()
        return Schema(names, types)

    @classmethod
    def from_dict(
        cls, dictionary: Mapping[str, Union[str, dt.DataType]]
    ) -> 'Schema':
        return Schema(*zip(*dictionary.items()))

    def equals(
        self,
        other: 'Schema',
        cache: Optional[Mapping[Tuple['Schema', 'Schema'], bool]] = None,
    ) -> bool:
        return self._names == other._names and self._types == other._types

    def __eq__(self, other: 'Schema') -> bool:  # type: ignore
        return self.equals(other)

    def __ne__(self, other: 'Schema') -> bool:  # type: ignore
        return not self.equals(other)

    def __gt__(self, other: 'Schema') -> bool:
        return frozenset(self.items()) > frozenset(other.items())

    def __ge__(self, other: 'Schema') -> bool:
        return frozenset(self.items()) >= frozenset(other.items())

    def append(self, schema: 'Schema') -> 'Schema':
        return Schema(self.names + schema.names, self.types + schema.types)

    def items(self) -> Iterator[Tuple[str, dt.DataType]]:
        return zip(self._names, self._types)

    def name_at_position(self, i: int) -> str:
        """Return the name of the column located at position `i`."""
        names = self._names
        upper = len(names) - 1
        if not 0 <= i <= upper:
            raise ValueError(
                'Column index must be between 0 and {:d}, inclusive'.format(
                    upper
                )
            )
        return names[i]


class HasSchema:
    """Class representing a structured dataset with a well-defined schema.

    Base implementation is for tables that do not reference a particular
    concrete dataset or database table.

    """

    def __repr__(self) -> str:
        return '{}({})'.format(type(self).__name__, repr(self.schema))

    def has_schema(self) -> bool:
        return True

    def equals(
        self,
        other: 'HasSchema',
        cache: Optional[Mapping[Tuple[Schema, Schema], bool]] = None,
    ) -> bool:
        return type(self) is type(other) and self.schema.equals(
            other.schema, cache=cache
        )

    def root_tables(self) -> List:
        return [self]

    @property
    def schema(self) -> Schema:
        """Return the schema of this object."""
        raise NotImplementedError()


schema = Dispatcher(
    'schema', doc="Construct a schema from various input types."
)
infer = Dispatcher('infer', doc="Infer the type of an object.")


@schema.register(Schema)
def identity(s: Schema) -> Schema:
    """Return a Schema."""
    return s


@schema.register(collections.Mapping)
def schema_from_mapping(d: Mapping[str, Union[str, dt.DataType]]) -> Schema:
    """Construct a Schema a Mapping."""
    return Schema.from_dict(d)


@schema.register(collections.Iterable)
def schema_from_pairs(
    lst: Iterable[Tuple[str, Union[str, dt.DataType]]]
) -> Schema:
    """Construct a Schema from an Iterable of pairs."""
    return Schema.from_tuples(lst)


@schema.register(collections.Iterable, collections.Iterable)
def schema_from_names_types(
    names: Iterable[str], types: Iterable[Union[str, dt.DataType]]
) -> Schema:
    """Make a Schema from an iterable of names and an iterable of types."""
    return Schema(names, types)
