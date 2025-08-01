from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Union

import ibis.expr.datatypes as dt
from ibis.common.annotations import attribute
from ibis.common.collections import FrozenOrderedDict, MapSet
from ibis.common.dispatch import lazy_singledispatch
from ibis.common.exceptions import InputTypeError, IntegrityError
from ibis.common.grounds import Concrete
from ibis.common.patterns import Coercible
from ibis.util import deprecated, indent

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa
    import sqlglot as sg
    import sqlglot.expressions as sge
    from pandas.api.extensions import ExtensionDtype
    from typing_extensions import Self, TypeAlias


class Schema(Concrete, Coercible, MapSet[str, dt.DataType]):
    """An ordered mapping of str -> [datatype](./datatypes.qmd), used to hold a [Table](./expression-tables.qmd#ibis.expr.tables.Table)'s schema."""

    fields: FrozenOrderedDict[str, dt.DataType]
    """A mapping of [](`str`) to
    [`DataType`](./datatypes.qmd#ibis.expr.datatypes.DataType)
    objects representing the type of each column."""

    def __repr__(self) -> str:
        space = 2 + max(map(len, self.names), default=0)
        return "ibis.Schema {{{}\n}}".format(
            indent(
                "".join(
                    f"\n{name.ljust(space)}{type!s}" for name, type in self.items()
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
        if isinstance(value, cls):
            return value
        return schema(value)

    @attribute
    def names(self):
        return tuple(self.keys())

    @attribute
    def types(self):
        return tuple(self.values())

    @attribute
    def geospatial(self) -> tuple[str, ...]:
        return tuple(name for name, typ in self.fields.items() if typ.is_geospatial())

    @attribute
    def null_fields(self) -> tuple[str, ...]:
        return tuple(name for name, typ in self.fields.items() if typ.is_null())

    @attribute
    def _name_locs(self) -> dict[str, int]:
        return {v: i for i, v in enumerate(self.names)}

    def equals(self, other: Schema) -> bool:
        """Return whether `other` is equal to `self`.

        The order of fields in the schema is taken into account when computing equality.

        Parameters
        ----------
        other
            Schema to compare `self` to.

        Examples
        --------
        >>> import ibis
        >>> xy = ibis.schema({"x": int, "y": str})
        >>> xy2 = ibis.schema({"x": int, "y": str})
        >>> yx = ibis.schema({"y": str, "x": int})
        >>> xy_float = ibis.schema({"x": float, "y": str})
        >>> assert xy.equals(xy2)
        >>> assert not xy.equals(yx)
        >>> assert not xy.equals(xy_float)
        """
        if not isinstance(other, Schema):
            raise TypeError(
                f"invalid equality comparison between Schema and {type(other)}"
            )
        return self == other

    @classmethod
    def from_tuples(
        cls,
        values: Iterable[tuple[str, str | dt.DataType]],
    ) -> Self:
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

        pairs = list(values)
        if not pairs:
            return cls({})

        names, types = zip(*pairs)

        # validate unique field names
        name_counts = Counter(names)
        [(_, most_common_count)] = name_counts.most_common(1)

        if most_common_count > 1:
            duplicate_names = [name for name, count in name_counts.items() if count > 1]
            raise IntegrityError(f"Duplicate column name(s): {duplicate_names}")

        # construct the schema
        return cls(dict(zip(names, types)))

    @classmethod
    def from_numpy(cls, numpy_schema) -> Self:
        """Return the equivalent ibis schema."""
        from ibis.formats.numpy import NumpySchema

        return NumpySchema.to_ibis(numpy_schema)

    @classmethod
    def from_pandas(cls, pandas_schema) -> Self:
        """Return the equivalent ibis schema."""
        from ibis.formats.pandas import PandasSchema

        return PandasSchema.to_ibis(pandas_schema)

    @classmethod
    def from_pyarrow(cls, pyarrow_schema) -> Self:
        """Return the equivalent ibis schema."""
        from ibis.formats.pyarrow import PyArrowSchema

        return PyArrowSchema.to_ibis(pyarrow_schema)

    @classmethod
    def from_polars(cls, polars_schema) -> Self:
        """Return the equivalent ibis schema."""
        from ibis.formats.polars import PolarsSchema

        return PolarsSchema.to_ibis(polars_schema)

    @classmethod
    def from_sqlglot(
        cls, schema: sge.Schema, dialect: str | sg.Dialect | None = None
    ) -> Self:
        """Construct an Ibis Schema from a SQLGlot Schema.

        Parameters
        ----------
        schema
            A SQLGlot Schema containing column definitions.
        dialect
            Optional dialect to use for type conversion.

        Returns
        -------
        Schema
            An Ibis Schema.

        Examples
        --------
        >>> import ibis
        >>> import sqlglot as sg
        >>> import sqlglot.expressions as sge
        >>> columns = [
        ...     sge.ColumnDef(
        ...         this=sg.to_identifier("a", quoted=True),
        ...         kind=sge.DataType(this=sge.DataType.Type.BIGINT),
        ...     ),
        ...     sge.ColumnDef(
        ...         this=sg.to_identifier("b", quoted=True),
        ...         kind=sge.DataType(this=sge.DataType.Type.VARCHAR),
        ...         constraints=[sge.ColumnConstraint(kind=sge.NotNullColumnConstraint())],
        ...     ),
        ... ]
        >>> schema_expr = sge.Schema(expressions=columns)
        >>> sch = ibis.Schema.from_sqlglot(schema_expr)
        >>> sch
        ibis.Schema {
          a  int64
          b  !string
        }

        Different source dialects are supported using the `dialect` keyword argument.

        >>> columns = [
        ...     sge.ColumnDef(
        ...         this=sg.to_identifier("a", quoted=True),
        ...         kind=sge.DataType(
        ...             this=sge.DataType.Type.ARRAY,
        ...             expressions=[sge.DataType(this=sge.DataType.Type.BIGINT, nested=False)],
        ...             nested=True,
        ...         ),
        ...     )
        ... ]
        >>> schema_expr = sge.Schema(expressions=columns)
        >>> snowflake_schema = ibis.Schema.from_sqlglot(schema_expr, dialect="snowflake")
        >>> snowflake_schema
        ibis.Schema {
          a  array<json>
        }
        >>> bigquery_schema = ibis.Schema.from_sqlglot(schema_expr, dialect="bigquery")
        >>> bigquery_schema
        ibis.Schema {
          a  array<int64>
        }
        """
        import sqlglot.expressions as sge

        from ibis.backends.sql.datatypes import TYPE_MAPPERS, SqlglotType

        expressions = schema.expressions
        if not expressions:
            return cls({})

        type_mapper_class = TYPE_MAPPERS.get(dialect, SqlglotType)
        type_mapper = type_mapper_class()
        fields = {}

        for column in expressions:
            name = column.this.this

            nullable = not any(
                isinstance(constraint.kind, sge.NotNullColumnConstraint)
                for constraint in (column.constraints or [])
            )

            if column.kind:
                ibis_dtype = type_mapper.to_ibis(column.kind, nullable=nullable)
            else:
                ibis_dtype = dt.String(nullable=nullable)

            fields[name] = ibis_dtype

        return cls(fields)

    def to_numpy(self) -> list[tuple[str, np.dtype]]:
        """Return the equivalent numpy dtypes."""
        from ibis.formats.numpy import NumpySchema

        return NumpySchema.from_ibis(self)

    def to_pandas(self) -> list[tuple[str, np.dtype | ExtensionDtype]]:
        """Return the equivalent pandas datatypes."""
        from ibis.formats.pandas import PandasSchema

        return PandasSchema.from_ibis(self)

    def to_pyarrow(self) -> pa.Schema:
        """Return the equivalent pyarrow schema."""
        from ibis.formats.pyarrow import PyArrowSchema

        return PyArrowSchema.from_ibis(self)

    def __arrow_c_schema__(self):
        return self.to_pyarrow().__arrow_c_schema__()

    def to_polars(self):
        """Return the equivalent polars schema."""
        from ibis.formats.polars import PolarsSchema

        return PolarsSchema.from_ibis(self)

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

    def to_sqlglot_column_defs(self, dialect: str | sg.Dialect) -> list[sge.ColumnDef]:
        """Convert the schema to a list of SQL column definitions.

        Parameters
        ----------
        dialect
            The SQL dialect to use.

        Returns
        -------
        list[sqlglot.expressions.ColumnDef]
            A list of SQL column definitions.

        Examples
        --------
        >>> import ibis
        >>> sch = ibis.schema({"a": "int", "b": "!string"})
        >>> sch
        ibis.Schema {
          a  int64
          b  !string
        }
        >>> columns = sch.to_sqlglot_column_defs(dialect="duckdb")
        >>> columns
        [ColumnDef(
          this=Identifier(this='a', quoted=True),
          kind=DataType(this=Type.BIGINT)), ColumnDef(
          this=Identifier(this='b', quoted=True),
          kind=DataType(this=Type.VARCHAR),
          constraints=[
            ColumnConstraint(
              kind=NotNullColumnConstraint())])]

        One use case for this method is to embed its output into a SQLGlot
        `CREATE TABLE` expression.

        >>> import sqlglot as sg
        >>> import sqlglot.expressions as sge
        >>> table = sg.table("t", quoted=True)
        >>> ct = sge.Create(
        ...     kind="TABLE",
        ...     this=sge.Schema(
        ...         this=table,
        ...         expressions=columns,
        ...     ),
        ... )
        >>> ct.sql(dialect="duckdb")
        'CREATE TABLE "t" ("a" BIGINT, "b" TEXT NOT NULL)'
        """
        import sqlglot as sg
        import sqlglot.expressions as sge

        from ibis.backends.sql.datatypes import TYPE_MAPPERS as type_mappers

        type_mapper = type_mappers[dialect]
        return [
            sge.ColumnDef(
                this=sg.to_identifier(name, quoted=True),
                kind=type_mapper.from_ibis(dtype),
                constraints=(
                    None
                    if dtype.nullable
                    else [sge.ColumnConstraint(kind=sge.NotNullColumnConstraint())]
                ),
            )
            for name, dtype in self.items()
        ]

    @deprecated(
        instead="use Schema.to_sqlglot_column_defs() instead. In a future release, to_sqlglot() will return a sqlglot.schema.Schema object"
    )
    def to_sqlglot(self, dialect: str | sg.Dialect) -> list[sge.ColumnDef]:
        """DEPRECATED: use `to_sqlglot_column_defs()` instead."""
        return self.to_sqlglot_column_defs(dialect)


SchemaLike: TypeAlias = Union[
    Schema,
    Mapping[str, Union[str, dt.DataType]],
    Iterable[tuple[str, Union[str, dt.DataType]]],
]


@lazy_singledispatch
def schema(value: Any) -> Schema:
    """Construct ibis schema from schema-like python objects."""
    raise InputTypeError(value)


@lazy_singledispatch
def infer(value: Any) -> Schema:
    """Infer the corresponding ibis schema for a python object."""
    raise InputTypeError(value)


@schema.register(Schema)
def from_schema(s):
    return s


@schema.register(Mapping)
def from_mapping(d):
    return Schema(d)


@schema.register(Iterable)
def from_pairs(lst):
    return Schema.from_tuples(lst)


@schema.register(type)
def from_class(cls):
    return Schema(dt.dtype(cls))


@schema.register("pandas.Series")
def from_pandas_series(s):
    from ibis.formats.pandas import PandasSchema

    return PandasSchema.to_ibis(s)


@schema.register("pyarrow.Schema")
def from_pyarrow_schema(schema):
    from ibis.formats.pyarrow import PyArrowSchema

    return PyArrowSchema.to_ibis(schema)


@infer.register("pandas.DataFrame")
def infer_pandas_dataframe(df):
    from ibis.formats.pandas import PandasData

    return PandasData.infer_table(df)


@infer.register("pyarrow.Table")
def infer_pyarrow_table(table):
    from ibis.formats.pyarrow import PyArrowSchema

    return PyArrowSchema.to_ibis(table.schema)


@infer.register("polars.DataFrame")
@infer.register("polars.LazyFrame")
def infer_polars_dataframe(df):
    from ibis.formats.polars import PolarsSchema

    return PolarsSchema.to_ibis(df.collect_schema())


# lock the dispatchers to avoid adding new implementations
del infer.register
del schema.register
