from __future__ import annotations

import contextlib
import tempfile
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow_hotfix  # noqa: F401
import sqlglot as sg
import sqlglot.expressions as sge

import ibis
import ibis.common.exceptions as com
import ibis.config
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
from ibis import util
from ibis.backends import clickhouse
from ibis.backends.sql.compiler import STAR, C

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

    import pandas as pd


class Backend(clickhouse.Backend):
    name = "chdb"

    def do_connect(self, path: str | Path | None = None, database: str | None = None):
        from chdb.session import Session

        self.con = Session(path=path)

        if database is not None:
            self.con.query(f"CREATE DATABASE IF NOT EXISTS {database}")
            self.con.query(f"USE {database}")

    @property
    def version(self) -> str:
        import chdb

        return chdb.__version__

    @contextlib.contextmanager
    def _safe_raw_sql(self, *args, fmt: str = "Arrow", **kwargs):
        yield self.raw_sql(*args, fmt=fmt, **kwargs)

    @property
    def current_database(self) -> str:
        import chdb

        with self._safe_raw_sql(
            sg.select(self.compiler.f.currentDatabase().as_("db"))
        ) as result:
            table = chdb.to_arrowTable(result)
        (db,) = table["db"].to_pylist()
        return db

    def list_databases(self, like: str | None = None) -> list[str]:
        import chdb

        with self._safe_raw_sql(
            sg.select(C.name).from_(sg.table("databases", db="system"))
        ) as result:
            table = chdb.to_arrowTable(result)

        databases = table["name"].to_pylist()
        return self._filter_with_like(databases, like)

    def list_tables(
        self, like: str | None = None, database: str | None = None
    ) -> list[str]:
        import chdb

        query = sg.select(C.name).from_(sg.table("tables", db="system"))

        if database is None:
            database = self.compiler.f.currentDatabase()
        else:
            database = sge.convert(database)

        query = query.where(C.database.eq(database).or_(C.is_temporary))

        with self._safe_raw_sql(query) as result:
            table = chdb.to_arrowTable(result)

        names = table["name"].to_pylist()
        return self._filter_with_like(names, like)

    def to_pyarrow(
        self,
        expr: ir.Expr,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        external_tables: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ):
        # we convert to batches first to avoid a bunch of rigmarole resulting
        # from the following rough edges
        #
        # 1. clickhouse's awkward
        #    client-settings-are-permissioned-on-the-server "feature"
        # 2. the bizarre conversion of `DateTime64` without scale to arrow
        #    uint32 inside of clickhouse
        # 3. the fact that uint32 cannot be cast to pa.timestamp without first
        #    casting it to int64
        #
        # the extra code to make this dance work without first converting to
        # record batches isn't worth it without some benchmarking
        with self.to_pyarrow_batches(
            expr=expr,
            params=params,
            limit=limit,
            external_tables=external_tables,
            **kwargs,
        ) as reader:
            table = reader.read_all()

        return expr.__pyarrow_result__(table)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        *,
        limit: int | str | None = None,
        params: Mapping[ir.Scalar, Any] | None = None,
        external_tables: Mapping[str, Any] | None = None,
        chunk_size: int = 1_000_000,
        **_: Any,
    ) -> pa.ipc.RecordBatchReader:
        """Execute expression and return an iterator of pyarrow record batches.

        This method is eager and will execute the associated expression
        immediately.

        Parameters
        ----------
        expr
            Ibis expression to export to pyarrow
        limit
            An integer to effect a specific row limit. A value of `None` means
            "no limit". The default is in `ibis/config.py`.
        params
            Mapping of scalar parameter expressions to value.
        external_tables
            External data
        chunk_size
            Maximum number of row to return in a single chunk

        Returns
        -------
        results
            RecordBatchReader

        Notes
        -----
        There are a variety of ways to implement clickhouse -> record batches.

        1. FORMAT ArrowStream -> record batches via raw_query
           This has the same type conversion problem(s) as `to_pyarrow`.
           It's harder to address due to lack of `cast` on `RecordBatch`.
           However, this is a ClickHouse problem: we should be able to get
           string data out without a bunch of settings/permissions rigmarole.
        2. Native -> Python objects -> pyarrow batches
           This is what is implemented, using `query_column_block_stream`.
        3. Native -> Python objects -> DataFrame chunks -> pyarrow batches
           This is not implemented because it adds an unnecessary pandas step in
           between Python object -> arrow. We can go directly to record batches
           without pandas in the middle.

        """
        table = expr.as_table()
        sql = self.compile(table, limit=limit, params=params)

        self._register_in_memory_tables(expr)

        blocks = self.con.query(sql, fmt="ArrowStream")

        self._log(sql)
        return pa.ipc.open_stream(blocks.get_memview().view())

    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        # only register if we haven't already done so
        if (name := op.name) not in self.list_tables():
            table = op.data.to_pyarrow(op.schema)
            with tempfile.NamedTemporaryFile(mode="w+b") as tmpfile:
                with pa.RecordBatchStreamWriter(tmpfile, schema=table.schema) as writer:
                    writer.write(table)

                # necessary so that the file can be read immediately after writing
                tmpfile.seek(0)

                # creating a table with ENGINE = Memory will return a table
                # with zero rows while using a specific on-disk format seems to
                # work
                ddl = sge.Create(
                    this=sg.to_identifier(name, quoted=self.compiler.quoted),
                    kind="TABLE",
                    properties=sge.Properties(
                        expressions=[
                            sge.EngineProperty(
                                this=self.compiler.v["File(ArrowStream)"]
                            )
                        ]
                    ),
                    expression=sg.select(STAR).from_(
                        self.compiler.f.file(tmpfile.name, "ArrowStream")
                    ),
                )
                sql = ddl.sql(self.dialect)
                self._log(sql)
                self.con.query(sql)

    def execute(
        self, expr: ir.Expr, limit: str | None = "default", **kwargs: Any
    ) -> Any:
        """Execute an expression."""
        import chdb
        import pandas as pd

        table = expr.as_table()
        sql = self.compile(table, limit=limit, **kwargs)

        schema = table.schema()
        self._log(sql)

        self._register_in_memory_tables(expr)
        res = self.con.query(sql, fmt="Arrow")
        df = chdb.to_df(res)

        if df.empty:
            df = pd.DataFrame(columns=schema.names)
        else:
            df.columns = list(schema.names)

        # TODO: remove the extra conversion
        #
        # the extra __pandas_result__ call is to work around slight differences
        # in single column conversion and whole table conversion
        return expr.__pandas_result__(table.__pandas_result__(df))

    def insert(self, name: str, obj: pd.DataFrame | ir.Table, overwrite: bool = False):
        if overwrite:
            self.truncate_table(name)

        self._register_in_memory_tables(obj)

        query = sge.insert(self.compile(obj), into=name, dialect=self.dialect)
        return self.con.command(query.sql(self.dialect))

    def raw_sql(self, query: str | sge.Expression, **kwargs) -> Any:
        """Execute a SQL string `query` against the database.

        Parameters
        ----------
        query
            Raw SQL string or sqlglot expression
        kwargs
            Backend specific query arguments

        Returns
        -------
        Cursor
            Clickhouse cursor

        """
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect, pretty=True)
        self._log(query)
        return self.con.query(query, **kwargs)

    def get_schema(
        self, table_name: str, database: str | None = None, schema: str | None = None
    ) -> sch.Schema:
        """Return a Schema object for the indicated table and database.

        Parameters
        ----------
        table_name
            May **not** be fully qualified. Use `database` if you want to
            qualify the identifier.
        database
            Database name
        schema
            Schema name, not supported by ClickHouse

        Returns
        -------
        sch.Schema
            Ibis schema

        """
        import chdb

        if schema is not None:
            raise com.UnsupportedBackendFeatureError(
                "`schema` namespaces are not supported by clickhouse"
            )
        query = sge.Describe(this=sg.table(table_name, db=database))
        with self._safe_raw_sql(query) as results:
            table = chdb.to_arrowTable(results)

        names = table["name"].to_pylist()
        types = table["type"].to_pylist()
        return sch.Schema(
            dict(zip(names, map(self.compiler.type_mapper.from_string, types)))
        )

    def _load_into_cache(self, name, expr):
        self.create_table(name, expr, schema=expr.schema())

    def _metadata(self, query: str) -> sch.Schema:
        import chdb

        with self._safe_raw_sql(
            f"EXPLAIN json = 1, header = 1, description = 0 {query}"
        ) as results:
            table = chdb.to_arrowTable(results)

        names = table["name"].to_pylist()
        types = table["type"].to_pylist()
        return zip(names, map(self.compiler.type_mapper.from_string, types))

    def create_database(
        self, name: str, *, force: bool = False, engine: str = "Atomic"
    ) -> None:
        src = sge.Create(
            this=sg.to_identifier(name),
            kind="DATABASE",
            exists=force,
            properties=sge.Properties(
                expressions=[sge.EngineProperty(this=sg.to_identifier(engine))]
            ),
        )
        with self._safe_raw_sql(src):
            pass

    def drop_database(self, name: str, *, force: bool = False) -> None:
        src = sge.Drop(this=sg.to_identifier(name), kind="DATABASE", exists=force)
        with self._safe_raw_sql(src):
            pass

    def truncate_table(self, name: str, database: str | None = None) -> None:
        ident = sg.table(name, db=database).sql(self.dialect)
        with self._safe_raw_sql(f"TRUNCATE TABLE {ident}"):
            pass

    def _get_temp_view_definition(self, name: str, definition: str) -> str:
        return sge.Create(
            this=sg.to_identifier(name, quoted=self.compiler.quoted),
            kind="VIEW",
            expression=definition,
            replace=True,
        )

    def _create_temp_view(self, table_name, source):
        sql = self._get_temp_view_definition(table_name, source)
        with self._safe_raw_sql(sql):
            pass

    def read_parquet(
        self, path: str | Path, table_name: str | None = None, **kwargs: Any
    ) -> ir.Table:
        if table_name is None:
            table_name = util.gen_name("chdb_read_parquet")
        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.file(str(path), "Parquet")),
        )
        return self.table(table_name)

    def create_table(
        self,
        name: str,
        obj: pd.DataFrame | pa.Table | ir.Table | None = None,
        *,
        schema: ibis.Schema | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
        # backend specific arguments
        engine: str = "MergeTree",
        order_by: Iterable[str] | None = None,
        partition_by: Iterable[str] | None = None,
        sample_by: str | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> ir.Table:
        """Create a table in a ClickHouse database.

        Parameters
        ----------
        name
            Name of the table to create
        obj
            Optional data to create the table with
        schema
            Optional names and types of the table
        database
            Database to create the table in
        temp
            Create a temporary table. This is not yet supported, and exists for
            API compatibility.
        overwrite
            Whether to overwrite the table
        engine
            The table engine to use. See [ClickHouse's `CREATE TABLE` documentation](https://clickhouse.com/docs/en/sql-reference/statements/create/table)
            for specifics. Defaults to [`MergeTree`](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
            with `ORDER BY tuple()` because `MergeTree` is the most
            feature-complete engine.
        order_by
            String column names to order by. Required for some table engines like `MergeTree`.
        partition_by
            String column names to partition by
        sample_by
            String column names to sample by
        settings
            Key-value pairs of settings for table creation

        Returns
        -------
        Table
            The new table

        """
        if temp and overwrite:
            raise com.IbisInputError(
                "Cannot specify both `temp=True` and `overwrite=True` for ClickHouse"
            )

        if obj is None and schema is None:
            raise com.IbisError("The `schema` or `obj` parameter is required")

        if obj is not None and not isinstance(obj, ir.Expr):
            obj = ibis.memtable(obj, schema=schema)

        if schema is None:
            schema = obj.schema()

        this = sge.Schema(
            this=sg.table(name, db=database),
            expressions=[
                sge.ColumnDef(
                    this=sg.to_identifier(name),
                    kind=self.compiler.type_mapper.from_ibis(typ),
                )
                for name, typ in schema.items()
            ],
        )
        properties = [
            # the engine cannot be quoted, since clickhouse won't allow e.g.,
            # "File(Native)"
            sge.EngineProperty(this=sg.to_identifier(engine, quoted=False))
        ]

        if temp:
            properties.append(sge.TemporaryProperty())

        if order_by is not None or engine == "MergeTree":
            # engine == "MergeTree" requires an order by clause, which is the
            # empty tuple if order_by is False-y
            properties.append(
                sge.Order(
                    expressions=[
                        sge.Ordered(
                            this=sge.Tuple(
                                expressions=list(map(sg.column, order_by or ()))
                            )
                        )
                    ]
                )
            )

        if partition_by is not None:
            properties.append(
                sge.PartitionedByProperty(
                    this=sge.Schema(
                        expressions=list(map(sg.to_identifier, partition_by))
                    )
                )
            )

        if sample_by is not None:
            properties.append(
                sge.SampleProperty(
                    this=sge.Tuple(expressions=list(map(sg.column, sample_by)))
                )
            )

        if settings:
            properties.append(
                sge.SettingsProperty(
                    expressions=[
                        sge.SetItem(
                            this=sge.EQ(
                                this=sg.to_identifier(name),
                                expression=sge.convert(value),
                            )
                        )
                        for name, value in settings.items()
                    ]
                )
            )

        expression = None

        if obj is not None:
            expression = self._to_sqlglot(obj)
            self._register_in_memory_tables(obj)

        code = sge.Create(
            this=this,
            kind="TABLE",
            replace=overwrite,
            expression=expression,
            properties=sge.Properties(expressions=properties),
        )

        # create the table
        sql = code.sql(self.dialect, pretty=True)
        self.con.query(sql)

        return self.table(name, database=database)

    def create_view(
        self,
        name: str,
        obj: ir.Table,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> ir.Table:
        expression = self._to_sqlglot(obj)
        src = sge.Create(
            this=sg.table(name, db=database),
            kind="VIEW",
            replace=overwrite,
            expression=expression,
        )
        self._register_in_memory_tables(obj)
        with self._safe_raw_sql(src):
            pass
        return self.table(name, database=database)
