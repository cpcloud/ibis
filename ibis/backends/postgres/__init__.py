"""PostgreSQL backend."""

from __future__ import annotations

import contextlib
from typing import Iterable, Literal

import sqlalchemy as sa
import toolz
from psycopg2.extensions import AsIs, register_adapter

import ibis.expr.datatypes as dt
from ibis import util
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.base.sql.alchemy.datatypes import to_sqla_type
from ibis.backends.postgres.compiler import PostgreSQLCompiler
from ibis.backends.postgres.datatypes import _get_type
from ibis.backends.postgres.udf import udf as _udf

_COMPOSITE_TYPES_SQL = """\
WITH types AS (
  SELECT
    n.nspname,
    pg_catalog.format_type (t.oid, NULL) AS obj_name
  FROM pg_catalog.pg_type t
  INNER JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
  WHERE (t.typrelid = 0
         OR (SELECT c.relkind = 'c'
             FROM pg_catalog.pg_class c
             WHERE c.oid = t.typrelid))
    AND NOT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_type el
      WHERE el.oid = t.typelem
        AND el.typarray = t.oid
    )
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
)
SELECT
  pg_catalog.format_type(t.oid, NULL) AS name,
  array_agg(CAST(a.attname AS TEXT) ORDER BY a.attnum) AS columns,
  array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) AS types,
  array_agg(NOT a.attnotnull ORDER BY a.attnum) AS nullables
FROM pg_catalog.pg_attribute a
INNER JOIN pg_catalog.pg_type t ON a.attrelid = t.typrelid
INNER JOIN pg_catalog.pg_namespace n ON (n.oid = t.typnamespace)
INNER JOIN types
  ON types.nspname = n.nspname
    AND types.obj_name = pg_catalog.format_type(t.oid, NULL)
WHERE a.attnum > 0
  AND NOT a.attisdropped
GROUP BY 1"""

_TYPE_INFO_SQL = """\
SELECT
  attname,
  format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = CAST(:raw_name AS regclass)
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum"""

register_adapter(util.frozendict, lambda fd: AsIs(dict(fd)))


class Backend(BaseAlchemyBackend):
    name = 'postgres'
    compiler = PostgreSQLCompiler

    def do_connect(
        self,
        host: str = 'localhost',
        user: str | None = None,
        password: str | None = None,
        port: int = 5432,
        database: str | None = None,
        url: str | None = None,
        driver: Literal["psycopg2"] = "psycopg2",
    ) -> None:
        """Create an Ibis client connected to PostgreSQL database.

        Parameters
        ----------
        host
            Hostname
        user
            Username
        password
            Password
        port
            Port number
        database
            Database to connect to
        url
            SQLAlchemy connection string.

            If passed, the other connection arguments are ignored.
        driver
            Database driver

        Examples
        --------
        >>> import os
        >>> import getpass
        >>> import ibis
        >>> host = os.environ.get('IBIS_TEST_POSTGRES_HOST', 'localhost')
        >>> user = os.environ.get('IBIS_TEST_POSTGRES_USER', getpass.getuser())
        >>> password = os.environ.get('IBIS_TEST_POSTGRES_PASSWORD')
        >>> database = os.environ.get('IBIS_TEST_POSTGRES_DATABASE',
        ...                           'ibis_testing')
        >>> con = connect(
        ...     database=database,
        ...     host=host,
        ...     user=user,
        ...     password=password
        ... )
        >>> con.list_tables()  # doctest: +ELLIPSIS
        [...]
        >>> t = con.table('functional_alltypes')
        >>> t
        PostgreSQLTable[table]
          name: functional_alltypes
          schema:
            index : int64
            Unnamed: 0 : int64
            id : int32
            bool_col : boolean
            tinyint_col : int16
            smallint_col : int16
            int_col : int32
            bigint_col : int64
            float_col : float32
            double_col : float64
            date_string_col : string
            string_col : string
            timestamp_col : timestamp
            year : int32
            month : int32
        """
        if driver != 'psycopg2':
            raise NotImplementedError('psycopg2 is currently the only supported driver')
        alchemy_url = self._build_alchemy_url(
            url=url,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            driver=f'postgresql+{driver}',
        )
        self.database_name = alchemy_url.database
        super().do_connect(sa.create_engine(alchemy_url))
        self._composite_types = self._get_composite_types()

        @sa.event.listens_for(self.meta, "column_reflect")
        def map_composite_types(inspector, tablename, column_info):
            if (new_type := self._composite_types.get(column_info["name"])) is not None:
                column_info["type"] = new_type

    def list_databases(self, like=None):
        with self.begin() as con:
            # http://dba.stackexchange.com/a/1304/58517
            databases = [
                row.datname
                for row in con.execute(
                    sa.text('SELECT datname FROM pg_database WHERE NOT datistemplate')
                )
            ]
        return self._filter_with_like(databases, like)

    @contextlib.contextmanager
    def begin(self):
        with super().begin() as bind:
            prev = bind.execute(sa.text('SHOW TIMEZONE')).scalar()
            bind.execute(sa.text('SET TIMEZONE = UTC'))
            yield bind
            bind.execute(sa.text("SET TIMEZONE = :prev").bindparams(prev=prev))

    def udf(
        self,
        pyfunc,
        in_types,
        out_type,
        schema=None,
        replace=False,
        name=None,
        language="plpythonu",
    ):
        """Decorator that defines a PL/Python UDF in-database.

        Parameters
        ----------
        pyfunc
            Python function
        in_types
            Input types
        out_type
            Output type
        schema
            The postgres schema in which to define the UDF
        replace
            replace UDF in database if already exists
        name
            name for the UDF to be defined in database
        language
            Language extension to use for PL/Python

        Returns
        -------
        Callable
            A callable ibis expression

        Function that takes in Column arguments and returns an instance
        inheriting from PostgresUDFNode
        """

        return _udf(
            client=self,
            python_func=pyfunc,
            in_types=in_types,
            out_type=out_type,
            schema=schema,
            replace=replace,
            name=name,
            language=language,
        )

    def _metadata(self, query: str) -> Iterable[tuple[str, dt.DataType]]:
        raw_name = util.guid()
        name = self._quote(raw_name)
        with self.begin() as con:
            con.execute(sa.text(f"CREATE TEMPORARY VIEW {name} AS {query}"))
            type_info = con.execute(
                sa.text(_TYPE_INFO_SQL).bindparams(raw_name=raw_name)
            )
            yield from ((col, _get_type(typestr)) for col, typestr in type_info)
            con.execute(sa.text(f"DROP VIEW IF EXISTS {name}"))

    def _get_composite_types(self) -> dt.DataType | None:
        import psycopg2.extras

        composite_types = {}

        with self.begin() as con:
            mappings = con.exec_driver_sql(_COMPOSITE_TYPES_SQL).mappings().fetchall()
            for (
                name,
                columns,
                types,
                nullables,
            ) in toolz.pluck(["name", "columns", "types", "nullables"], mappings):
                try:
                    psycopg2.extras.register_composite(
                        name, con.connection.connection, globally=True
                    )
                except psycopg2.ProgrammingError:
                    pass
                else:
                    composite_types[name] = to_sqla_type(
                        dt.Struct.from_tuples(
                            (field, _get_type(field_type)(nullable=nullable))
                            for field, field_type, nullable in zip(
                                columns, types, nullables
                            )
                        )
                    )
        return composite_types

    def _get_temp_view_definition(
        self, name: str, definition: sa.sql.compiler.Compiled
    ) -> str:
        yield f"CREATE OR REPLACE VIEW {name} AS {definition}"
