from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest
import sqlglot as sg

import ibis
from ibis.backends.conftest import TEST_TABLES
from ibis.backends.postgres.tests.conftest import TestConf as PostgresTestConf
from ibis.backends.tests.base import BackendTest, RoundAwayFromZero

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    import ibis.expr.types as ir

TRINO_USER = os.environ.get(
    "IBIS_TEST_TRINO_USER", os.environ.get("TRINO_USER", "user")
)
TRINO_PASS = os.environ.get(
    "IBIS_TEST_TRINO_PASSWORD", os.environ.get("TRINO_PASSWORD", "")
)
TRINO_HOST = os.environ.get(
    "IBIS_TEST_TRINO_HOST", os.environ.get("TRINO_HOST", "localhost")
)
TRINO_PORT = os.environ.get("IBIS_TEST_TRINO_PORT", os.environ.get("TRINO_PORT", 8080))
IBIS_TEST_TRINO_DB = os.environ.get(
    "IBIS_TEST_TRINO_DATABASE",
    os.environ.get("TRINO_DATABASE", "memory"),
)


class TrinoPostgresTestConf(PostgresTestConf):
    service_name = "trino-postgres"
    deps = "sqlalchemy", "psycopg2"

    @classmethod
    def name(cls) -> str:
        return "postgres"

    @property
    def test_files(self) -> Iterable[Path]:
        return self.data_dir.joinpath("csv").glob("*.csv")


class TestConf(BackendTest, RoundAwayFromZero):
    # trino rounds half to even for double precision and half away from zero
    # for numeric and decimal

    returned_timestamp_unit = "s"
    supports_structs = True
    supports_map = True
    supports_tpch = True
    service_name = "trino"
    deps = ("sqlalchemy", "trino.sqlalchemy")

    _tpch_data_schema = "tpch.sf1"
    _tpch_query_schema = "hive.ibis_sf1"

    def _transform_tpch_sql(self, parsed):
        def add_catalog_and_schema(node):
            if isinstance(node, sg.exp.Table):
                catalog, db = self._tpch_query_schema.split(".")
                return node.__class__(
                    db=db,
                    catalog=catalog,
                    **{
                        k: v for k, v in node.args.items() if k not in ("db", "catalog")
                    },
                )
            return node

        result = parsed.transform(add_catalog_and_schema)
        return result

    def load_tpch(self) -> None:
        """Create views of data in the TPC-H catalog that ships with Trino.

        This method create relations that have column names prefixed with the
        first one (or two in the case of partsupp -> ps) character table name
        to match the DuckDB TPC-H query conventions.
        """
        con = self.connection
        query_schema = self._tpch_query_schema
        data_schema = self._tpch_data_schema
        database, schema = query_schema.split(".")

        tables = con.list_tables(database=self._tpch_data_schema)
        con.create_schema(schema, database=database, force=True)

        prefixes = {"partsupp": "ps"}
        with con.begin() as c:
            for table in tables:
                prefix = prefixes.get(table, table[0])

                t = con.table(table, schema=data_schema)
                new_t = t.rename(**{f"{prefix}_{old}": old for old in t.columns})

                sql = ibis.to_sql(new_t, dialect="trino")
                c.exec_driver_sql(
                    f"CREATE OR REPLACE VIEW {query_schema}.{table} AS {sql}"
                )

    def _tpch_table(self, name: str):
        return self.connection.table(
            self.default_identifier_case_fn(name),
            schema=self._tpch_query_schema,
        )

    @classmethod
    def load_data(cls, data_dir: Path, tmpdir: Path, worker_id: str, **kw: Any) -> None:
        TrinoPostgresTestConf.load_data(data_dir, tmpdir, worker_id, port=5433)
        return super().load_data(data_dir, tmpdir, worker_id, **kw)

    @staticmethod
    def connect(*, tmpdir, worker_id, **kw):
        return ibis.trino.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            password=TRINO_PASS,
            database=IBIS_TEST_TRINO_DB,
            schema="default",
            **kw,
        )

    def _remap_column_names(
        self, table_name: str, schema: str | None = None
    ) -> dict[str, str]:
        table = self.connection.table(table_name, schema=schema)
        return table.rename(
            dict(zip(TEST_TABLES[table_name].names, table.schema().names))
        )

    @property
    def ddl_script(self):
        # keep this in sync with the postgresql backend because we have tests that expect
        # `functional_alltypes` to be available on the default connection
        yield "CREATE OR REPLACE VIEW memory.default.functional_alltypes AS SELECT * FROM postgresql.public.functional_alltypes"
        yield "CREATE OR REPLACE VIEW memory.default.astronauts AS SELECT * FROM postgresql.public.astronauts"
        yield from super().ddl_script

    @property
    def batting(self) -> ir.Table:
        return self._remap_column_names("batting", schema="postgresql.public")

    @property
    def awards_players(self) -> ir.Table:
        return self._remap_column_names("awards_players", schema="postgresql.public")

    @property
    def diamonds(self) -> ir.Table:
        return self.connection.table("diamonds", schema="postgresql.public")

    @property
    def astronauts(self) -> ir.Table:
        return self.connection.table("astronauts", schema="postgresql.public")

    @property
    def array_types(self) -> ir.Table | None:
        return self.connection.table("array_types", schema="postgresql.public")

    @property
    def json_t(self) -> ir.Table | None:
        return self.connection.table("json_t", schema="postgresql.public")

    @property
    def win(self) -> ir.Table | None:
        return self.connection.table("win", schema="postgresql.public")


@pytest.fixture(scope="session")
def con(tmp_path_factory, data_dir, worker_id):
    return TestConf.load_data(data_dir, tmp_path_factory, worker_id).connection


@pytest.fixture(scope="module")
def db(con):
    return con.database()


@pytest.fixture(scope="module")
def alltypes(db):
    return db.functional_alltypes


@pytest.fixture(scope="module")
def geotable(con):
    return con.table("geo")


@pytest.fixture(scope="module")
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope="module")
def gdf(geotable):
    return geotable.execute()


@pytest.fixture(scope="module")
def intervals(con):
    return con.table("intervals")


@pytest.fixture
def translate():
    from ibis.backends.trino import Backend

    context = Backend.compiler.make_context()
    return lambda expr: (Backend.compiler.translator_class(expr, context).get_result())
