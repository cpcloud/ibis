from __future__ import annotations

import concurrent.futures
import os
from typing import TYPE_CHECKING, Any

import pyarrow.parquet as pq
import pytest
import snowflake.connector
import sqlalchemy as sa
import sqlglot as sg
import tomli

import ibis
from ibis.backends.conftest import TEST_TABLES
from ibis.backends.snowflake.datatypes import SnowflakeType
from ibis.backends.tests.base import BackendTest, RoundAwayFromZero
from ibis.formats.pyarrow import PyArrowSchema

if TYPE_CHECKING:
    from pathlib import Path

    from ibis.backends.base import BaseBackend

CONNECTIONS_FILE_PATH = snowflake.connector.config_manager.CONNECTIONS_FILE
CONNECTION_NAME = os.environ.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
SNOWFLAKE_URL = os.environ.get("SNOWFLAKE_URL")


def _get_url():
    if SNOWFLAKE_URL is not None:
        return SNOWFLAKE_URL
    elif CONNECTIONS_FILE_PATH is not None and CONNECTION_NAME is not None:
        return "snowflake://"
    else:
        try:
            user, password, account, database, schema, warehouse = tuple(
                os.environ[f"SNOWFLAKE_{part}"]
                for part in [
                    "USER",
                    "PASSWORD",
                    "ACCOUNT",
                    "DATABASE",
                    "SCHEMA",
                    "WAREHOUSE",
                ]
            )
        except KeyError as e:
            pytest.skip(
                f"missing URL part: {e} or SNOWFLAKE_URL environment variable is not defined"
            )
        return f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"


def copy_into(con, data_dir: Path, table: str) -> None:
    stage = "ibis_testing"
    file = data_dir.joinpath("parquet", f"{table}.parquet").absolute()
    schema = PyArrowSchema.to_ibis(pq.read_metadata(file).schema.to_arrow_schema())
    columns = ", ".join(
        f"$1:{name}{'::VARCHAR' * typ.is_timestamp()}::{SnowflakeType.to_string(typ)}"
        for name, typ in schema.items()
    )
    con.exec_driver_sql(f"PUT {file.as_uri()} @{stage}/{file.name}")
    con.exec_driver_sql(
        f"""
        COPY INTO {table}
        FROM (SELECT {columns} FROM @{stage}/{file.name})
        FILE_FORMAT = (TYPE = PARQUET)
        """
    )


class TestConf(BackendTest, RoundAwayFromZero):
    supports_map = True
    default_identifier_case_fn = staticmethod(str.upper)
    deps = ("snowflake.connector", "snowflake.sqlalchemy")
    supports_tpch = True

    def load_tpch(self) -> None:
        """No-op, snowflake already defines these in `SNOWFLAKE_SAMPLE_DATA`."""

    def _tpch_table(self, name: str):
        t = self.connection.table(
            self.default_identifier_case_fn(name),
            schema="SNOWFLAKE_SAMPLE_DATA.TPCH_SF1",
        )
        return t.rename("snake_case")

    def _transform_tpch_sql(self, parsed):
        def add_catalog_and_schema(node):
            if isinstance(node, sg.exp.Table):
                return node.__class__(
                    db="TPCH_SF1",
                    catalog="SNOWFLAKE_SAMPLE_DATA",
                    **{
                        k: v for k, v in node.args.items() if k not in ("db", "catalog")
                    },
                )
            return node

        result = parsed.transform(add_catalog_and_schema)
        return result

    def _load_data(self, **_: Any) -> None:
        """Load test data into a Snowflake backend instance."""
        snowflake_url = _get_url()

        raw_url = sa.engine.make_url(snowflake_url)
        if raw_url.database is not None:
            _, schema = raw_url.database.rsplit("/", 1)
            url = raw_url.set(database="")
        else:
            url = raw_url
            schema = None

        connect_args = {"session_parameters": {"MULTI_STATEMENT_COUNT": "0"}}

        if CONNECTIONS_FILE_PATH is not None and CONNECTION_NAME is not None:
            connect_args["connections_file_path"] = CONNECTIONS_FILE_PATH
            connect_args["connection_name"] = CONNECTION_NAME
            with CONNECTIONS_FILE_PATH.open(mode="rb") as f:
                connect_args["schema"] = schema = tomli.load(f)[CONNECTION_NAME][
                    "schema"
                ]

        con = sa.create_engine(url, connect_args=connect_args)

        assert schema is not None
        dbschema = f"ibis_testing.{schema}"

        with con.begin() as c:
            c.exec_driver_sql(
                f"""\
CREATE DATABASE IF NOT EXISTS ibis_testing;
USE DATABASE ibis_testing;
CREATE SCHEMA IF NOT EXISTS {dbschema};
USE SCHEMA {dbschema};
CREATE TEMP STAGE ibis_testing;
{self.script_dir.joinpath("snowflake.sql").read_text()}"""
            )

        with con.begin() as c:
            # not much we can do to make this faster, but running these in
            # multiple threads seems to save about 2x
            with concurrent.futures.ThreadPoolExecutor() as exe:
                for future in concurrent.futures.as_completed(
                    exe.submit(copy_into, c, self.data_dir, table)
                    for table in TEST_TABLES.keys()
                ):
                    future.result()

    @staticmethod
    def connect(*, tmpdir, worker_id, **kw) -> BaseBackend:
        return ibis.connect(_get_url(), **kw)


@pytest.fixture(scope="session")
def con(data_dir, tmp_path_factory, worker_id):
    return TestConf.load_data(data_dir, tmp_path_factory, worker_id).connection
