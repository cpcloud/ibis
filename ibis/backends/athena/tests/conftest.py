from __future__ import annotations

import concurrent.futures
import getpass
import sys
from os import environ as env
from typing import TYPE_CHECKING, Any

import ibis
from ibis.backends.tests.base import BackendTest

if TYPE_CHECKING:
    import s3fs

    from ibis.backends import BaseBackend


IBIS_ATHENA_S3_STAGING_DIR = env.get("IBIS_ATHENA_S3_STAGING_DIR", "s3://ibis-testing/")
IBIS_ATHENA_REGION_NAME = env.get("IBIS_ATHENA_REGION_NAME", "us-east-2")
IBIS_ATHENA_PROFILE_NAME = env.get(
    "IBIS_ATHENA_PROFILE_NAME", "070284473168_PowerUserAccess"
)
CONNECT_ARGS = dict(
    s3_staging_dir=IBIS_ATHENA_S3_STAGING_DIR,
    region_name=IBIS_ATHENA_REGION_NAME,
    profile_name=IBIS_ATHENA_PROFILE_NAME,
)


def create_table(
    con, *, fs: s3fs.S3FileSystem, file: str, schema: str, folder: str
) -> None:
    name = file.with_suffix("").name

    fs.copy(file, f"{folder}/{file.name}")

    schema_string = ", ".join(pair.sql("athena") for pair in schema)
    con.execute(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {name} ({schema_string})
        STORED AS PARQUET
        LOCATION '{folder}/'
        """
    )


class TestConf(BackendTest):
    supports_map = True
    driver_supports_multiple_statements = False
    deps = ("pyathena", "s3fs")

    def _load_data(self, **_: Any) -> None:
        import pyarrow.parquet as pq
        import pyathena
        import s3fs

        from ibis.formats.pyarrow import PyArrowSchema

        files = list(self.data_dir.joinpath("parquet").glob("*.parquet"))

        user = getpass.getuser()
        python_version = "".join(map(str, sys.version_info[:3]))
        folder = f"{user}_{python_version}"

        fs = s3fs.S3FileSystem()

        with pyathena.connect(**CONNECT_ARGS) as con:
            with concurrent.futures.ThreadPoolExecutor() as exe:
                for fut in concurrent.futures.as_completed(
                    exe.submit(
                        create_table,
                        con,
                        fs=fs,
                        file=file,
                        schema=PyArrowSchema.to_ibis(
                            pq.read_metadata(file).schema.to_arrow_schema()
                        ).to_sqlglot("athena"),
                        folder=f"{IBIS_ATHENA_S3_STAGING_DIR}/{folder}",
                    )
                    for file in files
                ):
                    fut.result()

    @staticmethod
    def connect(*, tmpdir, worker_id, **kw) -> BaseBackend:
        return ibis.athena.connect(**CONNECT_ARGS, **kw)
