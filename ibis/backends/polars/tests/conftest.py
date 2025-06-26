from __future__ import annotations

from typing import Any, Literal

import numpy as np
import pytest
import sqlglot as sg

import ibis
from ibis.backends.conftest import TEST_TABLES
from ibis.backends.tests.base import BackendTest
from ibis.backends.tests.data import array_types, struct_types, topk, win


class TestConf(BackendTest):
    supports_structs = True
    supports_json = False
    supports_tpch = True
    supports_tpcds = True

    reduction_tolerance = 1e-3
    stateful = False
    deps = ("polars",)

    def _load_data(self, **_: Any) -> None:
        con = self.connection
        for table_name in TEST_TABLES:
            path = self.data_dir / "parquet" / f"{table_name}.parquet"
            con.read_parquet(path, table_name=table_name)
        con.create_table("array_types", array_types)
        con.create_table("struct", struct_types)
        con.create_table("win", win)
        con.create_table("topk", topk)

    @staticmethod
    def connect(*, tmpdir, worker_id, **kw):  # noqa: ARG004
        return ibis.polars.connect(**kw)

    @classmethod
    def assert_series_equal(cls, left, right, *args, **kwargs) -> None:
        check_dtype = kwargs.pop("check_dtype", True) and not (
            issubclass(left.dtype.type, np.timedelta64)
            and issubclass(right.dtype.type, np.timedelta64)
        )
        return super().assert_series_equal(
            left, right, *args, **kwargs, check_dtype=check_dtype
        )

    def _load_tpc(self, *, suite, scale_factor):
        con = self.connection
        schema = f"tpc{suite}"
        parquet_dir = self.data_dir.joinpath(schema, f"sf={scale_factor}", "parquet")
        assert parquet_dir.exists(), parquet_dir
        for path in parquet_dir.glob("*.parquet"):
            table_name = path.with_suffix("").name
            con.create_table(f"tpc{suite}_{table_name}", con.read_parquet(path))

    def _transform_tpc_sql(self, parsed, *, suite, leaves):
        def add_catalog_and_schema(node):
            if isinstance(node, sg.exp.Table) and (name := node.name) in leaves:
                res = node.__class__(this=sg.to_identifier(f"tpc{suite}_{name}"))
                return res
            return node

        return parsed.transform(add_catalog_and_schema)

    def _tpc_table(self, name: str, benchmark: Literal["h", "ds"]):
        if not getattr(self, f"supports_tpc{benchmark}"):
            pytest.skip(
                f"{self.name()} backend does not support testing TPC-{benchmark.upper()}"
            )
        return self.connection.table(f"tpc{benchmark}_{name}")


@pytest.fixture(scope="session")
def con(tmp_path_factory, data_dir, worker_id):
    with TestConf.load_data(data_dir, tmp_path_factory, worker_id) as be:
        yield be.connection


@pytest.fixture(scope="session")
def alltypes(con):
    return con.table("functional_alltypes")
