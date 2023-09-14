from __future__ import annotations

import pytest
from pytest import param

import ibis
from ibis.conftest import LINUX, MACOS, SANDBOXED

try:
    from clickhouse_connect.driver.exceptions import (
        DatabaseError as ClickHouseDatabaseError,
    )
except ImportError:
    ClickHouseDatabaseError = None

pytestmark = pytest.mark.examples


@pytest.mark.skipif(
    (LINUX or MACOS) and SANDBOXED,
    reason="nix on linux cannot download duckdb extensions or data due to sandboxing",
)
@pytest.mark.notimpl(["dask"])
@pytest.mark.notyet(["bigquery", "druid", "impala", "mssql", "trino"])
@pytest.mark.parametrize(
    ("example", "columns"),
    [
        (
            "wowah_locations_raw",
            ["Map_ID", "Location_Type", "Location_Name", "Game_Version"],
        ),
        param(
            "band_instruments",
            ["name", "plays"],
            marks=[
                pytest.mark.notimpl(
                    ["datafusion"],
                    reason="create_table not implemented",
                    raises=NotImplementedError,
                ),
            ],
        ),
        param(
            "AwardsManagers",
            ["player_id", "award_id", "year_id", "lg_id", "tie", "notes"],
            marks=[
                pytest.mark.notimpl(
                    ["clickhouse"],
                    reason="cannot determine all-null column type",
                    raises=ClickHouseDatabaseError,
                ),
                pytest.mark.notimpl(
                    ["datafusion"],
                    reason="create_table not implemented",
                    raises=NotImplementedError,
                ),
            ],
        ),
    ],
    ids=["parquet", "csv", "csv-all-null"],
)
def test_load_examples(con, example, columns):
    t = getattr(ibis.examples, example).fetch(backend=con)
    assert t.columns == columns
    assert t.count().execute() > 0
