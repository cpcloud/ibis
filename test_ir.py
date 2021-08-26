import subprocess
import time
from pathlib import Path

import pytest

import ibis


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("test") / "test.db"
    ddl = Path(__file__).parent.parent / "sqlite-ddl.sql"
    subprocess.run(
        [
            "sqlite3",
            str(db_path),
        ],
        stdin=ddl.open(mode="r"),
    )

    client = ibis.sqlite.connect(str(db_path))
    db = client.database()
    assert "lineitem" in db.tables
    return db


@pytest.fixture
def lineitem(db):
    return db.lineitem


def test_tpch1(lineitem):
    expr = (
        lineitem.filter(
            lambda t: (
                t.l_shipdate.cast("date")
                <= ibis.date("1998-12-01") - ibis.interval(days=90)
            )
        )
        .group_by(["l_returnflag", "l_linestatus"])
        .aggregate(
            sum_qty=lambda t: t.l_quantity.sum(),
            sum_base_price=lambda t: t.l_extendedprice.sum(),
            sum_disc_price=lambda t: (
                t.l_extendedprice * (1 - t.l_discount)
            ).sum(),
            sum_charge=lambda t: (
                t.l_extendedprice * (1 - t.l_discount) * (1 + t.l_tax)
            ).sum(),
            avg_qty=lambda t: t.l_quantity.mean(),
            avg_price=lambda t: t.l_extendedprice.mean(),
            avg_disc=lambda t: t.l_discount.mean(),
            count_order=lambda t: t.count(),
        )
        .sort_by(["l_returnflag", "l_linestatus"])
    )
    start = time.time()
    ir = expr.ir()
    stop = time.time()
    print(f"produced IR in: {stop - start:.6f}s")
    assert ir, "ir is empty"
    assert any(ir), "all bytes are 0"

    dump_json(ir)


def dump_json(ir):
    home = Path.home()

    bin_path = home / "tmp" / "ir.bin"
    bin_path.write_bytes(ir)

    subprocess.run(
        [
            "flatc",
            "--json",
            "--strict-json",
            "--raw-binary",
            "-o",
            str(home / "tmp" / "ir.json"),
            str(
                home
                / "src"
                / "arrow"
                / "format"
                / "experimental"
                / "computeir"
                / "Relation.fbs"
            ),
            "--",
            str(bin_path),
        ]
    )
