from __future__ import annotations

import ibis


def test_blob_raw(con):
    con.drop_table("blob_raw_blobs_blob_raw", force=True)

    with con.begin() as bind:
        bind.execute(
            """CREATE TABLE "blob_raw_blobs_blob_raw" ("blob" BLOB, "raw" RAW(255))"""
        )

    raw_blob = con.table("blob_raw_blobs_blob_raw")

    assert raw_blob.schema() == ibis.Schema(dict(blob="binary", raw="binary"))
