from __future__ import annotations

import ibis
from ibis.util import gen_name


def test_table_properties():
    con = ibis.trino.connect(database="hive", schema="default")
    name = ibis.util.gen_name("test_trino_table_properties")
    schema = ibis.schema(dict(a="int"))
    t = con.create_table(name, schema=schema, properties={"format": "ORC"})
    try:
        assert t.schema() == schema
        with con.begin() as c:
            ddl = c.exec_driver_sql(f"SHOW CREATE TABLE {name}").scalar()
        assert "ORC" in ddl
    finally:
        con.drop_table(name, force=True)


def test_cross_db_access(con):
    table = gen_name("tmp_table")
    con.raw_sql(f'CREATE TABLE postgresql.public."{table}" ("x" INT)')
    try:
        t = con.table(table, schema="postgresql.public")
        assert t.schema() == ibis.schema(dict(x="int"))
        assert t.execute().empty
    finally:
        con.drop_table(table, database="postgresql.public")
