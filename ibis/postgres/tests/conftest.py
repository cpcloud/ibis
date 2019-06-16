import os

import pytest

import ibis

PG_USER = os.environ.get(
    'IBIS_TEST_POSTGRES_USER', os.environ.get('PGUSER', 'postgres')
)
PG_PASS = os.environ.get(
    'IBIS_TEST_POSTGRES_PASSWORD', os.environ.get('PGPASSWORD', 'postgres')
)
PG_HOST = os.environ.get(
    'IBIS_TEST_POSTGRES_HOST', os.environ.get('PGHOST', 'localhost')
)
PG_PORT = os.environ.get(
    'IBIS_TEST_POSTGRES_PORT', os.environ.get('PGPORT', 5432)
)
IBIS_TEST_POSTGRES_DB = os.environ.get(
    'IBIS_TEST_POSTGRES_DATABASE', os.environ.get('PGDATABASE', 'ibis_testing')
)


@pytest.fixture(scope='session')
def con():
    return ibis.postgres.connect(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASS,
        database=IBIS_TEST_POSTGRES_DB,
        port=PG_PORT,
    )


@pytest.fixture(scope='module')
def db(con):
    return con.database()


@pytest.fixture(scope='module')
def alltypes(db):
    return db.functional_alltypes


@pytest.fixture(scope='module')
def geotable(con):
    return con.table('geo')


@pytest.fixture(scope='module')
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope='module')
def gdf(geotable):
    return geotable.execute()


@pytest.fixture(scope='module')
def at(alltypes):
    return alltypes.op().sqla_table


@pytest.fixture(scope='module')
def intervals(con):
    return con.table("intervals")


@pytest.fixture
def translate():
    from ibis.postgres.compiler import PostgreSQLDialect

    dialect = PostgreSQLDialect()
    context = dialect.make_context()
    return lambda expr: dialect.translator(expr, context).get_result()
