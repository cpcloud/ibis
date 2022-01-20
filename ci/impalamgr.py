#!/usr/bin/env python

import os
from io import BytesIO
from pathlib import Path

import click
import toolz
from plumbum import CommandNotFound, local
from plumbum.cmd import cmake, make

import ibis
from ibis.backends.impala.tests.conftest import IbisTestEnv
from ibis.common.exceptions import IbisError

SCRIPT_DIR = Path(__file__).parent.absolute()
DATA_DIR = Path(
    os.environ.get(
        'IBIS_TEST_DATA_DIRECTORY', SCRIPT_DIR / 'ibis-testing-data'
    )
)


logger = ibis.util.get_logger(Path(__file__).with_suffix('').name)


ENV = IbisTestEnv()

env_items = ENV.items()
maxlen = max(map(len, map(toolz.first, env_items))) + len('IbisTestEnv[""]')
format_string = f'%-{maxlen:d}s == %r'
for key, value in env_items:
    logger.info(format_string, f'IbisTestEnv[{key!r}]', value)


def make_ibis_client(env):
    hc = ibis.impala.hdfs_connect(
        host=env.nn_host,
        port=env.webhdfs_port,
        auth_mechanism=env.auth_mechanism,
        verify=env.auth_mechanism not in ['GSSAPI', 'LDAP'],
        user=env.webhdfs_user,
    )
    auth_mechanism = env.auth_mechanism
    if auth_mechanism == 'GSSAPI' or auth_mechanism == 'LDAP':
        logger.warning('Ignoring invalid Certificate Authority errors')
    return ibis.impala.connect(
        host=env.impala_host,
        port=env.impala_port,
        auth_mechanism=env.auth_mechanism,
        hdfs_client=hc,
        pool_size=16,
    )


def raise_if_cannot_write_to_hdfs(con):
    test_path = os.path.join(ENV.test_data_dir, ibis.util.guid())
    test_file = BytesIO(ibis.util.guid().encode('utf-8'))
    con.hdfs.put(test_path, test_file)
    con.hdfs.rm(test_path)


def can_build_udfs():
    try:
        local.which('cmake')
    except CommandNotFound:
        logger.exception('Could not find cmake on PATH')
        return False
    try:
        local.which('make')
    except CommandNotFound:
        logger.exception('Could not find make on PATH')
        return False
    try:
        local.which('clang++')
    except CommandNotFound:
        logger.exception(
            'Could not find LLVM on PATH; if IBIS_TEST_LLVM_CONFIG is set, '
            'try setting PATH="$($IBIS_TEST_LLVM_CONFIG --bindir):$PATH"'
        )
        return False
    return True


def is_impala_loaded(con):
    return con.hdfs.exists(ENV.test_data_dir) and con.exists_database(
        ENV.test_data_db
    )


def is_udf_loaded(con):
    return con.hdfs.exists(os.path.join(ENV.test_data_dir, 'udf'))


def upload_ibis_test_data_to_hdfs(con, data_path):
    hdfs = con.hdfs
    if hdfs.exists(ENV.test_data_dir):
        hdfs.rmdir(ENV.test_data_dir)
    hdfs.put(ENV.test_data_dir, data_path)


def create_test_database(con):
    if con.exists_database(ENV.test_data_db):
        con.drop_database(ENV.test_data_db, force=True)
    con.create_database(ENV.test_data_db)
    logger.info(f'Created database {ENV.test_data_db}')

    con.create_table(
        'alltypes',
        schema=ibis.schema(
            [
                ('a', 'int8'),
                ('b', 'int16'),
                ('c', 'int32'),
                ('d', 'int64'),
                ('e', 'float'),
                ('f', 'double'),
                ('g', 'string'),
                ('h', 'boolean'),
                ('i', 'timestamp'),
            ]
        ),
        database=ENV.test_data_db,
    )
    logger.info(f'Created empty table {ENV.test_data_db}.`alltypes`')


PARQUET_TABLES = {
    'functional_alltypes': ibis.schema(
        [
            ('id', 'int32'),
            ('bool_col', 'boolean'),
            ('tinyint_col', 'int8'),
            ('smallint_col', 'int16'),
            ('int_col', 'int32'),
            ('bigint_col', 'int64'),
            ('float_col', 'float'),
            ('double_col', 'double'),
            ('date_string_col', 'string'),
            ('string_col', 'string'),
            ('timestamp_col', 'timestamp'),
            ('year', 'int32'),
            ('month', 'int32'),
        ]
    ),
    'tpch_region': ibis.schema(
        [
            ('r_regionkey', 'int16'),
            ('r_name', 'string'),
            ('r_comment', 'string'),
        ]
    ),
}

AVRO_TABLES = {
    'tpch_region_avro': {
        'type': 'record',
        'name': 'a',
        'fields': [
            {'name': 'R_REGIONKEY', 'type': ['null', 'int']},
            {'name': 'R_NAME', 'type': ['null', 'string']},
            {'name': 'R_COMMENT', 'type': ['null', 'string']},
        ],
    }
}


def compute_stats(table):
    logger.info(f'Computing stats for {table.op().name}')
    table.compute_stats()


def create_parquet_tables(con):
    def create_table(table_name):
        logger.info(f'Creating {table_name}')
        path = os.path.join(ENV.test_data_dir, 'parquet', table_name)
        table = con.parquet_file(
            path,
            schema=PARQUET_TABLES.get(table_name),
            name=table_name,
            database=ENV.test_data_db,
            persist=True,
        )
        compute_stats(table)

    parquet_files = con.hdfs.ls(os.path.join(ENV.test_data_dir, 'parquet'))
    for table_name in parquet_files:
        create_table(table_name)


def create_avro_tables(con, executor):
    def create_table(table_name):
        logger.info(f'Creating {table_name}')
        path = os.path.join(ENV.test_data_dir, 'avro', table_name)
        table = con.avro_file(
            path,
            schema=AVRO_TABLES[table_name],
            name=table_name,
            database=ENV.test_data_db,
            persist=True,
        )
        compute_stats(table)

    avro_files = con.hdfs.ls(os.path.join(ENV.test_data_dir, 'avro'))
    for table_name in avro_files:
        create_table(table_name)


def build_udfs():
    logger.info('Building UDFs')
    ibis_home_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    udf_dir = os.path.join(ibis_home_dir, 'ci', 'udf')

    with local.cwd(udf_dir):
        assert cmake('.') and make('VERBOSE=1')


def upload_udfs(con):
    ibis_home_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    build_dir = os.path.join(ibis_home_dir, 'ci', 'udf', 'build')
    bitcode_dir = os.path.join(ENV.test_data_dir, 'udf')
    logger.info(f'Uploading UDFs to {bitcode_dir}')
    if con.hdfs.exists(bitcode_dir):
        con.hdfs.rmdir(bitcode_dir)
    con.hdfs.put(bitcode_dir, build_dir, verbose=True)


# ==========================================


@click.group()
def main():
    """Manage impala test data for Ibis."""


@main.command()
@click.option(
    '--data/--no-data', default=True, help='Load (skip) ibis testing data'
)
@click.option(
    '--udf/--no-udf', default=True, help='Build/upload (skip) test UDFs'
)
@click.option(
    '--data-dir',
    help=(
        'Path to testing data. This downloads data from Google Cloud Storage '
        'if unset'
    ),
    default=DATA_DIR,
)
@click.option(
    '--overwrite/--no-overwrite',
    default=True,
    help='Forces overwriting of data/UDFs',
)
def load(data, udf, data_dir, overwrite):
    """Load Ibis test data and build/upload UDFs"""
    con = make_ibis_client(ENV)

    # validate our environment before performing possibly expensive operations
    raise_if_cannot_write_to_hdfs(con)

    if udf and not can_build_udfs():
        raise IbisError('Build environment does not support building UDFs')

    # load the data files
    if data:
        load_impala_data(con, str(data_dir), overwrite)
    else:
        logger.info('Skipping Ibis test data load (--no-data)')

    # build and upload the UDFs
    if udf:
        already_loaded = is_udf_loaded(con)
        logger.info('Attempting to build and load test UDFs')
        if already_loaded and not overwrite:
            logger.info('UDFs already loaded and not overwriting; moving on')
        else:
            if already_loaded:
                logger.info('UDFs already loaded; attempting to overwrite')
            logger.info('Building UDFs')
            build_udfs()
            logger.info('Uploading UDFs')
            upload_udfs(con)
    else:
        logger.info('Skipping UDF build/load (--no-udf)')


def load_impala_data(con, data_dir, overwrite=False):
    already_loaded = is_impala_loaded(con)
    logger.info('Attempting to load Ibis Impala test data (--data)')
    if already_loaded and not overwrite:
        logger.info('Data is already loaded and not overwriting; moving on')
    else:
        if already_loaded:
            logger.info('Data is already loaded; attempting to overwrite')

        logger.info('Uploading to HDFS')
        upload_ibis_test_data_to_hdfs(con, data_dir)

        logger.info('Creating Ibis test data database')
        create_test_database(con)

        create_parquet_tables(con)
        create_avro_tables(con)


if __name__ == '__main__':
    main()
