import pytest

from ibis.tests.all.conftest import spark_client


@pytest.fixture(scope='session')
def client(data_directory):
    return spark_client(data_directory)


@pytest.fixture(scope='session')
def simple(client):
    return client.table('simple')


@pytest.fixture(scope='session')
def struct(client):
    return client.table('struct')


@pytest.fixture(scope='session')
def nested_types(client):
    return client.table('nested_types')


@pytest.fixture(scope='session')
def complicated(client):
    return client.table('complicated')
