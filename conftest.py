import os
import pathlib

import pytest

import ibis

collect_ignore = ["setup.py"]
ROOT = pathlib.Path(__file__).absolute().parent


# Ignore specific backend test collection if we weren't able to import the
# module when ibis/__init__.py is executed
for backend in ibis.BACKENDS:
    if getattr(ibis, backend, None) is None:
        # find all subdirectories named `tests`
        test_dirs = ROOT.joinpath("ibis", backend).rglob("tests")
        collect_ignore.extend(map(str, test_dirs))


@pytest.fixture(scope="session")
def data_directory():
    default = ROOT / "ci" / "ibis-testing-data"
    datadir = os.environ.get("IBIS_TEST_DATA_DIRECTORY", default)
    datadir = pathlib.Path(datadir)

    if not datadir.exists():
        pytest.skip("test data directory {!r} not found".format(str(datadir)))

    return datadir


@pytest.fixture(scope="session")
def test_database():
    return os.environ.get("IBIS_TEST_DATA_DB", "ibis_testing")
