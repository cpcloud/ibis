from .client import SQLiteClient
from .compiler import dialect, rewrites  # noqa: F401


def compile(expr, params=None):
    """
    Force compilation of expression for the SQLite target
    """
    from ibis.sql.alchemy import to_sqlalchemy

    return to_sqlalchemy(expr, dialect.make_context(params=params))


def connect(path=None, create=False):

    """
    Create an Ibis client connected to a SQLite database.

    Multiple database files can be created using the attach() method

    Parameters
    ----------
    path : string, default None
        File path to the SQLite database file. If None, creates an in-memory
        transient database and you can use attach() to add more files
    create : boolean, default False
        If file does not exist, create it
    """

    return SQLiteClient(path, create=create)
