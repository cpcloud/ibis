from typing import Any, Optional, Sequence

from .config import options  # noqa: F401
from .expr.api import *  # noqa: F401,F403
from .filesystems import HDFS  # noqa: F401

BACKENDS = (
    "bigquery",
    "clickhouse",
    "csv",
    "hdf5",
    "impala",
    "mysql",
    "omniscidb",
    "pandas",
    "parquet",
    "postgres",
    "sqlite",
)


def _load_backends(backends: Sequence[str]) -> None:
    """Import backends.

    Parameters
    ----------
    backends
        A sequence of submodule names.

    """
    import contextlib
    import importlib

    for module in map("ibis.{}".format, backends):
        with contextlib.suppress(ImportError):
            importlib.import_module(module)


_load_backends(BACKENDS)


def hdfs_connect(
    host: str = 'localhost',
    port: int = 50070,
    use_https: str = 'default',
    auth_mechanism: str = 'NOSASL',
    verify: bool = True,
    session: Optional[Any] = None,
    **kwargs: Any
):
    """Connect to HDFS, the Hadoop filesystem.

    Parameters
    ----------
    host
        Host name of the HDFS NameNode.
    port
        The NameNode's WebHDFS port.
    use_https
        Connect to WebHDFS with HTTPS if :data:`True`, otherwise plain HTTP.
        For secure authentication, the default is :data:`True`, otherwise
        :data:`False`.
    auth_mechanism
        Set to ``'NOSASL'`` or ``'PLAIN'`` for non-secure clusters.
        Set to ``'GSSAPI'`` or ``'LDAP'`` for Kerberos-secured clusters.
    verify
        Set to :data:`False` to turn off verifying SSL certificates.
    session : Optional[requests.Session]
        A custom :class:`requests.Session` object.

    Notes
    -----
    Other keywords are forwarded to HDFS library classes.

    Returns
    -------
    WebHDFS

    """
    import requests
    from ibis.common.exceptions import DependencyMissingError
    from ibis.filesystems import WebHDFS

    if session is None:
        session = requests.Session()

    session.verify = verify

    if use_https == 'default':
        prefix = 'http'
    else:
        prefix = 'https' if use_https else 'http'
    if auth_mechanism == 'GSSAPI' or auth_mechanism == 'LDAP':
        try:
            import requests_kerberos  # noqa: F401
        except ImportError as e:
            raise DependencyMissingError(
                "Unable to import requests-kerberos, which is required for "
                "Kerberos HDFS support. Install it by running `pip install "
                "requests-kerberos` or `pip install hdfs[kerberos]`."
            ) from e
        from hdfs.ext.kerberos import KerberosClient as Client

        # note SSL
        kwargs.setdefault('mutual_auth', 'OPTIONAL')
    else:
        from hdfs.client import InsecureClient as Client

    url = '{}://{}:{}'.format(prefix, host, port)
    hdfs_client = Client(url, session=session, **kwargs)
    return WebHDFS(hdfs_client)


from ._version import get_versions  # noqa: E402, isort:skip

__version__ = get_versions()['version']
del get_versions
