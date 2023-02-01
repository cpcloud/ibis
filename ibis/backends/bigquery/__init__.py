"""BigQuery public API."""

from __future__ import annotations

import contextlib
from typing import Iterable
from urllib.parse import parse_qs, urlparse

import google.auth.credentials
import google.cloud.bigquery as bq
import pydata_google_auth
import sqlalchemy as sa
from pydata_google_auth import cache

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.bigquery.client import (
    BigQueryDatabase,
    bigquery_field_to_ibis_dtype,
    parse_project_and_dataset,
)
from ibis.backends.bigquery.compiler import BigQueryCompiler

with contextlib.suppress(ImportError):
    from ibis.backends.bigquery.udf import udf  # noqa: F401

SCOPES = ["https://www.googleapis.com/auth/bigquery"]
EXTERNAL_DATA_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
]
CLIENT_ID = "546535678771-gvffde27nd83kfl6qbrnletqvkdmsese.apps.googleusercontent.com"
CLIENT_SECRET = "iU5ohAF2qcqrujegE3hQ1cPt"


def _create_client_info(application_name):
    from google.api_core.client_info import ClientInfo

    user_agent = []

    if application_name:
        user_agent.append(application_name)

    user_agent_default_template = f"ibis/{ibis.__version__}"
    user_agent.append(user_agent_default_template)

    return ClientInfo(user_agent=" ".join(user_agent))


class Backend(BaseAlchemyBackend):
    name = "bigquery"
    compiler = BigQueryCompiler
    database_class = BigQueryDatabase

    def _from_url(self, url):
        result = urlparse(url)
        params = parse_qs(result.query)
        return self.connect(
            project_id=result.netloc or params.get("project_id", [""])[0],
            dataset_id=result.path[1:] or params.get("dataset_id", [""])[0],
        )

    def do_connect(
        self,
        project_id: str | None = None,
        dataset_id: str = "",
        credentials: google.auth.credentials.Credentials | None = None,
        application_name: str | None = None,
        auth_local_webserver: bool = True,
        auth_external_data: bool = False,
        auth_cache: str = "default",
        partition_column: str | None = "PARTITIONTIME",
    ):
        """Create a :class:`Backend` for use with Ibis.

        Parameters
        ----------
        project_id
            A BigQuery project id.
        dataset_id
            A dataset id that lives inside of the project indicated by
            `project_id`.
        credentials
            Optional credentials.
        application_name
            A string identifying your application to Google API endpoints.
        auth_local_webserver
            Use a local webserver for the user authentication.  Binds a
            webserver to an open port on localhost between 8080 and 8089,
            inclusive, to receive authentication token. If not set, defaults to
            False, which requests a token via the console.
        auth_external_data
            Authenticate using additional scopes required to `query external
            data sources
            <https://cloud.google.com/bigquery/external-data-sources>`_,
            such as Google Sheets, files in Google Cloud Storage, or files in
            Google Drive. If not set, defaults to False, which requests the
            default BigQuery scopes.
        auth_cache
            Selects the behavior of the credentials cache.

            ``'default'``
                Reads credentials from disk if available, otherwise
                authenticates and caches credentials to disk.

            ``'reauth'``
                Authenticates and caches credentials to disk.

            ``'none'``
                Authenticates and does **not** cache credentials.

            Defaults to ``'default'``.
        partition_column
            Identifier to use instead of default ``_PARTITIONTIME`` partition
            column. Defaults to ``'PARTITIONTIME'``.

        Returns
        -------
        Backend
            An instance of the BigQuery backend.
        """
        default_project_id = ""

        if credentials is None:
            scopes = SCOPES
            if auth_external_data:
                scopes = EXTERNAL_DATA_SCOPES

            if auth_cache == "default":
                credentials_cache = cache.ReadWriteCredentialsCache(
                    filename="ibis.json"
                )
            elif auth_cache == "reauth":
                credentials_cache = cache.WriteOnlyCredentialsCache(
                    filename="ibis.json"
                )
            elif auth_cache == "none":
                credentials_cache = cache.NOOP
            else:
                raise ValueError(
                    f"Got unexpected value for auth_cache = '{auth_cache}'. "
                    "Expected one of 'default', 'reauth', or 'none'."
                )

            credentials, default_project_id = pydata_google_auth.default(
                scopes,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                credentials_cache=credentials_cache,
                use_local_webserver=auth_local_webserver,
            )

        project_id = project_id or default_project_id

        (
            self.data_project,
            self.billing_project,
            self.dataset,
        ) = parse_project_and_dataset(project_id, dataset_id)

        self.client = bq.Client(
            project=self.billing_project,
            credentials=credentials,
            client_info=_create_client_info(application_name),
        )
        self.partition_column = partition_column

        super().do_connect(
            sa.create_engine(
                f"ibisbigquery://{self.data_project}/{self.dataset}?user_supplied_client=true&priority=INTERACTIVE",
                connect_args={"client": self.client},
            )
        )
        self.meta.schema = self.dataset

    def _parse_project_and_dataset(self, dataset) -> tuple[str, str]:
        if not dataset and not self.dataset:
            raise ValueError("Unable to determine BigQuery dataset.")
        project, _, dataset = parse_project_and_dataset(
            self.billing_project,
            dataset or f"{self.data_project}.{self.dataset}",
        )
        return project, dataset

    @property
    def project_id(self):
        return self.data_project

    @property
    def dataset_id(self):
        return self.dataset

    @property
    def current_database(self) -> str:
        return self.dataset

    def database(self, name=None):
        if name is None and not self.dataset:
            raise ValueError(
                "Unable to determine BigQuery dataset. Call "
                "client.database('my_dataset') or set_database('my_dataset') "
                "to assign your client a dataset."
            )
        return self.database_class(name or self.dataset, self)

    def set_database(self, name):
        self.data_project, self.dataset = self._parse_project_and_dataset(name)

    @property
    def version(self):
        return bq.__version__

    def create_view(
        self, name: str, expr: ir.Table, database: str | None = None
    ) -> None:
        import sqlalchemy_views as sav

        create_view = sav.CreateView(sa.table(name), expr.compile())
        with self.begin() as con:
            con.execute(create_view)

    def drop_view(
        self, name: str, database: str | None = None, force: bool = False
    ) -> None:
        import sqlalchemy_views as sav

        drop_view = sav.DropView(sa.table(name), if_exists=not force)
        with self.begin() as con:
            con.execute(drop_view)

    def _metadata(self, query: str) -> Iterable[tuple[str, dt.DataType]]:
        job_config = bq.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(query, job_config=job_config)
        return ((col.name, bigquery_field_to_ibis_dtype(col)) for col in job.schema)


def compile(expr, params=None, **kwargs):
    """Compile an expression for BigQuery."""
    backend = Backend()
    return backend.compile(expr, params=params, **kwargs)


def connect(
    project_id: str | None = None,
    dataset_id: str = "",
    credentials: google.auth.credentials.Credentials | None = None,
    application_name: str | None = None,
    auth_local_webserver: bool = False,
    auth_external_data: bool = False,
    auth_cache: str = "default",
    partition_column: str | None = "PARTITIONTIME",
) -> Backend:
    """Create a :class:`Backend` for use with Ibis.

    Parameters
    ----------
    project_id
        A BigQuery project id.
    dataset_id
        A dataset id that lives inside of the project indicated by
        `project_id`.
    credentials
        Optional credentials.
    application_name
        A string identifying your application to Google API endpoints.
    auth_local_webserver
        Use a local webserver for the user authentication.  Binds a
        webserver to an open port on localhost between 8080 and 8089,
        inclusive, to receive authentication token. If not set, defaults
        to False, which requests a token via the console.
    auth_external_data
        Authenticate using additional scopes required to `query external
        data sources
        <https://cloud.google.com/bigquery/external-data-sources>`_,
        such as Google Sheets, files in Google Cloud Storage, or files in
        Google Drive. If not set, defaults to False, which requests the
        default BigQuery scopes.
    auth_cache
        Selects the behavior of the credentials cache.

        ``'default'``
            Reads credentials from disk if available, otherwise
            authenticates and caches credentials to disk.

        ``'reauth'``
            Authenticates and caches credentials to disk.

        ``'none'``
            Authenticates and does **not** cache credentials.

        Defaults to ``'default'``.
    partition_column
        Identifier to use instead of default ``_PARTITIONTIME`` partition
        column. Defaults to ``'PARTITIONTIME'``.

    Returns
    -------
    Backend
        An instance of the BigQuery backend
    """
    backend = Backend()
    return backend.connect(
        project_id=project_id,
        dataset_id=dataset_id,
        credentials=credentials,
        application_name=application_name,
        auth_local_webserver=auth_local_webserver,
        auth_external_data=auth_external_data,
        auth_cache=auth_cache,
        partition_column=partition_column,
    )


__all__ = [
    "Backend",
    "compile",
    "connect",
]
