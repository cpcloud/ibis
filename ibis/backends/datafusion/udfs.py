from __future__ import annotations

import datetime  # noqa: TCH003
import itertools
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pyarrow.compute as pc

import ibis.expr.datatypes as dt  # noqa: TCH001


def _extract_epoch_seconds(array) -> dt.int32:
    return pc.cast(pc.divide(pc.cast(array, pa.int64()), 1_000_000), pa.int32())


def extract_epoch_seconds_date(array: datetime.date) -> dt.int32:
    return _extract_epoch_seconds(array)


def extract_epoch_seconds_timestamp(array: datetime.date) -> dt.int32:
    return _extract_epoch_seconds(array)


def extract_second(array: dt.Timestamp(scale=6)) -> dt.int32:
    return pc.cast(pc.second(array), pa.int32())


def extract_millisecond(array: dt.Timestamp(scale=6)) -> dt.int32:
    return pc.cast(pc.millisecond(array), pa.int32())


def extract_microsecond(array: dt.Timestamp(scale=6)) -> dt.int32:
    arr = pc.multiply(pc.millisecond(array), 1000)
    return pc.cast(pc.add(pc.microsecond(array), arr), pa.int32())


def _extract_dow_name(array) -> str:
    return pc.choose(
        pc.day_of_week(array),
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    )


def extract_dow_name_date(array: dt.Date) -> str:
    return _extract_dow_name(array)


def extract_dow_name_timestamp(array: dt.Timestamp(scale=6)) -> str:
    return _extract_dow_name(array)


def _extract_query_arrow(
    arr: pa.StringArray, *, param: str | None = None
) -> pa.StringArray:
    if param is None:

        def _extract_query(url, param):
            return urlsplit(url).query

        params = itertools.repeat(None)
    else:

        def _extract_query(url, param):
            query = urlsplit(url).query
            value = parse_qs(query)[param]
            return value if len(value) > 1 else value[0]

        params = param.to_pylist()

    return pa.array(map(_extract_query, arr.to_pylist(), params))


def extract_query(array: str) -> str:
    return _extract_query_arrow(array)


def extract_query_param(array: str, param: str) -> str:
    return _extract_query_arrow(array, param=param)


def extract_user_info(arr: str) -> str:
    def _extract_user_info(url):
        url_parts = urlsplit(url)
        username = url_parts.username or ""
        password = url_parts.password or ""
        return f"{username}:{password}"

    return pa.array(map(_extract_user_info, arr.to_pylist()))


def extract_url_field(arr: str, field: str) -> str:
    field = field.as_py()
    return pa.array(getattr(url, field, "") for url in map(urlsplit, arr.to_pylist()))
