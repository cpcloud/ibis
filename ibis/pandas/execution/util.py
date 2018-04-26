import operator

import ibis
import ibis.common as com

from ibis.pandas.core import execute


def compute_sort_key(key, data, **kwargs):
    by = key.args[0]
    try:
        name = by.get_name()
    except com.ExpressionError:
        name = ibis.util.guid()
        new_scope = {t: data for t in by.op().root_tables()}
        new_column = execute(by, new_scope, **kwargs)
        new_column.name = name
        return name, new_column, False
    else:
        return name, None, name in data.index.names


def compute_sorted_frame(sort_keys, df, **kwargs):
    computed_sort_keys = []
    index_columns = []
    ascending = [key.op().ascending for key in sort_keys]
    new_columns = {}

    for i, key in enumerate(map(operator.methodcaller('op'), sort_keys)):
        computed_sort_key, temporary_column, index = compute_sort_key(
            key, df, **kwargs
        )
        if index:
            index_columns.append(computed_sort_key)
        computed_sort_keys.append(computed_sort_key)

        if temporary_column is not None:
            new_columns[computed_sort_key] = temporary_column

    assert not frozenset(index_columns) & new_columns.keys()

    result = df.assign(**new_columns)
    result = result.sort_values(
        computed_sort_keys, ascending=ascending, kind='mergesort'
    )
    result = result.drop(new_columns.keys(), axis=1)
    return result
