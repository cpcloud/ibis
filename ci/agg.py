import itertools
import textwrap

import numpy as np
import pandas as pd

from numba import jitclass, njit, float64, int64, jit


numpy_dtype_to_numba_type = {
    np.float64: float64,
    np.int64: int64,
}


@njit(nogil=True)
def aggregate(values, labels, nunique, aggregator):
    step = aggregator.step
    for label, value in zip(labels, values):
        if label >= 0:
            if not np.isnan(value):
                step(value, label)

    finalize = aggregator.finalize
    out = aggregator.out
    for label in range(nunique):
        out[label] = finalize(label) if aggregator.counts[label] else np.nan
    return aggregator.out


@jit(nogil=True)
def aggregate2(values, labels, nunique, step, finalize, out):
    for label, value in zip(labels, values):
        if label >= 0:
            if not np.isnan(value):
                step(value, label)

    for label in range(nunique):
        out[label] = finalize(label)


INIT_TEMPLATE = """\
def __init__(self, nunique):
{}"""


def grouped_reduce(*, over):
    def wrapper(cls):
        keys = over.keys()
        combinations = [
            dict(zip(keys, values))
            for values in itertools.product(*over.values())
        ]

        klasses = {}
        for field_pair in combinations:
            items = field_pair.items()
            lines = [
                'self.{} = np.zeros(nunique, dtype=np.{})'.format(
                    field, dtype.__name__
                ) for field, dtype in items
            ]
            joined_lines = '\n'.join(lines)
            code = INIT_TEMPLATE.format(textwrap.indent(joined_lines, ' ' * 4))
            scope = {'np': np}
            exec(code, scope)

            field_values = tuple(field_pair.values())

            clsname = '{}_{}'.format(
                cls.__name__, '_'.join(
                    map(str, (dt.__name__ for dt in field_values))))

            fs = [
                (f, numpy_dtype_to_numba_type[v][:]) for f, v in
                field_pair.items()
            ]
            klasses[field_values] = jitclass(fs)(
                type(clsname, (cls,), {'__init__': scope['__init__']}))

        def agg(series, labels):
            values = series.values
            labels, unique = pd.factorize(labels, sort=True)
            nunique = len(unique)
            aggregator = klasses[values.dtype.type, np.int64](nunique)
            raw = aggregate(values, labels, nunique, aggregator)
            return pd.Series(raw, index=unique)
        return agg
    return wrapper


@grouped_reduce(
    over=dict(
        totals=[np.float64, np.int64],
        counts=[np.int64],
    )
)
class nansum:
    def step(self, value, label):
        self.totals[label] += value
        self.counts[label] += 1

    @property
    def out(self):
        return self.totals

    def finalize(self, label):
        return self.totals[label]


@grouped_reduce(
    over=dict(
        totals=[np.float64, np.int64],
        counts=[np.int64],
    )
)
class nanmean:
    def step(self, value, label):
        self.totals[label] += value
        self.counts[label] += 1

    @property
    def out(self):
        return self.totals

    def finalize(self, label):
        return self.totals[label] / self.counts[label]


def my_sum(nunique, totals):
    counts = np.zeros(nunique, dtype=np.int64)

    @njit
    def step(value, label, out, counts):
        if not np.isnan(value):
            out[label] += value
            counts[label] += 1

    @njit
    def finalize(label):
        count = counts[label]
        if not count:
            return np.nan
        return totals[label]
    return step, finalize


def do_aggregate(series, labels, aggfunc):
    values = series.values
    labels, unique = pd.factorize(labels, sort=True)
    nunique = len(unique)
    out = np.zeros(nunique, dtype=series.dtype)
    step, finalize = aggfunc(nunique, out)
    aggregate2(values, labels, nunique, step, finalize, out)
    return pd.Series(out, index=unique)



if __name__ == '__main__':
    from time import time
    n = int(1e5)
    data = np.random.rand(n)
    data[data > 0.5] = np.nan
    s = pd.Series(data)
    # keys = np.random.choice(['a', 'b', 'c', 'e', 'f'], size=n)
    keys = np.random.randint(0, 4000, size=n)

    res1 = nansum(s, keys)
    tic = time()
    res1 = nansum(s, keys)
    toc = time()
    time1 = toc - tic

    df = pd.DataFrame({'value': data, 'key': keys})
    tic = time()
    res2 = df.groupby('key').value.sum()
    toc = time()
    time2 = toc - tic

    res3 = do_aggregate(s, keys, my_sum)
    tic = time()
    res3 = do_aggregate(s, keys, my_sum)
    toc = time()
    time3 = toc - tic

    pd.options.display.max_rows = 15

    print(f'numba:     {time1:.2f}')
    print(f'pandas:    {time2:.2f}')
    print(f'pandas1.5: {time3:.2f}')
