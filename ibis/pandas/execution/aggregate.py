import itertools

import numpy as np
import pandas as pd

from numba import jitclass, njit, float64, int64


@njit(nogil=True)
def aggregate(values, labels, nunique, aggregator):
    step = aggregator.step
    for label, value in zip(labels, values):
        if label >= 0:
            if not np.isnan(value):
                step(value, label)

    finalize = aggregator.finalize
    is_valid = aggregator.is_valid
    out = aggregator.out
    for label in range(nunique):
        out[label] = finalize(label) if is_valid(label) else np.nan
    return aggregator.out


def grouped_reduce(*, state, over):
    def wrapper(cls):
        keys = over.keys()
        combinations = [
            dict(zip(keys, values))
            for values in itertools.product(*over.values())
        ]

        klasses = {}
        for field_pair in combinations:
            def __init__(self, nunique):
                for field, values in field_pair.items():
                    setattr(self, field, state[field](nunique, *values))

            clsname = '{}_{}'.format(
                cls.__name__, ', '.join(map(str, field_pair.values())))

            def gen_type(clsname, __init__):
                return type(clsname, (), {'__init__': __init__})

            jc = jitclass(list(field_pair.items()))
            klasses[tuple(field_pair.values())] = jc(
                gen_type(clsname, __init__))

        cls.__init__ = __init__

        def agg(series):
            import pandas as pd
            values = series.values
            labels, unique = pd.factorize(values, sort=True)
            nunique = len(unique)
            aggregator = klasses[values.dtype, np.int64](nunique)
            return aggregate(values, labels, nunique, aggregator)

        return agg
    return wrapper


@grouped_reduce(
    signatures=[float64(float64[:]), int64(int64[:])],
    state=dict(
        totals=lambda nunique, dtype: np.zeros(nunique, dtype=dtype),
        counts=lambda nunique, dtype: np.zeros(nunique, dtype=dtype),
    ),
    over=dict(
        totals=dict(dtype=[np.float64, np.int64]),
        counts=dict(dtype=[np.int64]),
    ),
)
class nansum:
    def step(self, value, label):
        self.totals[label] += value
        self.counts[label] += 1

    @property
    def out(self):
        return self.totals

    def finalize(self, label):
        count = self.counts[label]
        out = self.totals
        if not count:
            out[label] = np.nan


if __name__ == '__main__':
    s = pd.Series([1, 2, np.nan, 3])
    res = nansum(s)
    print(res)
