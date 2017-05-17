from ibis.pandas.client import PandasClient


def connect(dictionary):
    return PandasClient(dictionary)
